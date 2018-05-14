//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core.db

import io.paradoxical.carlyle.core.api.modules.BigIntProvider
import io.paradoxical.carlyle.core.config.{ReloadableConfig, ServiceConfig}
import io.paradoxical.carlyle.core.db.extensions.SlickExtensions._
import io.paradoxical.carlyle.core.db.packing.{Bit, BitGroup}
import io.paradoxical.carlyle.core.model._
import io.paradoxical.common.execution.SequentialFutures
import java.math.BigInteger
import java.util.UUID
import javax.inject.Inject
import org.joda.time.DateTime
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace
import slick.jdbc.H2Profile

case class Paging(offset: Long = 0, limit: Long = 500)

case class BatchNotAvailableForAdding(batchId: BatchId) extends Exception(s"Batch ${batchId} is not available for adding to") with NoStackTrace

class Db @Inject()(
  provider: BigIntProvider,
  dataMappers: DataMappers,
  config: ReloadableConfig[ServiceConfig],
  batchItemsQuery: BatchItemsQuery,
  batchQuery: BatchQuery
)(implicit executionContext: ExecutionContext) {

  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  import dataMappers._
  import dataMappers.converter._
  import provider.driver._
  import provider.driver.api._

  private def withDB[R, S <: NoStream, E <: Effect]()(action: DBIOAction[R, S, E]): Future[R] = {
//    metrics.timer(metric("db"), slices = List(Slice("action", enclosingMethod.methodName))).timeAsync() {
    provider.withDB(action)
//    }
  }

  def getQueueBatchItem(id: BatchItemId): Future[Option[BatchItem]] = {
    getQueueBatchItemDao(id).map(_.map(group => {
      BatchItem(
        batchItemId = id,
        isSet = !group.items.testBit(id.index.toInt)
      )
    }))
  }

  def getQueueBatchItemDao(id: BatchItemId): Future[Option[BatchItemGroupDao]] = {
    withDB() {
      batchItemsQuery.find(_.batchItemGroupId === id.batchItemGroupId)
    }
  }

  def createBatch(userKey: Option[String] = None): Future[BatchId] = {
    withDB() {
      val n = now
      batchQuery.insert(BatchDao(
        isClosed = false,
        createdAt = n,
        updatedAt = n,
        userKey = userKey
      ), _.batchId)
    }
  }

  def getBatch(batchId: BatchId): Future[Option[BatchDao]] = {
    withDB() {
      batchQuery.find(_.batchId === batchId)
    }
  }

  def closeBatch(batchId: BatchId): Future[Boolean] = {
    val markAsClosed = batchQuery.updateWhere(_.batchId === batchId, b => (b.isClosed, b.updatedAt), (true, now))

    val query = (for {
      _ <- markAsClosed
      r <- Queries.isBatchEmpty(batchId)
    } yield {
      r
    }).transactionally

    withDB()(query)
  }

  def getBatchInfo(batchId: Set[BatchId]): Future[Seq[BatchDao]] = {
    withDB() {
      batchQuery.query.filter(_.batchId inSet batchId).result
    }
  }

  def ackBatch(items: Set[BatchItemId]): Future[Map[BatchId, Boolean]] = {
    // for each group process its group items

    SequentialFutures.serialize(items.groupBy(_.batchItemGroupId)) {
      case (groupId, itemsByGroup) => ackBatch(groupId, itemsByGroup).map(r => itemsByGroup.head.batchId -> r)
    }.map(_.toMap)
  }

  private def ackBatch(groupId: BatchItemGroupId, items: Set[BatchItemId], retryCount: Int = 0): Future[Boolean] = {
    if (items.isEmpty) {
      return Future.successful(false)
    }

    require(items.map(_.batchId).size == 1, "Cannot ack batch items with varying batches!")
    require(items.map(_.batchItemGroupId).size == 1, "Cannot ack batch items with varying hashes!")

    val groupId = items.head.batchItemGroupId

    // data is reversed since BigInteger is big endian.
    // That means that index 0 should be the most signficant bit, but here it is vice versa

    val mask =
      BitGroup.filled(config.currentValue().max_batch_group_size).
        setValues(items.map(_.index.toInt).toList, Bit.Zero).
        data.reverse.drop(1)

    val query = for {
      _ <- Queries.updateGroupMask(new BigInteger(1, mask), groupId)

      isEmpty <- Queries.isBatchEmpty(items.head.batchId)
    } yield {
      isEmpty
    }

    withDB()(query)
  }

  def ack(itemId: BatchItemId): Future[Boolean] = {
    ackBatch(Set(itemId)).map(_.head._2)
  }

  def isBatchEmpty(batchId: BatchId): Future[Boolean] = {
    withDB() {
      Queries.isBatchEmpty(batchId)
    }
  }

  def deleteExpiredBatches(since: DateTime, onlyOpen: Boolean): Future[BatchRemoval] = {
    withDB() {
      Queries.expireBatch(since, onlyOpen).transactionally
    }
  }

  def insertQueueBatchItem(batchId: BatchId): Future[BatchItemId] = {
    insertQueueBatchItems(batchId, 1).map(groupId => BatchItemId.generate(batchId, groupId.head.id, groupId.head.upto).head)
  }

  /**
   * Insert batch items. To make insertion fast and minimize data over the wire
   * items are inserted using a determistic ID function: batchId:hash:index
   *
   * All that is needed to re-create the ID's client side is the batch id and hash
   *
   * The order doesnt' matter, as all the item id will do is give you a marker to use for
   * any item
   *
   * @param batchId
   * @param numberOfItems
   * @return
   */
  def insertQueueBatchItems(batchId: BatchId, numberOfItems: Int): Future[List[BatchItemGroupInsert]] = {
    getBatch(batchId).map(r => r.isDefined && r.exists(_.isClosed)).flatMap(isClosed => {
      if (isClosed) {
        throw BatchNotAvailableForAdding(batchId)
      }

      val maxBatchGroupSize = Math.min(numberOfItems, config.currentValue().max_batch_group_size)

      val fullGroups = numberOfItems / maxBatchGroupSize

      val partialGroups = numberOfItems % maxBatchGroupSize

      val n = now

      val groupDaos = (0 until fullGroups).toIterator.map(_ => insertBatchDao(batchId, maxBatchGroupSize, n)).toIterable

      val partialDao = if (partialGroups > 0) List(insertBatchDao(batchId, partialGroups, n)) else Nil

      val allDaos = groupDaos ++ partialDao

      withDB() {
        batchItemsQuery.query.forceInsertAll(allDaos).transactionally
      }.map(_ => {
        groupDaos.map(r => BatchItemGroupInsert(r.batchItemGroupId, upto = maxBatchGroupSize)) ++
        partialDao.map(r => BatchItemGroupInsert(r.batchItemGroupId, partialGroups))
      }.toList)
    })
  }

  private def insertBatchDao(batchId: BatchId, numberOfItems: Int, when: DateTime): BatchItemGroupDao = {
    val groupId = BatchItemGroupId(UUID.randomUUID())

    val initialBitSet =
      BitGroup.zero(config.currentValue().max_batch_group_size).
        setValues((0 until numberOfItems).toList, Bit.One).
        data.reverse

    BatchItemGroupDao(
      batchItemGroupId = groupId,
      batchId = batchId,
      items = new BigInteger(1, initialBitSet),
      size = numberOfItems,
      version = 0,
      createdAt = when,
      updatedAt = when
    )
  }

  private def now = DateTime.now()

  private object Queries {
    def updateGroupMask(mask: BigInteger, groupId: BatchItemGroupId) = {
      if (slickDBProvider.driver.isInstanceOf[H2Profile]) {
        sqlu"""
            UPDATE batch_item_groups
            SET
              items = BITAND(items, $mask),
              updated_at = ${DateTime.now},
              version = version + 1
            WHERE
              id = ${groupId.value.toString}
        """
      } else {
        sqlu"""
            UPDATE batch_item_groups
            SET
              items = CAST(items as UNSIGNED) & CAST($mask as UNSIGNED),
              updated_at = ${DateTime.now},
              version = version + 1
            WHERE
              id = ${groupId.value.toString}
        """
      }
    }

    def findBatch(batchId: BatchId) = batchQuery.query.filter(_.batchId === batchId)

    def expireBatch(since: DateTime, onlyOpen: Boolean) = {
      // find expired batch ids
      val expireBatchIdsQuery =
        batchQuery.query.join(batchItemsQuery.query).
          on(_.batchId === _.batchId).
          filter { case (batch, item) => batch.isClosed === !onlyOpen && batch.updatedAt <= since }.
          map(_._2.batchId).
          distinct

      // drop into raw sql here so we don't have a giant `IN` clause
      // we need to force creation of a temp table of the data we want to purge
      // so use nested queries with scoping to support that

      val expiredBatchItemsQuery =
        sqlu"""
        DELETE FROM batch_item_groups
        WHERE id in (
          SELECT * FROM (
            SELECT BI2.id
            FROM batch_item_groups AS BI2
            INNER JOIN batches on BI2.batch_id = batches.batch_id
            WHERE batches.is_closed = ${!onlyOpen} AND batches.updated_at <= ${since}
        ) as t)
      """

      for {
        expiredBatchIds <- expireBatchIdsQuery.result
        batchItemsRemoved <- expiredBatchItemsQuery
        batchesRemoved <- batchQuery.query.filter(_.batchId inSet expiredBatchIds).delete
      } yield {
        BatchRemoval(batchItemsRemoved, batchesRemoved)
      }
    }

    def isBatchEmpty(batchId: BatchId) = {
      for {
        isClosed <- findBatch(batchId).map(_.isClosed).result.headOption
        // selecting a double negative since to know if all items are empty we have to know how many items there are
        // but if ANY non-empty exist we know we are not empty
        nonEmptyExists <- if (isClosed.exists(identity)) {
          batchItemsQuery.query.
            join(batchQuery.query).
            on(_.batchId === _.batchId).
            filter { case (item, batch) => batch.isClosed && item.batchId === batchId && item.items =!= BigInteger.ZERO }.
            exists.
            result
        } else {
          // if its not closed, it can't ever be empty
          DBIO.successful(true)
        }
      } yield {
        !nonEmptyExists
      }
    }
  }
}

object ByteArrays {
  object Implicits {

    import java.nio.{ByteBuffer, ByteOrder}

    implicit class RichByteArray(array: Array[Byte]) {
      def byteOrer(byteOrder: ByteOrder): Array[Byte] = {
        val bb = ByteBuffer.wrap(array)
        bb.order(byteOrder).array()
      }
    }
  }
}

case class BatchRemoval(itemsRemoved: Long, batchesRemoved: Long)


