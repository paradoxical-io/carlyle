package io.paradoxical.carlyle.core

import com.twitter.util
import io.paradoxical.carlyle.core.db.{BatchNotAvailableForAdding, Db, DbInitializer}
import io.paradoxical.carlyle.core.model._
import io.paradoxical.carlyle.core.modules.TestModules
import io.paradoxical.carlyle.core.modules.TestModules._
import io.paradoxical.common.extensions.Extensions._
import org.joda.time.DateTime
import org.scalatest.mockito.MockitoSugar
import io.paradoxical.carlyle.core.guice.GuiceUtil._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class TestBase extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
  util.logging.Slf4jBridgeUtility.attemptSlf4jBridgeHandlerInstallation()
}

class DbTests extends TestBase {
  def withDb(testCode: Db => Any): Unit = {
    val injector = TestModules().withH2().withMockRedis().injector()

    injector.make[DbInitializer].init

    testCode(injector.make[Db])
  }

  "Batch" should "create" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val batch = db.getBatch(batchId).waitForResult()

    assert(!batch.get.isClosed)
  }

  it should "close" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    db.closeBatch(batchId).waitForResult()

    val batch = db.getBatch(batchId).waitForResult()

    assert(batch.get.isClosed)
  }

  it should "make sure to set the right bits" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val items = BatchItemId.generate(batchId, db.insertQueueBatchItems(batchId, 20).waitForResult()).toList

    db.ack(items(19)).waitForResult()

    assert(db.getQueueBatchItem(items(19)).waitForResult().get.isSet)

    assert(!db.getQueueBatchItem(items(0)).waitForResult().get.isSet)
  }

  it should "quick insert massive blobs" ignore withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val ids = BatchItemId.generate(batchId, db.insertQueueBatchItems(batchId, 100000).waitForResult())

    assert(!db.closeBatch(batchId).waitForResult())

    ids.grouped(1000).foreach(group => {
      db.ackBatch(group.toSet).waitForResult()
    })

    assert(db.isBatchEmpty(batchId).waitForResult())
  }

  it should "add items if not closed" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val batchItemIds = (0 until 10).map(_ => db.insertQueueBatchItem(batchId).waitForResult())

    val itemIds = batchItemIds.flatMap(db.getQueueBatchItemDao(_).waitForResult())

    assert(itemIds.flatMap(_.asBatchItemIds).sortBy(_.value) == batchItemIds.sortBy(_.value))
  }

  it should "fail adding items if closed" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    db.closeBatch(batchId).waitForResult()

    assertThrows[BatchNotAvailableForAdding] {
      db.insertQueueBatchItem(batchId).waitForResult()
    }
  }

  it should "indicate empty batches if all acked" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val batchItemIds = (0 until 10).map(item => db.insertQueueBatchItem(batchId).waitForResult())

    val results = batchItemIds.map(db.ack(_).waitForResult())

    // even though all items were ack'd, the batch isn't closed yet so we cannot say
    // the batch is done
    assert(results.forall(isEmpty => !isEmpty))

    // verify its empty
    assert(db.closeBatch(batchId).waitForResult())
  }

  it should "expire batches" in withDb { db =>
    val openBatch = db.createBatch().waitForResult()

    val openBatchItems = (0 until 10).map(item => db.insertQueueBatchItem(openBatch).waitForResult())

    val closedBatch = db.createBatch().waitForResult()

    val closedBatchItems = (0 until 10).map(item => db.insertQueueBatchItem(closedBatch).waitForResult())

    db.closeBatch(closedBatch).waitForResult()

    Thread.sleep(100)

    db.deleteExpiredBatches(since = DateTime.now(), onlyOpen = true).waitForResult()

    // verify the open batch and its items are reaped
    assert(db.getBatch(openBatch).waitForResult().isEmpty)
    assert(openBatchItems.flatMap(db.getQueueBatchItemDao(_).waitForResult()).isEmpty)

    // verify the closed batch and its items still exist
    assert(db.getBatch(closedBatch).waitForResult().isDefined)
    assert(closedBatchItems.flatMap(db.getQueueBatchItemDao(_).waitForResult()).nonEmpty)

    // reap closed batches
    db.deleteExpiredBatches(since = DateTime.now(), onlyOpen = false).waitForResult()

    // verify the closed batch and its items are gone
    assert(db.getBatch(closedBatch).waitForResult().isEmpty)
    assert(closedBatchItems.flatMap(db.getQueueBatchItemDao(_).waitForResult()).isEmpty)
  }

  it should "indicate empty batch if double acked" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val batchItemIds = (0 until 10).map(item => db.insertQueueBatchItem(batchId).waitForResult())

    // close the batch and wait for all items to return
    db.closeBatch(batchId)

    // ack all items once
    batchItemIds.map(db.ack(_).waitForResult())

    // ack them all again and they should all say its empty
    val results = batchItemIds.map(db.ack(_).waitForResult())

    assert(results.forall(identity))
  }

  it should "indicate empty batch if all items are added and acked before close occurs" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val batchItemIds = (0 until 10).map(item => db.insertQueueBatchItem(batchId).waitForResult())

    // ack all items once
    batchItemIds.map(db.ack(_).waitForResult())

    // verify its empty
    assert(db.closeBatch(batchId).waitForResult())
  }
}