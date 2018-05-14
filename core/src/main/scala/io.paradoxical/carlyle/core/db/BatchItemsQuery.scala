package io.paradoxical.carlyle.core.db

import io.paradoxical.rdb.slick.dao.SlickDAO
import io.paradoxical.carlyle.core.model._
import java.math.BigInteger
import javax.inject.Inject
import org.joda.time.DateTime
import slick.jdbc.JdbcProfile

case class BatchItem (
  batchItemId: BatchItemId,
  isSet: Boolean
)

case class BatchItemGroupDao(
  batchItemGroupId: BatchItemGroupId,
  batchId: BatchId,
  items: BigInteger,
  size: Int,
  version: Long,
  updatedAt: DateTime,
  createdAt: DateTime
) {
  lazy val asBatchItemIds = BatchItemId.generate(batchId, batchItemGroupId, size)
}

class BatchItemsQuery @Inject()(
  val driver: JdbcProfile with BigIntegerTypesComponent,
  dataMappers: DataMappers
) extends SlickDAO {

  import dataMappers._
  import driver.api._
  import driver._
  import dataMappers.converter._

  override type RowType = BatchItemGroupDao
  override type TableType = BatchItemsTable

  class BatchItemsTable(tag: Tag) extends DAOTable(tag, "batch_item_groups") {
    def batchItemGroupId = column[BatchItemGroupId]("id", O.PrimaryKey)

    def batchId = column[BatchId]("batch_id")

    def items = column[BigInteger]("items")

    def size = column[Int]("size")

    def version = column[Long]("version")

    def createdAt = column[DateTime]("created_at")

    def updatedAt = column[DateTime]("updated_at")

    override def * =
      (
        batchItemGroupId,
        batchId,
        items,
        size,
        version,
        createdAt,
        updatedAt
      ) <> (BatchItemGroupDao.tupled, BatchItemGroupDao.unapply)
  }

  override val query = TableQuery[BatchItemsTable]
}
