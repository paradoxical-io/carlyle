package io.paradoxical.carlyle.core.db

import io.paradoxical.rdb.slick.dao.SlickDAO
import io.paradoxical.carlyle.core.model._
import javax.inject.Inject
import org.joda.time.DateTime
import slick.jdbc.JdbcProfile

case class BatchDao(
  batchId: Option[BatchId] = None,
  isClosed: Boolean = false,
  createdAt: DateTime,
  updatedAt: DateTime,
  userKey: Option[String]
)

class BatchQuery @Inject()(
  val driver: JdbcProfile,
  dataMappers: DataMappers
) extends SlickDAO {

  import dataMappers._
  import driver.api._
  import dataMappers.converter._

  override type RowType = BatchDao
  override type TableType = BatchTable

  class BatchTable(tag: Tag) extends DAOTable(tag, "batches") {
    def batchId = column[BatchId]("batch_id", O.PrimaryKey, O.AutoInc)

    def isClosed = column[Boolean]("is_closed")

    def createdAt = column[DateTime]("created_at")

    def updatedAt = column[DateTime]("updated_at")

    def userKey = column[Option[String]]("user_key")

    override def * =
      (
        batchId.?,
        isClosed,
        createdAt,
        updatedAt,
        userKey
      ) <> (BatchDao.tupled, BatchDao.unapply)
  }

  override val query = TableQuery[BatchTable]
}
