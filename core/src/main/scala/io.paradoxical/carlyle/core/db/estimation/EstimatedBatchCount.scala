package io.paradoxical.carlyle.core.db.estimation

import io.paradoxical.carlyle.core.model.EstimatedBatchId
import io.paradoxical.carlyle.core.redis.RedisClientBase
import io.paradoxical.common.extensions.Extensions._
import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

case class EstimatedBatch(
  id: EstimatedBatchId,
  pending: Long,
  closed: Boolean,
  userKey: Option[String]
)

class EstimatedBatchCountDb @Inject()(redisClientBase: RedisClientBase)(implicit executionContext: ExecutionContext) {
  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private final val UserKey = "userKey"

  private final val Count = "count"

  private final val Closed = "closed"

  def create(userKey: Option[String]): Future[EstimatedBatchId] = {
    val batchId = EstimatedBatchId(UUID.randomUUID())

    redisClientBase.hMSet(batchId.value.toString, Map(
      Count -> 0.toString,
      UserKey -> userKey.getOrElse(""),
      Closed -> false.toString
    )).map(_ => batchId)
  }

  def incr(id: EstimatedBatchId, count: Long): Future[Unit] = {
    redisClientBase.hIncrBy(id.value.toString, Count, by = count).map(n => {
      logger.info(s"$id incr by $count is now at $n")
    })
  }

  def decr(estimatedBatchId: EstimatedBatchId, by: Long): Future[EstimatedBatch] = {
    redisClientBase.hIncrBy(estimatedBatchId.value.toString, Count, -by).flatMap(_ =>
      get(estimatedBatchId)
    )
  }

  def close(estimatedBatchId: EstimatedBatchId): Future[EstimatedBatch] = {
    redisClientBase.hSet(estimatedBatchId.value.toString, Closed, true.toString).flatMap(_ =>
      get(estimatedBatchId)
    )
  }

  def get(estimatedBatchId: EstimatedBatchId): Future[EstimatedBatch] = {
    redisClientBase.hMGet(estimatedBatchId.value.toString, Set(Count, UserKey, Closed)).map(m => {
      EstimatedBatch(
        estimatedBatchId,
        pending = m(Count).toLong,
        closed = m(Closed).toBoolean,
        userKey = m.get(UserKey).flatMap(_.trimToOption)
      )
    })
  }

  def delete(estimatedBatchId: EstimatedBatchId): Future[Unit] = {
    redisClientBase.delete(List(estimatedBatchId.value.toString)).map(_ => {})
  }
}
