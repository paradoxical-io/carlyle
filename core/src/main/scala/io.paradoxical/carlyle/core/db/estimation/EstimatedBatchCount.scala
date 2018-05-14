package io.paradoxical.carlyle.core.db.estimation

import io.paradoxical.carlyle.core.model.EstimatedBatchId
import io.paradoxical.carlyle.core.redis.RedisClientBase
import io.paradoxical.common.extensions.Extensions._
import io.paradoxical.common.types.NotTypeImplicits._
import java.util.UUID
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

case class EstimatedBatch(
  id: EstimatedBatchId,
  pending: Long,
  userKey: Option[String]
)
class EstimatedBatchCountDb @Inject()(redisClientBase: RedisClientBase)(implicit executionContext: ExecutionContext) {
//  val doubleCountDetectedCounter = metrics.counter(List("estimation", "double-count"))

  private final val UserKey = "userKey"

  private final val Count = "count"

  def set(count: Long, userKey: Option[String]): Future[EstimatedBatchId] = {
    val batchId = EstimatedBatchId(UUID.randomUUID())

    redisClientBase.hMSet(batchId.value.toString, Map(
      Count -> count.toString,
      UserKey -> userKey.getOrElse("")
    )).map(_ => batchId)
  }

  def decr(estimatedBatchId: EstimatedBatchId, by: Long): Future[EstimatedBatch] = {
    redisClientBase.hIncrBy(estimatedBatchId.value.toString, Count, -by).flatMap(pending =>
      redisClientBase.hGet[String](estimatedBatchId.value.toString, UserKey).map(userKey => {
        if (pending < 0) {
//          doubleCountDetectedCounter.inc()
        }
        EstimatedBatch(estimatedBatchId, pending, userKey)
      })
    )
  }

  def get(estimatedBatchId: EstimatedBatchId): Future[EstimatedBatch] = {
    redisClientBase.hMGet(estimatedBatchId.value.toString, Set(Count, UserKey)).map(m => {
      EstimatedBatch(
        estimatedBatchId,
        pending = m(Count).toLong,
        userKey = m.get(UserKey).flatMap(_.trimToOption)
      )
    })
  }

  def delete(estimatedBatchId: EstimatedBatchId): Future[Unit] = {
    redisClientBase.delete(List(estimatedBatchId.value.toString)).map(_ => {})
  }
}
