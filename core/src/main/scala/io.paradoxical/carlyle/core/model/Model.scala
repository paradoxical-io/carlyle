package io.paradoxical.carlyle.core.model

import io.paradoxical.global.tiny.{LongValue, StringValue, UuidValue}
import java.util.UUID

case class CreateEstimatedBatchResponse(batchId: EstimatedBatchId)

case class CreateEstimatedRequest(batchSize: Long, userKey: Option[String])

case class GetEstimatedBatchResponse(pending: Long, userKey: Option[String])

case class CreateBatchRequest(userKey: Option[String])

case class CreateBatchResponse(batchId: BatchId)

case class AddBatchItemsRequest(batchId: BatchId, items: Int)

case class AddBatchItemsResponse(batchItemGroupIds: List[BatchItemGroupInsert])

case class AckBatchItemsRequest(batchItemIds: Set[BatchItemId])

case class AckBatchItemsResponse(batchCompletions: Seq[BatchCompletion]) {
  def isComplete(batchId: BatchId) = batchCompletions.exists(_.batchId == batchId)
}

case class CloseBatchResponse(isComplete: Boolean)

case class GetBatchResponse(batchId: BatchId, isComplete: Boolean, isClosed: Boolean, userKey: Option[String])

case class BatchItemGroupInsert(id: BatchItemGroupId, upto: Int)

case class BatchCompletion(batchId: BatchId, userKey: Option[String])

case class EstimatedBatchCompletion(batchId: EstimatedBatchId, userKey: Option[String], pending: Long)

case class EstimatedBatchCompletionAck(batchId: EstimatedBatchId, by: Long = 1)

case class EstimatedBatchCompletionAckRequest(acks: Seq[EstimatedBatchCompletionAck])

case class EstimatedBatchCompletionAckResult(results: Seq[EstimatedBatchCompletion])

case class BatchItemGroupId(value: UUID) extends UuidValue

object BatchItemId {
  def apply(batchId: BatchId, batchItemGroupId: BatchItemGroupId, index: Long): BatchItemId = BatchItemId(s"${batchId.value}:$batchItemGroupId:$index")

  def generate(batchId: BatchId, batchItemGroupId: BatchItemGroupId, numberOfItems: Int): Iterable[BatchItemId] = {
    Stream.from(0, step = 1).take(numberOfItems).map(idx => apply(batchId, batchItemGroupId, idx))
  }

  def generate(batchId: BatchId, batchItemGroupId: List[BatchItemGroupInsert]): Iterable[BatchItemId] = {
    batchItemGroupId.toStream.flatMap(group => generate(batchId, group.id, group.upto))
  }
}

case class BatchItemId(value: String) extends StringValue {
  val (batchId, batchItemGroupId, index) = value.split(':').toList match {
    case batch :: hash :: index :: _ => (BatchId(batch.toLong), BatchItemGroupId(UUID.fromString(hash)), index.toLong)
    case _ => throw new RuntimeException(s"Invalid batch item id format $value")
  }
}

case class BatchId(value: Long) extends LongValue
case class EstimatedBatchId(value: UUID) extends UuidValue