package io.paradoxical.carlyle.core.api.controllers

import com.twitter.finagle.http.Status
import com.twitter.finatra.http.exceptions.NotFoundException
import com.twitter.finatra.request.{QueryParam, RouteParam}
import io.paradoxical.carlyle.core.db.Db
import io.paradoxical.carlyle.core.model._
import io.paradoxical.finatra.Framework
import javax.inject.Inject
import org.joda.time.DateTime
import scala.concurrent.ExecutionContext

class ExactQueueBatchController @Inject()(db: Db)(implicit executionContext: ExecutionContext) extends Framework.RestApi {
  val tag = "Exact"

  prefix("/api/v1/batch/exact") {
    postWithDoc("/?") {
      _.summary("Create a batch").
        tag(tag).
        request[CreateBatchRequest].
        responseWith[CreateBatchResponse](status = Status.Ok.code)
    } { req: CreateBatchRequest =>
      db.createBatch(req.userKey).map(CreateBatchResponse)
    }

    getWithDoc("/:batchId") {
      _.summary("Get a batch information").
        tag(tag).
        request[GetBatchRequest].
        responseWith[GetBatchResponse](status = Status.Ok.code)
    } { req: GetBatchRequest =>
      for {
        batch <- db.getBatch(req.batchId).map(_.getOrElse(throw NotFoundException(errors = s"Batch id does not exist: ${req.batchId}")))
        isEmpty <- db.isBatchEmpty(req.batchId)
      } yield {
        GetBatchResponse(
          batch.batchId.get,
          isComplete = isEmpty,
          isClosed = batch.isClosed,
          userKey = batch.userKey
        )
      }
    }

    deleteWithDoc("/:batchId") {
      _.summary("Close a batch").
        tag(tag).
        request[CloseBatchRequest].
        responseWith[CloseBatchResponse](status = Status.Ok.code)
    } { req: CloseBatchRequest =>
      db.closeBatch(req.batchId).map(CloseBatchResponse)
    }

    postWithDoc("/items") {
      _.summary("""
                |Add to a batch. The response contains a payload of a hash and an 'upto' field.  The combination of batchId:hash:[0-upTo] creates a batch item
                |Items can be independently ack'd and the item id's should be correlated to only 1 logical item.  """.stripMargin).
        tag(tag).
        request[AddBatchItemsRequest].
        responseWith[AddBatchItemsResponse](status = Status.Ok.code)
    } { req: AddBatchItemsRequest =>
      db.insertQueueBatchItems(req.batchId, req.items).map(AddBatchItemsResponse)
    }

    postWithDoc("/items/ack") {
      _.summary("Ack items").
        tag(tag).
        request[AckBatchItemsRequest].
        responseWith[AckBatchItemsResponse](status = Status.Ok.code)
    } { req: AckBatchItemsRequest =>
      db.ackBatch(req.batchItemIds).flatMap { results =>
        db.getBatchInfo(results.keySet).map(batches => {
          // filter only successful batches
          batches.filter(x => results(x.batchId.get)).map(b => BatchCompletion(b.batchId.get, b.userKey))
        })
      }.map(AckBatchItemsResponse)
    }

    deleteWithDoc("/open") {
      _.summary("Delete open batches").
        tag(tag).
        request[DeleteBatches].
        responseWith[DeleteBatchesResponse](status = Status.Ok.code)
    } { req: DeleteBatches =>
      db.deleteExpiredBatches(req.since, onlyOpen = true).map(r => DeleteBatchesResponse(r.batchesRemoved, r.itemsRemoved))
    }

    deleteWithDoc("/closed") {
      _.summary("Delete closed batches").
        tag(tag).
        request[DeleteBatches].
        responseWith[DeleteBatchesResponse](status = Status.Ok.code)
    } { req: DeleteBatches =>
      db.deleteExpiredBatches(req.since, onlyOpen = false).map(r => DeleteBatchesResponse(r.batchesRemoved, r.itemsRemoved))
    }
  }
}

case class DeleteBatchesResponse(
  batchesRemoved: Long,
  itemsRemoved: Long
)

case class DeleteBatches(
  @QueryParam since: DateTime
)
case class CloseBatchRequest(
  @RouteParam batchId: BatchId
)

case class GetBatchRequest(
  @RouteParam batchId: BatchId
)