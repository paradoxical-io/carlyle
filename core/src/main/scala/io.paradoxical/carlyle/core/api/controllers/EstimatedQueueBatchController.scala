package io.paradoxical.carlyle.core.api.controllers

import com.twitter.finagle.http.Status
import com.twitter.finatra.request.{QueryParam, RouteParam}
import com.twitter.finatra.validation.Min
import io.paradoxical.carlyle.core.db.estimation.{EstimatedBatch, EstimatedBatchCountDb}
import io.paradoxical.carlyle.core.model._
import io.paradoxical.finatra.Framework
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

class EstimatedQueueBatchController @Inject()(
  db: EstimatedBatchCountDb
)(implicit executionContext: ExecutionContext ) extends Framework.RestApi {
  val tag = "Estimation"

  prefix("/api/v1/batch/estimated") {
    postWithDoc("/?") {
      _.summary("Create a batch").
        tag(tag).
        request[CreateEstimatedRequest].
        responseWith[CreateBatchResponse](status = Status.Ok.code)
    } { req: CreateEstimatedRequest =>
      db.create(req.userKey).map(id => CreateEstimatedBatchResponse(id))
    }

    postWithDoc("/:batchId/close") {
      _.summary("Close a batch").
        tag(tag).
        request[CloseEstimatedBatchRequest].
        responseWith[EstimatedBatchCompletion](status = Status.Ok.code)
    } { req: CloseEstimatedBatchRequest =>
      db.close(req.batchId).map(toResult)
    }

    postWithDoc("/:batchId/incr") {
      _.summary("Add items to a batch").
        tag(tag).
        request[AddToEstimatedBatchRequest].
        responseWith[Unit](status = Status.NoContent.code)
    } { req: AddToEstimatedBatchRequest =>
      db.incr(req.batchId, req.by)
    }

    postWithDoc("/decr") {
      _.summary("Decrement messages from batches").
        tag(tag).
        request[EstimatedBatchCompletionAckRequest].
        responseWith[EstimatedBatchCompletionAckResult](status = Status.Ok.code)
    } { req: EstimatedBatchCompletionAckRequest =>
      val results = req.acks.groupBy(_.batchId).map {
        case (_, values) =>
          val decr = values.head

          db.decr(decr.batchId, decr.by)
      }

      Future.sequence(results).map(results => {
        EstimatedBatchCompletionAckResult(results.map(toResult).toSeq)
      })
    }

    getWithDoc("/:batchId") {
      _.summary("Get a batch information").
        tag(tag).
        request[GetEstimatedBatchRequest].
        responseWith[GetEstimatedBatchResponse](status = Status.Ok.code)
    } { req: GetEstimatedBatchRequest =>
      db.get(req.batchId).map(r =>
        GetEstimatedBatchResponse(
          isComplete = r.pending <= 0 && r.closed,
          isClosed = r.closed,
          userKey = r.userKey
        )
      )
    }

    deleteWithDoc("/:batchId") {
      _.summary("Delete a batch").
        tag(tag).
        request[CloseEstimatedBatchRequest].
        responseWith[Unit](status = Status.NoContent.code)
    } { req: CloseEstimatedBatchRequest =>
      db.delete(req.batchId)
    }
  }

  private def toResult(r: EstimatedBatch): EstimatedBatchCompletion = {
    EstimatedBatchCompletion(r.id, r.userKey, r.pending <= 0 && r.closed)
  }
}

case class GetEstimatedBatchRequest(
  @RouteParam batchId: EstimatedBatchId
)

case class CloseEstimatedBatchRequest(
  @RouteParam batchId: EstimatedBatchId
)

case class AddToEstimatedBatchRequest(
  @RouteParam batchId: EstimatedBatchId,
  @Min(1) @QueryParam by: Long
)

