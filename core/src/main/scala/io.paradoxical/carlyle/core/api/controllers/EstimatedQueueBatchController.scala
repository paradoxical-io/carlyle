//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core.api.controllers

import com.twitter.finagle.http.Status
import com.twitter.finatra.request.RouteParam
import io.paradoxical.carlyle.core.db.estimation.EstimatedBatchCountDb
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
      db.set(req.batchSize, req.userKey).map(id => CreateEstimatedBatchResponse(id))
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
        EstimatedBatchCompletionAckResult(results.map(r => EstimatedBatchCompletion(r.id, r.userKey, r.pending)).toSeq)
      })
    }

    getWithDoc("/:batchId") {
      _.summary("Get a batch information").
        tag(tag).
        request[GetEstimatedBatchRequest].
        responseWith[GetEstimatedBatchResponse](status = Status.Ok.code)
    } { req: GetEstimatedBatchRequest =>
      db.get(req.batchId).map(r => {
        GetEstimatedBatchResponse(r.pending, r.userKey)
      })
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
}

case class GetEstimatedBatchRequest(
  @RouteParam batchId: EstimatedBatchId
)

case class CloseEstimatedBatchRequest(
  @RouteParam batchId: EstimatedBatchId
)
