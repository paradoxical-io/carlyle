package io.paradoxical.carlyle.core.clients

import io.paradoxical.carlyle.core.model._
import io.paradoxical.jackson.JacksonSerializer
import java.io.InputStream
import java.net.URL
import scalaj.http.{Http, HttpResponse}

class CarlyleClient(url: URL) {
  private val s = new JacksonSerializer()

  protected def http(path: String) = {
    Http(url.toString + path).header("content-type", "application/json")
  }

  private def parse[TRes: Manifest](stream: InputStream): TRes = {
    if(stream.available() == 0) {
      Unit.asInstanceOf[TRes]
    } else {
      val result = JacksonSerializer.default.readValue[TRes](stream)

      result
    }
  }

  protected def post[TReq: Manifest, TResp: Manifest](
    name: String,
    request: String,
    queryParams: Map[String, String] = Map.empty,
    data: Option[TReq] = None): HttpResponse[TResp] = {
    val req = http(request).method("POST").params(queryParams)

    val withBody = data.map(d => req.postData(s.toJson(d))).getOrElse(req)

    withBody.execute[TResp](parse[TResp])
  }

  private def get[TResult: Manifest](name: String, path: String, queryParams: Map[String, String] = Map.empty): HttpResponse[TResult] = {
    val req = http(path).method("GET")

    req.params(queryParams).execute[TResult](parse[TResult])
  }

  val exact = new Exact
  val estimated = new Estimated

  class Estimated {
    def createBatch(size: Long, userKey: Option[String] = None): CreateEstimatedBatchResponse = {
      post[CreateEstimatedRequest, CreateEstimatedBatchResponse](
        "createEstimatedBatch",
        request = "/api/v1/batch/estimated",
        data = Some(CreateEstimatedRequest(userKey = userKey))
      ).body
    }

    def closeBatch(id: EstimatedBatchId): EstimatedBatchCompletion = {
      post[Unit, EstimatedBatchCompletion](
        "closeEstimatedBatch",
        request = s"/api/v1/batch/estimated/${id.value.toString}/close"
      ).body
    }

    def getBatch(batchId: EstimatedBatchId): GetEstimatedBatchResponse = {
      get[GetEstimatedBatchResponse](
        name = "getEstimatedBatch",
        s"/api/v1/batch/estimated/${batchId.value}"
      ).body
    }

    def addToBatch(batchId: EstimatedBatchId, n: Int): Unit = {
      post[Unit, Unit](
        name = "incrementEstimatedBatch",
        request = s"/api/v1/batch/estimated/${batchId.value.toString}/incr",
        queryParams = Map("by" -> n.toString)
      ).body
    }

    def decrement(batchId: EstimatedBatchId, by: Long = 1): Boolean = {
      decrement(Seq(EstimatedBatchCompletionAck(batchId, by))).results.exists(_.isComplete)
    }

    def decrement(batches: Seq[EstimatedBatchCompletionAck]): EstimatedBatchCompletionAckResult = {
      post[EstimatedBatchCompletionAckRequest, EstimatedBatchCompletionAckResult](
        name = "decrementEstimatedBatch",
        s"/api/v1/batch/estimated/decr",
        data = Some(EstimatedBatchCompletionAckRequest(batches))
      ).body
    }
  }

  class Exact {
    def createBatch(userKey: Option[String] = None): CreateBatchResponse = {
      post[CreateBatchRequest, CreateBatchResponse](
        "createBatch",
        request = "/api/v1/batch/exact",
        data = Some(CreateBatchRequest(userKey))
      ).body
    }

    def getBatch(batchId: BatchId): GetBatchResponse = {
      get[GetBatchResponse](
        name = "getBatch",
        s"/api/v1/batch/exact/${batchId.value}"
      ).body
    }

    def closeBatch(batchId: BatchId): CloseBatchResponse = {
      post[Unit, CloseBatchResponse](
        "closeBatch",
        request = s"/api/v1/batch/exact/close/${batchId.value}"
      )
    }.body

    def addItemsToBatch(batchId: BatchId, numberOfItems: Int, maxPerBatch: Int = 10000): Iterable[BatchItemId] = {
      // add items to the batch in groups of maxPerBatch. this minimizes IO over the wire
      var remaining = numberOfItems

      val iterations = if (numberOfItems % maxPerBatch == 0) numberOfItems / maxPerBatch else (numberOfItems / maxPerBatch) + 1

      val batches =
        (0 until iterations).foldLeft(Seq.empty[AddBatchItemsResponse])((prev, iteration) => {
          val nextBatch = Math.min(remaining, maxPerBatch)

          remaining -= nextBatch

          val httpResult = post[AddBatchItemsRequest, AddBatchItemsResponse](
            "addItemsToBatch",
            request = s"/api/v1/batch/exact/items",
            data = Some(AddBatchItemsRequest(batchId, nextBatch))
          )

          val result = httpResult.body

          prev :+ result
        })

      batches.toStream.flatMap(r => {
        r.batchItemGroupIds.flatMap(batchItemGroup => {
          BatchItemId.generate(batchId, batchItemGroup.id, batchItemGroup.upto)
        })
      })
    }

    def ackItem(batchItemId: BatchItemId): Boolean = {
      ackItems(Set(batchItemId)).batchCompletions.nonEmpty
    }

    def ackItems(items: Iterable[BatchItemId], batchSize: Int = 100): AckBatchItemsResponse = {
      items.grouped(batchSize).foldLeft(AckBatchItemsResponse(Seq.empty))((prev, batch) => {
        val next = ackItemsBatch(batch)

        AckBatchItemsResponse(prev.batchCompletions ++ next.batchCompletions)
      })
    }

    private def ackItemsBatch(items: Iterable[BatchItemId]): AckBatchItemsResponse = {
      post[AckBatchItemsRequest, AckBatchItemsResponse](
        "addItemsToBatch",
        request = s"/api/v1/batch/exact/items/ack",
        data = Some(AckBatchItemsRequest(items.toSet))
      ).body
    }
  }
}