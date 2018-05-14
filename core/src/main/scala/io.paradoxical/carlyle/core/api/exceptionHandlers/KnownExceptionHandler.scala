//package io.paradoxical.carlyle.core.api.exceptionHandlers
//
//import io.paradoxical.carlyle.core.client.QueueBatcherServiceClient
//import io.paradoxical.carlyle.core.model.api.v1._
//import io.paradoxical.carlyle.core.model.api.v1.internal._
//import io.paradoxical.carlyle.core.model.api.v1.exceptions._
//import com.twitter.finagle.http.{Request, Response, Status}
//import com.twitter.finatra.http.exceptions.ExceptionMapper
//import com.twitter.finatra.http.response.ResponseBuilder
//import javax.inject.Inject
//
//class KnownExceptionHandler @Inject()(response: ResponseBuilder) extends ExceptionMapper[QueuebatcherApiException] {
//  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)
//
//  override def toResponse(request: Request, error: QueuebatcherApiException): Response = {
//    logger.error(s"${error} exception occurred. ErrorId=${error.errorId}", error)
//
//    val resp = QueuebatcherErrorResponse(error)
//
//    response.status(Status.InternalServerError).body(resp)
//  }
//}
