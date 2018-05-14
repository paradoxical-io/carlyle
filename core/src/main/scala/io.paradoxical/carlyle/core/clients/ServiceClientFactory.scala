//package io.paradoxical.carlyle.core.clients
//
//import io.paradoxical.carlyle.core.client._
//import io.paradoxical.monitoring.v3.Metrics
//import io.paradoxical.finagle.http.client.driver.typed.FinagleDriver
//import java.net.URL
//import scala.concurrent.ExecutionContext
//
//object ClientFactory {
//  def queuebatcherClient(url: URL)(implicit executionContext: ExecutionContext): QueueBatcherServiceClient = {
//    new QueueBatcherServiceClient(FinagleDriver[QueueBatcherServiceClient](url))
//  }
//}
