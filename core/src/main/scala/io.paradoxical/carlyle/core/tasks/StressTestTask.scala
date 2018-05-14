//package io.paradoxical.carlyle.core.tasks
//
//import io.paradoxical.common.execution.SequentialFutures
//import io.paradoxical.common.extensions.Extensions._
//import io.paradoxical.configuration.ConfigLoader
//import io.paradoxical.contracts.execution.ExecutionContexts.Implicits.global
//import io.paradoxical.finagle.http.client.driver.typed.FinagleDriver
//import io.paradoxical.guice.GuiceUtil._
//import io.paradoxical.monitoring.v3.Metrics
//import io.paradoxical.carlyle.core.client.QueueBatcherServiceClient
//import io.paradoxical.carlyle.core.client.config.QueuebatcherServiceClientConfig
//import io.paradoxical.carlyle.core.api.modules.Modules
//import io.paradoxical.tasks.{Task, TaskDefinition}
//import io.paradoxical.trace.api.TraceContextApi
//import com.google.inject.Provides
//import com.twitter.inject.TwitterModule
//import java.util.concurrent.atomic.AtomicInteger
//import javax.inject.Singleton
//import scala.util.Random
//
//case class StressTestConfig(batchSize: Int = 0, retryPercent: Double = 0.10)
//
//class StressTestTask extends Task {
//  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)
//
//  override type Config = StressTestConfig
//
//  override def emptyConfig: StressTestConfig = StressTestConfig()
//
//  override def definition: TaskDefinition[StressTestConfig] = {
//    new TaskDefinition(
//      name = "stress-test",
//      description = "Stresses the batch service",
//      args =
//        _.opt[Int]("batch-size").text("Batch size to test").action((b, c) => c.copy(batchSize = b)),
//      _.opt[Double]("retry-percent").text("The percent of requests that should double-ack").action((b, c) => c.copy(retryPercent = b))
//    )
//  }
//
//  override def execute(args: StressTestConfig): Unit = {
//    val r = new Random()
//
//    val client = (Modules() :+ new ClientModule()).injector().make[QueueBatcherServiceClient]
//
//    val trace = TraceContextApi.instance
//
//    trace.withTraceId(trace.newTraceId) {
//      val batchId = client.exact.createBatch().waitForResult().batchId
//
//      val idSet = client.exact.addItemsToBatch(batchId, args.batchSize).waitForResult()
//
//      client.exact.closeBatch(batchId).waitForResult()
//
//      val progress = new AtomicInteger(0)
//
//      val tenPercent = 0.10 * args.batchSize
//
//      def logProgress() = {
//        val current = progress.incrementAndGet()
//
//        val currentPercent = (current * 100.0) / args.batchSize
//
//        if (currentPercent % 10 == 0) {
//          logger.info(s"$currentPercent%")
//        }
//      }
//
//      val batchClosed = SequentialFutures.serializeBatched(idSet, 100) { item =>
//        logProgress()
//
//        def ackFuture = client.exact.ackItem(item)
//
//        // if it needs to retry do it, ignoring its original result
//        if (r.nextDouble < args.retryPercent) {
//          logger.info(s"Retrying ${item.value}!")
//
//          ackFuture.flatMap(_ => ackFuture)
//        } else {
//          ackFuture
//        }
//      }.waitForResult().exists(identity)
//
//      if (!batchClosed) {
//        logger.warn("Batch should have been closed but was not!")
//      }
//    }
//  }
//}
//
//class ClientModule() extends TwitterModule {
//  @Provides
//  @Singleton
//  def client(loader: ConfigLoader)(implicit metrics: Metrics) = {
//    import io.paradoxical.configuration.readers.ValueReaders._
//
//    val config = loader.loadType(QueuebatcherServiceClientConfig)
//
//    val driver = FinagleDriver[QueueBatcherServiceClient](config.currentValue().url)
//
//    new QueueBatcherServiceClient(driver)
//  }
//}