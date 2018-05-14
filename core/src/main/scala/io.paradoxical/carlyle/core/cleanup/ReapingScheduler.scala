package io.paradoxical.carlyle.core.cleanup

import io.paradoxical.carlyle.core.config.{ReloadableConfig, ServiceConfig}
import io.paradoxical.carlyle.core.db.Db
import io.paradoxical.common.execution.NamedThreadFactory
import io.paradoxical.common.extensions.Extensions._
import io.paradoxical.finatra.execution.TwitterExecutionContextProvider
import java.util.concurrent.{Executors, TimeUnit}
import javax.inject.Inject
import org.joda.time.DateTime
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

class ReapingScheduler @Inject()(db: Db, config: ReloadableConfig[ServiceConfig])(implicit executionContext: ExecutionContext) {
  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private lazy val reaper = TwitterExecutionContextProvider.of(Executors.newScheduledThreadPool(1, new NamedThreadFactory("Reaper scheduler")))

//  private lazy val openReapeLatency = metrics.timer(metric("reap", "open"))
//  private lazy val closedReaperLatency = metrics.timer(metric("reap", "closed"))

  def start(): Unit = {
    trigger(0 seconds)
  }

  private def trigger(timeout: FiniteDuration): Unit = {
    reaper.schedule(new Runnable {
      override def run(): Unit = {

        logger.info("Running reaper")

        val reapOpenF = reapOpen()
        val reapClosedF = reapClosed()

        (for {
          _ <- reapOpenF
          _ <- reapClosedF
        } yield {

        }).waitForResult()

        trigger(config.currentValue().cleanup.reapingSchedule)
      }
    }, timeout.toSeconds, TimeUnit.SECONDS)
  }

  private def reapClosed(): Future[Unit] = {
//    closedReaperLatency.timeAsync() {
    reap(config.currentValue().cleanup.completedBatchCleanupTime, onlyOpen = false).recover {
      case e: Exception =>
        logger.warn("Unable to reap closed buckets", e)
    }
//    }
  }

  private def reapOpen(): Future[Unit] = {
//    openReapeLatency.timeAsync() {
    reap(config.currentValue().cleanup.completedBatchCleanupTime, onlyOpen = true).recover {
      case e: Exception =>
        logger.warn("Unable to reap open buckets", e)
    }
//    }
  }

  private def reap(since: FiniteDuration, onlyOpen: Boolean) = {
    val sinceDate = DateTime.now().minusMinutes(since.toMinutes.toInt)

    db.deleteExpiredBatches(sinceDate, onlyOpen).map(r => {
      logger.info(s"Reaped batches: ${r.batchesRemoved}, items: ${r.itemsRemoved} for ${if (onlyOpen) "open batches" else "closed batches"}")
    })
  }
}
