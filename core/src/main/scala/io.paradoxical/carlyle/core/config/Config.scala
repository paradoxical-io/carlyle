package io.paradoxical.carlyle.core.config

import com.typesafe.config.ConfigFactory
import io.paradoxical.rdb.hikari.config.RdbConfigWithConnectionPool
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.AllValueReaderInstances
import scala.concurrent.duration.{FiniteDuration, _}

case class ServiceConfig(
  max_batch_group_size: Int = 64,
  db: RdbConfigWithConnectionPool,
  cleanup: Cleanup,
  cache: RedisConfig
)

case class RedisConfig(
  name: String,
  url: String,
  port: Int,
  requestTimeout: Duration = 5 seconds
)

case class Cleanup(
  completedBatchCleanupTime: FiniteDuration = 7 days,
  orphanedBatchCleanupTime: FiniteDuration = 2 hours,
  reapingSchedule: FiniteDuration = 60 seconds
)

trait ReloadableConfig[T] {
  def currentValue(): T
}

class StaticConfig[T](data: T) extends ReloadableConfig[T] {
  override def currentValue(): T = data
}

object ConfigLoader extends AllValueReaderInstances {
  def load(): ReloadableConfig[ServiceConfig] = {
    new StaticConfig(ConfigFactory.load().getConfig("aetr").as[ServiceConfig])
  }
}
