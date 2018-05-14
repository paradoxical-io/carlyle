package io.paradoxical.carlyle.core.api.modules

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import io.paradoxical.carlyle.core.config.{ConfigLoader, ReloadableConfig, ServiceConfig}
import io.paradoxical.carlyle.core.redis.{RedisClientBase, RedisClientProvider}
import io.paradoxical.finatra.modules.Defaults
import scala.concurrent.ExecutionContext


object Modules {
  def apply(metricsEnabled: Boolean = false)(implicit executionContext: ExecutionContext) = List(
    new ConfigModule(),
    new DbModule(),
    new EstimatedBatchCountModule()
  ) ++ Defaults()
}

class EstimatedBatchCountModule extends TwitterModule {
  @Provides
  @Singleton
  def cache(
    config: ReloadableConfig[ServiceConfig]
  )(implicit executionContext: ExecutionContext): RedisClientBase = {
    config.currentValue().cache.map(c => new RedisClientProvider(c).namespacedClient("carlyle")).orNull
  }
}

/**
 * Provides configuration
 */
class ConfigModule extends TwitterModule {
  @Provides
  @Singleton
  def config: ReloadableConfig[ServiceConfig] = {
    ConfigLoader.load()
  }
}
