package io.paradoxical.carlyle.core.redis

import com.twitter.finagle.Redis
import io.paradoxical.carlyle.core.config.RedisConfig
import io.paradoxical.jackson.JacksonSerializer
import scala.concurrent.ExecutionContext

class RedisClientProvider(
  config: RedisConfig
)(implicit executionContext: ExecutionContext) {

  private def rawClientBuilder: Redis.Client = {
    Redis.client.withLabel(config.url)
  }

  lazy val rawClient = rawClientBuilder.newTransactionalClient(s"${config.url}:${config.port}")

  def namespacedClient(namespace: String): RedisClientBase = {
    new RedisClientBase(rawClient, namespace, new RedisJackson(new JacksonSerializer()))
  }
}
