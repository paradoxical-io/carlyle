package io.paradoxical.carlyle.core.db.redis

import io.paradoxical.carlyle.core.config.RedisConfig
import io.paradoxical.carlyle.core.redis.RedisClientProvider
import io.paradoxical.rdb.slick.test.Container
import io.paradoxical.v2.DockerCreator
import io.paradoxical.{DockerClientConfig, LogLineMatchFormat}
import scala.concurrent.ExecutionContext

object Redis {
  private val DEFAULT_REDIS_PORT = 6379

  val VERSION_2_8_23 = "2.8.23"
  val VERSION_3_2 = "3.2"

  def docker(version: String = VERSION_2_8_23)(implicit executionContext: ExecutionContext): RedisDocker = {
    val container = DockerCreator.build(
      DockerClientConfig.
        builder.
        pullAlways(true).
        imageName(s"redis:$version").
        waitForLogLine("The server is now ready to accept connections on port 6379", LogLineMatchFormat.Exact, 5).
        port(DEFAULT_REDIS_PORT).
        build
    )

    new RedisDocker(Container(container.getDockerHost, container.getTargetPortToHostPortLookup.get(DEFAULT_REDIS_PORT), container), container)(
      executionContext
    )
  }
}

class RedisDocker(
  val container: Container,
  val rawContainer: io.paradoxical.v2.Container
)(implicit executionContext: ExecutionContext) {
  def close() = container.close()

  lazy val clientProvider = {
    val config = RedisConfig("test", url = container.host, port = container.port)

    new RedisClientProvider(config)
  }
}

