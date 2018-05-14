package io.paradoxical.carlyle.core.api

import io.paradoxical.tasks.{Task, TaskDefinition}
import io.paradoxical.carlyle.core.api.modules.Modules
import scala.concurrent.ExecutionContext

case class ServerConfig(
  metricsEnabled: Boolean = true
)
class ServerTask()(implicit executionContext: ExecutionContext) extends Task {
  override type Config = ServerConfig

  override def emptyConfig: ServerConfig = ServerConfig()

  override def definition: TaskDefinition[ServerConfig] = {
    new TaskDefinition[ServerConfig](
      name = "server",
      description = "Server",
      args =
        _.opt[Unit]("disableMetrics").
          action((_, config) => {
            config.copy(metricsEnabled = false)
          })
    )
  }

  override def execute(args: ServerConfig): Unit = {
    new Server(Modules(args.metricsEnabled)).main(Array.empty)
  }
}
