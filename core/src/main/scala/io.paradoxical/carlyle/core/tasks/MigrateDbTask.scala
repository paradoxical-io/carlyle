package io.paradoxical.carlyle.core.tasks

import io.paradoxical.carlyle.core.api.modules.Modules
import io.paradoxical.carlyle.core.db.Migrator
import io.paradoxical.carlyle.core.utils.guice.GuiceUtil._
import io.paradoxical.tasks.{Task, TaskDefinition}
import scala.concurrent.ExecutionContext.Implicits.global

class MigrateDbTask extends Task {
  override type Config = Unit

  override def emptyConfig: Unit = Unit

  override def definition: TaskDefinition[Unit] = {
    TaskDefinition(
      name = "migrate-db",
      description = "Migrates the carlyle db or creates a new one"
    )
  }

  override def execute(args: Unit): Unit = {
    val migrator = Modules().injector().make[Migrator]

    migrator.migrate()
  }
}
