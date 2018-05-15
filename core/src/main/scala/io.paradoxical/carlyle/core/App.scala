package io.paradoxical.carlyle.core

import io.paradoxical.carlyle.core.api.ServerTask
import io.paradoxical.carlyle.core.tasks.MigrateDbTask
import io.paradoxical.tasks.{Task, TaskEnabledApp}
import org.joda.time.DateTimeZone
import scala.concurrent.ExecutionContext.Implicits.global

object App extends TaskEnabledApp {
  DateTimeZone.setDefault(DateTimeZone.UTC)

  override def appName: String = "carlyle"

  override def tasks: List[Task] = List(new ServerTask(), new MigrateDbTask)
}
