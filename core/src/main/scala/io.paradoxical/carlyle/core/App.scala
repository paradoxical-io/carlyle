//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core

import io.paradoxical.carlyle.core.api.ServerTask
import io.paradoxical.tasks.{Task, TaskEnabledApp}
import org.joda.time.DateTimeZone
import scala.concurrent.ExecutionContext.Implicits.global

object App extends TaskEnabledApp {
  DateTimeZone.setDefault(DateTimeZone.UTC)

  override def appName: String = "carlyle"

  lazy val serverTask = new ServerTask()

  override def defaultTask: Option[Task] = Some(serverTask)

  override def tasks: List[Task] = List(serverTask)
}


