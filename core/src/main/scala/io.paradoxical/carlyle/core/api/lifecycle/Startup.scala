//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core.api.lifecycle

import com.google.inject.Singleton
import io.paradoxical.carlyle.core.cleanup.ReapingScheduler
import javax.inject.Inject

@Singleton
class Startup @Inject()(reapingScheduler: ReapingScheduler) {
  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  def start(): Unit = {
    reapingScheduler.start()
  }

  def stop(): Unit = {}
}
