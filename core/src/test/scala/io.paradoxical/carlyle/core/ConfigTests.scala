package io.paradoxical.carlyle.core

import io.paradoxical.carlyle.core.config.ConfigLoader
import org.scalatest.{FlatSpec, Matchers}

class ConfigTests extends FlatSpec with Matchers {
  "Configs" should "be valid" in {
    ConfigLoader.load()
  }
}
