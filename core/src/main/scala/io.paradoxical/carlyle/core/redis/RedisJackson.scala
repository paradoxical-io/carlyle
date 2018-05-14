package io.paradoxical.carlyle.core.redis

import io.paradoxical.jackson.JacksonSerializer
import scala.util.control.NonFatal

trait SimpleSerialzer {
  def serialize[T: Manifest](value: T): String

  def deserialize[T: Manifest](value: String): Option[T]
}

class RedisJackson(serializer: JacksonSerializer) extends SimpleSerialzer {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  override def serialize[T: Manifest](value: T): String = {
    // short circuit string writing since thats the basic primitive
    if (manifest[T].runtimeClass == classOf[String]) {
      return value.asInstanceOf[String]
    }

    serializer.toJson(value)
  }

  override def deserialize[T: Manifest](value: String): Option[T] = try {
    // short circuit string reading since thats the basic primitive
    if (manifest[T].runtimeClass == classOf[String]) {
      return Some(value.asInstanceOf[T])
    }

    Some(serializer.fromJson[T](value))
  }
  catch {
    case NonFatal(e) =>
      logger.warn(s"Unable to deserialize value: ${value}", e)
      None
  }
}

