//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core.redis

import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.redis.TransactionalClient
import com.twitter.finagle.redis.protocol.Reply
import com.twitter.io.Buf
import com.twitter.util.{Future => TFuture}
import io.paradoxical.carlyle.core.utils.FutureConversions._
import io.paradoxical.common.types.{NotNothing, NotUnit}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class RedisClientBase(
  val redis: TransactionalClient,
  val defaultNamespace: String,
  val serializer: SimpleSerialzer
)(implicit executionContext: ExecutionContext) {
  import serializer._

  private[redis] val namespaceSanitized = {
    defaultNamespace.stripPrefix(".").stripSuffix(".")
  }

  implicit private def cb2s(cb: Buf): Option[String] = Buf.Utf8.unapply(cb)

  implicit private def stringToBuffer(s: String): Buf = Buf.Utf8(s)

  private[redis] def toCacheKey(key: String): String = {
    if (namespaceSanitized.isEmpty) {
      key
    } else {
      s"$namespaceSanitized.$key"
    }
  }

  private[redis] def fromCacheKey(key: String): String = {
    key.stripPrefix(s"$namespaceSanitized.")
  }

  private val toSerializedCacheKey = (toCacheKey _).andThen(stringToBuffer)

  /**
   * Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type.
   *
   * @param key
   * @param value
   * @param expiration Optional ttl
   */
  def set[T: Manifest](key: String, value: T, expiration: Option[Duration] = None): Future[Unit] = {
    val serialized = serialize(value)

    (expiration match {
      case Some(time) => redis.pSetEx(toCacheKey(key), time.toMillis, serialized)
      case _ => redis.set(toCacheKey(key), serialized)
    }).toSFuture
  }

  /**
   * Get the value of key.
   *
   * @param key
   * @return The value or None if the key does not exist. An error is returned if the value stored at key is not a string.
   */
  def get[T: Manifest : NotNothing](key: String): Future[Option[T]] = {
    redis.get(toCacheKey(key)).map(_.flatMap(cb2s).flatMap(deserialize[T])).toSFuture
  }

  /**
   * Get the values of multiple keys
   *
   * @param keys
   * @tparam T
   * @return A Map whose key set contains the cache keys that were hit
   */
  def mGet[T: Manifest : NotUnit : NotNothing](keys: Seq[String]): Future[Map[String, T]] = {
    if (keys.isEmpty) {
      return Future.successful(Map.empty)
    }

    val serializedKeys = keys.map(toSerializedCacheKey)
    redis.mGet(serializedKeys).map(bufSeq => {
      keys.zip(bufSeq).map {
        case (key, buf) => key -> buf.flatMap(cb2s).flatMap(deserialize[T])
      }.collect {
        case (key, value) if value.isDefined => fromCacheKey(key) -> value.get
      }.toMap
    }).toSFuture
  }

  /**
   * Set multiple keys simultaneously, setting an optional TTL for each key
   *
   * @param keyValTtl
   * @tparam T
   * @return
   */
  def mSet[T: Manifest](
    keyValTtl: Seq[(String, T, Option[FiniteDuration])],
    maxOutstandingRequests: Int = 16
  ): Future[Seq[Try[String]]] = {
    if (keyValTtl.isEmpty) {
      return Future.successful(Seq.empty)
    }

    val hasTtlsSet = keyValTtl.exists { case (_, _, oTtl) => oTtl.isDefined }

    if (hasTtlsSet) {
      AsyncStream.fromSeq(keyValTtl).mapConcurrent(maxOutstandingRequests)(keyTup => {
        val (key, _, _) = keyTup
        (keyTup match {
          case (_, value, Some(ttl)) => redis.setPx(toSerializedCacheKey(key), ttl.toMillis, stringToBuffer(serialize(value)))
          case (_, value, None) => redis.set(toSerializedCacheKey(key), stringToBuffer(serialize(value)))
        }).map(_ => Success(key)).handle {
          case t => Failure(t)
        }
      }).toSeq().toSFuture
    } else {
      val keyValMap = keyValTtl.map {
        case (key, value, _) => toSerializedCacheKey(key) -> stringToBuffer(serialize(value))
      }.toMap

      redis.mSet(keyValMap).toSFuture.map(_ => {
        keyValTtl.map(_._1).map(Success(_))
      })
    }
  }

  /**
   * Set multiple keys simultaneously, optionally setting the TTL for all keys
   *
   * @param keyVals
   * @param ttl
   * @tparam T
   * @return
   */
  def mSet[T: Manifest : NotNothing : NotUnit](keyVals: Seq[(String, T)], ttl: Option[FiniteDuration]): Future[Seq[Try[String]]] = {
    val keyValTtls = keyVals.map {
      case (key, value) => (key, value, ttl)
    }

    mSet(keyValTtls)
  }

  /**
   * Removes the specified keys. A key is ignored if it does not exist.
   *
   * @param keys
   * @return The number of keys that were removed.
   */
  def delete(keys: List[String]): Future[Long] = {
    redis.dels(keys.map(toCacheKey).map(stringToBuffer)).map(_.toLong).toSFuture
  }

  /**
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
   *
   * @param key
   * @param expiration TTL no less than 1 second. Anything less than 1 second will expire immediately
   * @return 1 if the timeout was set. 0 if key does not exist or the timeout could not be set.
   */
  def expire(key: String, expiration: Duration): Future[Boolean] = {
    redis.expire(toCacheKey(key), expiration.toSeconds).map(_.booleanValue()).toSFuture
  }

  /**
   * Returns if key exists.
   *
   * @param key
   * @return True if the key exists, otherwise False.
   */
  def exists(key: String): Future[Boolean] = {
    redis.exists(toCacheKey(key)).map(_.booleanValue()).toSFuture
  }

  /**
   * Sets field in the hash stored at key to value.
   * If key does not exist, a new key holding a hash is created. If field already exists in the hash, it is overwritten.
   *
   * @param key
   * @param field
   * @param value
   * @return [[RedisResult]]
   */
  def hSet[T: Manifest](key: String, field: String, value: T): Future[RedisResult] = {
    redis.hSet(toCacheKey(key), field, serialize(value)).map(_.toLong).map {
      case 1L => RedisResult.Inserted
      case _ => RedisResult.Updated
    }.toSFuture
  }

  /**
   * Returns the value associated with field in the hash stored at key.
   *
   * @param key
   * @param field
   * @return The value or None if the key or field does not exist.
   */
  def hGet[T: Manifest : NotNothing](key: String, field: String): Future[Option[T]] = {
    redis.hGet(toCacheKey(key), field).map(_.flatMap(cb2s).flatMap(deserialize[T])).toSFuture
  }

  /**
   * Returns the entire hash map stored for a key
   *
   * @param key
   * @tparam T
   * @return
   */
  def hGetAll[T: Manifest : NotNothing](key: String): Future[Map[String, T]] = {
    redis.hGetAll(toCacheKey(key)).map(bufSeq => {
      for {
        (fieldBuf, valBuf) <- bufSeq
        fieldStr <- cb2s(fieldBuf).toSeq
        value <- cb2s(valBuf).flatMap(deserialize[T]).toSeq
      } yield fieldStr -> value
    }).map(_.toMap).toSFuture
  }

  /**
   * Removes the fields from the key's hash. Ignores if the field is missing.
   *
   * @param key
   * @param field
   * @return Number of fields deleted
   */
  def hDelete(key: String, field: List[String]): Future[Long] = {
    redis.hDel(toCacheKey(key), field.map(stringToBuffer)).map(_.toLong).toSFuture
  }

  /**
   * Returns the value associated with fields in the hash stored at key.
   *
   * @param key
   * @param fields The fields to extract
   * @return The field to value map or an empty Map if the key or field does not exist.
   */
  def hMGet(key: String, fields: Set[String]): Future[Map[String, String]] = {
    redis.hMGet(toCacheKey(key), fields.toSeq.map(stringToBuffer)).
      map(_.flatMap(cb2s)).
      toSFuture.
      map(values => fields.zip(values).filterNot(_._2.isEmpty).toMap)
  }

  /**
   * Sets the values to be associated with the given fields in the hash stored at key.
   *
   * @param key
   * @param valuesByFields
   * @return The field to value map or an empty Map if the key or field does not exist.
   */
  def hMSet(key: String, valuesByFields: Map[String, String]): Future[Unit] = {
    redis.hMSet(toCacheKey(key), valuesByFields.map {
      case (field, value) => (stringToBuffer(field), stringToBuffer(value))
    }).toSFuture
  }

  /**
   * Sets the value for a given field in a hash only if it isn't already defined
   *
   * @param key The key that references the hash
   * @param field The field to set conditionally
   * @param value The value to set if the field isn't already defined
   * @return True if the field was set.
   */
  def hSetNx(key: String, field: String, value: String): Future[Boolean] = {
    redis.hSetNx(toCacheKey(key), stringToBuffer(field), stringToBuffer(value)).toSFuture.map(_ == 1)
  }

  def hSetNx[T: Manifest : NotUnit : NotNothing](key: String, field: String, value: T): Future[Boolean] = {
    hSetNx(key, field, serializer.serialize(value))
  }

  /**
   * Returns if field is an existing field in the hash stored at key.
   *
   * @param key
   * @param field
   * @return 1 if the has contains field, otherwise 0.
   */
  def hExists(key: String, field: String): Future[Boolean] = {
    redis.hExists(toCacheKey(key), field).map(_.booleanValue()).toSFuture
  }

  /**
   * Return the number of fields in a hash
   * Complexity: O(1)
   *
   * @param key The key pointing to the hash
   * @return The number of fields in the hash
   */
  def hLen(key: String): Future[Long] = {
    redis.hLen(toCacheKey(key)).toSFuture
  }

  /**
   * Increment a field of a hash by N
   * Complexity: O(1)
   *
   * @param key   The key pointing to the hash
   * @param field The field within the hash to increment
   * @param by    Number to increment the field by
   * @return The new value of the hash field
   */
  def hIncrBy(key: String, field: String, by: Long): Future[Long] = {
    redis.hIncrBy(toCacheKey(key), field, by).map(_.toLong).toSFuture
  }

  /**
   * Increment a key by 1
   * Complexity: O(1)
   *
   * @param key The key to increment
   * @return The new value of the key
   */
  def incr(key: String): Future[Long] = {
    redis.incr(toCacheKey(key)).map(_.toLong).toSFuture
  }

  /**
   * Increment a key by N
   * Complexity: O(1)
   *
   * @param key The key to increment
   * @param by  The number to increment by
   * @return The new value that the key points to
   */
  def incrBy(key: String, by: Long): Future[Long] = {
    redis.incrBy(toCacheKey(key), by).map(_.toLong).toSFuture
  }

  /**
   * Decrement a field by 1
   * Complexity: O(1)
   *
   * @param key The key to decrement
   * @return The new value that the key points to
   */
  def decr(key: String): Future[Long] = {
    redis.decr(toCacheKey(key)).map(_.toLong).toSFuture
  }

  /**
   * Decrement a field by N
   * Complexity: O(1)
   *
   * @param key The key to decrement
   * @param by  The number to decrement by
   * @return The new value that the key points to
   */
  def decrBy(key: String, by: Long): Future[Long] = {
    redis.decrBy(toCacheKey(key), by).map(_.toLong).toSFuture
  }

  /**
   * Evaluate a Lua script
   * Complexity: Depends on the script
   *
   * @see https://redis.io/commands/eval
   * @param script A buffer containing the bytes of a Lua script to execute
   * @param keys Keys to pass in the "KEYS" array into the script
   * @param argv Arguments to pass in the "ARGV" array into the script
   * @param handler A response handler partial function which handles the raw Redis response
   * @tparam T
   * @return The output of the response handler, or a failed Future
   */
  def eval[T](script: Buf, keys: Seq[String], argv: Seq[String])(handler: PartialFunction[Reply, Future[T]]): Future[T] = {
    redis.eval(script, keys.map(stringToBuffer), argv.map(stringToBuffer)).toSFuture.flatMap(handler orElse defaultHandler)
  }

  /**
   * Finds all keys that match the given pattern and then attempts to delete them all.
   */
  def deleteKeysWithPattern(keyPattern: String): Future[Unit] = {
    /*
     * While the time complexity for this operation is O(N), the constant times are fairly low. For example, Redis running on an entry level laptop can
     * scan a 1 million key database in 40 milliseconds.
     *
     * Warning: consider keyS as a command that should only be used in production environments with extreme care. It may ruin performance when it is
     * executed against large databases. This command is intended for debugging and special operations, such as changing your keyspace layout. Don't use
     * keyS in your regular application code. If you're looking for a way to find keys in a subset of your keyspace, consider using SCAN or sets.
     *
     * Source: http://redis.io/commands/keys.
     *
     * NB, however, RedisClient has no SCAN exposed. So, given the estimated number of keys (bounded by our number of users), this should be sufficient for
     * cache invalidations.
     */
    redis.keys(toCacheKey(keyPattern)).flatMap(keys => {
      // Don't delete if no keys are found
      if (keys.nonEmpty) redis.dels(keys) else TFuture.value(0L)
    }).map(_ => ()).toSFuture
  }

  private val defaultHandler: PartialFunction[Reply, Future[Nothing]] = { case _ => Future.failed(new IllegalStateException) }
}
