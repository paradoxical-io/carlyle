//
// Copyright (c) 2011-2017 by Curalate, Inc.
//

package io.paradoxical.carlyle.core.redis

import io.paradoxical.common.types.{NotNothing, NotUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try

/**
 * A redis client that is as safe as can be without affecting business logic.
 *
 * This means that all get/set/delete/etc calls are NEVER FAILING and will return default
 * values if the underlying client is failing for any reason
 *
 * @param client The underlying client to use
 */
class NeverFailingRedisClient(
  client: RedisClientBase
)(implicit executionContext: ExecutionContext)
  extends RedisClientBase(client.redis, client.namespaceSanitized, client.serializer) {
  protected val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private def recovery[T](name: String, default: T): PartialFunction[Throwable, T] = {
    case ex: Exception =>
      logger.warn(s"An error occurred on the redis command ${name}. Suppressing!", ex)

      default
  }
  
  implicit class SafeFuture[T](f: Future[T]) {
    def safe(name: String, default: T) = f.recover(recovery(name, default))
  }

  /**
   * Set key to hold the string value. If key already holds a value, it is overwritten, regardless of its type.
   *
   * @param key
   * @param value
   * @param expiration Optional ttl
   */
  override def set[T: Manifest](key: String, value: T, expiration: Option[Duration]): Future[Unit] = {
    client.set(key, value, expiration).safe("set", Unit)
  }

  /**
   * Get the value of key.
   *
   * @param key
   * @return The value or None if the key does not exist. An error is returned if the value stored at key is not a string.
   */
  override def get[T: Manifest : NotNothing](key: String): Future[Option[T]] = {
    client.get(key).safe("get", None)
  }

  /**
   * Get the values of multiple keys
   *
   * @param keys
   * @tparam T
   * @return A Map whose key set contains the cache keys that were hit
   */
  override def mGet[T: Manifest : NotUnit : NotNothing](keys: Seq[String]): Future[Map[String, T]] = {
    client.mGet(keys).safe("mGet", Map.empty)
  }

  /**
   * Set multiple keys simultaneously, setting an optional TTL for each key
   *
   * @param keyValTtl
   * @tparam T
   * @return
   */
  override def mSet[T: Manifest](keyValTtl: Seq[(String, T, Option[FiniteDuration])], maxOutstandingRequests: Int): Future[Seq[Try[String]]] = {
    client.mSet(keyValTtl, maxOutstandingRequests).safe("mSet", Nil)
  }

  /**
   * Set multiple keys simultaneously, optionally setting the TTL for all keys
   *
   * @param keyVals
   * @param ttl
   * @tparam T
   * @return
   */
  override def mSet[T: Manifest : NotNothing : NotUnit](keyVals: Seq[(String, T)], ttl: Option[FiniteDuration]): Future[Seq[Try[String]]] = {
    client.mSet(keyVals, ttl).safe("mSet", Nil)
  }

  /**
   * Removes the specified keys. A key is ignored if it does not exist.
   *
   * @param keys
   * @return The number of keys that were removed.
   */
  override def delete(keys: List[String]): Future[Long] = {
    client.delete(keys).safe("delete", 0L)
  }

  /**
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
   *
   * @param key
   * @param expiration TTL no less than 1 second. Anything less than 1 second will expire immediately
   * @return 1 if the timeout was set. 0 if key does not exist or the timeout could not be set.
   */
  override def expire(key: String, expiration: Duration): Future[Boolean] = {
    client.expire(key, expiration).safe("expire", false)
  }

  /**
   * Returns if key exists.
   *
   * @param key
   * @return True if the key exists, otherwise False.
   */
  override def exists(key: String): Future[Boolean] = {
    client.exists(key).safe("exists", false)
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
  override def hSet[T: Manifest](key: String, field: String, value: T): Future[RedisResult] = {
    client.hSet(key, field, value).safe("hSet", RedisResult.Error)
  }

  /**
   * Returns the value associated with field in the hash stored at key.
   *
   * @param key
   * @param field
   * @return The value or None if the key or field does not exist.
   */
  override def hGet[T: Manifest : NotNothing](key: String, field: String): Future[Option[T]] = {
    client.hGet(key, field).safe("hGet", None)
  }

  /**
   * Returns the entire hash map stored for a key
   *
   * @param key
   * @tparam T
   * @return
   */
  override def hGetAll[T: Manifest : NotNothing](key: String): Future[Map[String, T]] = {
    client.hGetAll(key).safe("hGetAll", Map.empty)
  }

  /**
   * Removes the fields from the key's hash. Ignores if the field is missing.
   *
   * @param key
   * @param field
   * @return Number of fields deleted
   */
  override def hDelete(key: String, field: List[String]): Future[Long] = {
    client.hDelete(key, field).safe("hDelete", 0L)
  }

  /**
   * Returns the value associated with fields in the hash stored at key.
   *
   * @param key
   * @param fields The fields to extract
   * @return The field to value map or an empty Map if the key or field does not exist.
   */
  override def hMGet(key: String, fields: Set[String]): Future[Map[String, String]] = {
    client.hMGet(key, fields).safe("hMGet", Map.empty)
  }

  /**
   * Sets the values to be associated with the given fields in the hash stored at key.
   *
   * @param key
   * @param valuesByFields
   * @return The field to value map or an empty Map if the key or field does not exist.
   */
  override def hMSet(key: String, valuesByFields: Map[String, String]): Future[Unit] = {
    client.hMSet(key, valuesByFields).safe("hMSet", Unit)
  }

  /**
   * Returns if field is an existing field in the hash stored at key.
   *
   * @param key
   * @param field
   * @return 1 if the has contains field, otherwise 0.
   */
  override def hExists(key: String, field: String): Future[Boolean] = {
    client.hExists(key, field).safe("hExists", false)
  }

  /**
   * Return the number of fields in a hash
   * Complexity: O(1)
   *
   * @param key The key pointing to the hash
   * @return The number of fields in the hash
   */
  override def hLen(key: String): Future[Long] = {
    client.hLen(key).safe("hLen", 0)
  }

  /**
   * Increment a field of a hash by N
   * Complexity: O(1)
   *
   * UNSAFE CALL: cannot make this safe since this an atomic action and returning a default value is unsafe
   *
   * @param key   The key pointing to the hash
   * @param field The field within the hash to increment
   * @param by    Number to increment the field by
   * @return The new value of the hash field
   */
  override def hIncrBy(key: String, field: String, by: Long): Future[Long] = {
    client.hIncrBy(key, field, by)
  }

  /**
   * Increment a key by 1
   * Complexity: O(1)
   *
   * UNSAFE CALL: cannot make this safe since this an atomic action and returning a default value is unsafe
   *
   * @param key The key to increment
   * @return The new value of the key
   */
  override def incr(key: String): Future[Long] = {
    client.incr(key)
  }

  /**
   * Increment a key by N
   * Complexity: O(1)
   *
   * UNSAFE CALL: cannot make this safe since this an atomic action and returning a default value is unsafe
   *
   * @param key The key to increment
   * @param by  The number to increment by
   * @return The new value that the key points to
   */
  override def incrBy(key: String, by: Long): Future[Long] = {
    client.incrBy(key, by)
  }

  /**
   * Decrement a field by 1
   * Complexity: O(1)
   *
   * UNSAFE CALL: cannot make this safe since this an atomic action and returning a default value is unsafe
   *
   * @param key The key to decrement
   * @return The new value that the key points to
   */
  override def decr(key: String): Future[Long] = {
    client.decr(key)
  }

  /**
   * Decrement a field by N
   * Complexity: O(1)
   *
   * UNSAFE CALL: cannot make this safe since this an atomic action and returning a default value is unsafe
   *
   * @param key The key to decrement
   * @param by  The number to decrement by
   * @return The new value that the key points to
   */
  override def decrBy(key: String, by: Long): Future[Long] = {
    client.decrBy(key, by)
  }

  /**
   * Finds all keys that match the given pattern and then attempts to delete them all.
   */
  override def deleteKeysWithPattern(keyPattern: String): Future[Unit] = {
    client.deleteKeysWithPattern(keyPattern).safe("deleteKeysWithPattern", Unit)
  }
}
