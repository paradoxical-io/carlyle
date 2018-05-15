package io.paradoxical.carlyle.core

import com.twitter.finatra.http.EmbeddedHttpServer
import io.paradoxical.carlyle.core.api.Server
import io.paradoxical.carlyle.core.clients.CarlyleClient
import io.paradoxical.carlyle.core.db.DbInitializer
import io.paradoxical.carlyle.core.db.redis.Redis
import io.paradoxical.carlyle.core.modules.TestModules
import io.paradoxical.carlyle.core.modules.TestModules._
import java.net.URL
import org.scalatest._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class Tests extends FlatSpec with Matchers with Assertions with BeforeAndAfterAll {

  private lazy val redis = Redis.docker()

  private lazy val server = {
    val x = new EmbeddedHttpServer(new Server(
      TestModules().
        withH2().
        withRedis(redis.clientProvider.namespacedClient("test"))
    ))

    x.injector.instance[DbInitializer].init()

    x
  }

  override protected def afterAll(): Unit = {
    redis.close()
  }

  lazy val client = new CarlyleClient(new URL(s"http://${server.externalHttpHostAndPort}"))

  "A server" should "create and ack exact batches" in {
    val batchId = client.exact.createBatch().batchId

    val items = client.exact.addItemsToBatch(batchId, 1000)

    val currentBatchGet = client.exact.getBatch(batchId)

    assert(!currentBatchGet.isComplete)
    assert(!currentBatchGet.isClosed)

    assert(!client.exact.closeBatch(batchId).isComplete)

    // assert its complete
    assert(client.exact.ackItems(items).isComplete(batchId))

    val closedBatchGet = client.exact.getBatch(batchId)

    assert(closedBatchGet.isComplete)
    assert(closedBatchGet.isClosed)
  }

  it should "create and ack exact large batches" in {
    val batchId = client.exact.createBatch().batchId

    val batchSize = 25000 + Random.nextInt(1000)

    val items = client.exact.addItemsToBatch(batchId, batchSize)

    assert(items.size == batchSize)

    val currentBatchGet = client.exact.getBatch(batchId)

    assert(!currentBatchGet.isComplete)
    assert(!currentBatchGet.isClosed)

    assert(!client.exact.closeBatch(batchId).isComplete)

    // assert its complete
    assert(client.exact.ackItems(items).isComplete(batchId))

    val closedBatchGet = client.exact.getBatch(batchId)

    assert(closedBatchGet.isComplete)
    assert(closedBatchGet.isClosed)
  }

  it should "create and ack exact in batches" in {
    val batchId = client.exact.createBatch().batchId

    val batchSize = 25000 + Random.nextInt(1000)

    val items = client.exact.addItemsToBatch(batchId, batchSize)

    assert(items.size == batchSize)

    val currentBatchGet = client.exact.getBatch(batchId)

    assert(!currentBatchGet.isComplete)
    assert(!currentBatchGet.isClosed)

    assert(!client.exact.closeBatch(batchId).isComplete)

    // ack one batch
    assert(!client.exact.ackItems(items.take(1)).isComplete(batchId))

    // ack the rest
    assert(client.exact.ackItems(items).isComplete(batchId))

    val closedBatchGet = client.exact.getBatch(batchId)

    assert(closedBatchGet.isComplete)
    assert(closedBatchGet.isClosed)
  }

  it should "create and ack exact batches with a user key" in {
    val batchId = client.exact.createBatch(userKey = Some("foo")).batchId

    val items = client.exact.addItemsToBatch(batchId, 1000)

    val currentBatchGet = client.exact.getBatch(batchId)

    assert(!currentBatchGet.isComplete)
    assert(!currentBatchGet.isClosed)

    assert(!client.exact.closeBatch(batchId).isComplete)

    // assert its complete
    assert(client.exact.ackItems(items).isComplete(batchId))

    val closedBatchGet = client.exact.getBatch(batchId)


    assert(closedBatchGet.userKey.contains("foo"))
    assert(closedBatchGet.isComplete)
    assert(closedBatchGet.isClosed)
  }

  it should "create and ack estimated batches" in {
    val batchSize = 5
    val batchId = client.estimated.createBatch(batchSize).batchId

    (1 to batchSize).foreach(i => {
      client.estimated.decrement(batchId) shouldEqual false
    })

    client.estimated.getBatch(batchId).isComplete shouldEqual false
    client.estimated.getBatch(batchId).isClosed shouldEqual false

    client.estimated.closeBatch(batchId).isComplete shouldEqual true
  }

  it should "create and ack estimated batches with a user key" in {
    val batchSize = 5
    val batchId = client.estimated.createBatch(batchSize, userKey = Some("foo")).batchId

    (1 to batchSize).foreach(i => {
      client.estimated.decrement(batchId) shouldEqual false
    })

    val result = client.estimated.getBatch(batchId)

    client.estimated.getBatch(batchId).isComplete shouldEqual false
    client.estimated.getBatch(batchId).isClosed shouldEqual false

    client.estimated.closeBatch(batchId).isComplete shouldEqual true

    result.userKey shouldEqual Some("foo")
  }
}

