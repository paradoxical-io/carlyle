package io.paradoxical.carlyle.core

import io.paradoxical.carlyle.core.db.{Db, Migrator}
import io.paradoxical.carlyle.core.model.BatchItemId
import io.paradoxical.carlyle.core.modules.TestModules
import io.paradoxical.rdb.slick.test.Mysql
import org.scalatest.{BeforeAndAfterAll, Ignore}
import scala.concurrent.Future
import scala.util.{Random, Try}
import io.paradoxical.carlyle.core.guice.GuiceUtil._
import scala.concurrent.ExecutionContext.Implicits.global
import io.paradoxical.common.extensions.Extensions._
import TestModules._

@Ignore
class MysqlTests extends DbTests with BeforeAndAfterAll {
  lazy val mysql = Mysql.docker("5.7")

  val random = new Random()

  override protected def afterAll(): Unit = {
    Try(mysql.close())
  }

  "Mysql" should "prevent thread stomping from multiple acks" in withDb { db =>
    val batchId = db.createBatch().waitForResult()

    val max = 10000

    val batchItemIds = BatchItemId.generate(batchId, db.insertQueueBatchItems(batchId, max).waitForResult())

    db.closeBatch(batchId).waitForResult()

    Future.sequence(batchItemIds.grouped(5).map(group => db.ackBatch(group.toSet))).waitForResult()

    val batch = db.getBatch(batchId).waitForResult()

    assert(batch.get.isClosed)
  }

  override def withDb(testCode: Db => Any): Unit = {
    val dbName = "testDb" + random.nextInt(1000)

    mysql.createDatabase(dbName)

    val injector = TestModules().withMysql(mysql, dbName).withMockRedis().injector()

    injector.make[Migrator].migrate()

    try {
      testCode(injector.make[Db])
    } finally {
      mysql.dropDatabase(dbName)
    }
  }
}
