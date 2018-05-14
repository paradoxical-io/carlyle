package io.paradoxical.carlyle.core.modules

import com.google.inject.Module
import io.paradoxical.carlyle.core.api.modules.{DbModule, EstimatedBatchCountModule, Modules}
import io.paradoxical.carlyle.core.guice.GuiceModule
import io.paradoxical.carlyle.core.guice.GuiceUtil._
import io.paradoxical.carlyle.core.redis.RedisClientBase
import io.paradoxical.rdb.slick.test.MysqlDocker
import org.scalatest.mockito.MockitoSugar
import scala.concurrent.ExecutionContext.Implicits.global

object TestModules {
  def apply() = Modules().withTestConfig()

  implicit class RichTestModules(m: List[Module]) extends MockitoSugar {
    def withTestConfig() = {
      m
    }

    def withH2() = {
      // h2 should use the lower bit rate for testing
      m.overrideModule[DbModule](new H2Module)
    }

    def withRedis(redisClient: RedisClientBase) = {
      m.overlay[EstimatedBatchCountModule](new GuiceModule {
        instance[RedisClientBase](redisClient)
      })
    }

    def withMockRedis() = {
      m.module[EstimatedBatchCountModule].nowGivesInstance(mock[RedisClientBase])
    }

    def withMysql(docker: MysqlDocker, dbName: String) = {
      m.overrideModule[DbModule](new MysqlModule(docker, dbName))
    }
  }
}
