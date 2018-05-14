package io.paradoxical.carlyle.core.modules

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import io.paradoxical.carlyle.core.api.modules.{BigIntH2Provider, BigIntMysqlProvider, BigIntProvider}
import io.paradoxical.carlyle.core.config.{ReloadableConfig, ServiceConfig}
import io.paradoxical.common.types.NotTypeImplicits
import io.paradoxical.rdb.config.{BasicRdbConfig, RdbCredentials}
import io.paradoxical.rdb.slick.providers.custom.{H2DBProvider, ManualSourceProviders}
import io.paradoxical.rdb.slick.test.MysqlDocker
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import scala.util.Random

class MysqlModule(docker: MysqlDocker, dbName: String) extends TwitterModule with NotTypeImplicits {
  @Provides
  @Singleton
  def dataSource(config: ReloadableConfig[ServiceConfig]): DataSource = {
    ManualSourceProviders.withConfig[com.mysql.jdbc.Driver](
      BasicRdbConfig(
        docker.jdbc(dbName),
        RdbCredentials("root", ""),
        None
      )
    )
  }

  @Provides
  @Singleton
  def provider(dataSource: DataSource)(implicit executionContext: ExecutionContext): BigIntProvider = {
    new BigIntMysqlProvider(dataSource)
  }

  @Provides
  def driver(slickDBProvider: BigIntProvider) = slickDBProvider.driver
}

class H2Module extends TwitterModule {
  private val rand = new Random()

  @Provides
  @Singleton
  def dataSource: DataSource = {
    H2DBProvider.getInMemoryDataSource(
      rand.nextInt(1000).toString,
      H2DBProvider.DEFAULT_SETTINGS ++ Map("MVCC" -> "TRUE", "LOCK_TIMEOUT" -> "5000"))
  }

  @Provides
  @Singleton
  def provider(dataSource: DataSource): BigIntProvider = {
    new BigIntH2Provider(dataSource)
  }

  @Provides
  def driver(slickDBProvider: BigIntProvider) = slickDBProvider.driver
}
