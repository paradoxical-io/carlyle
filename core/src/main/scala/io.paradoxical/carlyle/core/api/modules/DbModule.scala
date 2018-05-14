package io.paradoxical.carlyle.core.api.modules

import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import io.paradoxical.carlyle.core.config.{ReloadableConfig, ServiceConfig}
import io.paradoxical.carlyle.core.db.BigIntegerTypesComponent
import io.paradoxical.common.types.NotTypeImplicits
import io.paradoxical.rdb.slick.providers.custom.{H2DBProvider, MySQLDBProvider}
import io.paradoxical.rdb.slick.providers.{DataSourceProviders, SlickDBProvider}
import javax.sql.DataSource
import scala.concurrent.ExecutionContext
import slick.jdbc.JdbcProfile

class DbModule extends TwitterModule with NotTypeImplicits {
  @Provides
  @Singleton
  def dataSource(config: ReloadableConfig[ServiceConfig]): DataSource = {
    DataSourceProviders.default(config.currentValue().db)
  }

  @Provides
  @Singleton
  def provider(dataSource: DataSource)(implicit executionContext: ExecutionContext): BigIntProvider = {
    new BigIntMysqlProvider(dataSource)
  }

  @Provides
  def driver(slickDBProvider: BigIntProvider): JdbcProfile with BigIntegerTypesComponent = slickDBProvider.driver
}

trait BigIntProvider extends SlickDBProvider {
  override val driver: JdbcProfile with BigIntegerTypesComponent
}

class BigIntMysqlProvider(dataSource: DataSource)(implicit executionContext: ExecutionContext) extends MySQLDBProvider(dataSource) with BigIntProvider {
  override val driver: JdbcProfile with BigIntegerTypesComponent = new slick.jdbc.MySQLProfile with BigIntegerTypesComponent
}

class BigIntH2Provider(dataSource: DataSource) extends H2DBProvider(dataSource) with BigIntProvider {
  override val driver: JdbcProfile with BigIntegerTypesComponent = new slick.jdbc.H2Profile with BigIntegerTypesComponent
}