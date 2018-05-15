package io.paradoxical.carlyle.core.db

import javax.inject.Inject
import javax.sql.DataSource
import org.flywaydb.core.Flyway

class Migrator @Inject()(dataSource: DataSource) {
  def migrate(): Unit = {
    val flyway: Flyway = new Flyway()
    flyway.setDataSource(dataSource)
    flyway.setLocations("classpath:/carlyle_db")
    flyway.migrate()
  }
}