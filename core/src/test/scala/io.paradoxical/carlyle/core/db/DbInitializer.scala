package io.paradoxical.carlyle.core.db

import io.paradoxical.carlyle.core.api.modules.BigIntProvider
import javax.inject.Inject
import io.paradoxical.common.extensions.Extensions._
import scala.concurrent.ExecutionContext

class DbInitializer @Inject()(
  provider: BigIntProvider,
  queuebatchTable: BatchItemsQuery,
  batchQuery: BatchQuery

)(implicit executionContext: ExecutionContext) {
  def init(): Unit = {
    List(queuebatchTable, batchQuery).foreach { table =>
      provider.withDB(table.create).waitForResult()
    }
  }
}
