package io.paradoxical.carlyle.core.api

import com.google.inject.{Key, Module, TypeLiteral}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import io.paradoxical.carlyle.core.api.controllers.{EstimatedQueueBatchController, ExactQueueBatchController}
import io.paradoxical.carlyle.core.api.lifecycle.Startup
import io.paradoxical.carlyle.core.config.{ReloadableConfig, ServiceConfig}
import io.paradoxical.finatra.swagger.{ApiDocumentationConfig, SwaggerDocs}

class Server(override val modules: Seq[Module]) extends HttpServer with SwaggerDocs {


  override protected def configureHttp(router: HttpRouter): Unit = {
    router.
      filter[LoggingMDCFilter[Request, Response]].
      filter[TraceIdMDCFilter[Request, Response]].
      filter[CommonFilters]
    
    swaggerInfo

    configureDocumentation(router)

    router.
      add[EstimatedQueueBatchController]

    val config = injector.instance(Key.get(new TypeLiteral[ReloadableConfig[ServiceConfig]](){}))

    if(config.currentValue().cache.isDefined) {
      router.add[ExactQueueBatchController]
    }
  }

  protected override def postWarmup(): Unit = {
    val startup = injector.instance[Startup]

    startup.start()

    onExit {
      startup.stop()
    }

    super.postWarmup()
  }

  override def documentation: ApiDocumentationConfig = new ApiDocumentationConfig {
    override val description: String = "Carlyle Api"
    override val title: String = "Carlyle"
    override val version: String = "V1"
  }
}
