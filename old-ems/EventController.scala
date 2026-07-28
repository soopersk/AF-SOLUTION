package com.example.controller

import java.util.{Map => JavaMap}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation._
import org.springframework.web.bind.annotation.RequestMethod.GET
import org.springframework.context.ApplicationContext
import com.typesafe.scalalogging.LazyLogging
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.{ApiResponse, ApiResponses}
import scala.jdk.CollectionConverters._ // Use scala.collection.JavaConverters._ for Scala 2.12 and earlier

@RestController
@RequestMapping(produces = Array(APPLICATION_JSON_VALUE))
class EventController extends LazyLogging {

  @Autowired var repository: EventRepository = _
  @Autowired var sender: EventSender = _
  @Autowired var filter: EventFilter = _
  @Autowired var listener: EventListener = _
  @Autowired var context: ApplicationContext = _
  @Autowired var authorizationManager: AuthorizationManager = _

  @Value("${spring.profiles.active}") var profile: String = _

  @Operation(summary = "Get events by their properties", tags = Array("Events"))
  @ApiResponses(value = Array(
    new ApiResponse(responseCode = "200", description = "Events found", content = Array(
      new Content(schema = new Schema(implementation = classOf[EventResponse]))
    )),
    new ApiResponse(responseCode = "404", description = "Events not found")
  ))
  @RequestMapping(path = Array("/event"), method = Array(GET))
  def event(@RequestHeader(value = "Authorization", required = false) authorizationHeader: String,
            @RequestParam params: JavaMap[String, String]): ResponseEntity[Any] = authorizationManager.authorize(authorizationHeader) match {
    case Some(response) => response
    case _ if params.isEmpty => ResponseEntity.badRequest().build()
    case _ =>
      val allParams = params.asScala
      val eventId = allParams.remove("event_id")
      val contextId = allParams.remove("context_id")
      val parentId = allParams.remove("parent_id")
      val dataParams = allParams.view.mapValues(_.split('|').toSeq).toMap

      logger.debug(s"Received /event request with parameters: $dataParams")
      sender.fetchEvents(eventId, contextId, parentId, dataParams) match {
        case Some(events) => ResponseEntity.ok(events)
        case _            => ResponseEntity.notFound().build()
      }
  }

  @Operation(summary = "Get contexts by their properties", tags = Array("Contexts"))
  @ApiResponses(value = Array(
    new ApiResponse(responseCode = "200", description = "Contexts found", content = Array(
      new Content(schema = new Schema(implementation = classOf[ContextResponse]))
    )),
    new ApiResponse(responseCode = "404", description = "Contexts not found")
  ))
  @RequestMapping(path = Array("/context"), method = Array(GET))
  def context(@RequestHeader(value = "Authorization", required = false) authorizationHeader: String,
              @RequestParam(name = "context_id") id: String): ResponseEntity[Any] = authorizationManager.authorize(authorizationHeader) match {
    case Some(response) => response
    case _ if id.isEmpty => ResponseEntity.badRequest().build()
    case _ =>
      logger.debug(s"Received /context request: contextId=[$id]")
      sender.fetchContext(id) match {
        case Some(ctx) => ResponseEntity.ok(ctx)
        case _         => ResponseEntity.notFound().build()
      }
  }

  @RequestMapping(path = Array("/parentcontext"), method = Array(GET))
  def parentcontext(@RequestHeader(value = "Authorization", required = false) authorizationHeader: String,
                    @RequestParam params: JavaMap[String, String]): ResponseEntity[Any] =
    authorizationManager.authorize(authorizationHeader) match {
      case Some(response) => response
      case _ if params.isEmpty || !params.containsKey("initial_context_id") => ResponseEntity.badRequest().build()
      case _ =>
        val allParams = params.asScala
        val initialContextId = allParams.remove("initial_context_id").getOrElse("")
        val requestedParams = allParams.toMap
        
        logger.debug(s"Received /parentcontext request: initialContextId=[$initialContextId], params=[$allParams]")
        sender.fetchParentContext(initialContextId, requestedParams) match {
          case Some(ctx) => ResponseEntity.ok(ctx)
          case _         => ResponseEntity.notFound().build()
        }
    }

  @Operation(summary = "Get child contexts by their properties", tags = Array("Contexts"))
  @RequestMapping(path = Array("/childcontext"), method = Array(GET))
  def downstreamcontext(@RequestHeader(value = "Authorization", required = false) authorizationHeader: String,
                        @RequestParam params: JavaMap[String, String]): ResponseEntity[Any] = 
    authorizationManager.authorize(authorizationHeader) match {
      case Some(response) => response
      case _ if params.isEmpty || !params.containsKey("initial_context_id") => ResponseEntity.badRequest().build()
      case _ =>
        val allParams = params.asScala
        val initialContextId = allParams.remove("initial_context_id").getOrElse("")
        val limit = allParams.remove("limit").getOrElse("1")
        val requestedParams = allParams.view.toMap

        logger.debug(s"Received /childcontext request: initialContextId=[$initialContextId], params=[$allParams]")
        sender.fetchChildContext(initialContextId, requestedParams, limit) match {
          case Some(ctx) => ResponseEntity.ok(ctx)
          case _         => ResponseEntity.notFound().build()
        }
    }
}