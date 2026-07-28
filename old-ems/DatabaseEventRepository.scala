package com.example.repository // Adjust package as needed

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import com.github.benmanes.caffeine.cache.AsyncCache
import com.typesafe.scalalogging.LazyLogging
import org.postgresql.util.PGobject
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.{JdbcTemplate, ResultSetExtractor, RowMapper}
import org.springframework.stereotype.Repository
import scalaflex.scaffeine.Scaffeine

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

@Repository
class DatabaseEventRepository @Autowired() (
  jdbcTemplate: JdbcTemplate,
  mapper: ObjectMapper with ClassTagExtensions
) extends CachedContextEventRepository[String, ContextResponse] with LazyLogging {

  private val INSERT_EVENT = "INSERT INTO event (event_id, json) VALUES (?,?) ON CONFLICT DO NOTHING"
  private val INSERT_CONTEXT = "INSERT INTO context (context_id, json) VALUES (?,?) ON CONFLICT DO NOTHING"
  private val SELECT_CONTEXT_BY_ID = "SELECT json FROM context WHERE context_id = ?"
  private val SELECT_EVENT_CONTEXT_BASE =
    "SELECT c.json AS context_json, e.json AS event_json FROM context c %s JOIN event e ON c.context_id=(e.json->>'contextId') %s"
  private val CONTEXT_ID_PARAMETER_TEMPLATE = "c.context_id = ?"
  private val EVENT_ID_PARAMETER_TEMPLATE = "e.event_id = ?"
  private val PARENT_ID_PARAMETER_TEMPLATE = "c.json->'parentIds' ?? ?"
  private val OTHER_PARAMETERS_TEMPLATE =
    "(e.json->>? IN (%s) or e.json->'additionalData'->>? IN (%s) or c.json->>? IN (%s) or c.json->'data'->>? IN (%s))"

  private val enrichedEventRowMapper: RowMapper[EnrichedEvent] = (rs, _) => {
    val event = Try(mapper.readValue[EventResponse](rs.getString("event_json"))).getOrElse(null)
    val context = Try(mapper.readValue[ContextResponse](rs.getString("context_json"))).getOrElse(null)
    EnrichedEvent(event, context)
  }

  private val EVICTION_CACHE_SIZE = 10000L
  private val asyncContextCache: AsyncCache[String, ContextResponse] = Scaffeine()
    .expireAfterWrite(24.hours)
    .maximumSize(EVICTION_CACHE_SIZE)
    .buildAsync[String, ContextResponse]()

  override def getContextCache(): AsyncCache[String, ContextResponse] = asyncContextCache

  implicit def extractor[T: ClassTag](colName: String): ResultSetExtractor[Option[T]] = rs =>
    Option.when(rs.next())(mapper.readValue[T](rs.getString(colName)))

  def toJson(from: Any): PGobject = {
    val json = new PGobject()
    json.setType("json")
    json.setValue(mapper.writeValueAsString(from))
    json
  }

  override def findEvents(
    eventId: Option[String],
    contextId: Option[String],
    parentId: Option[String]
  )(implicit dataMap: Map[String, Seq[String]]): Seq[EnrichedEvent] = {

    val joinType = (eventId, contextId) match {
      case (Some(_), Some(_)) => "INNER"
      case (Some(_), None)    => "RIGHT OUTER"
      case _                  => "LEFT OUTER"
    }

    val idTemplates = Seq(
      EVENT_ID_PARAMETER_TEMPLATE -> eventId,
      CONTEXT_ID_PARAMETER_TEMPLATE -> contextId,
      PARENT_ID_PARAMETER_TEMPLATE -> parentId
    ).collect { case (template, Some(param)) => (template, param) }

    val paramCount = OTHER_PARAMETERS_TEMPLATE.split("%s", -1).length - 1

    val dataMapTemplates = dataMap.values
      .map(_.map(_ => "?").mkString(","))
      .map(f => OTHER_PARAMETERS_TEMPLATE.format(List.fill(paramCount)(f): _*))

    val dataMapParams = dataMap.flatMap { case (k, v) =>
      List.fill(paramCount)(k +: v).flatten
    }

    val templates = idTemplates.map(_._1) ++ dataMapTemplates
    val whereClause = if (templates.isEmpty) "" else s"WHERE ${templates.mkString(" AND ")}"

    val queryParameters = idTemplates.map(_._2) ++ dataMapParams

    val enrichedEvents = jdbcTemplate.query(
      SELECT_EVENT_CONTEXT_BASE.format(joinType, whereClause),
      enrichedEventRowMapper,
      queryParameters.toArray: _*
    ).asScala.toSeq

    logger.debug(
      s"Find events by (eventId=$eventId, contextId=$contextId, parentId=$parentId, dataMap=$dataMap) returned:\n$enrichedEvents"
    )
    enrichedEvents
  }

  override def findContextById(id: String): Option[ContextResponse] = {
    val cachedContextResponse = Option(asyncContextCache.getIfPresent(id))
    cachedContextResponse match {
      case Some(future) =>
        val context = Option(Await.result(future, 10.seconds))
        logger.debug(s"Found a cached context by id $id, returned $context")
        context
      case None =>
        val context = jdbcTemplate.query[Option[ContextResponse]](
          SELECT_CONTEXT_BY_ID,
          extractor[ContextResponse]("json"),
          id
        )
        logger.debug(s"Find context by id $id returned $context")
        context
    }
  }

  private def saveEvent(event: EventResponse): Unit = {
    val success = jdbcTemplate.update(INSERT_EVENT, event.getId, toJson(event)) == 1
    logger.debug(s"Event ${event.getId} inserted successfully: $success")
  }

  override def save(enrichedEvent: EnrichedEvent): Unit = {
    if (enrichedEvent.event != null) saveEvent(enrichedEvent.event)
    if (enrichedEvent.context != null) save(enrichedEvent.context)
  }

  override def save(context: ContextResponse): Unit = {
    val contextId = context.getId
    asyncContextCache.put(contextId, Future.successful(context))
    logger.debug(s"Context $contextId successfully added to a cache")

    val success = jdbcTemplate.update(INSERT_CONTEXT, contextId, toJson(context)) == 1
    logger.debug(s"Context $contextId inserted successfully: $success")
  }
}