package com.example.filter // Adjust package as needed

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.kafka.listener.adapter.RecordFilterStrategy
import org.springframework.stereotype.Component

import scala.jdk.CollectionConverters._

@Component
class EventFilter @Autowired() (
  mapper: ObjectMapper,
  @Value("${eventorchestration.filter.post}") postFilterDefinitions: String,
  @Value("${eventorchestration.filter.persist}") persistFilterDefinitions: String,
  @Value("${eventorchestration.post.filter.control.map}") postFilterControlMapJson: String
) extends LazyLogging {

  lazy val postFilterRuleset: List[JsonFilterRuleset] = createRuleset(postFilterDefinitions)

  lazy val postFilterRulesWithDagId: List[(JsonFilterRuleset, String)] =
    createRulesetFromControlMapWithDagId(postFilterControlMapJson)

  lazy val persistFilterRuleset: List[JsonFilterRuleset] = createRuleset(persistFilterDefinitions)

  def filter(event: String, ruleset: List[JsonFilterRuleset]): Boolean =
    ruleset.exists(rule => rule.filter(event))

  /** Returns dagIds for all matching POST rules (empty list if none match) */
  def filterPostWithDagIds(enrichedEvent: EnrichedEvent): List[String] = {
    val merged = mapper.valueToTree[ObjectNode](enrichedEvent.event)
    if (enrichedEvent.context != null) {
      val contextNode = mapper.valueToTree[ObjectNode](enrichedEvent.context)
      normalizeContextDataValues(contextNode)
      merged.set("context", contextNode)
    }

    val json = mapper.writeValueAsString(merged)
    val found = postFilterRulesWithDagId.collect {
      case (rule, dagId) if rule.filter(json) => dagId
    }

    logger.trace(s"Post filtering dagids result of [${enrichedEvent.event.getId}] = [$found]")
    found
  }

  // Backward-compatible overload used by existing unit tests and legacy call sites.
  def filterPostWithDagIds(event: EventResponse): List[String] =
    filterPostWithDagIds(EnrichedEvent(event, null))

  def filterPost(event: EventResponse): Boolean = {
    val filterPostResult = filter(mapper.writeValueAsString(event), postFilterRuleset)
    logger.trace(s"Post filtering result of [${event.getId}] = [$filterPostResult]")
    filterPostResult
  }

  def filterPersist(event: EventResponse): Boolean = {
    val filterPersistResult = filter(mapper.writeValueAsString(event), persistFilterRuleset)
    logger.trace(s"Persistence filtering result of [${event.getId}] = [$filterPersistResult]")
    filterPersistResult
  }

  @Bean
  def eventFilterStrategy: RecordFilterStrategy[Int, EventResponse] = record =>
    !(filterPersist(record.value()) || filterPost(record.value()))

  // Legacy
  private def createRuleset(filterDefinitions: String): List[JsonFilterRuleset] = {
    val ruleset = mapper.readValue(filterDefinitions, new TypeReference[List[JsonFilterRuleset]] {})
    logger.debug(s"Filter ruleset created: $ruleset")
    ruleset
  }

  /** New method for post_filter_control_dag_map */
  private def createRulesetFromControlMapWithDagId(definitionsJson: String): List[(JsonFilterRuleset, String)] = {
    val items: List[PostFilterControlDagMapItem] =
      mapper.readValue(definitionsJson, new TypeReference[List[PostFilterControlDagMapItem]] {})

    val active = items.filter(_.enabled)
    active.map { item =>
      val ruleMap = PostFilterControlDagMapItem.parseConditionString(item.filterCondition)
      (JsonFilterRuleset(ruleMap), item.dagId)
    }
  }

  private def normalizeContextDataValues(contextNode: ObjectNode): Unit = {
    val dataNode = contextNode.path("data")
    if (dataNode.isObject) {
      val dataObjectNode = dataNode.asInstanceOf[ObjectNode]
      dataObjectNode.fields().asScala.foreach { entry =>
        val key = entry.getKey
        val value = entry.getValue
        if (value != null && value.isObject && value.has("value")) {
          dataObjectNode.set(key, value.get("value"))
        }
      }
    }
  }
}

object EventFilter {
  def apply(
    postFilterControlMapJson: String,
    persistFilterDefinitions: String,
    postFilterDefinitions: String = ""
  ): EventFilter = {
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    new EventFilter(
      mapper = mapper,
      postFilterDefinitions = postFilterDefinitions,
      persistFilterDefinitions = persistFilterDefinitions,
      postFilterControlMapJson = postFilterControlMapJson
    )
  }
}