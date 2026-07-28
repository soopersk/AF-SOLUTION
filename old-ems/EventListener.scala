package com.example.listener // Adjust package as needed

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import com.fasterxml.jackson.databind.ObjectMapper
import scala.util.Try

@Component
class EventListener @Autowired() (
  eventFilter: EventFilter,
  eventSender: EventSender,
  repository: EventRepository,
  mappers: ObjectMapper
) extends LazyLogging {

  @KafkaListener(topics = Array("${tedf.topic}", "${tedf.merival.topic}"), filter = "eventFilterStrategy")
  def handleEvent(response: ConsumerRecord[Int, EventResponse], acknowledgment: Acknowledgment): Unit = {
    val event = response.value()
    logger.debug(s"received event = $event")

    val enrichedEvent = eventSender.enrichEvent(event)
    logger.debug(s"enriched event = $enrichedEvent")

    Try(repository.save(enrichedEvent)) match {
      case util.Success(_) => 
        acknowledgment.acknowledge()
      case util.Failure(ex) => 
        logger.error(s"Failed to save enriched event: $enrichedEvent", ex)
    }

    // Direct iteration avoids MatchError when dagIds is empty
    eventFilter.filterPostWithDagIds(enrichedEvent).foreach { dagId =>
      logger.debug(s"event recognized as Postable - posting to airflow with dagId = $dagId")
      eventSender.sendEvent(enrichedEvent, dagId)
    }
  }
}