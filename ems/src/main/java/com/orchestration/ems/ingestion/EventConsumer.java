package com.orchestration.ems.ingestion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The Kafka entry point of the ingestion pipeline (ems-design §4.2 step 1). Consumes EDF event records
 * and delegates each to {@link IngestionService#process(String)}, then acknowledges — <b>manual ack
 * only after durable processing</b> ({@code AckMode.MANUAL_IMMEDIATE}, wired by the Batch-G
 * {@code KafkaConfig}).
 *
 * <p><b>No try/catch by design.</b> If {@code process} throws — poison (invalid JSON / contract) or a
 * transient dependency outage ({@link EdfUnavailableException}, PG unavailability) — the exception
 * propagates <em>without</em> acking. The container's error handler then classifies it (Batch G): poison
 * → DLQ + ack; transient → unbounded backoff / park (Amendment A1). A crash between {@code process} and
 * {@code acknowledge()} simply redelivers, and every write is idempotent, so redelivery is a safe no-op.
 *
 * <p>Gated by {@code ems.consumer.enabled} (default off) so non-consuming roles (e.g. an API-only pod, or
 * the pre-cutover shadow phase) run without a live listener.
 *
 * <p><b>Why the {@code ems_events_consumed_total} counter lives here</b> and not in
 * {@link IngestionService}: this is the only layer that knows the source topic, and §10 requires the
 * counter to carry it. Counting happens after {@code process} returns and before the ack, so the only
 * records it misses are the ones that were not consumed — a poison record (counted by the error
 * handler's recoverer while dead-lettering) and a parked transient failure (counted by nobody, because
 * it will be redelivered).
 */
@Component
@ConditionalOnProperty(prefix = "ems.consumer", name = "enabled", havingValue = "true")
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final IngestionService ingestionService;
    private final MeterRegistry meterRegistry;

    public EventConsumer(IngestionService ingestionService, MeterRegistry meterRegistry) {
        this.ingestionService = ingestionService;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Process one record, then ack. The raw record value is the byte-verbatim payload EMS persists.
     *
     * @param record the consumed record (value = raw event JSON)
     * @param ack    manual acknowledgment — invoked only after {@code process} returns normally
     */
    @KafkaListener(
            topics = "#{'${ems.consumer.topics}'.split(',')}",
            groupId = "${ems.consumer.group-id:ems-ingest}")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (log.isTraceEnabled()) {
            log.trace("Consuming {}-{}@{}", record.topic(), record.partition(), record.offset());
        }
        IngestOutcome outcome = ingestionService.process(record.value());
        meterRegistry.counter(IngestOutcome.METRIC,
                IngestOutcome.TAG_TOPIC, record.topic(),
                IngestOutcome.TAG_OUTCOME, outcome.tagValue()).increment();
        ack.acknowledge();
    }
}
