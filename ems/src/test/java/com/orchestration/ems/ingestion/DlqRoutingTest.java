package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.orchestration.ems.config.KafkaConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * EmbeddedKafka proof of the Batch-G error taxonomy (Amendment A1: DLQ is poison-only), with the DB write
 * mocked ({@link DlqRecorder}) and the real {@link KafkaConfig} error handler wired (runs locally — real
 * PostgreSQL DLQ assertions are Batch I's {@code PoisonDlqIT}):
 * <ul>
 *   <li><b>Poison</b> ({@link IllegalArgumentException}) ⇒ published to {@code <topic>.ems.dlq}, a
 *       {@code dlq_record} written, and the partition <em>not</em> stalled (a following good record is
 *       processed).</li>
 *   <li><b>Transient</b> ({@link EdfUnavailableException}) ⇒ record redelivered (offset uncommitted, the
 *       partition parks) and <em>nothing</em> reaches the DLQ.</li>
 * </ul>
 *
 * <p><b>Isolation.</b> The two cases use <em>separate</em> source+DLQ topics (independent partitions and
 * independent DLQ assertions), and the transient case is ordered <b>last</b>: it parks its record — and its
 * consumer thread — forever, so running it after the poison case guarantees it can neither starve nor
 * pollute the other. No {@code @DirtiesContext} rebuild is needed (single fast broker).
 */
@SpringJUnitConfig(DlqRoutingTest.Config.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestPropertySource(properties = {
        "ems.consumer.enabled=true",
        "ems.consumer.topics=" + DlqRoutingTest.POISON_TOPIC + "," + DlqRoutingTest.TRANSIENT_TOPIC,
        "ems.consumer.group-id=ems-dlq-test",
        "ems.consumer.park-backoff-ms=200"
})
@EmbeddedKafka(partitions = 1,
        topics = {
                DlqRoutingTest.POISON_TOPIC, DlqRoutingTest.POISON_DLQ,
                DlqRoutingTest.TRANSIENT_TOPIC, DlqRoutingTest.TRANSIENT_DLQ },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class DlqRoutingTest {

    static final String POISON_TOPIC = "edf.events.dlq.poison";
    static final String POISON_DLQ = POISON_TOPIC + ".ems.dlq";
    static final String TRANSIENT_TOPIC = "edf.events.dlq.transient";
    static final String TRANSIENT_DLQ = TRANSIENT_TOPIC + ".ems.dlq";

    @Autowired
    private IngestionService ingestionService; // mock
    @Autowired
    private DlqRecorder dlqRecorder;            // mock
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private EmbeddedKafkaBroker broker;
    @Autowired
    private MeterRegistry meters;

    private KafkaTemplate<String, String> producer;

    @BeforeEach
    void setUp() {
        reset(ingestionService, dlqRecorder); // shared beans across ordered methods — clear prior interactions
        when(ingestionService.process(anyString())).thenReturn(IngestOutcome.PERSISTED);
        producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        // one listener container subscribed to both source topics ⇒ two assigned partitions
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 2 * broker.getPartitionsPerTopic());
        }
    }

    @Test
    @Order(1)
    void poison_isDeadLettered_recorded_partitionNotStalled() {
        String poison = "{ this is not valid json";
        doThrow(new IllegalArgumentException("event payload is not valid JSON"))
                .when(ingestionService).process(poison);

        producer.send(POISON_TOPIC, poison);

        // the poison payload lands, byte-verbatim, on the DLQ
        try (Consumer<String, String> dlq = dlqVerifier()) {
            broker.consumeFromAnEmbeddedTopic(dlq, POISON_DLQ);
            ConsumerRecord<String, String> dead =
                    KafkaTestUtils.getSingleRecord(dlq, POISON_DLQ, Duration.ofSeconds(10));
            assertThat(dead.value()).isEqualTo(poison);
        }
        // best-effort triage row written
        verify(dlqRecorder, timeout(5_000)).record(any(), any());

        // §10: the poison outcome is the one ems_events_consumed_total value the pipeline cannot report,
        // because process() threw before returning one — the recoverer owns it.
        assertThat(poisonCount(POISON_TOPIC)).isEqualTo(1.0);

        // partition committed past the poison ⇒ a subsequent good record is processed (not stalled)
        String good = "{\"id\":\"evt-after-poison\"}";
        producer.send(POISON_TOPIC, good);
        verify(ingestionService, timeout(5_000)).process(eq(good));
    }

    @Test
    @Order(2)
    void transient_isParkedAndRedelivered_nothingDeadLettered() {
        String payload = "{\"id\":\"evt-transient\"}";
        doThrow(new EdfUnavailableException("EDF returned status 503"))
                .when(ingestionService).process(payload);

        producer.send(TRANSIENT_TOPIC, payload);

        // unbounded park backoff ⇒ the same record is redelivered (offset never committed)
        verify(ingestionService, timeout(5_000).atLeast(2)).process(eq(payload));

        // nothing dead-lettered: no triage row, no DLQ message, and — the point of A1 — no poison count
        verify(dlqRecorder, after(500).never()).record(any(), any());
        assertThat(poisonCount(TRANSIENT_TOPIC)).isZero();
        try (Consumer<String, String> dlq = dlqVerifier()) {
            broker.consumeFromAnEmbeddedTopic(dlq, TRANSIENT_DLQ);
            ConsumerRecords<String, String> none = KafkaTestUtils.getRecords(dlq, Duration.ofMillis(500));
            assertThat(none.count()).isZero();
        }
    }

    /** {@code ems_events_consumed_total{topic,outcome="poison"}} for one topic, 0 when the series is absent. */
    private double poisonCount(String topic) {
        var counter = meters.find(IngestOutcome.METRIC)
                .tag(IngestOutcome.TAG_TOPIC, topic)
                .tag(IngestOutcome.TAG_OUTCOME, IngestOutcome.POISON)
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    /** A throwaway earliest-reading consumer for asserting DLQ contents. */
    private Consumer<String, String> dlqVerifier() {
        var props = KafkaTestUtils.consumerProps("dlq-verifier-" + UUID.randomUUID(), "true", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
    }

    @Configuration
    @Import(KafkaConfig.class)
    static class Config {

        @Bean
        IngestionService ingestionService() {
            return mock(IngestionService.class);
        }

        @Bean
        DlqRecorder dlqRecorder() {
            return mock(DlqRecorder.class);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        EventConsumer eventConsumer(IngestionService ingestionService, MeterRegistry meterRegistry) {
            return new EventConsumer(ingestionService, meterRegistry);
        }
    }
}
