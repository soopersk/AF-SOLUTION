package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.config.KafkaConfig;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * The Batch-I poison → DLQ drill (Amendment A1: DLQ is poison-only; §13 exit gate) end-to-end over a real
 * PostgreSQL: the real {@link KafkaConfig} error handler and a real {@link DlqRecorder} run against the
 * container, with only {@link IngestionService} mocked to raise the poison classification. Auto-skips
 * locally ({@code disabledWithoutDocker}); runs in CI on the embedded broker + Testcontainers Postgres.
 *
 * <p>A poison record (valid JSON, but rejected by processing) is verified-published byte-verbatim to
 * {@code <topic>.ems.dlq}, a {@code dlq_record} triage row is written with the correlation keys extracted
 * from the payload, and the partition is <b>not stalled</b> — a following good record is processed.
 */
@SpringJUnitConfig(PoisonDlqIT.Config.class)
@TestPropertySource(properties = {
        "ems.consumer.enabled=true",
        "ems.consumer.topics=" + PoisonDlqIT.TOPIC,
        "ems.consumer.group-id=ems-poison-dlq-it",
        "ems.consumer.park-backoff-ms=200"
})
@EmbeddedKafka(partitions = 1,
        topics = { PoisonDlqIT.TOPIC, PoisonDlqIT.DLQ },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class PoisonDlqIT extends AbstractPostgresIT {

    static final String TOPIC = "edf.events.poison.it";
    static final String DLQ = TOPIC + ".ems.dlq";

    /** Valid JSON so DlqRecorder can extract correlation keys — but rejected by (mocked) processing. */
    private static final String POISON = "{\"id\":\"evt-poison-1\",\"taskId\":\"task-77\",\"contextId\":\"ctx-9\"}";

    @Autowired
    private IngestionService ingestionService; // mock
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private EmbeddedKafkaBroker broker;

    private KafkaTemplate<String, String> producer;

    @BeforeEach
    void setUp() {
        jdbcClient().sql("TRUNCATE dlq_record").update();
        producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
        }
    }

    @Test
    void poison_isDeadLettered_recordedWithCorrelations_partitionNotStalled() {
        doThrow(new IllegalArgumentException("event contract violation"))
                .when(ingestionService).process(POISON);

        producer.send(TOPIC, POISON);

        // 1) the poison payload lands byte-verbatim on the DLQ
        try (Consumer<String, String> dlq = dlqVerifier()) {
            broker.consumeFromAnEmbeddedTopic(dlq, DLQ);
            ConsumerRecord<String, String> dead = KafkaTestUtils.getSingleRecord(dlq, DLQ, Duration.ofSeconds(10));
            assertThat(dead.value()).isEqualTo(POISON);
        }

        // 2) a dlq_record triage row is written in the real DB, with correlation keys from the payload
        DlqRow row = await(() -> jdbcClient().sql("""
                        SELECT topic, event_id, task_id, context_id, error
                        FROM dlq_record WHERE event_id = 'evt-poison-1'""")
                .query((rs, n) -> new DlqRow(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5)))
                .optional());
        assertThat(row.topic()).isEqualTo(TOPIC);
        assertThat(row.taskId()).isEqualTo("task-77");
        assertThat(row.contextId()).isEqualTo("ctx-9");
        assertThat(row.error()).contains("IllegalArgumentException");

        // 3) the offset committed past the poison ⇒ a subsequent good record is processed (not stalled)
        String good = "{\"id\":\"evt-after-poison\"}";
        producer.send(TOPIC, good);
        verify(ingestionService, timeout(5_000)).process(eq(good));
    }

    /** Poll the DB until the best-effort triage row has been written (recoverer runs asynchronously). */
    private static <T> T await(java.util.function.Supplier<java.util.Optional<T>> query) {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            java.util.Optional<T> found = query.get();
            if (found.isPresent()) {
                return found.get();
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("dlq_record row was not written within the drill window");
    }

    /** A throwaway earliest-reading consumer for asserting DLQ contents. */
    private Consumer<String, String> dlqVerifier() {
        var props = KafkaTestUtils.consumerProps("dlq-verifier-" + UUID.randomUUID(), "true", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
    }

    private record DlqRow(String topic, String eventId, String taskId, String contextId, String error) { }

    @Configuration
    @Import(KafkaConfig.class)
    static class Config {

        @Bean
        IngestionService ingestionService() {
            return mock(IngestionService.class);
        }

        @Bean
        DlqRecorder dlqRecorder() {
            return new DlqRecorder(jdbcClient(), new ObjectMapper()); // real, over the Testcontainers Postgres
        }

        @Bean
        EventConsumer eventConsumer(IngestionService ingestionService) {
            return new EventConsumer(ingestionService);
        }
    }
}
