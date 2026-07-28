package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
 * The Batch-I transient-outage park drill (Amendment A1: transient failures never dead-letter; §13 exit
 * gate) end-to-end over a real PostgreSQL. A dependency outage — modelled here as {@link IngestionService}
 * raising {@link EdfUnavailableException} — must <b>park</b> the partition (unbounded backoff, offset never
 * committed ⇒ the record is redelivered), dead-letter <b>nothing</b>, and let the record complete once the
 * dependency recovers. Auto-skips locally; runs in CI.
 *
 * <p>The mock throws the transient exception on the first two deliveries, then succeeds — proving the record
 * survives the outage and is processed after recovery, while the DLQ topic and the {@code dlq_record} table
 * both stay empty (the real {@link DlqRecorder} is wired precisely so its silence is a real assertion).
 */
@SpringJUnitConfig(TransientOutageIT.Config.class)
@TestPropertySource(properties = {
        "ems.consumer.enabled=true",
        "ems.consumer.topics=" + TransientOutageIT.TOPIC,
        "ems.consumer.group-id=ems-transient-it",
        "ems.consumer.park-backoff-ms=200"
})
@EmbeddedKafka(partitions = 1,
        topics = { TransientOutageIT.TOPIC, TransientOutageIT.DLQ },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class TransientOutageIT extends AbstractPostgresIT {

    static final String TOPIC = "edf.events.transient.it";
    static final String DLQ = TOPIC + ".ems.dlq";

    private static final String PAYLOAD = "{\"id\":\"evt-transient-1\"}";

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
    void transientOutage_parksAndRedelivers_nothingDeadLettered_processedAfterRecovery() {
        // down twice, then recovered: the same record is retried until it finally succeeds
        doThrow(new EdfUnavailableException("EDF returned status 503"))
                .doThrow(new EdfUnavailableException("EDF returned status 503"))
                .doNothing()
                .when(ingestionService).process(PAYLOAD);

        producer.send(TOPIC, PAYLOAD);

        // parked + redelivered (>= the two failures) and ultimately processed after recovery
        verify(ingestionService, timeout(10_000).atLeast(3)).process(PAYLOAD);

        // nothing transient is ever dead-lettered: empty DLQ topic and empty triage table
        try (Consumer<String, String> dlq = dlqVerifier()) {
            broker.consumeFromAnEmbeddedTopic(dlq, DLQ);
            ConsumerRecords<String, String> none = KafkaTestUtils.getRecords(dlq, Duration.ofMillis(500));
            assertThat(none.count()).isZero();
        }
        assertThat(jdbcClient().sql("SELECT count(*) FROM dlq_record").query(Integer.class).single()).isZero();
    }

    /** A throwaway earliest-reading consumer for asserting the DLQ stays empty. */
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
            return new DlqRecorder(jdbcClient(), new ObjectMapper()); // real, over the Testcontainers Postgres
        }

        @Bean
        EventConsumer eventConsumer(IngestionService ingestionService) {
            return new EventConsumer(ingestionService);
        }
    }
}
