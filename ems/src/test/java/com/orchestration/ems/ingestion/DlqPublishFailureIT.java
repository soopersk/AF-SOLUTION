package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * The Batch-I DLQ-publish-failure drill (ems-design §4.2/§10; §13 exit gate): the dead-letter publish is the
 * <b>authoritative</b> step — if it fails, the offset must NOT commit, so the poison record is redelivered
 * rather than silently lost, and the best-effort {@code dlq_record} row is never written (it runs only after
 * a successful publish). Here the DLT {@link KafkaTemplate} is deliberately pointed at a dead broker so every
 * verified publish fails. Auto-skips locally; runs in CI.
 *
 * <p>The error handler wiring mirrors the production {@code KafkaConfig} one-for-one (verified publish via
 * {@code failIfSendResultIsError(true)}, then the {@link DlqRecorder}, {@code IllegalArgumentException}
 * not-retryable); only the DLT bootstrap is swapped to inject the send failure. The real recoverer +
 * real {@link DlqRecorder} run against the Testcontainers Postgres so "nothing recorded" is a true assertion.
 */
@SpringJUnitConfig(DlqPublishFailureIT.Config.class)
@TestPropertySource(properties = {
        "ems.consumer.enabled=true",
        "ems.consumer.topics=" + DlqPublishFailureIT.TOPIC,
        "ems.consumer.group-id=ems-dlq-fail-it"
})
@EmbeddedKafka(partitions = 1,
        topics = DlqPublishFailureIT.TOPIC,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class DlqPublishFailureIT extends AbstractPostgresIT {

    static final String TOPIC = "edf.events.dlqfail.it";
    private static final String POISON = "{\"id\":\"evt-dlqfail-1\"}";

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
    void dltPublishFailure_offsetUncommitted_recordRedelivered_nothingRecorded() {
        doThrow(new IllegalArgumentException("event contract violation")).when(ingestionService).process(POISON);

        producer.send(TOPIC, POISON);

        // the verified DLT publish keeps failing ⇒ the offset never commits ⇒ the record is redelivered
        verify(ingestionService, timeout(15_000).atLeast(2)).process(POISON);

        // the best-effort triage row runs only after a successful publish, so it is never written
        assertThat(jdbcClient().sql("SELECT count(*) FROM dlq_record").query(Integer.class).single()).isZero();
    }

    @Configuration
    @EnableKafka
    static class Config {

        @Bean
        ConsumerFactory<String, String> consumerFactory(EmbeddedKafkaBroker broker) {
            Map<String, Object> props = KafkaTestUtils.consumerProps("ems-dlq-fail-it", "false", broker);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
        }

        /** A DLT producer pointed at a dead broker: every send fails fast (bounded max.block.ms). */
        @Bean
        KafkaTemplate<String, String> deadDltTemplate() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1"); // nothing listening
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
            ProducerFactory<String, String> pf =
                    new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new StringSerializer());
            return new KafkaTemplate<>(pf);
        }

        /**
         * The production error-handler wiring (verified publish, then best-effort record, IAE not-retryable),
         * reproduced with the dead DLT template so the publish throws and recovery never reaches the recorder.
         */
        @Bean
        DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> deadDltTemplate, DlqRecorder dlqRecorder) {
            DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(deadDltTemplate,
                    (record, ex) -> new TopicPartition(record.topic() + ".ems.dlq", -1));
            publisher.setFailIfSendResultIsError(true);
            publisher.setWaitForSendResultTimeout(Duration.ofSeconds(3));

            DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
                publisher.accept(record, ex);          // authoritative verified publish — THROWS (dead broker)
                dlqRecorder.record(record, ex);         // unreachable: only runs after a successful publish
            }, new FixedBackOff(200, FixedBackOff.UNLIMITED_ATTEMPTS));
            handler.addNotRetryableExceptions(IllegalArgumentException.class);
            return handler;
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler kafkaErrorHandler) {
            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
            factory.setCommonErrorHandler(kafkaErrorHandler);
            return factory;
        }

        @Bean
        DlqRecorder dlqRecorder() {
            return new DlqRecorder(jdbcClient(), new ObjectMapper()); // real, over the Testcontainers Postgres
        }

        @Bean
        IngestionService ingestionService() {
            return mock(IngestionService.class);
        }

        @Bean
        EventConsumer eventConsumer(IngestionService ingestionService) {
            return new EventConsumer(ingestionService);
        }
    }
}
