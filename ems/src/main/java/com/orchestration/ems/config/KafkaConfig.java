package com.orchestration.ems.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import com.orchestration.ems.ingestion.DlqRecorder;

import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka consumer wiring for the ingest path (ems-design §4.2 error taxonomy, §10; Amendment A1). Gated by
 * {@code ems.consumer.enabled} so it only overrides Boot's Kafka auto-configuration where consuming is on.
 *
 * <p><b>The two-way classification (A1: DLQ is poison-only).</b> The listener never acks on failure
 * ({@link EventConsumer} has no try/catch); this {@link DefaultErrorHandler} decides the fate of the throw:
 * <ul>
 *   <li><b>Poison</b> — a payload that can never succeed: an {@link ErrorHandlingDeserializer}
 *       {@link DeserializationException} or the {@link IllegalArgumentException} thrown when the JSON /
 *       contract is invalid. These are <em>not retryable</em> ⇒ recovered immediately: verified-published
 *       to {@code <topic>.ems.dlq} and a {@code dlq_record} triage row written, then the offset is
 *       committed so the partition is <b>not stalled</b>.</li>
 *   <li><b>Transient</b> — a dependency is merely down: {@code EdfUnavailableException} (EDF 5xx, A1) or a
 *       PostgreSQL availability failure ({@code TransientDataAccessException},
 *       {@code DataAccessResourceFailureException}). Everything not explicitly poison is treated this way
 *       and retried with an <b>unbounded</b> fixed backoff ⇒ the partition <b>parks</b> until the
 *       dependency recovers. Nothing transient is ever dead-lettered.</li>
 * </ul>
 *
 * <p>The park backoff interval ({@code ems.consumer.park-backoff-ms}, default 5s) is kept well under
 * {@code max.poll.interval.ms} so an indefinitely-parked partition never trips a rebalance (§10).
 */
@Configuration
@EnableKafka
@ConditionalOnProperty(prefix = "ems.consumer", name = "enabled", havingValue = "true")
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    /** Suffix appended to the source topic for the poison dead-letter topic (A1). */
    static final String DLQ_SUFFIX = ".ems.dlq";

    /**
     * Consumer factory with an {@link ErrorHandlingDeserializer} wrapping the value {@link StringDeserializer}
     * (a deserialization failure becomes a recorded {@link DeserializationException} instead of killing the
     * container), manual commits, {@code read_committed} isolation (never see an outbox producer's aborted
     * writes), and {@code earliest} reset (§10 deliberate correction — do not silently skip a backlog).
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${ems.consumer.group-id:ems-ingest}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new StringDeserializer()));
    }

    /** DLT producer: {@code acks=all} so a dead-letter is durably replicated before the offset commits. */
    @Bean
    public ProducerFactory<String, String> dltProducerFactory(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new StringSerializer());
    }

    @Bean
    public KafkaTemplate<String, String> dltKafkaTemplate(ProducerFactory<String, String> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    /**
     * The poison-only error handler. The recoverer verified-publishes to the DLT ({@code failIfSendResultIsError}
     * ⇒ a failed publish throws, leaving the offset uncommitted so the record redelivers rather than being
     * silently lost — the "DLQ-publish-failure" drill), then writes the best-effort {@code dlq_record} row.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> dltKafkaTemplate,
            DlqRecorder dlqRecorder,
            @Value("${ems.consumer.park-backoff-ms:5000}") long parkBackoffMs) {

        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + DLQ_SUFFIX, -1)); // -1 ⇒ let Kafka choose
        publisher.setFailIfSendResultIsError(true);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            publisher.accept(record, ex);            // authoritative verified publish (may throw ⇒ redeliver)
            dlqRecorder.record(record, unwrap(ex));  // best-effort triage row (swallows its own failures)
            log.warn("Dead-lettered {}-{}@{} to {}{}", record.topic(), record.partition(), record.offset(),
                    record.topic(), DLQ_SUFFIX);
        }, new FixedBackOff(parkBackoffMs, FixedBackOff.UNLIMITED_ATTEMPTS));

        // Poison ⇒ recover immediately (no park). DeserializationException is already in the default
        // not-retryable set; IllegalArgumentException is the invalid-JSON/contract throw from EventRow.of.
        handler.addNotRetryableExceptions(IllegalArgumentException.class, DeserializationException.class);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    /** The container wraps a listener throw in {@code ListenerExecutionFailedException}; record the cause. */
    private static Exception unwrap(Exception ex) {
        Throwable cause = ex.getCause();
        return (ex instanceof org.springframework.kafka.listener.ListenerExecutionFailedException
                && cause instanceof Exception c) ? c : ex;
    }
}
