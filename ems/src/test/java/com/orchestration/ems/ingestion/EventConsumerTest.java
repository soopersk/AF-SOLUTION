package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * EmbeddedKafka proof of {@link EventConsumer}'s manual-ack contract with a mocked
 * {@link IngestionService} (no DB — runs locally): a successful {@code process} acks (the record is not
 * redelivered), and a throwing {@code process} does <em>not</em> ack, so the container's error handler
 * redelivers the record (offset uncommitted). Real DLQ/park classification is Batch G.
 */
@SpringJUnitConfig(EventConsumerTest.Config.class)
@TestPropertySource(properties = {
        "ems.consumer.enabled=true",
        "ems.consumer.topics=" + EventConsumerTest.TOPIC,
        "ems.consumer.group-id=ems-consumer-test"
})
@EmbeddedKafka(partitions = 1, topics = EventConsumerTest.TOPIC)
class EventConsumerTest {

    static final String TOPIC = "edf.events.test";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private IngestionService ingestionService;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private MeterRegistry meters;

    @BeforeEach
    void waitForAssignment() {
        when(ingestionService.process(anyString())).thenReturn(IngestOutcome.PERSISTED);
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, broker.getPartitionsPerTopic());
        }
    }

    @Test
    void successfulProcess_isAcknowledged_recordNotRedelivered() {
        String payload = "{\"id\":\"evt-ok\",\"source\":\"MERIVAL\"}";

        kafkaTemplate.send(TOPIC, payload);

        // processed once; after the ack, the same record is not redelivered
        verify(ingestionService, timeout(5_000)).process(eq(payload));
        verify(ingestionService, after(1_500).times(1)).process(eq(payload));
    }

    @Test
    void throwingProcess_isNotAcknowledged_recordRedelivered() {
        String payload = "{\"id\":\"evt-boom\",\"source\":\"MERIVAL\"}";
        doThrow(new RuntimeException("processing failure")).when(ingestionService).process(eq(payload));
        double consumedBefore = totalConsumed();

        kafkaTemplate.send(TOPIC, payload);

        // no ack ⇒ the error handler seeks back and redelivers (offset uncommitted)
        verify(ingestionService, timeout(5_000).atLeast(2)).process(eq(payload));

        // ...and a record that was never consumed is never counted as consumed, on any retry (§10)
        assertThat(totalConsumed()).isEqualTo(consumedBefore);
    }

    /**
     * §10 {@code ems_events_consumed_total{topic,outcome}} — the pipeline's outcome becomes the tag, and
     * the topic tag comes from the record, which is the reason the counter lives in the consumer.
     */
    @Test
    void eachOutcomeIsCountedUnderItsOwnSeries_taggedWithTheSourceTopic() {
        when(ingestionService.process(eq("{\"id\":\"evt-drop\"}"))).thenReturn(IngestOutcome.DROPPED);
        when(ingestionService.process(eq("{\"id\":\"evt-dup\"}"))).thenReturn(IngestOutcome.DUPLICATE);

        kafkaTemplate.send(TOPIC, "{\"id\":\"evt-drop\"}");
        kafkaTemplate.send(TOPIC, "{\"id\":\"evt-dup\"}");
        kafkaTemplate.send(TOPIC, "{\"id\":\"evt-keep\"}");

        verify(ingestionService, timeout(5_000)).process(eq("{\"id\":\"evt-keep\"}"));
        assertThat(consumed(IngestOutcome.DROPPED)).isEqualTo(1.0);
        assertThat(consumed(IngestOutcome.DUPLICATE)).isEqualTo(1.0);
        assertThat(consumed(IngestOutcome.PERSISTED)).isGreaterThanOrEqualTo(1.0);
    }

    /** Every {@code ems_events_consumed_total} series for this topic, summed. */
    private double totalConsumed() {
        return meters.find(IngestOutcome.METRIC).tag(IngestOutcome.TAG_TOPIC, TOPIC).counters()
                .stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }

    private double consumed(IngestOutcome outcome) {
        var counter = meters.find(IngestOutcome.METRIC)
                .tag(IngestOutcome.TAG_TOPIC, TOPIC)
                .tag(IngestOutcome.TAG_OUTCOME, outcome.tagValue())
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    @Configuration
    @EnableKafka
    static class Config {

        @Bean
        IngestionService ingestionService() {
            return mock(IngestionService.class);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        EventConsumer eventConsumer(IngestionService ingestionService, MeterRegistry meterRegistry) {
            return new EventConsumer(ingestionService, meterRegistry);
        }

        @Bean
        ConsumerFactory<String, String> consumerFactory(EmbeddedKafkaBroker broker) {
            Map<String, Object> props = KafkaTestUtils.consumerProps("ems-consumer-test", "false", broker);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, String> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
            return factory;
        }

        @Bean
        ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker broker) {
            return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker),
                    new StringSerializer(), new StringSerializer());
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }
    }
}
