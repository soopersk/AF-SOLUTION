package com.orchestration.ems.recon;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The real-broker proof of {@link ConsumerLagProbe}: {@code ems_consumer_lag} and
 * {@code ems_consumer_retention_headroom_records} are read from a genuine consumer group's committed
 * offsets, not from a mock's idea of the {@code AdminClient} API. Auto-skips locally
 * ({@code disabledWithoutDocker}); runs in CI.
 *
 * <p>What this catches that {@code ConsumerLagProbeTest} cannot: whether {@code listConsumerGroupOffsets}
 * and {@code listOffsets} actually key their responses by the same {@link TopicPartition} instances the
 * probe asks with, and whether a group that has committed part-way through a topic really does report
 * {@code end − committed} records of lag. Both would compile and pass against mocks while returning
 * nothing at all in production.
 *
 * <p>No Spring context: the probe is a plain object over an {@link Admin} client, and standing up a
 * container-plus-context here would test the wiring, not the offsets.
 */
@Testcontainers(disabledWithoutDocker = true)
class ConsumerLagProbeIT {

    private static final String TOPIC = "edf.events.lag.it";
    private static final String GROUP = "ems-lag-probe-it";
    private static final int PRODUCED = 25;
    private static final int CONSUMED = 10;

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private static Admin admin;

    @BeforeAll
    static void createTopic() throws Exception {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
        // One partition so the arithmetic is unambiguous — partition fan-out is ConsumerLagProbeTest's job.
        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
    }

    @AfterAll
    static void closeAdmin() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(5));
        }
    }

    @Test
    void lagAndHeadroomAreReadFromARealConsumerGroupsCommittedOffsets() {
        produce(PRODUCED);
        commitAfterConsuming(CONSUMED);

        ConsumerLagProbe probe = new ConsumerLagProbe(admin, GROUP, Duration.ofSeconds(10));

        assertThat(probe.probe()).containsExactly(
                // 25 produced, 10 consumed ⇒ 15 behind the head; 10 ahead of a log start still at 0
                new PartitionLag(TOPIC, 0, PRODUCED - CONSUMED, CONSUMED));
    }

    /** A group with no committed offsets at all publishes no series — there is no honest number yet. */
    @Test
    void anUnknownGroupPublishesNoSeries() {
        ConsumerLagProbe probe = new ConsumerLagProbe(admin, "group-that-never-ran", Duration.ofSeconds(10));

        assertThat(probe.probe()).isEmpty();
    }

    private static void produce(int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "k" + i, "{\"n\":" + i + "}"));
            }
            producer.flush();
        }
    }

    /**
     * Commits exactly {@code count} records' worth of progress. The offset is set explicitly rather than
     * by polling-until-count so the assertion is exact and the test cannot flake on a short poll.
     */
    private static void commitAfterConsuming(int count) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(10));
            assertThat(polled.count()).isGreaterThanOrEqualTo(count); // the fixture itself must be sound
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(count)));
        }
    }
}
