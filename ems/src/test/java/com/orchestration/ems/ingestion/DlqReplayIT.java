package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.orchestration.ems.model.DlqReplay;
import com.orchestration.ems.model.DlqReplay.Reason;
import com.orchestration.ems.model.DlqReplay.Skipped;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * The {@code POST /admin/replay} drill (ems-design §4.5:220, §12 "replay via {@code POST /admin/replay}
 * heals end-to-end") over a real broker and a real PostgreSQL: a {@code dlq_record} row is re-driven by
 * seeking its recorded coordinate on the source topic and re-publishing the original bytes, then stamped
 * with {@code replayed_at}/{@code replayed_by}. Auto-skips locally (no Docker); runs in CI.
 *
 * <p>The three ways a selected row is <em>not</em> replayed are proven too, because each one leaves the
 * row re-replayable and is therefore an answer the operator must be able to trust.
 */
@SpringJUnitConfig(DlqReplayIT.Config.class)
@EmbeddedKafka(partitions = 1,
        topics = { DlqReplayIT.TOPIC, DlqReplayIT.DUP_TOPIC },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class DlqReplayIT extends AbstractPostgresIT {

    static final String TOPIC = "edf.events.replay.it";
    static final String DUP_TOPIC = "edf.events.replay.dup.it";

    private static final String PAYLOAD = "{\"id\":\"evt-poison-9\",\"taskId\":\"task-31\"}";
    private static final String OPERATOR = "ops@bank";

    @Autowired
    private EmbeddedKafkaBroker broker;

    private KafkaTemplate<String, String> producer;
    private DlqReplayService service;

    @BeforeEach
    void setUp() {
        jdbcClient().sql("TRUNCATE dlq_record").update();
        producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        service = new DlqReplayService(jdbcClient(), consumerFactory("ems-admin-replay-it"), producer);
    }

    @Test
    void replay_rePublishesTheOriginalMessage_andStampsTheAudit() throws Exception {
        SendResult<String, String> original = producer.send(TOPIC, "evt-poison-9", PAYLOAD)
                .get(10, TimeUnit.SECONDS);
        long id = recordDlqRow(TOPIC, original.getRecordMetadata().partition(),
                original.getRecordMetadata().offset());

        DlqReplay.Result result = service.replay(List.of(id), OPERATOR);

        assertThat(result).isEqualTo(new DlqReplay.Result(1, 1, List.of()));

        // the source topic now carries the message twice — the replay is byte-verbatim, key included
        List<ConsumerRecord<String, String>> records = drain(TOPIC, 2);
        assertThat(records).extracting(ConsumerRecord::value).containsExactly(PAYLOAD, PAYLOAD);
        assertThat(records.get(1).key()).isEqualTo("evt-poison-9"); // key preserved ⇒ same partition

        Stamp stamp = stampOf(id);
        assertThat(stamp.replayedBy()).isEqualTo(OPERATOR);
        assertThat(stamp.replayedAt()).isNotNull();
    }

    @Test
    void duplicateIdsInOneRequest_arePublishedOnce() throws Exception {
        SendResult<String, String> original = producer.send(DUP_TOPIC, "evt-dup", PAYLOAD)
                .get(10, TimeUnit.SECONDS);
        long id = recordDlqRow(DUP_TOPIC, original.getRecordMetadata().partition(),
                original.getRecordMetadata().offset());

        DlqReplay.Result result = service.replay(List.of(id, id, id), OPERATOR);

        assertThat(result).isEqualTo(new DlqReplay.Result(1, 1, List.of()));
        assertThat(drain(DUP_TOPIC, 2)).hasSize(2); // the original + exactly one replay
    }

    @Test
    void alreadyReplayedRow_isSkipped_keepingTheFirstAuditIntact() {
        long id = recordDlqRow(TOPIC, 0, 0L);
        jdbcClient().sql("UPDATE dlq_record SET replayed_at = now(), replayed_by = 'earlier-ops' WHERE id = ?")
                .param(id).update();

        DlqReplay.Result result = service.replay(List.of(id), OPERATOR);

        assertThat(result.replayed()).isZero();
        assertThat(result.skipped()).containsExactly(new Skipped(id, Reason.ALREADY_REPLAYED));
        assertThat(stampOf(id).replayedBy()).isEqualTo("earlier-ops"); // not overwritten
    }

    @Test
    void unknownId_isReportedNotFound() {
        DlqReplay.Result result = service.replay(List.of(9_999_999L), OPERATOR);

        assertThat(result).isEqualTo(new DlqReplay.Result(1, 0,
                List.of(new Skipped(9_999_999L, Reason.NOT_FOUND))));
    }

    @Test
    void offsetOutsideRetention_isPayloadUnavailable_andLeavesTheRowReplayable() {
        long id = recordDlqRow(TOPIC, 0, 9_999L); // beyond the log end — as if retention had passed it

        DlqReplay.Result result = service.replay(List.of(id), OPERATOR);

        assertThat(result.skipped()).containsExactly(new Skipped(id, Reason.PAYLOAD_UNAVAILABLE));
        assertThat(stampOf(id).replayedAt()).isNull(); // unstamped ⇒ still replayable if it reappears
    }

    @Test
    void emptySelection_isANoop() {
        assertThat(service.replay(List.of(), OPERATOR)).isEqualTo(new DlqReplay.Result(0, 0, List.of()));
    }

    // --- fixtures -----------------------------------------------------------------------------------

    /** Write the triage row DlqRecorder would have written for a poison record at that coordinate. */
    private static long recordDlqRow(String topic, int partition, long offset) {
        return jdbcClient().sql("""
                        INSERT INTO dlq_record (topic, kafka_partition, kafka_offset, event_id, error)
                        VALUES (?, ?, ?, 'evt-poison-9', 'IllegalArgumentException: contract violation')
                        RETURNING id""")
                .param(topic).param(partition).param(offset)
                .query(Long.class).single();
    }

    private record Stamp(Instant replayedAt, String replayedBy) {
    }

    private static Stamp stampOf(long id) {
        return jdbcClient().sql("SELECT replayed_at, replayed_by FROM dlq_record WHERE id = ?")
                .param(id)
                .query((rs, n) -> new Stamp(
                        rs.getTimestamp(1) == null ? null : rs.getTimestamp(1).toInstant(), rs.getString(2)))
                .single();
    }

    /** Read a topic from the beginning until {@code expected} records arrive (or the window closes). */
    private List<ConsumerRecord<String, String>> drain(String topic, int expected) {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (Consumer<String, String> verifier =
                consumerFactory("drain-" + UUID.randomUUID()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(verifier, topic);
            long deadline = System.currentTimeMillis() + 10_000L;
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                verifier.poll(Duration.ofMillis(200)).records(topic).forEach(collected::add);
            }
        }
        return collected;
    }

    private DefaultKafkaConsumerFactory<String, String> consumerFactory(String groupId) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(groupId, "false", broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer());
    }

    /** {@code @EmbeddedKafka} needs a Spring test context; the service under test is built by hand. */
    @Configuration
    static class Config {
    }
}
