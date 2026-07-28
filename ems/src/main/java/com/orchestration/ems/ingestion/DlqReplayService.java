package com.orchestration.ems.ingestion;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.orchestration.ems.model.DlqReplay;
import com.orchestration.ems.model.DlqReplay.Reason;
import com.orchestration.ems.model.DlqReplay.Skipped;

/**
 * The {@code POST /admin/replay} engine (ems-design §4.5:220, §12 poison drill): re-publish selected
 * dead-lettered messages to the topic they came from, and stamp the audit columns V5 provides
 * ({@code dlq_record.replayed_at} / {@code replayed_by}).
 *
 * <p><b>Where the payload comes from.</b> {@code dlq_record} stores triage keys, not the message — the
 * bytes live on Kafka. The row records the <em>source</em> topic/partition/offset of the original record
 * ({@link DlqRecorder} is handed the pre-DLT record), so this service seeks that exact coordinate on the
 * source topic and re-sends what it finds, key included, back to the same topic. The alternative — reading
 * {@code <topic>.ems.dlq} and matching {@code kafka_dlt-original-*} headers — needs a scan of the DLT
 * because those coordinates are not stored, for the same bytes and the same retention exposure.
 *
 * <p><b>Replay heals only because the code changed.</b> The message is re-published verbatim, so the
 * operator's fix (a corrected subscription, a deployed parser fix) is what makes the second attempt
 * succeed; the endpoint deliberately offers no payload editing. Everything downstream is idempotent
 * ({@code ux_event_id}, {@code ux_rd_l0}, the outbox's deterministic {@code dag_run_id}), so a replay
 * that turns out to be unnecessary costs nothing (§4.5:220).
 *
 * <p><b>Order of operations: publish, then stamp.</b> Stamping first would let a failed publish leave a
 * row that claims to have been replayed and can never be replayed again — the strictly worse failure. The
 * reverse (published, then the stamp fails) surfaces as a 5xx over a message that is already on the topic:
 * the operator may replay it again, which is safe.
 */
@Service
public class DlqReplayService {

    private static final Logger log = LoggerFactory.getLogger(DlqReplayService.class);

    private static final String SELECT = """
            SELECT id, topic, kafka_partition, kafka_offset, (replayed_at IS NOT NULL) AS replayed
            FROM dlq_record
            WHERE id IN (:ids)
            """;

    private static final String STAMP = """
            UPDATE dlq_record SET replayed_at = now(), replayed_by = ? WHERE id = ?
            """;

    /** Consumer group for the seek/read. Never commits (the consumer is {@code assign}ed, not subscribed). */
    private static final String REPLAY_GROUP = "ems-admin-replay";

    private static final Duration METADATA_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final long PUBLISH_TIMEOUT_MS = 10_000L;

    private final JdbcClient jdbc;
    private final ConsumerFactory<String, String> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public DlqReplayService(JdbcClient jdbc, ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.jdbc = jdbc;
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Re-publish the named {@code dlq_record} rows, stamping each one that is actually re-sent.
     *
     * <p>Ids are de-duplicated first: within a single invocation the same id twice would otherwise publish
     * twice, because the {@code ALREADY_REPLAYED} guard reads the rows once up front.
     *
     * @param ids        the selected {@code dlq_record} ids (may be empty)
     * @param replayedBy the operator identity to audit
     * @return the per-invocation outcome, with a {@link Reason} for every id that was not replayed
     */
    public DlqReplay.Result replay(List<Long> ids, String replayedBy) {
        List<Long> requested = ids.stream().distinct().toList();
        if (requested.isEmpty()) {
            return new DlqReplay.Result(0, 0, List.of());
        }

        Map<Long, DlqRow> rows = load(requested);
        List<Skipped> skipped = new ArrayList<>();
        int replayed = 0;

        try (Consumer<String, String> consumer =
                consumerFactory.createConsumer(REPLAY_GROUP, "replay-" + UUID.randomUUID())) {
            for (Long id : requested) {
                Optional<Reason> failure = replayOne(consumer, rows.get(id), id, replayedBy);
                if (failure.isPresent()) {
                    skipped.add(new Skipped(id, failure.get()));
                } else {
                    replayed++;
                }
            }
        }

        log.info("admin replay by {}: {} requested, {} re-published, {} skipped",
                replayedBy, requested.size(), replayed, skipped.size());
        return new DlqReplay.Result(requested.size(), replayed, List.copyOf(skipped));
    }

    /** @return empty when the message was re-published and stamped, else why it was not */
    private Optional<Reason> replayOne(Consumer<String, String> consumer, DlqRow row, long id,
            String replayedBy) {
        if (row == null) {
            return Optional.of(Reason.NOT_FOUND);
        }
        if (row.replayed()) {
            return Optional.of(Reason.ALREADY_REPLAYED);
        }
        Optional<ConsumerRecord<String, String>> original = fetch(consumer, row);
        if (original.isEmpty()) {
            return Optional.of(Reason.PAYLOAD_UNAVAILABLE);
        }
        if (!publish(row.topic(), original.get())) {
            return Optional.of(Reason.PUBLISH_FAILED);
        }
        jdbc.sql(STAMP).param(replayedBy).param(id).update();
        return Optional.empty();
    }

    private Map<Long, DlqRow> load(List<Long> ids) {
        Map<Long, DlqRow> byId = new HashMap<>();
        jdbc.sql(SELECT)
                .param("ids", ids)
                .query((rs, n) -> new DlqRow(rs.getLong("id"), rs.getString("topic"),
                        rs.getInt("kafka_partition"), rs.getLong("kafka_offset"), rs.getBoolean("replayed")))
                .list()
                .forEach(row -> byId.put(row.id(), row));
        return byId;
    }

    /**
     * Read the original record at its recorded coordinate.
     *
     * <p>The offset is range-checked against the partition's beginning/end before seeking: a coordinate the
     * retention window has passed would otherwise trip {@code auto.offset.reset} and hand back an unrelated
     * record. The identity check on the polled offset is the belt-and-braces for a compacted topic, where a
     * present-but-collapsed offset is skipped over rather than being out of range.
     */
    private Optional<ConsumerRecord<String, String>> fetch(Consumer<String, String> consumer, DlqRow row) {
        TopicPartition partition = new TopicPartition(row.topic(), row.partition());
        try {
            consumer.assign(List.of(partition));
            long earliest = offsetOf(consumer.beginningOffsets(List.of(partition), METADATA_TIMEOUT), partition);
            long tail = offsetOf(consumer.endOffsets(List.of(partition), METADATA_TIMEOUT), partition);
            if (row.offset() < earliest || row.offset() >= tail) {
                log.warn("dlq_record {} points at {}-{}@{}, outside the retained range [{}, {})",
                        row.id(), row.topic(), row.partition(), row.offset(), earliest, tail);
                return Optional.empty();
            }

            consumer.seek(partition, row.offset());
            long deadline = System.currentTimeMillis() + FETCH_TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(POLL_INTERVAL);
                for (ConsumerRecord<String, String> candidate : polled.records(partition)) {
                    if (candidate.offset() == row.offset()) {
                        return Optional.of(candidate);
                    }
                    if (candidate.offset() > row.offset()) {
                        return Optional.empty(); // the offset itself is gone (compaction)
                    }
                }
            }
            return Optional.empty();
        } catch (KafkaException brokerProblem) {
            log.warn("could not read {}-{}@{} for dlq_record {}",
                    row.topic(), row.partition(), row.offset(), row.id(), brokerProblem);
            return Optional.empty();
        }
    }

    private static long offsetOf(Map<TopicPartition, Long> offsets, TopicPartition partition) {
        Long offset = offsets.get(partition);
        return offset == null ? 0L : offset;
    }

    /**
     * Re-send the original bytes (and key, so partitioning is preserved) to the source topic, waiting for
     * the broker acknowledgement — an unacknowledged send must not be audited as a replay.
     */
    private boolean publish(String topic, ConsumerRecord<String, String> original) {
        try {
            kafkaTemplate.send(topic, original.key(), original.value())
                    .get(PUBLISH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("interrupted while re-publishing to {}", topic, e);
            return false;
        } catch (ExecutionException | TimeoutException | KafkaException e) {
            log.warn("re-publish to {} was not acknowledged", topic, e);
            return false;
        }
    }

    /** The triage columns needed to locate and audit one dead-lettered message. */
    private record DlqRow(long id, String topic, int partition, long offset, boolean replayed) {
    }
}
