package com.orchestration.ems.recon;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

/**
 * Reads the ingest consumer group's position straight from the broker, for
 * {@code ems_consumer_lag{topic,partition}} and
 * {@code ems_consumer_retention_headroom_records{topic,partition}} (§10).
 *
 * <p><b>Why an {@link Admin} client and not the consumer's own metrics.</b> Spring Kafka's client-side
 * {@code records-lag} only exists while a consumer is assigned and polling — exactly the state that fails
 * when a partition parks or a pod dies. Committed offsets are broker-side state, so this poll answers
 * "where is the group?" even from a pod that consumes nothing at all ({@code ems.consumer.enabled=false}),
 * which is the whole point of the {@link ReconciliationSweep} being independent of the ingest path. The
 * group id is read from the same {@code ems.consumer.group-id} property the listener uses, so the probe
 * can never end up watching a different group than the one EMS commits to.
 *
 * <p><b>Failure is empty, never an exception and never a stale number.</b> Every broker call is bounded by
 * {@code ems.recon.kafka.timeout-ms} and any failure — unreachable broker, timeout, authorization —
 * logs at WARN and yields an empty list, which clears both series rather than freezing them. That is a
 * deliberate difference from the sweep's SQL gauges, which keep their last-known value: a frozen
 * <em>lag</em> is actively misleading, because it stops rising at the moment lag actually starts to matter
 * and a {@code max(ems_consumer_lag) > N} rule would then never fire. An absent series is honest and
 * detectable ({@code absent()}); a stale one is a silent alert.
 *
 * <p>The two {@code listOffsets} requests are issued before either is awaited, so a tick costs at most two
 * timeouts, not three.
 */
@Component
@ConditionalOnProperty(prefix = "ems.recon.kafka", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class ConsumerLagProbe {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagProbe.class);

    private final Admin admin;
    private final String groupId;
    private final long timeoutMs;
    /** True only when this bean built the client and therefore owns closing it. */
    private final boolean ownsAdmin;

    @Autowired
    public ConsumerLagProbe(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            @Value("${ems.consumer.group-id:ems-ingest}") String groupId,
            @Value("${ems.recon.kafka.timeout-ms:5000}") long timeoutMs) {
        // Admin.create() does not connect: an unreachable broker fails the first probe (WARN + empty),
        // it does not fail startup. A metrics probe must never be able to keep the pod from booting.
        this(Admin.create(adminProperties(bootstrapServers, timeoutMs)), groupId, timeoutMs, true);
    }

    /** Test/IT seam: run against a supplied client whose lifecycle the caller owns. */
    ConsumerLagProbe(Admin admin, String groupId, Duration timeout) {
        this(admin, groupId, timeout.toMillis(), false);
    }

    private ConsumerLagProbe(Admin admin, String groupId, long timeoutMs, boolean ownsAdmin) {
        this.admin = admin;
        this.groupId = groupId;
        this.timeoutMs = timeoutMs;
        this.ownsAdmin = ownsAdmin;
    }

    private static Map<String, Object> adminProperties(String bootstrapServers, long timeoutMs) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Bound the client internally too, so a retrying call cannot outlive the future.get() below and
        // leave a background request queue growing one entry per tick.
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeoutMs);
        return props;
    }

    /**
     * One offsets poll. Returns one row per partition the group has committed to; an empty list means
     * either "this group has never committed anywhere" or "the broker could not be read" — in both cases
     * there is no honest lag number to publish.
     */
    public List<PartitionLag> probe() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    await(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
            if (committed.isEmpty()) {
                return List.of();
            }
            // Both requests in flight before either is awaited.
            KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> ends =
                    admin.listOffsets(specFor(committed, OffsetSpec.latest())).all();
            KafkaFuture<Map<TopicPartition, ListOffsetsResultInfo>> starts =
                    admin.listOffsets(specFor(committed, OffsetSpec.earliest())).all();
            return lags(committed, await(ends), await(starts));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return warn(e);
        } catch (RuntimeException | ExecutionException | TimeoutException e) {
            return warn(e);
        }
    }

    private <T> T await(KafkaFuture<T> future)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private static Map<TopicPartition, OffsetSpec> specFor(
            Map<TopicPartition, OffsetAndMetadata> committed, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        committed.keySet().forEach(partition -> request.put(partition, spec));
        return request;
    }

    private static List<PartitionLag> lags(Map<TopicPartition, OffsetAndMetadata> committed,
            Map<TopicPartition, ListOffsetsResultInfo> ends,
            Map<TopicPartition, ListOffsetsResultInfo> starts) {

        List<PartitionLag> lags = new ArrayList<>(committed.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            TopicPartition partition = entry.getKey();
            ListOffsetsResultInfo end = ends.get(partition);
            ListOffsetsResultInfo start = starts.get(partition);
            // A partition can be committed-to and gone (topic deleted between the two calls); skipping it
            // is right — the series then disappears rather than reporting a number against nothing.
            if (entry.getValue() == null || end == null || start == null) {
                continue;
            }
            long position = entry.getValue().offset();
            lags.add(new PartitionLag(partition.topic(), partition.partition(),
                    clampToZero(end.offset() - position), clampToZero(position - start.offset())));
        }
        return lags;
    }

    /**
     * Offsets are read in three separate broker calls, so a partition that moves between them can produce
     * a momentarily negative difference. Zero is the truthful floor for both distances, and it keeps a
     * transient skew from being read as "headroom exhausted".
     */
    private static long clampToZero(long delta) {
        return Math.max(0L, delta);
    }

    private static List<PartitionLag> warn(Exception e) {
        log.warn("Consumer-lag probe could not read group offsets — publishing no lag series this tick", e);
        return List.of();
    }

    @PreDestroy
    void close() {
        if (ownsAdmin) {
            admin.close(Duration.ofSeconds(5));
        }
    }
}
