package com.orchestration.ems.recon;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;

/**
 * The §10 loss backstop: one scheduled bean that answers "is anything quietly stuck?" from committed
 * state, on a pod that may be doing nothing else at all.
 *
 * <p><b>Why it is deliberately ungated.</b> Every other signal EMS publishes is a side effect of work
 * actually happening — a consumed record increments a counter, a delivered trigger clears a row. The
 * failures that matter most produce no work and therefore no side effect: a parked consumer, a dispatcher
 * that does not exist because {@code ems.dispatch.enabled=false}, a run whose terminal event never
 * arrives. This sweep is independent of both the ingest path and the dispatch path precisely so those
 * silences are still visible. It is on by default and only switched off explicitly
 * ({@code ems.recon.enabled=false}).
 *
 * <p><b>Gauge ownership.</b> {@code ems_outbox_pending_age_seconds} lives here, not in
 * {@code OutboxDispatcher}. The dispatcher bean is conditional on {@code ems.dispatch.enabled=true}, so
 * in the {@code shadow} profile — the one profile where outbox rows accumulate <em>by design</em> and
 * nothing drains them — the gauge did not exist at all. Same metric name, same meaning, one owner that
 * is always present.
 *
 * <p><b>Degradation.</b> Each source is refreshed under its own guard. A failing query logs at WARN and
 * leaves that gauge at its last-known value; it neither propagates (a {@code @Scheduled} method that
 * throws can stop being rescheduled, which would silently disable the backstop) nor takes the other
 * gauges down with it. Stale-but-present beats absent: an operator can see a flat line, and the alert
 * rules still evaluate.
 *
 * <p><b>The Kafka pair is the one exception to that.</b> {@code ems_consumer_lag} and
 * {@code ems_consumer_retention_headroom_records} come from {@link ConsumerLagProbe}, which yields an
 * empty result on failure and so <em>clears</em> its series instead of freezing them. A stale backlog age
 * is still a fact about the database; a stale lag is a number that stopped rising exactly when lag began
 * to matter, and no threshold rule would ever fire against it.
 */
@Component
@ConditionalOnProperty(prefix = "ems.recon", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReconciliationSweep {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationSweep.class);

    // Public so the alert rules can be cross-checked against the names this code actually registers —
    // a typo'd metric name in a PrometheusRule is invisible until the alert silently never fires.
    /** Unreplayed DLQ rows per source topic — the actionable triage depth (§10, page > 0 for 5 m). */
    public static final String DLQ_DEPTH_METRIC = "ems_dlq_depth";
    /** Age of the oldest undelivered outbox row — a rising value means delivery is stalled (§12). */
    public static final String OUTBOX_AGE_METRIC = "ems_outbox_pending_age_seconds";
    /** Runs that started, went quiet and never reached a terminal event (§10 loss backstop). */
    public static final String OVERDUE_RUNS_METRIC = "ems_overdue_inflight_runs";
    /** Records between the group's committed offset and the log head — a parked partition (§10, page). */
    public static final String CONSUMER_LAG_METRIC = "ems_consumer_lag";
    /** Records between the log start and the committed offset — slack before retention deletes (§10). */
    public static final String RETENTION_HEADROOM_METRIC = "ems_consumer_retention_headroom_records";

    public static final String TOPIC_TAG = "topic";
    public static final String PARTITION_TAG = "partition";

    private final ReconRepository repo;
    private final Optional<ConsumerLagProbe> lagProbe;
    private final Duration overdueWindow;
    private final Duration horizon;

    private final MultiGauge dlqDepth;
    private final MultiGauge consumerLag;
    private final MultiGauge retentionHeadroom;

    /** Last successful readings; a failed refresh leaves the previous value in place (see class javadoc). */
    private volatile double outboxPendingAgeSeconds;
    private volatile long overdueInflightRuns;

    public ReconciliationSweep(ReconRepository repo, Optional<ConsumerLagProbe> lagProbe,
            MeterRegistry meterRegistry,
            @Value("${ems.recon.overdue-window:6h}") Duration overdueWindow,
            @Value("${ems.recon.horizon:7d}") Duration horizon) {
        this.repo = repo;
        this.lagProbe = lagProbe;
        this.overdueWindow = overdueWindow;
        this.horizon = horizon;

        Gauge.builder(OUTBOX_AGE_METRIC, this, sweep -> sweep.outboxPendingAgeSeconds)
                .baseUnit("seconds")
                .description("Age of the oldest undelivered dag_trigger_outbox row")
                .register(meterRegistry);
        Gauge.builder(OVERDUE_RUNS_METRIC, this, sweep -> sweep.overdueInflightRuns)
                .description("Runs with no terminal event past the overdue window")
                .register(meterRegistry);
        this.dlqDepth = MultiGauge.builder(DLQ_DEPTH_METRIC)
                .description("Unreplayed dlq_record rows awaiting triage, by source topic")
                .register(meterRegistry);
        this.consumerLag = MultiGauge.builder(CONSUMER_LAG_METRIC)
                .description("Records between the ingest group's committed offset and the log head")
                .register(meterRegistry);
        this.retentionHeadroom = MultiGauge.builder(RETENTION_HEADROOM_METRIC)
                .description("Records between the log start and the ingest group's committed offset")
                .register(meterRegistry);
    }

    /**
     * One sweep tick. {@code fixedDelay} (not {@code fixedRate}) so a slow database can never overlap two
     * sweeps on one pod — under load the cadence stretches instead of piling up.
     */
    @Scheduled(fixedDelayString = "${ems.recon.interval-ms:60000}")
    public void sweep() {
        guard("outbox pending age", () -> outboxPendingAgeSeconds = repo.oldestPendingOutboxAgeSeconds());
        guard("overdue in-flight runs",
                () -> overdueInflightRuns = repo.overdueInflightRuns(overdueWindow, horizon));
        guard("dlq depth", this::refreshDlqDepth);
        guard("consumer lag", this::refreshConsumerLag);
    }

    /**
     * Republishes both Kafka series sets from a single offsets poll, so lag and headroom are always read
     * from the same broker snapshot and can never disagree about where the group is.
     *
     * <p>An empty probe — no {@link ConsumerLagProbe} bean ({@code ems.recon.kafka.enabled=false}), a group
     * that has never committed, or a failed poll — clears both series rather than leaving them frozen.
     * See the failure note on {@link ConsumerLagProbe}: unlike the SQL gauges above, a stale lag is worse
     * than an absent one.
     */
    private void refreshConsumerLag() {
        List<PartitionLag> partitions = lagProbe.map(ConsumerLagProbe::probe).orElseGet(List::of);
        consumerLag.register(rows(partitions, PartitionLag::lag), true);
        retentionHeadroom.register(rows(partitions, PartitionLag::retentionHeadroom), true);
    }

    private static List<MultiGauge.Row<?>> rows(List<PartitionLag> partitions,
            ToLongFunction<PartitionLag> value) {
        return partitions.stream()
                .<MultiGauge.Row<?>>map(partition -> MultiGauge.Row.of(
                        Tags.of(TOPIC_TAG, partition.topic(),
                                PARTITION_TAG, Integer.toString(partition.partition())),
                        value.applyAsLong(partition)))
                .toList();
    }

    /**
     * Republishes the whole {@code ems_dlq_depth} series set, removing topics that no longer have
     * unreplayed rows ({@code true} = overwrite). A topic left frozen at its last depth would keep the
     * page firing after the operator finished replaying it.
     */
    private void refreshDlqDepth() {
        dlqDepth.register(repo.dlqDepthByTopic().stream()
                .map(depth -> MultiGauge.Row.of(Tags.of(TOPIC_TAG, depth.topic()), depth.depth()))
                .toList(), true);
    }

    /** Runs one refresh in isolation: a failure degrades exactly one gauge, and never the schedule. */
    private void guard(String source, Runnable refresh) {
        try {
            refresh.run();
        } catch (RuntimeException e) {
            log.warn("Reconciliation sweep could not refresh {} — leaving the last-known value", source, e);
        }
    }
}
