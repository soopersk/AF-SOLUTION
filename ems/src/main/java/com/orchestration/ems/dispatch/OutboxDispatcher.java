package com.orchestration.ems.dispatch;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.orchestration.ems.dispatch.AirflowTriggerClient.Outcome;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Drains the transactional outbox and delivers each trigger to Airflow (ems-design §4.2 item 7 / §12;
 * Amendment A1: Airflow is triggered asynchronously off the ingest path). Every pod runs a dispatcher;
 * {@code FOR UPDATE SKIP LOCKED} in {@link OutboxRepo#drainPending(int)} makes concurrent drains safe —
 * each tick claims a disjoint slice of undelivered rows for the life of the transaction.
 *
 * <p><b>Per-tick cycle</b> (one transaction so the row locks are held while delivering): claim a batch,
 * then for each row call {@link AirflowTriggerClient}; a {@link Outcome#DELIVERED} flips {@code delivered_at},
 * a {@link Outcome#RETRIABLE}/{@link Outcome#NON_RETRIABLE} increments {@code attempts} and records the error.
 *
 * <p><b>Backoff (30s–600s, jittered).</b> The table stores no eligibility timestamp (only {@code attempts}):
 * the dispatcher gates retries in-memory, computing the next-eligible instant from the failure count with
 * equal jitter. A row still inside its backoff window is skipped this tick (it stays undelivered and is
 * re-claimed on a later tick), so a persistent outage never busy-loops the failing rows at the poll cadence.
 * The map is process-local — a pod restart simply retries immediately once, which is harmless (delivery is
 * idempotent on the 409 = already-triggered rule, A6).
 *
 * <p>Gated by {@code ems.dispatch.enabled}: a non-dispatching role (API-only pod, shadow phase) runs no drain.
 */
@Component
@ConditionalOnProperty(prefix = "ems.dispatch", name = "enabled", havingValue = "true")
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    /** Age of the oldest undelivered outbox row — a rising value means delivery is stalled (§12). */
    static final String PENDING_AGE_METRIC = "ems_outbox_pending_age_seconds";

    private final OutboxRepo outboxRepo;
    private final AirflowTriggerClient triggerClient;
    private final TransactionTemplate txTemplate;

    private final int batchSize;
    private final long baseBackoffSeconds;
    private final long maxBackoffSeconds;

    /** dag_run_id → instant it next becomes eligible for another delivery attempt (in-memory backoff gate). */
    private final Map<String, Instant> nextEligible = new ConcurrentHashMap<>();
    /** Last-observed oldest-pending age, published via the {@link #PENDING_AGE_METRIC} gauge. */
    private volatile double oldestPendingAgeSeconds;

    public OutboxDispatcher(OutboxRepo outboxRepo, AirflowTriggerClient triggerClient,
            MeterRegistry meterRegistry, PlatformTransactionManager transactionManager,
            @Value("${ems.dispatch.batch-size:100}") int batchSize,
            @Value("${ems.dispatch.base-backoff-seconds:30}") long baseBackoffSeconds,
            @Value("${ems.dispatch.max-backoff-seconds:600}") long maxBackoffSeconds) {
        this.outboxRepo = outboxRepo;
        this.triggerClient = triggerClient;
        this.txTemplate = new TransactionTemplate(transactionManager);
        this.batchSize = batchSize;
        this.baseBackoffSeconds = baseBackoffSeconds;
        this.maxBackoffSeconds = maxBackoffSeconds;
        Gauge.builder(PENDING_AGE_METRIC, this, d -> d.oldestPendingAgeSeconds)
                .baseUnit("seconds")
                .description("Age of the oldest undelivered dag_trigger_outbox row")
                .register(meterRegistry);
    }

    /**
     * One drain tick: claim and deliver a batch of undelivered rows, then refresh the pending-age gauge.
     * {@code fixedDelay} (not {@code fixedRate}) so a slow Airflow can never overlap two drains on one pod.
     */
    @Scheduled(fixedDelayString = "${ems.dispatch.poll-interval-ms:2000}")
    public void drain() {
        txTemplate.executeWithoutResult(status -> {
            List<PendingTrigger> pending = outboxRepo.drainPending(batchSize);
            for (PendingTrigger trigger : pending) {
                dispatchOne(trigger);
            }
        });
        refreshPendingAge();
    }

    private void dispatchOne(PendingTrigger trigger) {
        Instant gate = nextEligible.get(trigger.dagRunId());
        if (gate != null && Instant.now().isBefore(gate)) {
            return; // still within its backoff window — leave it for a later tick
        }
        switch (triggerClient.trigger(trigger.dagId(), trigger.dagRunId(), trigger.conf())) {
            case DELIVERED -> {
                outboxRepo.markDelivered(trigger.dagRunId());
                nextEligible.remove(trigger.dagRunId());
            }
            case RETRIABLE -> retain(trigger, "retriable: Airflow unavailable");
            case NON_RETRIABLE -> {
                retain(trigger, "non-retriable: Airflow rejected the trigger");
                log.error("Outbox row {} (DAG {}) is non-retriable — it will not deliver until corrected",
                        trigger.dagRunId(), trigger.dagId());
            }
        }
    }

    /** Record a failed attempt and arm the in-memory backoff gate from the new attempt count. */
    private void retain(PendingTrigger trigger, String error) {
        int attempts = trigger.attempts() + 1;
        outboxRepo.recordAttempt(trigger.dagRunId(), error);
        Duration backoff = backoffFor(attempts);
        nextEligible.put(trigger.dagRunId(), Instant.now().plus(backoff));
        log.debug("Retaining outbox row {} (attempt {}), next eligible in {}",
                trigger.dagRunId(), attempts, backoff);
    }

    /**
     * Exponential backoff with equal jitter: {@code min(max, base·2^(attempts-1))} seconds, then split into
     * a fixed half plus a random half so concurrent dispatchers don't retry in lock-step, while never
     * dropping below half the computed delay.
     */
    Duration backoffFor(int attempts) {
        int shift = Math.min(Math.max(attempts - 1, 0), 20); // guard against 1L<<large overflow
        long capped = Math.min(maxBackoffSeconds, baseBackoffSeconds * (1L << shift));
        long half = capped / 2;
        long jitter = half > 0 ? ThreadLocalRandom.current().nextLong(half + 1) : 0;
        return Duration.ofSeconds(half + jitter);
    }

    private void refreshPendingAge() {
        this.oldestPendingAgeSeconds = outboxRepo.oldestPendingCreatedAt()
                .map(oldest -> Math.max(0.0, Duration.between(oldest, Instant.now()).toMillis() / 1000.0))
                .orElse(0.0);
    }
}
