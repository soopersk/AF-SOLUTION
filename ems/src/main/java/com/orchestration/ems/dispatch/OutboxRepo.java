package com.orchestration.ems.dispatch;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Transactional-outbox repository for {@code dag_trigger_outbox} (ems-design §4.2/§5, trigger-plan
 * §4.4). The trigger intent is written in the SAME transaction as the event persist (Amendment A1:
 * Airflow is off the ingest path), then an asynchronous dispatcher drains it with
 * {@code FOR UPDATE SKIP LOCKED} — every pod runs a dispatcher, and SKIP LOCKED makes concurrent
 * drains safe (§12).
 *
 * <p>Delivery backoff is <em>not</em> persisted as an eligibility timestamp: the table has only
 * {@code attempts}/{@code last_error}, and the 30s–600s jittered backoff is derived from
 * {@code attempts} by the dispatcher between poll ticks (§4.2 item 7 / §12).
 */
@Repository
public class OutboxRepo {

    private static final String INSERT = """
            INSERT INTO dag_trigger_outbox (dag_run_id, dag_id, conf)
            VALUES (?, ?, ?::jsonb)
            ON CONFLICT (dag_run_id) DO NOTHING
            """;

    private static final String DRAIN_PENDING = """
            SELECT dag_run_id, dag_id, conf::text AS conf, attempts
            FROM dag_trigger_outbox
            WHERE delivered_at IS NULL
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT ?
            """;

    private static final String MARK_DELIVERED = """
            UPDATE dag_trigger_outbox SET delivered_at = now()
            WHERE dag_run_id = ? AND delivered_at IS NULL
            """;

    private static final String RECORD_ATTEMPT = """
            UPDATE dag_trigger_outbox SET attempts = attempts + 1, last_error = ?
            WHERE dag_run_id = ?
            """;

    private final JdbcClient jdbc;

    public OutboxRepo(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Insert a trigger intent (typically in the ingest transaction). Idempotent on the deterministic
     * {@code dag_run_id} (A6) — a duplicate is a no-op.
     *
     * @return 1 when inserted, 0 when an existing row made it a no-op
     */
    public int insert(String dagRunId, String dagId, String conf) {
        return jdbc.sql(INSERT).param(dagRunId).param(dagId).param(conf).update();
    }

    /**
     * Claim up to {@code batchSize} undelivered rows for delivery, oldest first, skipping rows already
     * locked by a concurrent dispatcher ({@code FOR UPDATE SKIP LOCKED}).
     *
     * <p><b>Must be called inside an active transaction:</b> the row locks are held until the caller's
     * transaction commits/rolls back, so a claimed row stays invisible to other dispatchers until then.
     * When invoked outside a transaction (autocommit), the rows are still returned but their locks are
     * released immediately — safe for read-only inspection, not for the drain→deliver→mark cycle.
     */
    public List<PendingTrigger> drainPending(int batchSize) {
        return jdbc.sql(DRAIN_PENDING)
                .param(batchSize)
                .query((rs, rowNum) -> new PendingTrigger(
                        rs.getString("dag_run_id"),
                        rs.getString("dag_id"),
                        rs.getString("conf"),
                        rs.getInt("attempts")))
                .list();
    }

    /**
     * Mark a row delivered (Airflow returned 200 or 409 — 409 = already triggered = delivered, A6).
     *
     * @return 1 when the row flipped to delivered, 0 if it was already delivered
     */
    public int markDelivered(String dagRunId) {
        return jdbc.sql(MARK_DELIVERED).param(dagRunId).update();
    }

    /**
     * Record a failed delivery attempt: increment {@code attempts} and store {@code last_error}. The
     * dispatcher derives the next-attempt backoff from {@code attempts} (30s–600s, jitter — §4.2/§12);
     * eligibility is not persisted.
     *
     * @return 1 when the row was updated
     */
    public int recordAttempt(String dagRunId, String error) {
        return jdbc.sql(RECORD_ATTEMPT).param(error).param(dagRunId).update();
    }

    // The oldest-pending-age query is deliberately NOT here. It backs a gauge that must be published on
    // pods that never dispatch (§11 shadow), so it belongs to recon.ReconRepository — see the ownership
    // note on OutboxDispatcher.
}
