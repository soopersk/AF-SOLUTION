package com.orchestration.ems.recon;

import java.time.Duration;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.orchestration.ems.store.RunStatusRepository;

/**
 * The out-of-band read model behind {@link ReconciliationSweep} (ems-design §10). Three queries, one per
 * gauge, answering "is anything quietly stuck?" from state that is already committed — deliberately
 * <em>not</em> from anything the ingest or dispatch path holds in memory. That independence is the whole
 * point: these numbers must be right on a pod whose consumer is parked and whose dispatcher does not
 * exist (the {@code shadow} profile).
 *
 * <p>Every query is read-only, runs on the sweep's cadence (default 60 s), and is bounded by an index:
 * <ul>
 *   <li>{@link #dlqDepthByTopic()} — a grouped count over {@code dlq_record}, a table sized by operator
 *       triage backlog (small by construction; a large one is itself the alert).</li>
 *   <li>{@link #oldestPendingOutboxAgeSeconds()} — rides the partial index {@code ix_outbox_pending}
 *       ({@code V5}), so it is an index-only min over undelivered rows however large the table grows.</li>
 *   <li>{@link #overdueInflightRuns(Duration, Duration)} — bounded by a {@code created_at} horizon so it
 *       rides {@code idx_event_created_at} ({@code V2}) instead of scanning all history.</li>
 * </ul>
 */
@Repository
public class ReconRepository {

    private static final String DLQ_DEPTH = """
            SELECT topic, count(*) AS depth
            FROM dlq_record
            WHERE replayed_at IS NULL
            GROUP BY topic
            """;

    /**
     * Zero — not NULL — when the outbox is fully drained: the gauge's "no backlog" reading and its
     * "nothing to report" reading must be the same number, or the page threshold has a hole in it.
     */
    private static final String OLDEST_PENDING_OUTBOX_AGE = """
            SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::double precision
            FROM dag_trigger_outbox
            WHERE delivered_at IS NULL
            """;

    /**
     * Runs (= {@code context_id} groups) whose last event is older than the overdue window and that
     * contain no terminal event, over a bounded horizon.
     *
     * <p><b>The COALESCE is load-bearing, not defensive.</b> {@code state} and {@code task_event_type} are
     * NULL for most events (they are generated columns over optional JSON keys), and in SQL
     * {@code NULL IN ('FINISH','FAILED')} is NULL, not false. {@code bool_or} skips NULL inputs entirely,
     * so a run whose events all lack both columns — precisely the stuck run this gauge exists to find —
     * would aggregate to NULL, and {@code NOT NULL} is NULL, which fails the HAVING and drops the group.
     * Folding each side to false first makes {@code bool_or} total: false when no terminal was seen.
     */
    private static final String OVERDUE_INFLIGHT_RUNS = """
            SELECT count(*) FROM (
                SELECT context_id
                FROM event
                WHERE context_id IS NOT NULL
                  AND created_at >= now() - make_interval(secs => ?)
                GROUP BY context_id
                HAVING max(created_at) < now() - make_interval(secs => ?)
                   AND NOT bool_or(COALESCE(state IN (?, ?), false)
                                OR COALESCE(task_event_type = ?, false))
            ) overdue
            """;

    private final JdbcClient jdbc;

    public ReconRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Unreplayed DLQ rows grouped by source topic. Topics with nothing pending are simply absent from the
     * result — the sweep turns that absence into a removed series, which is what lets the page recover.
     */
    public List<DlqDepth> dlqDepthByTopic() {
        return jdbc.sql(DLQ_DEPTH)
                .query((rs, rowNum) -> new DlqDepth(rs.getString("topic"), rs.getLong("depth")))
                .list();
    }

    /**
     * Age in seconds of the oldest undelivered {@code dag_trigger_outbox} row, or {@code 0} when the
     * outbox is drained. A rising value means delivery is stalled (§12) — and because this lives here
     * rather than in the dispatcher, it is reported in {@code shadow} too, where rows accumulate by design
     * and nothing drains them.
     */
    public double oldestPendingOutboxAgeSeconds() {
        return jdbc.sql(OLDEST_PENDING_OUTBOX_AGE).query(Double.class).single();
    }

    /**
     * Count of runs that started, went quiet for longer than {@code window}, and never reached a terminal
     * event — the §10 loss backstop, independent of the Phase-D heartbeat.
     *
     * <p>Terminal is {@link RunStatusRepository}'s vocabulary verbatim ({@code state ∈ {FINISH, FAILED}}
     * or {@code task_event_type = COMPLETED}), bound as parameters from that class's constants so the two
     * readers cannot drift apart.
     *
     * @param window  quiet time after which a run without a terminal event is considered overdue
     * @param horizon how far back to look; older runs are out of scope (and keep the query on the index)
     */
    public long overdueInflightRuns(Duration window, Duration horizon) {
        return jdbc.sql(OVERDUE_INFLIGHT_RUNS)
                .param(seconds(horizon))
                .param(seconds(window))
                .param(RunStatusRepository.STATE_SUCCESS)
                .param(RunStatusRepository.STATE_FAILURE)
                .param(RunStatusRepository.TASK_TERMINAL)
                .query(Long.class)
                .single();
    }

    /** {@code make_interval(secs => ?)} takes a double; sub-second precision is preserved. */
    private static double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }
}
