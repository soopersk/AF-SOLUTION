package com.orchestration.ems.recon;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Integration proof for {@link ReconRepository} against a real PostgreSQL — the three SQL-sourced §10
 * gauges over rows that exercise each query's exclusion rule:
 * <ul>
 *   <li>{@code ems_dlq_depth}: a replayed row is <em>not</em> depth (otherwise the page never clears);</li>
 *   <li>{@code ems_outbox_pending_age_seconds}: a delivered row is not backlog, and an empty backlog reads
 *       {@code 0}, not NULL;</li>
 *   <li>{@code ems_overdue_inflight_runs}: a run with either terminal form is excluded, a quiet run with
 *       <b>no</b> terminal-bearing columns at all is counted (the {@code bool_or}/NULL trap), a recently
 *       active run is not yet overdue, and a run older than the horizon has left the window.</li>
 * </ul>
 *
 * <p>Rows are aged by writing {@code created_at} explicitly. {@code event.created_at} has a default but is
 * not generated, so it is directly insertable — which is the only way to test a time-based query without
 * sleeping. Auto-skips locally; runs in CI.
 */
class ReconRepositoryIT extends AbstractPostgresIT {

    private static final Duration WINDOW = Duration.ofHours(6);
    private static final Duration HORIZON = Duration.ofDays(7);

    private JdbcClient jdbc;
    private ReconRepository repo;

    @BeforeEach
    void clean() {
        jdbc = jdbcClient();
        jdbc.sql("TRUNCATE dlq_record, dag_trigger_outbox, event").update();
        repo = new ReconRepository(jdbc);
    }

    // ---- ems_dlq_depth{topic} ------------------------------------------------------------------

    @Test
    void dlqDepth_countsUnreplayedPerTopic_andExcludesReplayedRows() {
        dlqRow("edf.events", null);
        dlqRow("edf.events", null);
        dlqRow("edf.events", "2026-07-28T10:00:00Z"); // replayed — triaged, no longer depth
        dlqRow("meg.events", null);

        List<DlqDepth> depths = repo.dlqDepthByTopic();

        assertThat(depths).containsExactlyInAnyOrder(
                new DlqDepth("edf.events", 2), new DlqDepth("meg.events", 1));
    }

    @Test
    void dlqDepth_fullyTriagedTopicDisappears_soTheSeriesCanBeRemoved() {
        dlqRow("edf.events", "2026-07-28T10:00:00Z");

        assertThat(repo.dlqDepthByTopic()).isEmpty();
    }

    // ---- ems_outbox_pending_age_seconds --------------------------------------------------------

    @Test
    void outboxAge_measuresOldestUndelivered_ignoringDeliveredRows() {
        outboxRow("run-old-delivered", "now() - interval '2 hours'", true);
        outboxRow("run-pending", "now() - interval '30 minutes'", false);

        // ~1800s from the pending row; the older delivered row must not win
        assertThat(repo.oldestPendingOutboxAgeSeconds()).isBetween(1700.0, 1900.0);
    }

    @Test
    void outboxAge_drainedOutboxReadsZeroNotNull() {
        outboxRow("run-done", "now() - interval '2 hours'", true);

        assertThat(repo.oldestPendingOutboxAgeSeconds()).isZero();
    }

    // ---- ems_overdue_inflight_runs -------------------------------------------------------------

    @Test
    void overdueRuns_countsAQuietRunWithNoTerminalEvent() {
        event("evt-1", "ctx-stuck", "STARTED", null, "now() - interval '10 hours'");
        event("evt-2", "ctx-stuck", "RUNNING", null, "now() - interval '9 hours'");

        assertThat(repo.overdueInflightRuns(WINDOW, HORIZON)).isEqualTo(1);
    }

    /**
     * The three-valued-logic case: neither {@code STATE} nor {@code taskEventType} is present, so both
     * promoted columns are NULL for every event in the group. Without the COALESCE in the query,
     * {@code bool_or} would return NULL and the group would silently vanish — the stuck run this gauge
     * exists to surface would be the one run it could never see.
     */
    @Test
    void overdueRuns_countsARunWhoseEventsCarryNoTerminalColumnsAtAll() {
        jdbc.sql("INSERT INTO event (event_id, json, created_at) VALUES (?, ?::jsonb, now() - interval '10 hours')")
                .param("evt-bare")
                .param("{\"id\":\"evt-bare\",\"contextId\":\"ctx-bare\",\"additionalData\":{}}")
                .update();

        assertThat(repo.overdueInflightRuns(WINDOW, HORIZON)).isEqualTo(1);
    }

    @Test
    void overdueRuns_excludeRunsThatReachedEitherTerminalForm() {
        event("evt-a1", "ctx-finished", "STARTED", null, "now() - interval '10 hours'");
        event("evt-a2", "ctx-finished", "FINISH", null, "now() - interval '9 hours'");
        event("evt-b1", "ctx-failed", "STARTED", null, "now() - interval '10 hours'");
        event("evt-b2", "ctx-failed", "FAILED", null, "now() - interval '9 hours'");
        event("evt-c1", "ctx-task", null, "STARTED", "now() - interval '10 hours'");
        event("evt-c2", "ctx-task", null, "COMPLETED", "now() - interval '9 hours'");

        assertThat(repo.overdueInflightRuns(WINDOW, HORIZON)).isZero();
    }

    @Test
    void overdueRuns_excludeRunsStillInsideTheQuietWindow() {
        event("evt-fresh", "ctx-active", "STARTED", null, "now() - interval '10 minutes'");

        assertThat(repo.overdueInflightRuns(WINDOW, HORIZON)).isZero();
    }

    @Test
    void overdueRuns_excludeRunsOlderThanTheHorizon() {
        event("evt-ancient", "ctx-ancient", "STARTED", null, "now() - interval '30 days'");

        assertThat(repo.overdueInflightRuns(WINDOW, HORIZON)).isZero();
    }

    @Test
    void overdueRuns_countsEachStuckRunOnce_notEachEvent() {
        event("evt-x1", "ctx-x", "STARTED", null, "now() - interval '10 hours'");
        event("evt-x2", "ctx-x", "RUNNING", null, "now() - interval '9 hours'");
        event("evt-y1", "ctx-y", "STARTED", null, "now() - interval '8 hours'");

        assertThat(repo.overdueInflightRuns(WINDOW, HORIZON)).isEqualTo(2);
    }

    // ---- fixtures ------------------------------------------------------------------------------

    private void dlqRow(String topic, String replayedAt) {
        jdbc.sql("""
                INSERT INTO dlq_record (topic, kafka_partition, kafka_offset, error, replayed_at)
                VALUES (?, 0, 0, 'boom', ?::timestamptz)
                """)
                .param(topic)
                .param(replayedAt)
                .update();
    }

    private void outboxRow(String dagRunId, String createdAtExpr, boolean delivered) {
        jdbc.sql("""
                INSERT INTO dag_trigger_outbox (dag_run_id, dag_id, conf, created_at, delivered_at)
                VALUES (?, 'dag_x', '{}'::jsonb, %s, %s)
                """.formatted(createdAtExpr, delivered ? "now()" : "NULL"))
                .param(dagRunId)
                .update();
    }

    /** An event whose promoted {@code state}/{@code task_event_type} come from the JSON the columns read. */
    private void event(String eventId, String contextId, String state, String taskEventType,
            String createdAtExpr) {
        String additional = state != null
                ? "\"STATE\":\"" + state + "\""
                : "\"taskEventType\":\"" + taskEventType + "\"";
        jdbc.sql("""
                INSERT INTO event (event_id, json, created_at)
                VALUES (?, ?::jsonb, %s)
                """.formatted(createdAtExpr))
                .param(eventId)
                .param("{\"id\":\"" + eventId + "\",\"contextId\":\"" + contextId + "\","
                        + "\"additionalData\":{" + additional + "}}")
                .update();
    }
}
