package com.orchestration.ems.store;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.orchestration.ems.model.RunStatus;

/**
 * Read path for {@code GET /run/status} (ems-design §4.5) — the framework-F0-unblocking lifecycle probe.
 * Summarizes a run from the event store in one indexed pass keyed by the {@code /event} correlation
 * vocabulary ({@code context_id} = the framework's {@code triggerContextId}, and/or {@code task_id}),
 * plus a {@code dlq_record} correlation-key existence check.
 *
 * <p><b>Terminal vocabulary</b> is the framework's two completion schemes, verbatim
 * (framework_redesign_final_implementation_plan.md:333):
 * <ul>
 *   <li>{@code MERIVAL_CALC_EVENT} — terminal iff {@code STATE ∈ {FINISH, FAILED}}; successful iff
 *       {@code STATE = FINISH}. (The {@code state} promoted column is already upper-cased.)</li>
 *   <li>{@code MEG_TASK_EVENT} — terminal iff {@code taskEventType = COMPLETED}; successful iff the event's
 *       {@code successful} flag is true. (The {@code task_event_type} promoted column is upper-cased; the
 *       {@code successful} flag is read from the raw payload — see the SQL note below.)</li>
 * </ul>
 *
 * <p><b>Indexing:</b> the {@code context_id}/{@code task_id} equality predicates ride
 * {@code idx_event_context_id}/{@code idx_event_task_id}; the DLQ probe rides
 * {@code ix_dlq_context}/{@code ix_dlq_task}. The per-run matched set is small (one lifecycle = a handful of
 * events), so terminal classification is done in-service over the returned rows — the same "small qualifying
 * set" pattern the design uses for {@code /gate/groups} (ems-design §4.5).
 *
 * <p><b>Assumption flagged (oracle gap):</b> no {@code taskEventType=COMPLETED} sample exists in the repo to
 * pin where the MEG {@code successful} flag lives, so it is read best-effort from both
 * {@code additionalData.successful} and top-level {@code successful} ({@code ->>} renders a JSON boolean as
 * {@code "true"}/{@code "false"}). If a real COMPLETED payload later contradicts this, record it as an
 * amendment before relying on it. The MERIVAL {@code STATE} path is fully grounded by the sample payloads.
 */
@Repository
public class RunStatusRepository {

    /**
     * Upper-cased {@code state} values that end a MERIVAL_CALC_EVENT lifecycle (framework §333).
     *
     * <p>These three constants are the <b>single</b> terminal vocabulary in EMS. {@code ReconRepository}
     * binds them into its {@code ems_overdue_inflight_runs} query rather than restating the literals, so
     * "this run has finished" can never come to mean two different things in two places.
     */
    public static final String STATE_SUCCESS = "FINISH";
    public static final String STATE_FAILURE = "FAILED";
    /** Upper-cased {@code task_event_type} value that ends a MEG_TASK_EVENT lifecycle (framework §333). */
    public static final String TASK_TERMINAL = "COMPLETED";

    private static final String EVENT_SELECT =
            "SELECT event_id, state, task_event_type, "
            + "COALESCE(json->'additionalData'->>'successful', json->>'successful') AS successful_flag, "
            + "created_at "
            + "FROM event %s ORDER BY created_at";

    private static final String CONTEXT_ID_PREDICATE = "context_id = ?";
    private static final String TASK_ID_PREDICATE = "task_id = ?";

    private final JdbcClient jdbc;

    public RunStatusRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Summarize the run identified by the correlation criteria. Always returns a {@link RunStatus} (never
     * a "not found" — a run with no events is a legitimate {@code NEVER_STARTED} candidate the framework
     * interprets from {@code started=false}). At least one of {@code contextId}/{@code taskId} must be
     * present; the controller enforces that.
     *
     * @param contextId optional {@code context_id} (the framework's {@code triggerContextId}) equality filter
     * @param taskId    optional {@code task_id} equality filter
     * @return the lifecycle summary
     */
    public RunStatus summarize(Optional<String> contextId, Optional<String> taskId) {
        List<String> predicates = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        contextId.ifPresent(v -> {
            predicates.add(CONTEXT_ID_PREDICATE);
            params.add(v);
        });
        taskId.ifPresent(v -> {
            predicates.add(TASK_ID_PREDICATE);
            params.add(v);
        });
        String where = predicates.isEmpty() ? "" : "WHERE " + String.join(" AND ", predicates);

        List<EventRow> rows = jdbc.sql(EVENT_SELECT.formatted(where))
                .params(params)
                .query((rs, rowNum) -> new EventRow(
                        rs.getString("event_id"),
                        rs.getString("state"),
                        rs.getString("task_event_type"),
                        rs.getString("successful_flag"),
                        rs.getObject("created_at", OffsetDateTime.class)))
                .list();

        boolean started = !rows.isEmpty();
        String lastEventAt = rows.isEmpty()
                ? null
                : rows.get(rows.size() - 1).createdAt().toInstant().toString();

        RunStatus.Terminal terminal = terminalOf(rows);
        boolean dlqHint = dlqMatches(contextId, taskId);

        // scheduled collapses into started: EMS has no scheduler view distinct from the event stream.
        return new RunStatus(started, started, terminal, dlqHint, lastEventAt);
    }

    /** The latest terminal event among {@code rows} (they arrive {@code created_at}-ascending), or absent. */
    private static RunStatus.Terminal terminalOf(List<EventRow> rows) {
        RunStatus.Terminal terminal = RunStatus.Terminal.absent();
        for (EventRow row : rows) {
            if (row.isTerminal()) {
                terminal = new RunStatus.Terminal(true, row.isSuccessful(), row.eventId());
            }
        }
        return terminal;
    }

    /** True iff a {@code dlq_record} carries the same correlation key(s) (ix_dlq_context / ix_dlq_task). */
    private boolean dlqMatches(Optional<String> contextId, Optional<String> taskId) {
        List<String> predicates = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        contextId.ifPresent(v -> {
            predicates.add(CONTEXT_ID_PREDICATE);
            params.add(v);
        });
        taskId.ifPresent(v -> {
            predicates.add(TASK_ID_PREDICATE);
            params.add(v);
        });
        String where = "WHERE " + String.join(" AND ", predicates);
        return jdbc.sql("SELECT 1 FROM dlq_record " + where + " LIMIT 1")
                .params(params)
                .query(Integer.class)
                .optional()
                .isPresent();
    }

    /** One matching event's terminal-relevant fields ({@code state}/{@code task_event_type} are upper-cased). */
    private record EventRow(String eventId, String state, String taskEventType,
            String successfulFlag, OffsetDateTime createdAt) {

        boolean isTerminal() {
            return STATE_SUCCESS.equals(state) || STATE_FAILURE.equals(state)
                    || TASK_TERMINAL.equals(taskEventType);
        }

        boolean isSuccessful() {
            if (STATE_SUCCESS.equals(state)) {
                return true;
            }
            if (STATE_FAILURE.equals(state)) {
                return false;
            }
            // MEG COMPLETED task event: success from the payload's `successful` flag.
            return "true".equals(successfulFlag);
        }
    }
}
