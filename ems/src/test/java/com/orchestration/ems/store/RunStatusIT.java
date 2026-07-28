package com.orchestration.ems.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.orchestration.ems.model.RunStatus;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Lifecycle derivation for {@link RunStatusRepository} over a real PostgreSQL (ems-design §4.5). Exercises
 * the four framework categories the {@code SlaAwareHttpTrigger} classifies from the body — {@code NEVER_STARTED}
 * ({@code started=false}), {@code STARTED_NO_TERMINAL} ({@code started=true, terminal.present=false}), terminal
 * present (success/fail via {@code STATE=FINISH|FAILED}), and {@code TERMINAL_IN_DLQ} ({@code dlq_hint=true}) —
 * plus MEG {@code taskEventType=COMPLETED} terminal via {@code task_id} and latest-terminal ordering.
 * Auto-skips locally (no Docker); runs in CI.
 */
class RunStatusIT extends AbstractPostgresIT {

    private JdbcClient jdbc;
    private RunStatusRepository repo;

    @BeforeEach
    void setUp() {
        DataSource ds = dataSource();
        jdbc = JdbcClient.create(ds);
        jdbc.sql("TRUNCATE event, context, dlq_record").update();
        repo = new RunStatusRepository(jdbc);
    }

    @Test
    void neverStarted_noEvents_startedFalse() {
        RunStatus s = repo.summarize(Optional.of("ctx-empty"), Optional.empty());

        assertThat(s.started()).isFalse();
        assertThat(s.scheduled()).isFalse();
        assertThat(s.terminal().present()).isFalse();
        assertThat(s.dlqHint()).isFalse();
        assertThat(s.lastEventAt()).isNull();
    }

    @Test
    void startedNoTerminal_onlyStartEvent() {
        insertEvent("e-start", "{\"id\":\"e-start\",\"contextId\":\"ctx-run\","
                + "\"additionalData\":{\"STATE\":\"START\"}}", "2026-07-26T10:00:00Z");

        RunStatus s = repo.summarize(Optional.of("ctx-run"), Optional.empty());

        assertThat(s.started()).isTrue();
        assertThat(s.scheduled()).isTrue();
        assertThat(s.terminal().present()).isFalse();
        assertThat(s.lastEventAt()).isEqualTo("2026-07-26T10:00:00Z");
    }

    @Test
    void terminalSuccess_finishState() {
        insertEvent("e-start", "{\"id\":\"e-start\",\"contextId\":\"ctx-run\","
                + "\"additionalData\":{\"STATE\":\"START\"}}", "2026-07-26T10:00:00Z");
        insertEvent("e-fin", "{\"id\":\"e-fin\",\"contextId\":\"ctx-run\",\"type\":\"CALC_EVENT\","
                + "\"additionalData\":{\"STATE\":\"FINISH\"}}", "2026-07-26T10:05:00Z");

        RunStatus s = repo.summarize(Optional.of("ctx-run"), Optional.empty());

        assertThat(s.terminal().present()).isTrue();
        assertThat(s.terminal().successful()).isTrue();
        assertThat(s.terminal().eventId()).isEqualTo("e-fin");
        assertThat(s.lastEventAt()).isEqualTo("2026-07-26T10:05:00Z");
    }

    @Test
    void terminalFailure_failedState() {
        insertEvent("e-fail", "{\"id\":\"e-fail\",\"contextId\":\"ctx-run\","
                + "\"additionalData\":{\"STATE\":\"FAILED\"}}", "2026-07-26T10:05:00Z");

        RunStatus s = repo.summarize(Optional.of("ctx-run"), Optional.empty());

        assertThat(s.terminal().present()).isTrue();
        assertThat(s.terminal().successful()).isFalse();
        assertThat(s.terminal().eventId()).isEqualTo("e-fail");
    }

    @Test
    void latestTerminalWins_whenMultipleTerminalEvents() {
        // an earlier FINISH then a later FAILED (e.g. re-run) → the newest terminal governs
        insertEvent("e-fin", "{\"id\":\"e-fin\",\"contextId\":\"ctx-run\","
                + "\"additionalData\":{\"STATE\":\"FINISH\"}}", "2026-07-26T10:05:00Z");
        insertEvent("e-fail", "{\"id\":\"e-fail\",\"contextId\":\"ctx-run\","
                + "\"additionalData\":{\"STATE\":\"FAILED\"}}", "2026-07-26T10:09:00Z");

        RunStatus s = repo.summarize(Optional.of("ctx-run"), Optional.empty());

        assertThat(s.terminal().present()).isTrue();
        assertThat(s.terminal().successful()).isFalse();
        assertThat(s.terminal().eventId()).isEqualTo("e-fail");
    }

    @Test
    void terminalInDlq_dlqHintTrue() {
        insertEvent("e-fin", "{\"id\":\"e-fin\",\"contextId\":\"ctx-dlq\","
                + "\"additionalData\":{\"STATE\":\"FINISH\"}}", "2026-07-26T10:05:00Z");
        insertDlq("ctx-dlq", null, "boom");

        RunStatus s = repo.summarize(Optional.of("ctx-dlq"), Optional.empty());

        assertThat(s.dlqHint()).isTrue();
    }

    @Test
    void megCompletedTaskEvent_terminalViaTaskId() {
        // MEG_TASK_EVENT scheme (framework §333): taskEventType=COMPLETED, successful flag → success.
        // A7: MEG nests taskId/taskEventType under additionalData; the promoted columns COALESCE them up.
        insertEvent("e-meg", "{\"id\":\"e-meg\",\"additionalData\":"
                + "{\"taskId\":\"task-9\",\"taskEventType\":\"COMPLETED\",\"successful\":true}}",
                "2026-07-26T10:05:00Z");

        RunStatus s = repo.summarize(Optional.empty(), Optional.of("task-9"));

        assertThat(s.started()).isTrue();
        assertThat(s.terminal().present()).isTrue();
        assertThat(s.terminal().successful()).isTrue();
        assertThat(s.terminal().eventId()).isEqualTo("e-meg");
    }

    @Test
    void megCompletedTaskEvent_unsuccessfulFlag_terminalButFailed() {
        insertEvent("e-meg", "{\"id\":\"e-meg\",\"additionalData\":"
                + "{\"taskId\":\"task-9\",\"taskEventType\":\"COMPLETED\",\"successful\":false}}",
                "2026-07-26T10:05:00Z");

        RunStatus s = repo.summarize(Optional.empty(), Optional.of("task-9"));

        assertThat(s.terminal().present()).isTrue();
        assertThat(s.terminal().successful()).isFalse();
    }

    private void insertEvent(String eventId, String json, String createdAtIso) {
        jdbc.sql("INSERT INTO event (event_id, json, created_at) VALUES (?, ?::jsonb, ?::timestamptz)")
                .param(eventId).param(json).param(createdAtIso).update();
    }

    private void insertDlq(String contextId, String taskId, String error) {
        jdbc.sql("INSERT INTO dlq_record (topic, kafka_partition, kafka_offset, context_id, task_id, error) "
                + "VALUES ('t', 0, 0, ?, ?, ?)")
                .param(contextId).param(taskId).param(error).update();
    }
}
