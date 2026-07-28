package com.orchestration.ems.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.model.EventRow;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Integration proof for {@link EventRepository}/{@link ContextRepository} against a real PostgreSQL:
 * the byte-verbatim JSONB upsert populates the typed generated columns (A7/A8), stored content
 * round-trips faithfully, re-insert is a silent no-op (A3), and {@code parentIds} is array-containment
 * queried via the GIN index (A9). {@code disabledWithoutDocker} auto-skips locally; runs in CI.
 */
class EventContextRepositoryIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final EventRepository events = new EventRepository(jdbcClient());
    private final ContextRepository contexts = new ContextRepository(jdbcClient(), MAPPER);

    @BeforeEach
    void clean() {
        jdbcClient().sql("TRUNCATE event, context").update();
    }

    @Test
    void upsertEvent_persistsRawPayload_andA7GeneratedColumnsPopulate() throws Exception {
        String json = resource("/samples/event_meg_started.json");

        assertThat(events.upsert(EventRow.of(json, MAPPER))).isEqualTo(1);

        record Cols(String taskId, String taskEventType, String state, String source, String contextId) { }
        Cols c = jdbcClient().sql(
                        "SELECT task_id, task_event_type, state, source, context_id FROM event WHERE event_id = ?")
                .param("evt-meg-1")
                .query((rs, n) -> new Cols(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5)))
                .single();
        assertThat(c.taskId()).isEqualTo("task-abc");        // A7: from additionalData.taskId
        assertThat(c.taskEventType()).isEqualTo("STARTED");  // A7 + upper()
        assertThat(c.state()).isEqualTo("START");
        assertThat(c.source()).isEqualTo("MEG");
        assertThat(c.contextId()).isEqualTo("ctx-200");

        // stored jsonb is content-verbatim with the received payload (jsonb only canonicalizes format)
        Boolean matches = jdbcClient().sql("SELECT json = ?::jsonb FROM event WHERE event_id = ?")
                .param(json).param("evt-meg-1")
                .query(Boolean.class).single();
        assertThat(matches).isTrue();
    }

    @Test
    void upsertEvent_isIdempotent_A3() throws Exception {
        EventRow row = EventRow.of(resource("/samples/event_meg_started.json"), MAPPER);

        assertThat(events.upsert(row)).isEqualTo(1);
        assertThat(events.upsert(row)).isZero(); // ON CONFLICT DO NOTHING

        Integer count = jdbcClient().sql("SELECT count(*) FROM event WHERE event_id = ?")
                .param("evt-meg-1").query(Integer.class).single();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void upsertContext_populatesFrequencyRegion_andParentIdsAreContainmentQueried_A8_A9() throws Exception {
        String json = resource("/samples/context_merival.json");

        assertThat(contexts.upsert(ContextRow.of(json, MAPPER))).isEqualTo(1);

        record Cols(String frequency, String h3Region, String reportingDate) { }
        Cols c = jdbcClient().sql(
                        "SELECT frequency, h3_region, reporting_date FROM context WHERE context_id = ?")
                .param("ctx-300")
                .query((rs, n) -> new Cols(rs.getString(1), rs.getString(2), rs.getString(3)))
                .single();
        assertThat(c.frequency()).isEqualTo("DAILY");   // ems_norm_freq(DAILY)
        assertThat(c.h3Region()).isEqualTo("EMEA");     // ems_norm_region(emea) passthrough-upper
        assertThat(c.reportingDate()).isEqualTo("2026-07-17"); // A8: reporting-date (hyphen spelling)

        // A9: parentIds is array-containment queried via the GIN index — not a scalar column
        String child = jdbcClient().sql(
                        "SELECT context_id FROM context WHERE (json->'parentIds') @> to_jsonb(?::text)")
                .param("ctx-200")
                .query(String.class).single();
        assertThat(child).isEqualTo("ctx-300");
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = EventContextRepositoryIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
