package com.orchestration.ems.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Foundation exit-criterion proof (ems-design §13 phase 1): {@code flyway migrate} runs clean
 * on a real PostgreSQL 16, and the typed {@code GENERATED ALWAYS AS ... STORED} columns
 * populate from raw JSONB — including the code-grounding corrections:
 *
 * <ul>
 *   <li><b>A7</b> — MEG {@code taskId}/{@code taskEventType} nested under {@code additionalData}
 *       are COALESCE'd to the top level.</li>
 *   <li><b>A8</b> — cross-family key spellings ({@code reporting-date}|{@code reportingDate},
 *       {@code run-category}|{@code runCategory}, {@code h3Region}|{@code regionCode}) both map.</li>
 *   <li><b>A9</b> — {@code parentIds} is array-containment queried via the GIN index (V2), not a
 *       scalar column.</li>
 *   <li>Normalization functions {@code ems_norm_freq}/{@code ems_norm_region} match the intended
 *       value maps (Java↔SQL equivalence is exhaustively checked elsewhere; here we spot-check).</li>
 *   <li>Write-path idempotency: {@code ON CONFLICT DO NOTHING} makes re-insert a silent no-op (A3).</li>
 * </ul>
 *
 * <p>Runs under Failsafe ({@code *IT}). {@code disabledWithoutDocker = true} makes the whole
 * class self-skip (JUnit "disabled", not error) when no Docker environment is reachable — so
 * {@code mvn verify} stays green on a dev box without Docker, while CI (Docker present) runs it
 * for real. This is the Foundation gate's integration half; it must be green in CI before the
 * Foundation phase is signed off.
 *
 * <p>Container/Flyway/datasource wiring now lives in {@link AbstractPostgresIT}; this class only
 * keeps its assertions. The {@code @Testcontainers(disabledWithoutDocker = true)} skip and the
 * Flyway V1–V5 migration are inherited unchanged from the base.
 */
class FlywayMigrationIT extends AbstractPostgresIT {

    // ---- clean migrate -----------------------------------------------------

    @Test
    void cleanMigrate_appliedAllOfV1throughV5() {
        // The base asserts only success (stays valid under container reuse); the exact executed
        // count is this class's own exit criterion — a clean container applies all of V1..V5.
        assertThat(migrateResult.migrationsExecuted).isGreaterThanOrEqualTo(5);
    }

    @Test
    void jdbcClient_reachesMigratedSchema() {
        // Exercises the inherited non-pooled accessor end-to-end (no pool to leak).
        Integer one = jdbcClient().sql("SELECT 1").query(Integer.class).single();
        assertThat(one).isEqualTo(1);
    }

    // ---- generated columns -------------------------------------------------

    @Test
    void megEvent_taskIdAndTaskEventType_comeFromAdditionalData_A7() throws Exception {
        insertEvent("evt-meg-1", resource("/samples/event_meg_started.json"));

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT task_id, task_event_type, context_id, source, state FROM event WHERE event_id = ?")) {
            ps.setString(1, "evt-meg-1");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("task_id")).isEqualTo("task-abc");        // A7: from additionalData
                assertThat(rs.getString("task_event_type")).isEqualTo("STARTED"); // A7 + upper()
                assertThat(rs.getString("context_id")).isEqualTo("ctx-200");
                assertThat(rs.getString("source")).isEqualTo("MEG");
                assertThat(rs.getString("state")).isEqualTo("START");
            }
        }
    }

    @Test
    void calcEvent_eventType_comesFromLowercaseTypeKey() throws Exception {
        insertEvent("evt-calc-1", resource("/samples/event_calc_complete.json"));

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT task_id, event_type, state FROM event WHERE event_id = ?")) {
            ps.setString(1, "evt-calc-1");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("task_id")).isEqualTo("calc-task-9");
                assertThat(rs.getString("event_type")).isEqualTo("CALC_EVENT"); // from additionalData.type
                assertThat(rs.getString("state")).isEqualTo("FINISH");
            }
        }
    }

    @Test
    void merivalEvent_datasetAndType_populate() throws Exception {
        insertEvent("evt-mer-1", resource("/samples/event_merival_ingestion.json"));

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT dataset_id, event_type, state, source, logical_business_date FROM event WHERE event_id = ?")) {
            ps.setString(1, "evt-mer-1");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("dataset_id")).isEqualTo("ds-uuid-777");
                assertThat(rs.getString("event_type")).isEqualTo("INGESTION"); // from additionalData.TYPE
                assertThat(rs.getString("state")).isEqualTo("FINISH");
                assertThat(rs.getString("source")).isEqualTo("MERIVAL");
                assertThat(rs.getString("logical_business_date")).isEqualTo("2026-07-17");
            }
        }
    }

    @Test
    void context_camelCaseSpelling_A8() throws Exception {
        insertContext("ctx-200", resource("/samples/context_calc.json"));

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT dataset_id, reporting_date, run_category, h3_region, frequency FROM context WHERE context_id = ?")) {
            ps.setString(1, "ctx-200");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("dataset_id")).isEqualTo("ds-calc-1");
                assertThat(rs.getString("reporting_date")).isEqualTo("2026-07-17"); // A8: reportingDate
                assertThat(rs.getString("run_category")).isEqualTo("TOPSIDE");       // upper(runCategory)
                assertThat(rs.getString("h3_region")).isEqualTo("AMER");             // ems_norm_region(AMERICAS)
                assertThat(rs.getString("frequency")).isEqualTo("DAILY");            // ems_norm_freq(D)
            }
        }
    }

    @Test
    void context_hyphenSpelling_A8() throws Exception {
        insertContext("ctx-300", resource("/samples/context_merival.json"));

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT reporting_date, run_category, h3_region, frequency, dataset_id FROM context WHERE context_id = ?")) {
            ps.setString(1, "ctx-300");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("reporting_date")).isEqualTo("2026-07-17"); // A8: reporting-date (hyphen)
                assertThat(rs.getString("run_category")).isEqualTo("TOPSIDE");       // upper(run-category)
                assertThat(rs.getString("h3_region")).isEqualTo("EMEA");             // upper passthrough
                assertThat(rs.getString("frequency")).isEqualTo("DAILY");
                assertThat(rs.getString("dataset_id")).isNull();                     // absent in Merival — expected
            }
        }
    }

    // ---- A9: parentIds array-containment via GIN ---------------------------

    @Test
    void parentIds_arrayContainment_findsChild_A9() throws Exception {
        insertContext("ctx-200", resource("/samples/context_calc.json")); // parentIds: [ctx-100, ctx-050]

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT context_id FROM context WHERE json->'parentIds' @> ?::jsonb")) {
            ps.setString(1, "\"ctx-100\"");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("context_id")).isEqualTo("ctx-200");
            }
        }
    }

    // ---- normalization functions ------------------------------------------

    @Test
    void normFunctions_mapKnownValues() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            assertThat(scalar(st, "SELECT ems_norm_freq('D')")).isEqualTo("DAILY");
            assertThat(scalar(st, "SELECT ems_norm_freq('daily')")).isEqualTo("DAILY");
            assertThat(scalar(st, "SELECT ems_norm_freq('Q')")).isEqualTo("QUARTERLY");
            assertThat(scalar(st, "SELECT ems_norm_region('AMERICAS')")).isEqualTo("AMER");
            assertThat(scalar(st, "SELECT ems_norm_region('emea')")).isEqualTo("EMEA"); // passthrough upper
        }
    }

    // ---- write-path idempotency (A3) --------------------------------------

    @Test
    void reinsertSameEventId_isSilentNoOp_A3() throws Exception {
        String json = resource("/samples/event_meg_started.json");
        int first = insertEvent("evt-dedup", json);
        int second = insertEvent("evt-dedup", json);
        assertThat(first).isEqualTo(1);
        assertThat(second).isZero(); // ON CONFLICT DO NOTHING

        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM event WHERE event_id = ?")) {
            ps.setString(1, "evt-dedup");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }
    }

    // ---- helpers -----------------------------------------------------------

    private static Connection conn() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static int insertEvent(String eventId, String json) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO event (event_id, json) VALUES (?, ?::jsonb) ON CONFLICT DO NOTHING")) {
            ps.setString(1, eventId);
            ps.setString(2, json);
            return ps.executeUpdate();
        }
    }

    private static int insertContext(String contextId, String json) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO context (context_id, json) VALUES (?, ?::jsonb) ON CONFLICT DO NOTHING")) {
            ps.setString(1, contextId);
            ps.setString(2, json);
            return ps.executeUpdate();
        }
    }

    private static String scalar(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = FlywayMigrationIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
