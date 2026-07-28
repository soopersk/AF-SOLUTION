package com.orchestration.ems.decisions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.DecisionRecord;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Persistence proof for the {@code POST /decisions} write path
 * ({@link RoutingDecisionRepo#insert}/{@link RoutingDecisionRepo#insertBatch}) against a real PostgreSQL:
 * any tier persists with the caller's {@code decided_by}, {@code detail} lands as queryable {@code jsonb},
 * L0 records stay idempotent under a caller retry while other tiers append, and the columns that are null
 * by contract ({@code target_dag_id} on {@code L1_SUMMARY}) accept null. Auto-skips locally (no Docker).
 */
class DecisionIngestIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CONTROL_DAG = "capital_control_dag";
    private static final String REGISTRY = "v42/ab99f0";
    private static final String ENGINE = "celpy==0.1.5";

    private JdbcClient jdbc;
    private RoutingDecisionRepo repo;

    @BeforeEach
    void setUp() {
        jdbc = jdbcClient();
        jdbc.sql("TRUNCATE routing_decision").update();
        repo = new RoutingDecisionRepo(jdbc);
    }

    @Test
    void insertBatch_persistsEveryTier_withCallerDecidedBy() {
        int inserted = repo.insertBatch(List.of(
                record("evt-1", "CAPITAL", "L1_SUMMARY", null, "MATCHED", "{\"matched\":1,\"errors\":0}"),
                record("evt-1", "CAPITAL", "L1_OUTCOME", "amer_d_b3f_dag", "TRIGGERED", null),
                record("evt-1", "CAPITAL", "GATE", "portfolio_daily_dag", "GATE_WAITING", null)),
                CONTROL_DAG);

        assertThat(inserted).isEqualTo(3);

        record Row(String tier, String decision, String decidedBy) { }
        List<Row> rows = jdbc.sql("SELECT tier, decision, decided_by FROM routing_decision "
                        + "WHERE event_id = ? ORDER BY tier")
                .param("evt-1")
                .query((rs, n) -> new Row(rs.getString(1), rs.getString(2), rs.getString(3)))
                .list();

        assertThat(rows).extracting(Row::tier, Row::decision, Row::decidedBy).containsExactly(
                tuple("GATE", "GATE_WAITING", CONTROL_DAG),
                tuple("L1_OUTCOME", "TRIGGERED", CONTROL_DAG),
                tuple("L1_SUMMARY", "MATCHED", CONTROL_DAG));
    }

    @Test
    void detail_isQueryableJsonb() {
        repo.insert(record("evt-2", "CAPITAL", "L1_SUMMARY", null, "MATCHED",
                "{\"matched\":3,\"errors\":0}"), CONTROL_DAG);

        // exactly the trigger-plan §7 audit query shape: WHERE tier='L1_SUMMARY' AND (detail->>'matched')::int = 0
        Integer matched = jdbc.sql("SELECT (detail->>'matched')::int FROM routing_decision WHERE event_id = ?")
                .param("evt-2").query(Integer.class).single();

        assertThat(matched).isEqualTo(3);
    }

    @Test
    void l0Record_isIdempotentUnderCallerRetry() {
        DecisionRecord l0 = record("evt-3", "CAPITAL", "L0_SUBSCRIPTION", "dag_capital", "FORWARDED", null);

        assertThat(repo.insert(l0, CONTROL_DAG)).isEqualTo(1);
        assertThat(repo.insert(l0, CONTROL_DAG)).isZero(); // absorbed by ux_rd_l0

        assertThat(countFor("evt-3")).isEqualTo(1);
    }

    @Test
    void nonL0Records_appendRatherThanDedupe() {
        // two evaluations of the same gate at different times are two facts, not a duplicate
        DecisionRecord gate = record("evt-4", "CAPITAL", "GATE", "portfolio_daily_dag", "GATE_WAITING", null);

        assertThat(repo.insert(gate, CONTROL_DAG)).isEqualTo(1);
        assertThat(repo.insert(gate, CONTROL_DAG)).isEqualTo(1);

        assertThat(countFor("evt-4")).isEqualTo(2);
    }

    @Test
    void nullableColumns_acceptNull_andDecidedAtDefaults() {
        repo.insert(new DecisionRecord("evt-5", null, "L1_SUMMARY", null, "MATCHED", null, null, null),
                "capital_heartbeat");

        record Row(String tenantId, String targetDagId, String detail, String registryVersion,
                String engineVersion, Object decidedAt) { }
        Row r = jdbc.sql("SELECT tenant_id, target_dag_id, detail::text, registry_version, engine_version, "
                        + "decided_at FROM routing_decision WHERE event_id = ?")
                .param("evt-5")
                .query((rs, n) -> new Row(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getObject(6)))
                .single();

        assertThat(r.tenantId()).isNull();
        assertThat(r.targetDagId()).isNull();   // null by contract for L1_SUMMARY (V4:335)
        assertThat(r.detail()).isNull();
        assertThat(r.registryVersion()).isNull();
        assertThat(r.engineVersion()).isNull();
        assertThat(r.decidedAt()).isNotNull();  // DEFAULT now()
    }

    @Test
    void emptyBatch_insertsNothing() {
        assertThat(repo.insertBatch(List.of(), CONTROL_DAG)).isZero();
    }

    // --- fixtures -----------------------------------------------------------------------------------

    private static DecisionRecord record(String eventId, String tenantId, String tier, String targetDagId,
            String decision, String detailJson) {
        return new DecisionRecord(eventId, tenantId, tier, targetDagId, decision, tree(detailJson),
                REGISTRY, ENGINE);
    }

    private static JsonNode tree(String json) {
        if (json == null) {
            return null;
        }
        try {
            return MAPPER.readTree(json);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new AssertionError("not valid JSON: " + json, e);
        }
    }

    private Integer countFor(String eventId) {
        return jdbc.sql("SELECT count(*) FROM routing_decision WHERE event_id = ?")
                .param(eventId).query(Integer.class).single();
    }
}
