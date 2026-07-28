package com.orchestration.ems.decisions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Integration proof for {@link RoutingDecisionRepo} against a real PostgreSQL: L0 verdicts are
 * idempotent per {@code (event_id, tenant_id)} via {@code ux_rd_l0} (redelivery-safe), distinct
 * tenants for the same event both persist, {@code decided_by='ems'}/{@code tier='L0_SUBSCRIPTION'}
 * are fixed, and {@code NOT_SUBSCRIBED} with a null tenant/detail persists. Auto-skips locally.
 */
class RoutingDecisionRepoIT extends AbstractPostgresIT {

    private static final String ENGINE = "cel-java==0.4.4";

    private final RoutingDecisionRepo repo = new RoutingDecisionRepo(jdbcClient());

    @BeforeEach
    void clean() {
        jdbcClient().sql("TRUNCATE routing_decision").update();
    }

    @Test
    void insertL0_isIdempotentPerEventTenant_A3() {
        L0Decision d = L0Decision.forwarded(
                "evt-1", "CAPITAL", "orchestration_control_dag_capital", null, "seed-0", ENGINE);

        assertThat(repo.insertL0(d)).isEqualTo(1);
        assertThat(repo.insertL0(d)).isZero(); // ux_rd_l0 dedup under redelivery

        Integer count = jdbcClient().sql(
                        "SELECT count(*) FROM routing_decision WHERE event_id = ? AND tier = 'L0_SUBSCRIPTION'")
                .param("evt-1").query(Integer.class).single();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void insertL0_distinctTenantsForSameEvent_bothPersist() {
        repo.insertL0(L0Decision.forwarded("evt-2", "CAPITAL", "dag_capital", null, "seed-0", ENGINE));
        repo.insertL0(L0Decision.forwarded("evt-2", "LIQUIDITY", "dag_liquidity", null, "seed-0", ENGINE));

        Integer count = jdbcClient().sql("SELECT count(*) FROM routing_decision WHERE event_id = ?")
                .param("evt-2").query(Integer.class).single();
        assertThat(count).isEqualTo(2);
    }

    @Test
    void insertL0_notSubscribed_persistsWithNullTenant_fixedTierAndDecidedBy() {
        assertThat(repo.insertL0(L0Decision.notSubscribed("evt-3", "{\"reason\":\"no match\"}", ENGINE)))
                .isEqualTo(1);

        record Row(String decision, String decidedBy, String tier, String tenantId, String reason) { }
        Row r = jdbcClient().sql(
                        "SELECT decision, decided_by, tier, tenant_id, detail->>'reason' AS reason "
                                + "FROM routing_decision WHERE event_id = ?")
                .param("evt-3")
                .query((rs, n) -> new Row(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5)))
                .single();
        assertThat(r.decision()).isEqualTo("NOT_SUBSCRIBED");
        assertThat(r.decidedBy()).isEqualTo("ems");
        assertThat(r.tier()).isEqualTo("L0_SUBSCRIPTION");
        assertThat(r.tenantId()).isNull();
        assertThat(r.reason()).isEqualTo("no match"); // detail jsonb bound from a String param
    }

    @Test
    void insertL0Batch_insertsFanOut_returnsInsertedCount() {
        int inserted = repo.insertL0Batch(List.of(
                L0Decision.forwarded("evt-4", "CAPITAL", "dag_capital", null, "seed-0", ENGINE),
                L0Decision.forwarded("evt-4", "LIQUIDITY", "dag_liquidity", null, "seed-0", ENGINE)));

        assertThat(inserted).isEqualTo(2);
    }
}
