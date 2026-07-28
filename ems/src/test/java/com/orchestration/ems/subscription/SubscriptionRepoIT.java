package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Integration proof for {@link SubscriptionRepo} against a real PostgreSQL: {@code loadEnabled}
 * returns only enabled rows, mapped 1:1 from the V3 columns (stage enum, control_dag_id, generated
 * IDENTITY id). {@code disabledWithoutDocker} auto-skips locally; runs in CI.
 */
class SubscriptionRepoIT extends AbstractPostgresIT {

    private final SubscriptionRepo repo = new SubscriptionRepo(jdbcClient());

    @BeforeEach
    void clean() {
        jdbcClient().sql("TRUNCATE subscription").update();
    }

    @Test
    void loadEnabled_returnsOnlyEnabledRows_mappedFromV3Columns() {
        insert("PLATFORM", "PERSIST", "persist_frca", "event.additionalData.tenant == \"FRCA\"", null, true);
        insert("CAPITAL", "FORWARD", "cap_frca", "event.additionalData.tenant == \"FRCA\"",
                "orchestration_control_dag_capital", true);
        insert("NSFR", "FORWARD", "nsfr_off", "event.additionalData.tenant == \"ACTL\"",
                "orchestration_control_dag_liquidity", false); // disabled → excluded

        var rows = repo.loadEnabled();

        assertThat(rows).extracting(SubscriptionRow::ruleName)
                .containsExactlyInAnyOrder("persist_frca", "cap_frca");

        SubscriptionRow forward = rows.stream()
                .filter(r -> r.stage() == Stage.FORWARD).findFirst().orElseThrow();
        assertThat(forward.tenantId()).isEqualTo("CAPITAL");
        assertThat(forward.controlDagId()).isEqualTo("orchestration_control_dag_capital");
        assertThat(forward.registryVersion()).isEqualTo("seed-0");
        assertThat(forward.enabled()).isTrue();
        assertThat(forward.id()).isPositive(); // GENERATED ALWAYS AS IDENTITY

        SubscriptionRow persist = rows.stream()
                .filter(r -> r.stage() == Stage.PERSIST).findFirst().orElseThrow();
        assertThat(persist.controlDagId()).isNull(); // PERSIST has no control DAG
    }

    private void insert(String tenant, String stage, String rule, String cel, String dag, boolean enabled) {
        jdbcClient().sql("""
                INSERT INTO subscription
                    (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
                VALUES (?, ?, ?, ?, ?, 'seed-0', ?, 'test')
                """)
                .param(tenant).param(stage).param(rule).param(dag).param(cel).param(enabled)
                .update();
    }
}
