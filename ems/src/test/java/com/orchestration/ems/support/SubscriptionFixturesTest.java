package com.orchestration.ems.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;

/**
 * Unit test (runs under Surefire, no Docker) proving the seed-0 fixture loads as
 * {@link SubscriptionRow} and matches the legacy inventory shape: 16 rows = 7 PERSIST +
 * 8 CAPITAL FORWARD + 1 disabled NSFR FORWARD.
 */
class SubscriptionFixturesTest {

    @Test
    void loadsAll16Rows() {
        List<SubscriptionRow> rows = SubscriptionFixtures.seed0();
        assertThat(rows).hasSize(16);
        assertThat(rows).allSatisfy(r -> assertThat(r.registryVersion()).isEqualTo("seed-0"));
        // Fixtures are non-DB rows: id is the sentinel 0L.
        assertThat(rows).allSatisfy(r -> assertThat(r.id()).isZero());
    }

    @Test
    void persistRows_areSevenLifecycleWideDropGate() {
        List<SubscriptionRow> persist = SubscriptionFixtures.seed0().stream()
                .filter(r -> r.stage() == Stage.PERSIST)
                .toList();
        assertThat(persist).hasSize(7);
        assertThat(persist).allSatisfy(r -> {
            assertThat(r.controlDagId()).isNull();
            assertThat(r.enabled()).isTrue();
            assertThat(r.tenantId()).isEqualTo("PLATFORM");
        });
    }

    @Test
    void forwardRows_eightCapitalEnabled_oneNsfrDisabled() {
        List<SubscriptionRow> forward = SubscriptionFixtures.seed0().stream()
                .filter(r -> r.stage() == Stage.FORWARD)
                .toList();
        assertThat(forward).hasSize(9);

        List<SubscriptionRow> capital = forward.stream().filter(r -> r.tenantId().equals("CAPITAL")).toList();
        assertThat(capital).hasSize(8);
        assertThat(capital).allSatisfy(r -> {
            assertThat(r.controlDagId()).isEqualTo("orchestration_control_dag_capital");
            assertThat(r.enabled()).isTrue();
        });

        List<SubscriptionRow> nsfr = forward.stream().filter(r -> r.tenantId().equals("NSFR")).toList();
        assertThat(nsfr).hasSize(1);
        assertThat(nsfr.get(0).controlDagId()).isEqualTo("orchestration_control_dag_liquidity");
        assertThat(nsfr.get(0).enabled()).isFalse();
    }

    @Test
    void tenantStageRuleName_triplesAreUnique() {
        // The V3 uq_subscription (tenant_id, stage, rule_name) constraint — and the PLATFORM-sentinel
        // rationale for PERSIST rows — depend on these triples being distinct.
        List<String> triples = SubscriptionFixtures.seed0().stream()
                .map(r -> r.tenantId() + "|" + r.stage() + "|" + r.ruleName())
                .toList();
        assertThat(triples).doesNotHaveDuplicates();
    }

    @Test
    void stage_isOnlyPersistOrForward() {
        assertThat(SubscriptionFixtures.seed0())
                .allSatisfy(r -> assertThat(r.stage()).isIn(Stage.PERSIST, Stage.FORWARD));
    }

    @Test
    void frcaCuration_translatesTopsidePrefixClause() {
        SubscriptionRow frca = SubscriptionFixtures.seed0().stream()
                .filter(r -> r.ruleName().equals("cap_data_update.FRCA_CURATION"))
                .findFirst()
                .orElseThrow();
        assertThat(frca.whenCel()).contains("context.data.runcategory.startsWith(\"topside\")");
    }
}
