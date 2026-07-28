package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Persistence proof for the {@code PUT /admin/subscriptions} write path
 * ({@link SubscriptionRepo#upsert}/{@link SubscriptionRepo#upsertAll}) against a real PostgreSQL: a
 * rendered row inserts with its audit columns, a re-render of the same rule replaces it <em>in place</em>
 * via the V3 {@code uq_subscription} key, and disabling a rule removes it from the very query
 * {@link SubscriptionService} refreshes from. Auto-skips locally (no Docker).
 */
class SubscriptionUpsertIT extends AbstractPostgresIT {

    private static final String CI = "registry-ci";

    private final SubscriptionRepo repo = new SubscriptionRepo(jdbcClient());

    @BeforeEach
    void clean() {
        jdbcClient().sql("TRUNCATE subscription").update();
    }

    @Test
    void upsert_insertsRenderedRow_withAuditColumns() {
        assertThat(repo.upsert(forward("cap_data_update.MER_batch", "v42/ab99f0", true), CI)).isEqualTo(1);

        Row row = read("cap_data_update.MER_batch");
        assertThat(row.tenantId()).isEqualTo("CAPITAL");
        assertThat(row.stage()).isEqualTo("FORWARD");
        assertThat(row.controlDagId()).isEqualTo("orchestration_control_dag_capital");
        assertThat(row.whenCel()).isEqualTo("event.additionalData.tenant == \"FRCA\"");
        assertThat(row.registryVersion()).isEqualTo("v42/ab99f0");
        assertThat(row.enabled()).isTrue();
        assertThat(row.updatedBy()).isEqualTo(CI);   // V3 updated_by — the break-glass/CI audit
        assertThat(row.updatedAt()).isNotNull();
    }

    @Test
    void upsert_replacesExistingRuleInPlace_keepingItsIdentity() {
        repo.upsert(forward("cap_data_update.MER_batch", "v41/0001aa", true), CI);
        Row before = read("cap_data_update.MER_batch");

        // the registry re-renders the same rule (same tenant/stage/rule_name) with a new predicate
        SubscriptionRow rerendered = new SubscriptionRow(0L, "CAPITAL", Stage.FORWARD,
                "cap_data_update.MER_batch", "orchestration_control_dag_capital",
                "event.additionalData.tenant == \"MR\"", "v42/ab99f0", true);
        assertThat(repo.upsert(rerendered, "break-glass:ops@bank")).isEqualTo(1);

        assertThat(count()).isEqualTo(1); // ON CONFLICT (tenant_id, stage, rule_name) — not a second row
        Row after = read("cap_data_update.MER_batch");
        assertThat(after.id()).isEqualTo(before.id()); // the CI renders rules, not our IDENTITY values
        assertThat(after.whenCel()).isEqualTo("event.additionalData.tenant == \"MR\"");
        assertThat(after.registryVersion()).isEqualTo("v42/ab99f0");
        assertThat(after.updatedBy()).isEqualTo("break-glass:ops@bank");
        assertThat(after.updatedAt()).isAfterOrEqualTo(before.updatedAt());
    }

    @Test
    void upsert_disablingRule_removesItFromLoadEnabled() {
        repo.upsert(forward("cap_retired", "v42/ab99f0", true), CI);
        assertThat(repo.loadEnabled()).extracting(SubscriptionRow::ruleName).contains("cap_retired");

        repo.upsert(forward("cap_retired", "v43/cc10de", false), CI);

        // loadEnabled is exactly what the SubscriptionService cache reloads, so a retired rule stops
        // being evaluated within one refresh interval — no restart, no row deletion, audit intact.
        assertThat(repo.loadEnabled()).isEmpty();
        assertThat(count()).isEqualTo(1);
        assertThat(read("cap_retired").registryVersion()).isEqualTo("v43/cc10de");
    }

    @Test
    void upsertAll_appliesEveryRowOfTheSlice() {
        SubscriptionRow persist = new SubscriptionRow(0L, "PLATFORM", Stage.PERSIST, "persist_frca", null,
                "event.additionalData.tenant == \"FRCA\"", "v42/ab99f0", true);

        int applied = repo.upsertAll(List.of(persist, forward("cap_frca", "v42/ab99f0", true)), CI);

        assertThat(applied).isEqualTo(2);
        assertThat(repo.loadEnabled()).extracting(SubscriptionRow::ruleName)
                .containsExactlyInAnyOrder("persist_frca", "cap_frca");
        assertThat(read("persist_frca").controlDagId()).isNull(); // PERSIST has no target DAG
    }

    // --- fixtures -----------------------------------------------------------------------------------

    private static SubscriptionRow forward(String ruleName, String registryVersion, boolean enabled) {
        return new SubscriptionRow(0L, "CAPITAL", Stage.FORWARD, ruleName,
                "orchestration_control_dag_capital", "event.additionalData.tenant == \"FRCA\"",
                registryVersion, enabled);
    }

    private record Row(long id, String tenantId, String stage, String controlDagId, String whenCel,
            String registryVersion, boolean enabled, Instant updatedAt, String updatedBy) {
    }

    private static Row read(String ruleName) {
        return jdbcClient().sql("""
                        SELECT id, tenant_id, stage, control_dag_id, when_cel, registry_version, enabled,
                               updated_at, updated_by
                        FROM subscription WHERE rule_name = ?""")
                .param(ruleName)
                .query((rs, n) -> new Row(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getBoolean(7),
                        rs.getTimestamp(8).toInstant(), rs.getString(9)))
                .single();
    }

    private static Integer count() {
        return jdbcClient().sql("SELECT count(*) FROM subscription").query(Integer.class).single();
    }
}
