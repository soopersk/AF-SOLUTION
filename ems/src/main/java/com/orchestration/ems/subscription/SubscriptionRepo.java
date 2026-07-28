package com.orchestration.ems.subscription;

import java.util.Collection;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;

/**
 * Reads and writes Level-0 subscriptions in the {@code subscription} table (V3) as
 * {@link SubscriptionRow}s:
 * <ul>
 *   <li>{@link #loadEnabled()} — the runtime read. The service compiles + caches these; this repo only
 *       loads.</li>
 *   <li>{@link #upsert} / {@link #upsertAll} — the {@code PUT /admin/subscriptions} write path
 *       (ems-design §4.5:221), which the registry CI owns from trigger-plan Phase B onward.</li>
 * </ul>
 *
 * <p>Uses {@link JdbcClient} (constructor-injected, so it is both a Spring {@code @Repository} bean and
 * directly constructible in ITs), consistent with the other Phase 2 repositories.
 */
@Repository
public class SubscriptionRepo {

    private static final String LOAD_ENABLED = """
            SELECT id, tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled
            FROM subscription
            WHERE enabled = true
            """;

    private static final String UPSERT = """
            INSERT INTO subscription
                (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled,
                 updated_at, updated_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, now(), ?)
            ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
                control_dag_id   = EXCLUDED.control_dag_id,
                when_cel         = EXCLUDED.when_cel,
                registry_version = EXCLUDED.registry_version,
                enabled          = EXCLUDED.enabled,
                updated_at       = now(),
                updated_by       = EXCLUDED.updated_by
            """;

    private final JdbcClient jdbc;

    public SubscriptionRepo(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Load every enabled subscription row (both stages). Disabled rows are excluded at the source so the
     * service never compiles or evaluates them.
     *
     * @return the enabled rows, mapped 1:1 from the V3 columns
     */
    public List<SubscriptionRow> loadEnabled() {
        return jdbc.sql(LOAD_ENABLED)
                .query((rs, rowNum) -> new SubscriptionRow(
                        rs.getLong("id"),
                        rs.getString("tenant_id"),
                        Stage.valueOf(rs.getString("stage")),
                        rs.getString("rule_name"),
                        rs.getString("control_dag_id"),
                        rs.getString("when_cel"),
                        rs.getString("registry_version"),
                        rs.getBoolean("enabled")))
                .list();
    }

    /**
     * Insert or replace one subscription, keyed by its natural identity
     * {@code (tenant_id, stage, rule_name)} — the V3 {@code uq_subscription} constraint. The surrogate
     * {@code id} on the passed row is ignored: the registry CI renders rules, it does not track our
     * IDENTITY values, so an update keeps the existing id rather than churning it.
     *
     * <p>{@code updated_at}/{@code updated_by} are stamped on both paths, which is what makes a hand edit
     * distinguishable from a CI render (§7 "hand edits are break-glass, audited via {@code updated_by}").
     *
     * @param row       the rendered rule; assumed already validated (shape + compilable CEL)
     * @param updatedBy the writing identity — batch-level, never taken from the row itself
     * @return 1 (an upsert always applies)
     */
    public int upsert(SubscriptionRow row, String updatedBy) {
        return jdbc.sql(UPSERT)
                .param(row.tenantId())
                .param(row.stage().name())
                .param(row.ruleName())
                .param(row.controlDagId())
                .param(row.whenCel())
                .param(row.registryVersion())
                .param(row.enabled())
                .param(updatedBy)
                .update();
    }

    /**
     * Apply a rendered slice atomically: either the whole slice lands or none of it does, so a failure
     * mid-way can never leave the ruleset in a state that is half of one registry version and half of
     * another (which the 60-second {@link SubscriptionService} refresh would then load and evaluate).
     *
     * <p>As with {@code RoutingDecisionRepo.insertBatch}, the transaction is Spring-proxy-provided:
     * constructing this repository directly (as the ITs do) gets per-statement autocommit instead.
     *
     * @return the number of rows applied (equal to {@code rows.size()})
     */
    @Transactional
    public int upsertAll(Collection<SubscriptionRow> rows, String updatedBy) {
        int applied = 0;
        for (SubscriptionRow row : rows) {
            applied += upsert(row, updatedBy);
        }
        return applied;
    }
}
