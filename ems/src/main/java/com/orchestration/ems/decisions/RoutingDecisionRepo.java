package com.orchestration.ems.decisions;

import java.util.Collection;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.orchestration.ems.model.DecisionRecord;

/**
 * Write path into {@code routing_decision} (ems-design §5 / trigger-plan §4.5), covering both halves of
 * the audit trail:
 * <ul>
 *   <li>{@link #insertL0} / {@link #insertL0Batch} — the L0 verdicts EMS decides itself inside the ingest
 *       transaction ({@code tier='L0_SUBSCRIPTION'}, {@code decided_by='ems'} are fixed).</li>
 *   <li>{@link #insert} / {@link #insertBatch} — any tier posted by a dispatcher or heartbeat via
 *       {@code POST /decisions} (Phase B onward), where tier, decision and {@code decided_by} all come
 *       from the caller.</li>
 * </ul>
 *
 * <p>Both paths share one {@code ON CONFLICT … WHERE tier = 'L0_SUBSCRIPTION' DO NOTHING} clause, which
 * infers the {@code ux_rd_l0} partial unique index (V4). Because that index covers only L0 rows, the
 * clause makes L0 writes idempotent under Kafka redelivery <em>and</em> under a caller retry, while rows
 * of any other tier can never conflict with it and simply append. That is the correct semantics: a gate
 * evaluation at 09:00 and the same verdict at 09:05 are two distinct facts, not a duplicate.
 */
@Repository
public class RoutingDecisionRepo {

    private static final String INSERT_L0 = """
            INSERT INTO routing_decision
                (event_id, tenant_id, tier, target_dag_id, decision, detail, registry_version, engine_version, decided_by)
            VALUES (?, ?, 'L0_SUBSCRIPTION', ?, ?, ?::jsonb, ?, ?, 'ems')
            ON CONFLICT (event_id, tenant_id) WHERE tier = 'L0_SUBSCRIPTION' DO NOTHING
            """;

    private static final String INSERT = """
            INSERT INTO routing_decision
                (event_id, tenant_id, tier, target_dag_id, decision, detail, registry_version, engine_version, decided_by)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
            ON CONFLICT (event_id, tenant_id) WHERE tier = 'L0_SUBSCRIPTION' DO NOTHING
            """;

    private final JdbcClient jdbc;

    public RoutingDecisionRepo(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Insert one L0 subscription verdict. Idempotent under Kafka redelivery via {@code ux_rd_l0}
     * ({@code (event_id, tenant_id)} where {@code tier='L0_SUBSCRIPTION'}): a duplicate is a no-op.
     *
     * @return 1 when inserted, 0 when an existing row made it a no-op
     */
    public int insertL0(L0Decision d) {
        return jdbc.sql(INSERT_L0)
                .param(d.eventId())
                .param(d.tenantId())
                .param(d.targetDagId())
                .param(d.decision().name())
                .param(d.detailJson())
                .param(d.registryVersion())
                .param(d.engineVersion())
                .update();
    }

    /**
     * Insert the FORWARD fan-out for one event as a batch of L0 verdicts; each row is individually
     * idempotent (partial-index dedup), so a redelivery re-inserts nothing.
     *
     * @return the number of rows actually inserted (0..n)
     */
    public int insertL0Batch(Collection<L0Decision> decisions) {
        int inserted = 0;
        for (L0Decision d : decisions) {
            inserted += insertL0(d);
        }
        return inserted;
    }

    /**
     * Insert one caller-supplied decision record of <b>any</b> tier ({@code POST /decisions}).
     *
     * @param d         the record; assumed already validated ({@link DecisionRecord#isValid()})
     * @param decidedBy the posting identity — batch-level, never taken from the record itself
     * @return 1 when inserted, 0 when an L0 duplicate made it a no-op
     */
    public int insert(DecisionRecord d, String decidedBy) {
        return jdbc.sql(INSERT)
                .param(d.eventId())
                .param(d.tenantId())
                .param(d.tier())
                .param(d.targetDagId())
                .param(d.decision())
                .param(d.detailJson())
                .param(d.registryVersion())
                .param(d.engineVersion())
                .param(decidedBy)
                .update();
    }

    /**
     * Persist a posted batch atomically and report how many rows it produced.
     *
     * <p>All-or-nothing: a mid-batch database failure rolls the whole batch back, so the caller's
     * retry-then-alert loop (trigger-plan §768 — "audit never blocks dispatch") re-posts a batch that was
     * either wholly written or wholly absent, and never accumulates half-duplicated audit rows. Note that
     * this transaction is Spring-proxy-provided: constructing the repository directly (as the ITs do)
     * gets per-statement autocommit instead.
     *
     * @return the number of rows actually inserted (0..n; short of {@code n} when L0 duplicates were
     *         absorbed by {@code ux_rd_l0})
     */
    @Transactional
    public int insertBatch(Collection<DecisionRecord> records, String decidedBy) {
        int inserted = 0;
        for (DecisionRecord d : records) {
            inserted += insert(d, decidedBy);
        }
        return inserted;
    }
}
