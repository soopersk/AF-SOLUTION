package com.orchestration.ems.decisions;

import java.util.Objects;

/**
 * One L0 (subscription-tier) routing verdict that EMS writes to {@code routing_decision} inside the
 * ingest transaction (ems-design §4.2 / §5). {@code tier} is always {@code L0_SUBSCRIPTION} and
 * {@code decided_by} is always {@code ems} — both fixed by {@link RoutingDecisionRepo}; this record
 * carries only the per-verdict fields.
 *
 * <p>Idempotency under Kafka redelivery is by {@code (eventId, tenantId)} via the {@code ux_rd_l0}
 * partial unique index (V4). A {@link Verdict#NOT_SUBSCRIBED} verdict has a {@code null} tenant, so
 * its redelivery-dedup is provided by the upstream event-insert guard, not this index (§4.2).
 *
 * @param eventId         the event this verdict is about (required)
 * @param tenantId        the matched subscription's tenant; {@code null} for {@code NOT_SUBSCRIBED}
 * @param targetDagId     the control DAG the event is forwarded to; {@code null} for {@code NOT_SUBSCRIBED}
 * @param decision        {@link Verdict#FORWARDED} or {@link Verdict#NOT_SUBSCRIBED} (required)
 * @param detailJson      optional audit detail as a JSON string (matched clause, counts); may be {@code null}
 * @param registryVersion the subscription registry version that produced the match (e.g. {@code seed-0})
 * @param engineVersion   the CEL engine version used (recomputability anchor, e.g. {@code cel-java==0.4.4})
 */
public record L0Decision(
        String eventId,
        String tenantId,
        String targetDagId,
        Verdict decision,
        String detailJson,
        String registryVersion,
        String engineVersion) {

    /** The L0 verdicts EMS emits (the broader {@code routing_decision.decision} vocabulary is L1+/Phase B). */
    public enum Verdict { FORWARDED, NOT_SUBSCRIBED }

    public L0Decision {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(decision, "decision");
    }

    /** A {@code FORWARDED} verdict for one matched subscription (tenant + target control DAG). */
    public static L0Decision forwarded(String eventId, String tenantId, String targetDagId,
                                       String detailJson, String registryVersion, String engineVersion) {
        return new L0Decision(eventId, tenantId, targetDagId, Verdict.FORWARDED,
                detailJson, registryVersion, engineVersion);
    }

    /** A {@code NOT_SUBSCRIBED} verdict — no tenant/target; records that the event matched no FORWARD rule. */
    public static L0Decision notSubscribed(String eventId, String detailJson, String engineVersion) {
        return new L0Decision(eventId, null, null, Verdict.NOT_SUBSCRIBED, detailJson, null, engineVersion);
    }
}
