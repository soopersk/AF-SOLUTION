package com.orchestration.ems.model;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * One slim decision record posted to {@code POST /decisions} by a dispatcher or heartbeat DAG
 * (ems-design §4.5:219, trigger-plan §8/§765) — the L1/GATE half of the audit trail whose L0 half EMS
 * writes in-process during ingest.
 *
 * <p>Field names are the {@code routing_decision} column names (V4) verbatim, so the audit queries in
 * trigger-plan §7 read the same vocabulary the caller wrote. Every field maps 1:1 to a column except
 * {@code decided_by}, which is <b>not on the wire at all</b>: it is the authenticated caller's identity
 * (§4.5:201), taken from the JWT/Basic principal so the audit trail cannot be self-declared. A
 * {@code decided_by} in the body — as the Python client's
 * {@code post_decision_records(..., decided_by="capital_control_dag")} still sends — is ignored rather
 * than rejected, so that client keeps working unchanged.
 *
 * @param eventId         the event the decision is about (required)
 * @param tenantId        the tenant whose registry produced it; {@code null} where not tenant-scoped
 * @param tier            one of {@link #TIERS} (required)
 * @param targetDagId     the DAG the decision concerns; {@code null} for {@code L1_SUMMARY} (V4:335)
 * @param decision        one of {@link #DECISIONS} (required)
 * @param detail          audit detail (counts / failing clause / missing set / completing ids) — a JSON
 *                        <b>object</b> or absent; stored as {@code jsonb}
 * @param registryVersion the registry version that produced the decision (recomputability anchor)
 * @param engineVersion   the CEL engine version that evaluated it (e.g. {@code celpy==0.x.y})
 */
public record DecisionRecord(
        @JsonProperty("event_id") String eventId,
        @JsonProperty("tenant_id") String tenantId,
        String tier,
        @JsonProperty("target_dag_id") String targetDagId,
        String decision,
        JsonNode detail,
        @JsonProperty("registry_version") String registryVersion,
        @JsonProperty("engine_version") String engineVersion) {

    /** The {@code routing_decision.tier} vocabulary (V4:334). */
    public static final Set<String> TIERS =
            Set.of("L0_SUBSCRIPTION", "L1_SUMMARY", "L1_OUTCOME", "GATE");

    /** The {@code routing_decision.decision} vocabulary (V4:336-337). */
    public static final Set<String> DECISIONS = Set.of(
            "FORWARDED", "NOT_SUBSCRIBED", "MATCHED", "TRIGGERED", "ERROR", "GATE_OPEN", "GATE_WAITING");

    /**
     * Whether this record can be persisted. {@code tier}/{@code decision} are checked against the closed
     * V4 vocabularies rather than passed through: they are {@code text} columns that the §7 audit queries
     * filter on ({@code WHERE tier = 'L1_SUMMARY'}, {@code tier IN (...)}), so a caller-side typo would
     * silently disappear from every audit answer instead of failing where it can be fixed.
     */
    public boolean isValid() {
        return hasText(eventId)
                && tier != null && TIERS.contains(tier)
                && decision != null && DECISIONS.contains(decision)
                && (detail == null || detail.isNull() || detail.isObject());
    }

    /** {@code detail} as a JSON string for the {@code ?::jsonb} bind, or {@code null} when absent. */
    public String detailJson() {
        return (detail == null || detail.isNull()) ? null : detail.toString();
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    /**
     * The {@code POST /decisions} request envelope: {@code {"decisions": [ … ]}}.
     *
     * <p>Neither ems-design nor the trigger plan pins the request shape, so it is wrapped rather than a
     * bare array — which leaves room for batch-level fields without breaking the callers written against
     * this shape. The posting identity is <em>not</em> one of them: it comes from the authenticated
     * principal (see the class javadoc).
     *
     * @param decisions the records to persist; required (may be empty — a no-op batch is not an error)
     */
    public record Batch(List<DecisionRecord> decisions) {
    }

    /**
     * The {@code POST /decisions} response: how many records arrived and how many became rows.
     *
     * <p>{@code inserted < received} is normal, not a failure — an {@code L0_SUBSCRIPTION} record that
     * duplicates an existing verdict is absorbed by {@code ux_rd_l0} (V4), so a caller retry after an
     * ambiguous timeout re-posts safely and simply reports fewer inserts.
     */
    public record Ingested(int received, int inserted) {
    }
}
