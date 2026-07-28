package com.orchestration.ems.model;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.orchestration.ems.model.SubscriptionRow.Stage;

/**
 * One rendered subscription row on the {@code PUT /admin/subscriptions} wire (ems-design §4.5:221):
 * "Upsert of rendered subscription rows {@code {tenant, stage, rule_name, control_dag_id, when,
 * registry_version}}". Those six names are the design's, verbatim — including the two that differ from
 * the V3 column names ({@code tenant} → {@code tenant_id}, {@code when} → {@code when_cel}) — because
 * from Phase B the registry CI writes against this shape, and the design sentence is the only pin on it.
 *
 * <p>{@code enabled} is accepted as a seventh, optional field defaulting to {@code true}. The design's
 * list omits it, but V3 has the column and retiring a rule has to be expressible: without it a rule
 * could only ever be added or rewritten, never switched off, and the registry CI would be forced into
 * the direct SQL that §4.5:221 revokes.
 *
 * @param tenant          owning orchestration team — {@code subscription.tenant_id} (required)
 * @param stage           {@code PERSIST} or {@code FORWARD} (required)
 * @param ruleName        legacy filter name, unique within {@code (tenant, stage)} (required)
 * @param controlDagId    target DAG; required for {@code FORWARD} (V3 {@code forward_requires_dag}),
 *                        unused for {@code PERSIST}
 * @param whenCel         the CEL predicate — {@code when} on the wire (required)
 * @param registryVersion provenance tag the registry CI renders (required; V3 {@code NOT NULL})
 * @param enabled         optional, default {@code true}
 */
public record SubscriptionUpsert(
        String tenant,
        String stage,
        @JsonProperty("rule_name") String ruleName,
        @JsonProperty("control_dag_id") String controlDagId,
        @JsonProperty("when") String whenCel,
        @JsonProperty("registry_version") String registryVersion,
        Boolean enabled) {

    /**
     * Convert to the domain row, or empty when this wire row could never be persisted.
     *
     * <p>Rejected here: a missing required field, an unknown {@code stage}, and a {@code FORWARD} row
     * with no {@code control_dag_id} — the last one is the V3 {@code forward_requires_dag} CHECK, caught
     * at the edge so the caller gets a 400 it can fix rather than a 500 from a constraint violation.
     *
     * <p>Deliberately <b>not</b> rejected: a {@code PERSIST} row that carries a {@code control_dag_id}.
     * V3 permits it, PERSIST never reads it, and §4.5:221 names exactly two rejections (uncompilable CEL,
     * and A4) — inventing a third would let this endpoint refuse rows the schema accepts.
     *
     * <p>CEL validity is <em>not</em> checked here: it needs the compiler, so the controller applies
     * {@code CelPrograms} to the row this returns (which is also what enforces A4, structurally).
     */
    public Optional<SubscriptionRow> toRow() {
        if (!hasText(tenant) || !hasText(ruleName) || !hasText(whenCel) || !hasText(registryVersion)) {
            return Optional.empty();
        }
        Optional<Stage> parsed = parseStage();
        if (parsed.isEmpty()) {
            return Optional.empty();
        }
        Stage resolved = parsed.get();
        if (resolved == Stage.FORWARD && !hasText(controlDagId)) {
            return Optional.empty();
        }
        return Optional.of(new SubscriptionRow(0L, tenant.trim(), resolved, ruleName.trim(),
                controlDagId, whenCel, registryVersion.trim(), enabled == null || enabled));
    }

    private Optional<Stage> parseStage() {
        if (stage == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Stage.valueOf(stage.trim()));
        } catch (IllegalArgumentException unknownStage) {
            return Optional.empty();
        }
    }

    private static boolean hasText(String s) {
        return s != null && !s.isBlank();
    }

    /**
     * The {@code PUT /admin/subscriptions} request envelope: {@code {"subscriptions": [ … ]}}.
     *
     * <p>Wrapped rather than a bare array for the same reason as {@code POST /decisions}: room for
     * batch-level fields later. The writing identity is not one of them — V3's {@code updated_by}
     * ("'seed' | 'ci' | break-glass identity") is stamped from the authenticated principal, which is what
     * makes §4.5:221's "CI-only principal" and §7's audited break-glass edit distinguishable at all.
     *
     * @param subscriptions the rendered rows; required, may be empty (an empty slice is not an error)
     */
    public record Batch(List<SubscriptionUpsert> subscriptions) {
    }

    /**
     * The {@code PUT /admin/subscriptions} response: how many rows arrived and how many were applied.
     *
     * <p>Unlike {@code POST /decisions}, {@code upserted} normally equals {@code received} — an upsert
     * either inserts or updates, so nothing is silently absorbed. The count is reported anyway so the
     * caller verifies the whole slice landed rather than assuming it.
     */
    public record Applied(int received, int upserted) {
    }
}
