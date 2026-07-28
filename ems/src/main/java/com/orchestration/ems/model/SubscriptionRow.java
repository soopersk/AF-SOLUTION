package com.orchestration.ems.model;

/**
 * One row of the {@code subscription} table (V3): a tenant-owned CEL rule at a given stage. Mirrors
 * the V3 columns 1:1 (see {@code db/migration/V3__subscription.sql}).
 *
 * <p>{@code PERSIST} is the pre-enrichment drop gate (event.* CEL only); {@code FORWARD} is the
 * post-enrichment routing rule (event.* + context.*) that names a {@code controlDagId}.
 *
 * @param id              DB IDENTITY primary key; {@code 0L} for fixtures / non-DB rows
 * @param tenantId        owning orchestration team (CAPITAL, NSFR, PLATFORM) — NOT the upstream
 *                        {@code additionalData.tenant} label
 * @param stage           {@link Stage#PERSIST} (drop gate) or {@link Stage#FORWARD} (routing)
 * @param ruleName        legacy filter name, unique within {@code (tenantId, stage)}
 * @param controlDagId    target DAG for FORWARD rows; {@code null} for PERSIST
 * @param whenCel         the CEL predicate
 * @param registryVersion provenance tag (e.g. {@code "seed-0"})
 * @param enabled         whether the subscription is active
 */
public record SubscriptionRow(
        long id,
        String tenantId,
        Stage stage,
        String ruleName,
        String controlDagId,
        String whenCel,
        String registryVersion,
        boolean enabled) {

    /** Subscription stage: the drop gate ({@code PERSIST}) or the routing rule ({@code FORWARD}). */
    public enum Stage {
        PERSIST,
        FORWARD
    }
}
