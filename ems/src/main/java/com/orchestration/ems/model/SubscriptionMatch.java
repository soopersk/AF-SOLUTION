package com.orchestration.ems.model;

/**
 * A FORWARD stage-2 hit: an enriched event matched a tenant's routing rule. Each match later
 * becomes one outbox row and one {@code FORWARDED} routing_decision targeting {@code controlDagId}.
 *
 * @param tenantId        owning orchestration team of the matched FORWARD rule
 * @param controlDagId    target control DAG for the forward
 * @param ruleName        the matched rule's name (for provenance / routing_decision)
 * @param registryVersion the matched subscription's registry version (e.g. {@code seed-0}); written to
 *                        {@code routing_decision.registry_version} so each FORWARDED verdict records the
 *                        rule provenance that produced it
 */
public record SubscriptionMatch(String tenantId, String controlDagId, String ruleName,
        String registryVersion) {
}
