package com.orchestration.ems.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An event paired with its resolved context (or {@code null}). {@link #toConf()} produces the conf
 * that feeds {@code dag_run_id} and the control DAGs.
 *
 * @param event   the event tree (never {@code null})
 * @param context the resolved context tree, or {@code null} if none
 */
public record EnrichedEvent(JsonNode event, JsonNode context) {

    /**
     * Produce the legacy merge shape (old-ems/EventFilter.scala:35-43): the event object with a
     * nested {@code "context"} field when {@code context != null}; no {@code context} key otherwise.
     *
     * <p>Deliberately NO {@code contractVersion} — it is deferred to Phase B (Amendment A5); adding
     * it here would change {@code dag_run_id} and break cutover dedup.
     *
     * @return the conf object node
     * @throws IllegalArgumentException if {@code event} is null or not a JSON object
     */
    public ObjectNode toConf() {
        if (!(event instanceof ObjectNode obj)) {
            throw new IllegalArgumentException("event must be a JSON object to build conf, was: "
                    + (event == null ? "null" : event.getNodeType()));
        }
        ObjectNode conf = obj.deepCopy();
        if (context != null) {
            conf.set("context", context.deepCopy());
        }
        return conf;
    }
}
