package com.orchestration.ems.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The read-path pairing of a stored event with its stored context, serialized to the wire as
 * {@code {"event": …, "context": …}} — the byte-compatible shape of the legacy Scala
 * {@code EnrichedEvent(event, context)} case class (old-ems/DatabaseEventRepository.scala:37-41).
 *
 * <p>Both nodes come <b>straight from the stored {@code json} columns</b> (parsed, then re-serialized by
 * Jackson), so the query API's wire format matches today's — normalization never touches the stored
 * payload (ems-design §4.3/§4.4). Either side may be {@code null}: an outer join can yield an event with
 * no matching context (or vice versa), and a stored payload that fails to parse maps to {@code null},
 * exactly as the legacy {@code Try(...).getOrElse(null)} row mapper did. A {@code null} field serializes
 * as {@code "context": null} (Jackson default, no {@code NON_NULL}) — matching the legacy default.
 *
 * @param event   the event tree, or {@code null}
 * @param context the context tree, or {@code null}
 */
public record EnrichedEventView(JsonNode event, JsonNode context) {
}
