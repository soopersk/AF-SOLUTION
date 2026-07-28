package com.orchestration.ems.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * One upstream event as persisted: its app-supplied id, the exact bytes received, and the parsed
 * tree. {@code rawJson} is kept byte-verbatim (never a re-serialization of {@code parsed}) so the
 * stored form matches what upstream sent.
 *
 * <p>The id key is {@code "id"} and the context id key is {@code "contextId"}, both taken from the
 * real payloads (see {@code samples/event_meg_started.json}).
 *
 * @param eventId app-supplied event id (payload {@code "id"}); {@code null} when the payload has no
 *                scalar {@code "id"} — NOT-NULL is enforced at persist, not here
 * @param rawJson the exact JSON text received, preserved byte-for-byte (authoritative form)
 * @param parsed  the parsed event tree; treat as read-only — {@code rawJson} is authoritative
 */
public record EventRow(String eventId, String rawJson, JsonNode parsed) {

    /**
     * Parse {@code rawJson} and extract the event id, keeping {@code rawJson} byte-verbatim.
     *
     * @param rawJson the exact JSON text received
     * @param mapper  Jackson mapper used to parse (not to re-serialize {@code rawJson})
     * @return the row
     * @throws IllegalArgumentException if {@code rawJson} is not valid JSON
     */
    public static EventRow of(String rawJson, ObjectMapper mapper) {
        JsonNode parsed = parse(rawJson, mapper);
        return new EventRow(parsed.path("id").asText(null), rawJson, parsed);
    }

    /** @return the event's context id (payload {@code "contextId"}), or {@code null} if absent. */
    public String contextId() {
        return parsed.path("contextId").asText(null);
    }

    private static JsonNode parse(String rawJson, ObjectMapper mapper) {
        try {
            return mapper.readTree(rawJson);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("event payload is not valid JSON", e);
        }
    }
}
