package com.orchestration.ems.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * One context as persisted: its app-supplied id, the exact bytes received, and the parsed tree.
 * The {@code context} table PK is the payload's own {@code "id"} (ems-design §5); {@code rawJson}
 * is kept byte-verbatim.
 *
 * <p>The context id key is {@code "id"}, taken from the real payloads
 * (see {@code samples/context_calc.json}, {@code samples/context_merival.json}).
 *
 * @param contextId app-supplied context id (payload {@code "id"}); {@code null} when the payload has
 *                  no scalar {@code "id"} — NOT-NULL is enforced at persist, not here
 * @param rawJson   the exact JSON text received, preserved byte-for-byte (authoritative form)
 * @param parsed    the parsed context tree; treat as read-only — {@code rawJson} is authoritative
 */
public record ContextRow(String contextId, String rawJson, JsonNode parsed) {

    /**
     * Parse {@code rawJson} and extract the context id, keeping {@code rawJson} byte-verbatim.
     *
     * @param rawJson the exact JSON text received
     * @param mapper  Jackson mapper used to parse (not to re-serialize {@code rawJson})
     * @return the row
     * @throws IllegalArgumentException if {@code rawJson} is not valid JSON
     */
    public static ContextRow of(String rawJson, ObjectMapper mapper) {
        JsonNode parsed = parse(rawJson, mapper);
        return new ContextRow(parsed.path("id").asText(null), rawJson, parsed);
    }

    private static JsonNode parse(String rawJson, ObjectMapper mapper) {
        try {
            return mapper.readTree(rawJson);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("context payload is not valid JSON", e);
        }
    }
}
