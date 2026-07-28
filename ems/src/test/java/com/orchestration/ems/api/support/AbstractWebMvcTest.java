package com.orchestration.ems.api.support;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared base for the Phase-3 controller {@code @WebMvcTest} slices (ems-design §13 phase 3). Holds a
 * byte-oriented JSON-equality helper so contract assertions compare the response against a golden body
 * as parsed trees — object key order is irrelevant (Jackson/JSONB both canonicalize it), array element
 * order is significant (it carries meaning, e.g. {@code parentIds}).
 *
 * <p>Deliberately <b>not</b> annotated with {@code @WebMvcTest} itself: each concrete slice names the one
 * controller under test. This base only carries the common assertion helper and a mapper.
 */
public abstract class AbstractWebMvcTest {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Assert two JSON documents are equal as trees (object-key-order-insensitive,
     * array-order-sensitive). This is the byte-compat contract oracle at the controller layer.
     */
    protected static void assertJsonEquals(String expected, String actual) {
        assertThat(tree(actual)).isEqualTo(tree(expected));
    }

    private static JsonNode tree(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new AssertionError("not valid JSON: " + json, e);
        }
    }
}
