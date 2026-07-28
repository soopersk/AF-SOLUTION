package com.orchestration.ems.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

/**
 * Unit test (Surefire, no Docker) for {@link ContextRow}: the id key ({@code "id"}) comes from the
 * real context payload ({@code samples/context_calc.json}) and {@code rawJson} is byte-verbatim.
 */
class ContextRowTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void of_extractsContextId_andPreservesRawJsonByteVerbatim() {
        String raw = load("/samples/context_calc.json");

        ContextRow row = ContextRow.of(raw, MAPPER);

        // "id" is the context id key (samples/context_calc.json:1).
        assertThat(row.contextId()).isEqualTo("ctx-200");
        assertThat(row.rawJson()).isEqualTo(raw);
        assertThat(row.rawJson().getBytes(StandardCharsets.UTF_8))
                .isEqualTo(raw.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void of_yieldsNullContextId_whenPayloadHasNoScalarId() {
        // NOT-NULL is enforced at persist, not here — an absent id yields null.
        assertThat(ContextRow.of("{\"data\":{}}", MAPPER).contextId()).isNull();
    }

    private static String load(String resource) {
        try (InputStream in = ContextRowTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
