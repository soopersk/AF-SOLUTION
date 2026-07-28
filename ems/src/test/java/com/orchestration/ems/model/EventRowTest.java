package com.orchestration.ems.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

/**
 * Unit test (Surefire, no Docker) for {@link EventRow}: the id/context-id keys come from the real
 * sample payloads ({@code samples/event_*.json}) and {@code rawJson} is preserved byte-verbatim.
 */
class EventRowTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void of_extractsEventId_andPreservesRawJsonByteVerbatim() {
        String raw = load("/samples/event_meg_started.json");

        EventRow row = EventRow.of(raw, MAPPER);

        // "id" is the event id key (samples/event_meg_started.json:2).
        assertThat(row.eventId()).isEqualTo("evt-meg-1");
        // rawJson must be the exact input, byte-for-byte — never a re-serialization of parsed.
        assertThat(row.rawJson()).isEqualTo(raw);
        assertThat(row.rawJson().getBytes(StandardCharsets.UTF_8))
                .isEqualTo(raw.getBytes(StandardCharsets.UTF_8));
        assertThat(row.parsed().get("id").asText()).isEqualTo("evt-meg-1");
    }

    @Test
    void contextId_extractsEventContextIdKey() {
        String raw = load("/samples/event_meg_started.json");

        // "contextId" is the event's context id key (samples/event_meg_started.json:3).
        assertThat(EventRow.of(raw, MAPPER).contextId()).isEqualTo("ctx-200");
    }

    @Test
    void of_yieldsNullEventId_whenPayloadHasNoScalarId() {
        // NOT-NULL is enforced at persist, not here — an absent id yields null.
        assertThat(EventRow.of("{\"source\":\"x\"}", MAPPER).eventId()).isNull();
    }

    private static String load(String resource) {
        try (InputStream in = EventRowTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
