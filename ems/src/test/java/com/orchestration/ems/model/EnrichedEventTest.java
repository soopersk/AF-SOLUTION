package com.orchestration.ems.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import com.orchestration.ems.canonical.CanonicalJson;
import com.orchestration.ems.canonical.DagRunId;

/**
 * Unit test (Surefire, no Docker) for {@link EnrichedEvent#toConf()}: it must reproduce the legacy
 * merge shape (event object with a nested {@code "context"} field; no key when context is absent —
 * old-ems/EventFilter.SCALA:35-43) and feed {@link CanonicalJson}/{@link DagRunId} deterministically.
 */
class EnrichedEventTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void toConf_nestsContextUnderEvent() {
        JsonNode event = parse("/samples/event_calc_complete.json");
        JsonNode context = parse("/samples/context_calc.json");

        ObjectNode conf = new EnrichedEvent(event, context).toConf();

        // Conf is the event node with a nested "context" equal to the context node.
        assertThat(conf.get("id").asText()).isEqualTo("evt-calc-1");
        assertThat(conf.get("source").asText()).isEqualTo("calculator");
        assertThat(conf.has("context")).isTrue();
        assertThat(conf.get("context")).isEqualTo(context);
    }

    @Test
    void toConf_omitsContextKeyWhenContextNull() {
        JsonNode event = parse("/samples/event_calc_complete.json");

        ObjectNode conf = new EnrichedEvent(event, null).toConf();

        assertThat(conf.has("context")).isFalse();
        assertThat(conf.get("id").asText()).isEqualTo("evt-calc-1");
    }

    @Test
    void toConf_deepCopies_soMutatingConfDoesNotTouchSourceNodes() {
        JsonNode event = parse("/samples/event_calc_complete.json");
        JsonNode context = parse("/samples/context_calc.json");

        ObjectNode conf = new EnrichedEvent(event, context).toConf();
        // Mutate both the top-level conf and its nested context.
        conf.put("id", "MUTATED");
        ((ObjectNode) conf.get("context")).put("id", "MUTATED");

        // The source nodes must be untouched — toConf() must deep-copy.
        assertThat(event.get("id").asText()).isEqualTo("evt-calc-1");
        assertThat(context.get("id").asText()).isEqualTo("ctx-200");
    }

    @Test
    void toConf_rejectsNonObjectEvent() {
        assertThatThrownBy(() -> new EnrichedEvent(MAPPER.getNodeFactory().textNode("x"), null).toConf())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EnrichedEvent(null, null).toConf())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toConf_feedsDeterministicCanonicalizationAndDagRunId() {
        JsonNode event = parse("/samples/event_calc_complete.json");
        JsonNode context = parse("/samples/context_calc.json");
        EnrichedEvent enriched = new EnrichedEvent(event, context);

        String confA = enriched.toConf().toString();
        String confB = enriched.toConf().toString();
        assertThat(CanonicalJson.canonicalize(confA))
                .isEqualTo(CanonicalJson.canonicalize(confB));

        String runId = DagRunId.derive("orchestration_control_dag_capital", confA);
        assertThat(runId).hasSize(16).matches("[0-9a-f]{16}");
    }

    private static JsonNode parse(String resource) {
        try (InputStream in = EnrichedEventTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + resource);
            }
            return MAPPER.readTree(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
