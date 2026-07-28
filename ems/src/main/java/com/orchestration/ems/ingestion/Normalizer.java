package com.orchestration.ems.ingestion;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The single Java authority for value canonicalization on the ingestion path. It mirrors, byte for
 * byte, the SQL functions {@code ems_norm_freq}/{@code ems_norm_region} in the Flyway migration
 * ({@code V1__event_context.sql}) and the legacy Scala context-value unwrap
 * ({@code old-ems/EventFilter.scala:91-103}).
 *
 * <p>These normalized values feed CEL subscription matching and the outgoing Airflow {@code conf}.
 * This class NEVER mutates the stored raw JSONB payload — {@link #unwrapContextValues(JsonNode)},
 * {@link #normalizeEvent(JsonNode)} and {@link #normalizeContext(JsonNode)} each deep-copy their
 * input and return a new tree; the argument is left untouched (the raw payload stays byte-verbatim,
 * ems-design §4.4).
 *
 * <p>Java↔SQL value parity for {@link #normFreq(String)}/{@link #normRegion(String)} is exhaustively
 * asserted against a real PostgreSQL by {@code NormalizerSqlParityIT}.
 */
public final class Normalizer {

    /** Counter name emitted once per actual context-value mutation, tagged {@code field}. */
    static final String MUTATIONS_COUNTER = "ems_normalization_mutations_total";

    /** Legacy unwrap marker: a data entry object carrying this key is replaced by that key's node. */
    private static final String VALUE_FIELD = "value";

    /** Upper-casing map for the plain enumerated fields ({@code state}, {@code type}, {@code taskEventType}). */
    private static final UnaryOperator<String> UPPER = s -> s.toUpperCase(Locale.ROOT);

    private final MeterRegistry meterRegistry;

    /**
     * @param meterRegistry Micrometer registry for the {@value #MUTATIONS_COUNTER} counter
     *                      (constructor injection; a {@code SimpleMeterRegistry} in unit tests)
     */
    public Normalizer(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Canonicalize a frequency value, mirroring SQL {@code ems_norm_freq(raw text)} exactly:
     * upper-cases, maps {@code D→DAILY}, {@code M→MONTHLY}, {@code Q→QUARTERLY} (and their full
     * forms to themselves), and passes any other value through upper-cased. {@code RETURNS NULL ON
     * NULL INPUT} — {@code null} in, {@code null} out.
     *
     * @param raw the raw frequency, or {@code null}
     * @return the canonical frequency, or {@code null} when {@code raw} is {@code null}
     */
    public static String normFreq(String raw) {
        if (raw == null) {
            return null;
        }
        String upper = raw.toUpperCase(Locale.ROOT);
        switch (upper) {
            case "D":
            case "DAILY":
                return "DAILY";
            case "M":
            case "MONTHLY":
                return "MONTHLY";
            case "Q":
            case "QUARTERLY":
                return "QUARTERLY";
            default:
                return upper;
        }
    }

    /**
     * Canonicalize a region value, mirroring SQL {@code ems_norm_region(raw text)} exactly:
     * upper-cases and maps {@code AMERICAS→AMER}; any other value passes through upper-cased.
     * {@code RETURNS NULL ON NULL INPUT} — {@code null} in, {@code null} out.
     *
     * @param raw the raw region, or {@code null}
     * @return the canonical region, or {@code null} when {@code raw} is {@code null}
     */
    public static String normRegion(String raw) {
        if (raw == null) {
            return null;
        }
        String upper = raw.toUpperCase(Locale.ROOT);
        return "AMERICAS".equals(upper) ? "AMER" : upper;
    }

    /**
     * Apply the legacy context-value unwrap to a copy of {@code context}: for each entry under the
     * {@code data} object whose value is a JSON object containing a {@code "value"} field, replace
     * that entry with the {@code "value"} node. Non-object values, or objects lacking {@code
     * "value"}, are left unchanged (old-ems/EventFilter.scala:91-103).
     *
     * <p>The input is never mutated — a deep copy is returned. Each actual replacement increments
     * the {@value #MUTATIONS_COUNTER} counter tagged {@code field=<dataKey>}.
     *
     * @param context a context tree (typically a JSON object with a {@code data} object); may be any
     *                node — non-objects and objects without a {@code data} object are returned as an
     *                unchanged deep copy
     * @return a new, deep-copied tree with {@code data.{key}.value} entries unwrapped
     */
    public JsonNode unwrapContextValues(JsonNode context) {
        if (context == null) {
            return null;
        }
        JsonNode copy = context.deepCopy();
        if (!(copy instanceof ObjectNode copyObj)) {
            return copy;
        }
        JsonNode dataNode = copyObj.get("data");
        if (!(dataNode instanceof ObjectNode data)) {
            return copy;
        }
        // Snapshot the keys first: we mutate `data` in place on the copy while iterating.
        List<String> keys = new ArrayList<>();
        data.fieldNames().forEachRemaining(keys::add);
        for (String key : keys) {
            JsonNode value = data.get(key);
            if (value instanceof ObjectNode obj && obj.has(VALUE_FIELD)) {
                data.set(key, obj.get(VALUE_FIELD));
                meterRegistry.counter(MUTATIONS_COUNTER, "field", key).increment();
            }
        }
        return copy;
    }

    /**
     * Canonicalize the enumerated EVENT fields on a copy of {@code event} (ems-design §4.4). Upper-cases
     * the string values at the exact paths the SQL generated columns read
     * ({@code V1__event_context.sql}):
     * <ul>
     *   <li>{@code additionalData.STATE} &mdash; tagged {@code field=state}</li>
     *   <li>{@code additionalData.TYPE}, else {@code additionalData.type} (A8: both spellings live in
     *       prod; the {@code TYPE} spelling wins when present) &mdash; tagged {@code field=type}</li>
     *   <li>{@code additionalData.taskEventType}, else top-level {@code taskEventType} (A7) &mdash;
     *       tagged {@code field=taskEventType}</li>
     * </ul>
     * Only textual values are touched; absent keys and non-textual values are left unchanged. The input
     * is never mutated (deep copy). Region/frequency live on the context payload — see
     * {@link #normalizeContext(JsonNode)}.
     *
     * @param event an event payload node; non-objects are returned as an unchanged deep copy
     * @return a new, deep-copied event with enumerated fields upper-cased
     */
    public JsonNode normalizeEvent(JsonNode event) {
        if (event == null) {
            return null;
        }
        JsonNode copy = event.deepCopy();
        if (!(copy instanceof ObjectNode obj)) {
            return copy;
        }
        ObjectNode additionalData = obj.get("additionalData") instanceof ObjectNode ad ? ad : null;
        if (additionalData != null) {
            normalizeTextField(additionalData, "STATE", UPPER, "state");
            if (additionalData.hasNonNull("TYPE")) {
                normalizeTextField(additionalData, "TYPE", UPPER, "type");
            } else {
                normalizeTextField(additionalData, "type", UPPER, "type");
            }
            if (additionalData.hasNonNull("taskEventType")) {
                normalizeTextField(additionalData, "taskEventType", UPPER, "taskEventType");
            } else {
                normalizeTextField(obj, "taskEventType", UPPER, "taskEventType");
            }
        } else {
            normalizeTextField(obj, "taskEventType", UPPER, "taskEventType");
        }
        return copy;
    }

    /**
     * Canonicalize the CONTEXT payload on a copy of {@code context} (ems-design §4.4): first apply the
     * legacy {@code data.{key}.value} unwrap ({@link #unwrapContextValues(JsonNode)}), then canonicalize
     * the enumerated context values at the paths the SQL generated columns read:
     * <ul>
     *   <li>{@code data.frequency} via {@link #normFreq(String)} &mdash; tagged {@code field=frequency}</li>
     *   <li>{@code data.h3Region}, else {@code data.regionCode} (A8), via {@link #normRegion(String)}
     *       &mdash; tagged {@code field=region}</li>
     * </ul>
     * The unwrap runs first so a {@code {"value": ...}} wrapper collapses to a scalar before
     * {@code normFreq}/{@code normRegion} sees it. Only textual values are touched; the input is never
     * mutated (deep copy).
     *
     * @param context a context payload node; non-objects (and objects without a {@code data} object)
     *                are returned as an unwrapped deep copy
     * @return a new, deep-copied context with values unwrapped and frequency/region canonicalized
     */
    public JsonNode normalizeContext(JsonNode context) {
        JsonNode copy = unwrapContextValues(context);
        if (!(copy instanceof ObjectNode obj) || !(obj.get("data") instanceof ObjectNode data)) {
            return copy;
        }
        normalizeTextField(data, "frequency", Normalizer::normFreq, "frequency");
        if (data.hasNonNull("h3Region")) {
            normalizeTextField(data, "h3Region", Normalizer::normRegion, "region");
        } else {
            normalizeTextField(data, "regionCode", Normalizer::normRegion, "region");
        }
        return copy;
    }

    /**
     * Apply {@code fn} to the textual value at {@code parent.key}, replacing it in place (on the copy)
     * and incrementing {@value #MUTATIONS_COUNTER} tagged {@code field=<field>} only when the value
     * actually changed. No-op when the key is absent, non-textual, or already canonical.
     */
    private void normalizeTextField(ObjectNode parent, String key, UnaryOperator<String> fn, String field) {
        JsonNode value = parent.get(key);
        if (value == null || !value.isTextual()) {
            return;
        }
        String original = value.asText();
        String normalized = fn.apply(original);
        if (normalized != null && !normalized.equals(original)) {
            parent.set(key, TextNode.valueOf(normalized));
            meterRegistry.counter(MUTATIONS_COUNTER, "field", field).increment();
        }
    }
}
