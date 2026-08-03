package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Pins the A15 fold. Every property here is load-bearing for the rule dialect: if keys stop folding,
 * every seed rule silently stops matching; if non-strings start folding, a numeric or boolean field
 * changes type under the rules.
 */
class MatchViewTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void objectKeysAreFolded() {
        JsonNode folded = fold("""
                {"additionalData": {"TYPE": "INGESTION", "batchType": "INTRA"}}
                """);

        assertThat(folded.has("additionaldata")).isTrue();
        assertThat(folded.get("additionaldata").has("type")).isTrue();
        assertThat(folded.get("additionaldata").has("batchtype")).isTrue();
    }

    @Test
    void stringValuesAreFolded() {
        JsonNode folded = fold("""
                {"source": "MERIVAL", "state": "FINISH"}
                """);

        assertThat(folded.get("source").textValue()).isEqualTo("merival");
        assertThat(folded.get("state").textValue()).isEqualTo("finish");
    }

    /**
     * Numbers, booleans and nulls carry through untouched. Folding a number would mean re-typing it, and
     * no rule compares against one case-insensitively.
     */
    @Test
    void nonStringScalarsAreUntouched() {
        JsonNode folded = fold("""
                {"count": 42, "ratio": 1.5, "enabled": true, "missing": null}
                """);

        assertThat(folded.get("count").intValue()).isEqualTo(42);
        assertThat(folded.get("ratio").doubleValue()).isEqualTo(1.5);
        assertThat(folded.get("enabled").booleanValue()).isTrue();
        assertThat(folded.get("missing").isNull()).isTrue();
    }

    @Test
    void nestedObjectsAndArraysAreFoldedElementWise() {
        JsonNode folded = fold("""
                {"Data": {"Parents": ["CTX-1", "CTX-2"],
                          "Nested": [{"Key": "VALUE"}],
                          "Mixed": ["TEXT", 7, false]}}
                """);

        JsonNode data = folded.get("data");
        assertThat(data.get("parents").get(0).textValue()).isEqualTo("ctx-1");
        assertThat(data.get("nested").get(0).get("key").textValue()).isEqualTo("value");
        assertThat(data.get("mixed").get(0).textValue()).isEqualTo("text");
        assertThat(data.get("mixed").get(1).intValue()).isEqualTo(7);
        assertThat(data.get("mixed").get(2).booleanValue()).isFalse();
    }

    /**
     * A8's real payload shape: {@code TYPE} and {@code type} both live in production. Folding merges
     * them, and the survivor is the last in document order — the same answer the legacy
     * case-insensitive map lookup gave.
     */
    @Test
    void aKeyCollisionResolvesLastWinsInDocumentOrder() {
        JsonNode folded = fold("""
                {"TYPE": "INGESTION", "type": "CALC_EVENT"}
                """);

        assertThat(folded.size()).isEqualTo(1);
        assertThat(folded.get("type").textValue()).isEqualTo("calc_event");
    }

    // --- A18: hyphen-free aliases, camelCase preferred ---------------------------------------------

    @Test
    void aHyphenatedKeyIsAlsoReachableWithoutItsHyphens() {
        JsonNode folded = fold("""
                {"data": {"run-category": "TOPSIDE_EU", "reporting-date": "2026-08-02"}}
                """);

        JsonNode data = folded.get("data");
        assertThat(data.get("runcategory").textValue()).isEqualTo("topside_eu");
        assertThat(data.get("reportingdate").textValue()).isEqualTo("2026-08-02");
    }

    /** The hyphenated spelling stays reachable too, so an older rule does not silently stop matching. */
    @Test
    void theHyphenatedKeyIsKeptAlongsideItsAlias() {
        JsonNode folded = fold("""
                {"data": {"run-category": "TOPSIDE_EU"}}
                """);

        assertThat(folded.get("data").get("run-category").textValue()).isEqualTo("topside_eu");
    }

    /**
     * The precedence the fold is required to give: where a payload carries both spellings, the camelCase
     * value is what a rule sees. It folds directly onto the alias name, and the alias pass never
     * overwrites a key that is already present.
     */
    @Test
    void camelCaseWinsOverTheHyphenatedSpellingWhenBothArePresent() {
        JsonNode hyphenFirst = fold("""
                {"data": {"run-category": "HYPHENATED", "runCategory": "CAMEL"}}
                """);
        JsonNode camelFirst = fold("""
                {"data": {"runCategory": "CAMEL", "run-category": "HYPHENATED"}}
                """);

        assertThat(hyphenFirst.get("data").get("runcategory").textValue()).isEqualTo("camel");
        assertThat(camelFirst.get("data").get("runcategory").textValue())
                .as("precedence must not depend on which spelling a producer happens to emit first")
                .isEqualTo("camel");
    }

    @Test
    void aliasingIsRecursiveAndAppliesInsideArrays() {
        JsonNode folded = fold("""
                {"parents": [{"business-date": "2026-08-02"}]}
                """);

        assertThat(folded.get("parents").get(0).get("businessdate").textValue()).isEqualTo("2026-08-02");
    }

    @Test
    void theInputTreeIsNotMutated() {
        JsonNode original = parse("""
                {"additionalData": {"TYPE": "INGESTION"}}
                """);

        MatchView.fold(original);

        assertThat(original.has("additionalData")).isTrue();
        assertThat(original.get("additionalData").get("TYPE").textValue()).isEqualTo("INGESTION");
    }

    @Test
    void nullFoldsToNull() {
        assertThat(MatchView.fold(null)).isNull();
    }

    private static JsonNode fold(String json) {
        return MatchView.fold(parse(json));
    }

    private static JsonNode parse(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(json, e);
        }
    }
}
