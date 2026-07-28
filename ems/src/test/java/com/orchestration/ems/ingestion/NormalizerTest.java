package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit contract for {@link Normalizer}: value canonicalization mirroring the SQL
 * {@code ems_norm_freq}/{@code ems_norm_region} functions, the legacy context-value unwrap, and
 * the {@code ems_normalization_mutations_total} mutation counter. Plain JUnit 5 + AssertJ, no
 * Spring, {@link SimpleMeterRegistry}.
 */
class NormalizerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MeterRegistry registry;
    private Normalizer normalizer;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        normalizer = new Normalizer(registry);
    }

    // ---- normFreq: mirrors ems_norm_freq exactly --------------------------

    @Test
    void normFreq_mapsAllAbbreviationsAndFullFormsCaseInsensitively() {
        assertThat(Normalizer.normFreq("D")).isEqualTo("DAILY");
        assertThat(Normalizer.normFreq("daily")).isEqualTo("DAILY");
        assertThat(Normalizer.normFreq("M")).isEqualTo("MONTHLY");
        assertThat(Normalizer.normFreq("monthly")).isEqualTo("MONTHLY");
        assertThat(Normalizer.normFreq("Q")).isEqualTo("QUARTERLY");
        assertThat(Normalizer.normFreq("quarterly")).isEqualTo("QUARTERLY");
    }

    @Test
    void normFreq_unmappedValuePassesThroughUpperCased() {
        assertThat(Normalizer.normFreq("weekly")).isEqualTo("WEEKLY");
    }

    @Test
    void normFreq_nullInNullOut() {
        assertThat(Normalizer.normFreq(null)).isNull();
    }

    // ---- normRegion: mirrors ems_norm_region exactly ----------------------

    @Test
    void normRegion_americasAliasMapsToAmerRegardlessOfCase() {
        assertThat(Normalizer.normRegion("Americas")).isEqualTo("AMER");
        assertThat(Normalizer.normRegion("AMERICAS")).isEqualTo("AMER");
    }

    @Test
    void normRegion_unmappedValuePassesThroughUpperCased() {
        assertThat(Normalizer.normRegion("emea")).isEqualTo("EMEA");
    }

    @Test
    void normRegion_nullInNullOut() {
        assertThat(Normalizer.normRegion(null)).isNull();
    }

    // ---- context data unwrap ----------------------------------------------

    @Test
    void unwrap_replacesDataEntryWhoseValueIsObjectWithValueField() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"reporting-date\":{\"value\":\"2026-07-17\"}}}");

        JsonNode out = normalizer.unwrapContextValues(ctx);

        assertThat(out.path("data").path("reporting-date").isTextual()).isTrue();
        assertThat(out.path("data").path("reporting-date").asText()).isEqualTo("2026-07-17");
    }

    @Test
    void unwrap_leavesObjectWithoutValueFieldUnchanged() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"meta\":{\"other\":\"x\"}}}");

        JsonNode out = normalizer.unwrapContextValues(ctx);

        assertThat(out.path("data").path("meta").isObject()).isTrue();
        assertThat(out.path("data").path("meta").path("other").asText()).isEqualTo("x");
    }

    @Test
    void unwrap_leavesScalarEntryUnchanged() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"frequency\":\"D\"}}");

        JsonNode out = normalizer.unwrapContextValues(ctx);

        assertThat(out.path("data").path("frequency").asText()).isEqualTo("D");
    }

    @Test
    void unwrap_doesNotMutateInputNode() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"reporting-date\":{\"value\":\"2026-07-17\"}}}");

        normalizer.unwrapContextValues(ctx);

        // original still carries the nested object — proof of deep-copy, no in-place mutation
        assertThat(ctx.path("data").path("reporting-date").isObject()).isTrue();
        assertThat(ctx.path("data").path("reporting-date").path("value").asText())
                .isEqualTo("2026-07-17");
    }

    // ---- mutation counter --------------------------------------------------

    @Test
    void counter_incrementsOncePerActualUnwrap() throws Exception {
        JsonNode ctx = MAPPER.readTree(
                "{\"data\":{\"reporting-date\":{\"value\":\"2026-07-17\"},"
                        + "\"h3Region\":{\"value\":\"AMERICAS\"}}}");

        normalizer.unwrapContextValues(ctx);

        assertThat(totalMutations()).isEqualTo(2.0);
    }

    @Test
    void counter_doesNotIncrementWhenNothingChanges() throws Exception {
        JsonNode ctx = MAPPER.readTree(
                "{\"data\":{\"meta\":{\"other\":\"x\"},\"frequency\":\"D\"}}");

        normalizer.unwrapContextValues(ctx);

        assertThat(totalMutations()).isZero();
    }

    // ---- normalizeEvent: enumerated event fields (§4.4) --------------------

    @Test
    void normalizeEvent_upperCasesStateTypeTaskEventTypeUnderAdditionalData() throws Exception {
        JsonNode ev = MAPPER.readTree(
                "{\"additionalData\":{\"STATE\":\"finish\",\"type\":\"calc_event\","
                        + "\"taskEventType\":\"done\"}}");

        JsonNode ad = normalizer.normalizeEvent(ev).path("additionalData");

        assertThat(ad.path("STATE").asText()).isEqualTo("FINISH");
        assertThat(ad.path("type").asText()).isEqualTo("CALC_EVENT");
        assertThat(ad.path("taskEventType").asText()).isEqualTo("DONE");
    }

    @Test
    void normalizeEvent_typeUsesUppercaseSpellingWhenPresent() throws Exception {
        JsonNode ev = MAPPER.readTree("{\"additionalData\":{\"TYPE\":\"ingestion\"}}");

        JsonNode out = normalizer.normalizeEvent(ev);

        assertThat(out.path("additionalData").path("TYPE").asText()).isEqualTo("INGESTION");
    }

    @Test
    void normalizeEvent_taskEventTypeFallsBackToTopLevel() throws Exception {
        JsonNode ev = MAPPER.readTree("{\"taskEventType\":\"done\"}");

        JsonNode out = normalizer.normalizeEvent(ev);

        assertThat(out.path("taskEventType").asText()).isEqualTo("DONE");
    }

    @Test
    void normalizeEvent_leavesAbsentFieldsAndDoesNotMutateInput() throws Exception {
        JsonNode ev = MAPPER.readTree(
                "{\"additionalData\":{\"STATE\":\"finish\"},\"source\":\"calc\"}");

        JsonNode out = normalizer.normalizeEvent(ev);

        assertThat(out.path("source").asText()).isEqualTo("calc"); // non-enumerated field untouched
        assertThat(ev.path("additionalData").path("STATE").asText()).isEqualTo("finish"); // input intact
        assertThat(totalMutations()).isEqualTo(1.0); // only STATE actually changed
    }

    @Test
    void normalizeEvent_alreadyCanonicalProducesNoMutations() throws Exception {
        JsonNode ev = MAPPER.readTree(
                "{\"additionalData\":{\"STATE\":\"FINISH\",\"type\":\"CALC_EVENT\"}}");

        normalizer.normalizeEvent(ev);

        assertThat(totalMutations()).isZero();
    }

    // ---- normalizeContext: unwrap + frequency/region (§4.4) ----------------

    @Test
    void normalizeContext_canonicalizesFrequencyAndRegion() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"frequency\":\"d\",\"h3Region\":\"americas\"}}");

        JsonNode data = normalizer.normalizeContext(ctx).path("data");

        assertThat(data.path("frequency").asText()).isEqualTo("DAILY");
        assertThat(data.path("h3Region").asText()).isEqualTo("AMER");
    }

    @Test
    void normalizeContext_unwrapsBeforeCanonicalizingFrequency() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"frequency\":{\"value\":\"m\"}}}");

        JsonNode out = normalizer.normalizeContext(ctx);

        assertThat(out.path("data").path("frequency").asText()).isEqualTo("MONTHLY");
    }

    @Test
    void normalizeContext_usesRegionCodeFallbackWhenNoH3Region() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"regionCode\":\"americas\"}}");

        JsonNode out = normalizer.normalizeContext(ctx);

        assertThat(out.path("data").path("regionCode").asText()).isEqualTo("AMER");
    }

    @Test
    void normalizeContext_doesNotMutateInput() throws Exception {
        JsonNode ctx = MAPPER.readTree("{\"data\":{\"frequency\":\"d\"}}");

        normalizer.normalizeContext(ctx);

        assertThat(ctx.path("data").path("frequency").asText()).isEqualTo("d");
    }

    private double totalMutations() {
        return registry.find("ems_normalization_mutations_total").counters().stream()
                .mapToDouble(c -> c.count())
                .sum();
    }
}
