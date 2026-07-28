package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.orchestration.ems.ingestion.Normalizer;
import com.orchestration.ems.model.EnrichedEvent;
import com.orchestration.ems.model.EventRow;
import com.orchestration.ems.model.SubscriptionMatch;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.support.SubscriptionFixtures;

/**
 * Unit proof for the two-stage subscription pipeline against the seed-0 fixtures, using the REAL
 * cel-java engine over in-memory rows (no DB, no Docker — runs locally). Covers the drop gate
 * (PERSIST), the routing fan-out (FORWARD, incl. a context-dependent rule), the A4 PERSIST-cannot-
 * reference-context rejection, and the §7 {@code PERSIST ⊇ FORWARD} invariant on representative events.
 */
class SubscriptionServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CAPITAL_DAG = "orchestration_control_dag_capital";

    private final SubscriptionService service = new SubscriptionService(
            SubscriptionFixtures::seed0, new Normalizer(new SimpleMeterRegistry()), new CelPrograms());

    // --- MERIVAL BATCH INGESTION: persists AND forwards to CAPITAL (identical PERSIST/FORWARD CEL) ---

    @Test
    void merivalBatchIngestion_persists_andForwardsToCapital() throws Exception {
        EventRow event = event("""
                {"id":"evt-mer-1","source":"MERIVAL",
                 "additionalData":{"TYPE":"INGESTION","RUN_TYPE":"BATCH"}}""");

        assertThat(service.persistMatches(event)).isTrue();

        var matches = service.forwardMatches(new EnrichedEvent(event.parsed(), null));
        assertThat(matches).extracting(SubscriptionMatch::ruleName)
                .containsExactly("cap_data_update.MER_batch");
        assertThat(matches).extracting(SubscriptionMatch::tenantId).containsExactly("CAPITAL");
        assertThat(matches).extracting(SubscriptionMatch::controlDagId).containsExactly(CAPITAL_DAG);
    }

    // --- Firehose noise: matches nothing → dropped at the PERSIST gate ---

    @Test
    void nonMatchingFirehoseEvent_doesNotPersist() throws Exception {
        EventRow event = event("""
                {"id":"evt-noise-1","source":"FIREHOSE",
                 "additionalData":{"tenant":"NOBODY","msgTypeEventType":"HEARTBEAT"}}""");

        assertThat(service.persistMatches(event)).isFalse();
        assertThat(service.forwardMatches(new EnrichedEvent(event.parsed(), null))).isEmpty();
    }

    // --- FRCA CURATION: FORWARD depends on context.data["run-category"].startsWith("TOPSIDE") ---

    @Test
    void frcaCuration_forwardsToCapital_onlyWhenRunCategoryIsTopside() throws Exception {
        EventRow event = event("""
                {"id":"evt-frca-1",
                 "additionalData":{"tenant":"FRCA","msgTypeEventType":"DATA-UPDATE","updateType":"CURATION"}}""");

        JsonNode topside = json("""
                {"id":"ctx-1","data":{"run-category":"TOPSIDE_EU"}}""");
        JsonNode other = json("""
                {"id":"ctx-2","data":{"run-category":"OTHER"}}""");

        assertThat(service.forwardMatches(new EnrichedEvent(event.parsed(), topside)))
                .extracting(SubscriptionMatch::ruleName)
                .containsExactly("cap_data_update.FRCA_CURATION");

        assertThat(service.forwardMatches(new EnrichedEvent(event.parsed(), other))).isEmpty();
    }

    @Test
    void frcaCuration_withNullContext_doesNotForward_contextRuleCannotMatch() throws Exception {
        EventRow event = event("""
                {"id":"evt-frca-2",
                 "additionalData":{"tenant":"FRCA","msgTypeEventType":"DATA-UPDATE","updateType":"CURATION"}}""");

        assertThat(service.forwardMatches(new EnrichedEvent(event.parsed(), null))).isEmpty();
    }

    // --- A4: a PERSIST rule referencing context is rejected at compile time ---

    @Test
    void persistRuleReferencingContext_isRejected_A4() {
        SubscriptionRow persistTouchesContext = new SubscriptionRow(
                0L, "PLATFORM", Stage.PERSIST, "bad_persist_uses_context", null,
                "event.source == \"MERIVAL\" && context.data.frequency == \"DAILY\"", "seed-0", true);

        assertThatThrownBy(() -> new CelPrograms().compile(persistTouchesContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("A4");
    }

    @Test
    void forwardRuleMayReferenceContext_compilesFine() {
        SubscriptionRow forwardUsesContext = new SubscriptionRow(
                0L, "CAPITAL", Stage.FORWARD, "fwd_uses_context", CAPITAL_DAG,
                "event.source == \"MERIVAL\" && context.data.frequency == \"DAILY\"", "seed-0", true);

        assertThat(new CelPrograms().compile(forwardUsesContext)).isNotNull();
    }

    // --- §7 invariant: PERSIST ⊇ FORWARD for representative forward-matching events ---

    @Test
    void persistSupersetForward_holdsForRepresentativeEvents() throws Exception {
        // MERIVAL batch (persist_merival_batch ⊇ cap MER_batch — identical CEL)
        EventRow mer = event("""
                {"id":"e-mer","source":"MERIVAL","additionalData":{"TYPE":"INGESTION","RUN_TYPE":"BATCH"}}""");
        assertThat(service.forwardImpliesPersist(mer, null)).isTrue();

        // FRCA curation (persist_frca_tenant ⊇ cap FRCA_CURATION — persist gate is broader)
        EventRow frca = event("""
                {"id":"e-frca",
                 "additionalData":{"tenant":"FRCA","msgTypeEventType":"DATA-UPDATE","updateType":"CURATION"}}""");
        JsonNode topside = json("""
                {"id":"c","data":{"run-category":"TOPSIDE_EU"}}""");
        assertThat(service.forwardImpliesPersist(frca, topside)).isTrue();

        // RWA MR monthly finish (persist_rwa_mr_monthly ⊇ cap_RWA — forward adds STATE==FINISH)
        EventRow rwa = event("""
                {"id":"e-rwa","source":"RWA",
                 "additionalData":{"tenant":"MR","FREQUENCY":"MONTHLY","STATE":"FINISH"}}""");
        assertThat(service.forwardImpliesPersist(rwa, null)).isTrue();
    }

    private static EventRow event(String rawJson) {
        return EventRow.of(rawJson, MAPPER);
    }

    private static JsonNode json(String rawJson) throws JsonProcessingException {
        return MAPPER.readTree(rawJson);
    }
}
