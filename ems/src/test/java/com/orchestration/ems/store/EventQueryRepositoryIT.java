package com.orchestration.ems.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.EnrichedEventView;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Byte-compat round-trip for {@link EventQueryRepository} over a real PostgreSQL (ems-design §4.3;
 * legacy old-ems/DatabaseEventRepository.scala:26-104). Seeds the {@code samples/} Merival + MEG + CALC
 * payloads and asserts the ported semantics: join-type selection, the A10 four-location OR,
 * {@code |}-multivalue, case-sensitivity / no value canonicalization, and the A9 {@code parentIds}
 * array-containment. Auto-skips locally (no Docker); runs in CI.
 */
class EventQueryRepositoryIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JdbcClient jdbc;
    private EventQueryRepository repo;

    @BeforeEach
    void setUp() {
        DataSource ds = dataSource();
        jdbc = JdbcClient.create(ds);
        jdbc.sql("TRUNCATE event, context").update();
        repo = new EventQueryRepository(jdbc, MAPPER);

        insertEvent("evt-mer-1", "/samples/event_merival_ingestion.json"); // contextId ctx-300, source merival, TYPE INGESTION, STATE FINISH
        insertEvent("evt-meg-1", "/samples/event_meg_started.json");       // contextId ctx-200, STATE START
        insertEvent("evt-calc-1", "/samples/event_calc_complete.json");    // contextId ctx-200, STATE FINISH
        insertContext("ctx-300", "/samples/context_merival.json");         // parentIds ["ctx-200"]
        insertContext("ctx-200", "/samples/context_calc.json");            // parentIds ["ctx-100","ctx-050"]
    }

    @Test
    void eventIdAndContextId_innerJoin_returnsThePair() {
        List<EnrichedEventView> rows = repo.findEvents(
                Optional.of("evt-mer-1"), Optional.of("ctx-300"), Optional.empty(), Map.of());

        assertThat(rows).hasSize(1);
        assertThat(id(rows.get(0).event())).isEqualTo("evt-mer-1");
        assertThat(id(rows.get(0).context())).isEqualTo("ctx-300");
    }

    @Test
    void contextIdOnly_leftOuter_returnsAllEventsForThatContext() {
        List<EnrichedEventView> rows = repo.findEvents(
                Optional.empty(), Optional.of("ctx-200"), Optional.empty(), Map.of());

        assertThat(rows).hasSize(2);
        assertThat(rows).allSatisfy(r -> assertThat(id(r.context())).isEqualTo("ctx-200"));
        assertThat(rows.stream().map(r -> id(r.event())))
                .containsExactlyInAnyOrder("evt-meg-1", "evt-calc-1");
    }

    @Test
    void eventIdOnly_rightOuter_returnsEventWithItsContext() {
        List<EnrichedEventView> rows = repo.findEvents(
                Optional.of("evt-meg-1"), Optional.empty(), Optional.empty(), Map.of());

        assertThat(rows).hasSize(1);
        assertThat(id(rows.get(0).event())).isEqualTo("evt-meg-1");
        assertThat(id(rows.get(0).context())).isEqualTo("ctx-200");
    }

    @Test
    void parentId_arrayContainment_matchesContextsWhoseParentIdsContainIt() {
        // A9: ctx-300's parentIds = ["ctx-200"] → parent_id=ctx-200 selects ctx-300 → its event evt-mer-1
        List<EnrichedEventView> rows = repo.findEvents(
                Optional.empty(), Optional.empty(), Optional.of("ctx-200"), Map.of());

        assertThat(rows).hasSize(1);
        assertThat(id(rows.get(0).event())).isEqualTo("evt-mer-1");
        assertThat(id(rows.get(0).context())).isEqualTo("ctx-300");
    }

    @Test
    void dataParam_matchesInAdditionalDataLocation() {
        // TYPE lives at event.additionalData.TYPE — one of the four OR locations (A10)
        List<EnrichedEventView> rows = repo.findEvents(
                Optional.empty(), Optional.of("ctx-300"), Optional.empty(),
                Map.of("TYPE", List.of("INGESTION")));

        assertThat(rows).hasSize(1);
        assertThat(id(rows.get(0).event())).isEqualTo("evt-mer-1");
    }

    @Test
    void dataParam_multiValueAlternation_matchesEither() {
        // STATE=FINISH|FAILED → evt-calc-1 (FINISH); evt-meg-1 (START) excluded
        List<EnrichedEventView> rows = repo.findEvents(
                Optional.empty(), Optional.of("ctx-200"), Optional.empty(),
                Map.of("STATE", List.of("FINISH", "FAILED")));

        assertThat(rows).hasSize(1);
        assertThat(id(rows.get(0).event())).isEqualTo("evt-calc-1");
    }

    @Test
    void dataParam_caseSensitive_noValueCanonicalization() {
        // raw stored source is "merival" (lowercase, byte-verbatim). A10: case-sensitive, no canonicalization.
        assertThat(repo.findEvents(Optional.empty(), Optional.empty(), Optional.empty(),
                Map.of("source", List.of("merival")))).hasSize(1);
        assertThat(repo.findEvents(Optional.empty(), Optional.empty(), Optional.empty(),
                Map.of("source", List.of("MERIVAL")))).isEmpty();
    }

    private void insertEvent(String eventId, String resource) {
        jdbc.sql("INSERT INTO event (event_id, json) VALUES (?, ?::jsonb)")
                .param(eventId).param(resource(resource)).update();
    }

    private void insertContext(String contextId, String resource) {
        jdbc.sql("INSERT INTO context (context_id, json) VALUES (?, ?::jsonb)")
                .param(contextId).param(resource(resource)).update();
    }

    private static String id(com.fasterxml.jackson.databind.JsonNode node) {
        return node == null ? null : node.get("id").asText();
    }

    private static String resource(String path) {
        try (var in = EventQueryRepositoryIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
