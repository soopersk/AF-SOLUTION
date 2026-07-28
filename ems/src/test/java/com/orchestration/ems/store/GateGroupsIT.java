package com.orchestration.ems.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.GateGroups;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Grouping derivation for {@link GateGroupsRepository} over a real PostgreSQL (ems-design §4.5): the
 * {@code idx_event_created_at} lookback window + criteria residual filter (SQL) feeding in-service json-path
 * extraction of caller-supplied {@code group_by}/{@code contributor} paths over the qualifying set.
 * Auto-skips locally (no Docker); runs in CI.
 */
class GateGroupsIT extends AbstractPostgresIT {

    private static final String REPORTING_DATE = "context.data.reporting-date";
    private static final String COMPANY_CODE = "context.data.companyCode";
    private static final Duration FIVE_DAYS = Duration.ofDays(5);
    private static final Map<String, List<String>> NO_CRITERIA = Map.of();

    private JdbcClient jdbc;
    private GateGroupsRepository repo;

    @BeforeEach
    void setUp() {
        DataSource ds = dataSource();
        jdbc = JdbcClient.create(ds);
        jdbc.sql("TRUNCATE event, context, dlq_record").update();
        repo = new GateGroupsRepository(jdbc, new ObjectMapper());
    }

    @Test
    void groupsByContextPath_collectsContributors() {
        seedContribution("e-amer", "ctx-a", "2026-07-17", "AMER", "FINISH");
        seedContribution("e-asia", "ctx-b", "2026-07-17", "ASIA", "FINISH");
        seedContribution("e-amer2", "ctx-c", "2026-07-18", "AMER", "FINISH");

        GateGroups g = repo.groups(REPORTING_DATE, Optional.of(COMPANY_CODE), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(
                        tuple("2026-07-17", List.of("AMER", "ASIA")),
                        tuple("2026-07-18", List.of("AMER")));
    }

    @Test
    void lookbackWindow_excludesOldEvents() {
        insertContext("ctx-old", context("2026-07-01", "AMER"));
        insertEventAt("e-old", event("ctx-old", "FINISH"), "now() - interval '10 days'");
        seedContribution("e-new", "ctx-a", "2026-07-17", "ASIA", "FINISH");

        GateGroups g = repo.groups(REPORTING_DATE, Optional.of(COMPANY_CODE), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups()).extracting(GateGroups.Group::group).containsExactly("2026-07-17");
    }

    @Test
    void criteriaResidualFilter_narrowsQualifyingSet() {
        seedContribution("e-fin", "ctx-a", "2026-07-17", "AMER", "FINISH");
        seedContribution("e-start", "ctx-b", "2026-07-17", "ASIA", "START");

        // criterion STATE=FINISH matches e.json->'additionalData'->>'STATE' (4-location OR) — START drops out
        GateGroups g = repo.groups(REPORTING_DATE, Optional.of(COMPANY_CODE), FIVE_DAYS,
                Map.of("STATE", List.of("FINISH")));

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(tuple("2026-07-17", List.of("AMER")));
    }

    @Test
    void groupByEventPath_groupsOnEventValue() {
        seedContribution("e-fin", "ctx-a", "2026-07-17", "AMER", "FINISH");
        seedContribution("e-fail", "ctx-b", "2026-07-17", "ASIA", "FAILED");

        GateGroups g = repo.groups("event.additionalData.STATE", Optional.of(COMPANY_CODE),
                FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(
                        tuple("FAILED", List.of("ASIA")),
                        tuple("FINISH", List.of("AMER")));
    }

    @Test
    void unresolvedGroupBy_dropsRow() {
        // context has data but no reporting-date → group_by resolves null → the row belongs to no group
        insertContext("ctx-x", "{\"id\":\"ctx-x\",\"data\":{\"companyCode\":\"AMER\"}}");
        insertEvent("e-x", event("ctx-x", "FINISH"));
        seedContribution("e-ok", "ctx-a", "2026-07-17", "ASIA", "FINISH");

        GateGroups g = repo.groups(REPORTING_DATE, Optional.of(COMPANY_CODE), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups()).extracting(GateGroups.Group::group).containsExactly("2026-07-17");
    }

    @Test
    void nullContributor_groupPresentNoContributor() {
        // reporting-date present (group forms) but companyCode absent → empty contributor set
        insertContext("ctx-nc", "{\"id\":\"ctx-nc\",\"data\":{\"reporting-date\":\"2026-07-17\"}}");
        insertEvent("e-nc", event("ctx-nc", "FINISH"));

        GateGroups g = repo.groups(REPORTING_DATE, Optional.of(COMPANY_CODE), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(tuple("2026-07-17", List.of()));
    }

    @Test
    void bracketNotationPath_resolvesSameAsDotted() {
        seedContribution("e-amer", "ctx-a", "2026-07-17", "AMER", "FINISH");

        GateGroups g = repo.groups("context.data[\"reporting-date\"]",
                Optional.of("context.data[\"companyCode\"]"), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(tuple("2026-07-17", List.of("AMER")));
    }

    @Test
    void distinctContributors_deduped() {
        seedContribution("e-1", "ctx-a", "2026-07-17", "AMER", "FINISH");
        seedContribution("e-2", "ctx-b", "2026-07-17", "AMER", "FINISH");

        GateGroups g = repo.groups(REPORTING_DATE, Optional.of(COMPANY_CODE), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(tuple("2026-07-17", List.of("AMER")));
    }

    @Test
    void noContributorPath_groupsWithEmptyContributors() {
        seedContribution("e-amer", "ctx-a", "2026-07-17", "AMER", "FINISH");

        GateGroups g = repo.groups(REPORTING_DATE, Optional.empty(), FIVE_DAYS, NO_CRITERIA);

        assertThat(g.groups())
                .extracting(GateGroups.Group::group, GateGroups.Group::contributors)
                .containsExactly(tuple("2026-07-17", List.of()));
    }

    // --- fixtures ---------------------------------------------------------------------------------------

    /** Insert a context (reporting-date + companyCode) and one in-window event linked to it. */
    private void seedContribution(String eventId, String contextId, String reportingDate,
            String companyCode, String state) {
        insertContext(contextId, context(reportingDate, companyCode));
        insertEvent(eventId, event(contextId, state));
    }

    private static String context(String reportingDate, String companyCode) {
        return "{\"id\":\"" + companyCode + "\",\"data\":{\"reporting-date\":\"" + reportingDate
                + "\",\"companyCode\":\"" + companyCode + "\"}}";
    }

    private static String event(String contextId, String state) {
        return "{\"id\":\"e\",\"contextId\":\"" + contextId
                + "\",\"additionalData\":{\"STATE\":\"" + state + "\"}}";
    }

    private void insertContext(String contextId, String json) {
        jdbc.sql("INSERT INTO context (context_id, json) VALUES (?, ?::jsonb)")
                .param(contextId).param(json).update();
    }

    private void insertEvent(String eventId, String json) {
        jdbc.sql("INSERT INTO event (event_id, json) VALUES (?, ?::jsonb)")
                .param(eventId).param(json).update();
    }

    /** {@code createdAtExpr} is a test-controlled SQL expression (e.g. {@code now() - interval '10 days'}). */
    private void insertEventAt(String eventId, String json, String createdAtExpr) {
        jdbc.sql("INSERT INTO event (event_id, json, created_at) VALUES (?, ?::jsonb, " + createdAtExpr + ")")
                .param(eventId).param(json).update();
    }
}
