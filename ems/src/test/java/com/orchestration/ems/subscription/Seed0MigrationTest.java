package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.orchestration.ems.ingestion.Normalizer;
import com.orchestration.ems.model.EventRow;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.support.SubscriptionFixtures;

/**
 * Local (no Docker) proof of {@code db/seed/V6__subscription_seed0.sql}.
 *
 * <p><b>Read this before trusting it.</b> Everything asserted here is a <em>structural</em> property:
 * the migration is well-formed, its 16 rules compile against the real cel-java engine, no PERSIST rule
 * reaches for {@code context.*}, the rows agree with the test fixture, {@code PERSIST ⊇ FORWARD} holds
 * over the sample payloads, and every assumption in the register is referenced from the SQL.
 *
 * <p><b>None of that proves the rows are right.</b> "Right" means these rules route the same events the
 * legacy service routes, and the evidence for that is {@code docs/ems-seed0-assumptions.md} — signed off
 * by a human — plus the §11 stage-1 shadow parity run. A green build here is necessary and nowhere near
 * sufficient.
 *
 * <p>The SQL is parsed as text rather than executed: {@code Seed0MigrationIT} runs it through Flyway
 * against a real PostgreSQL, and that needs Docker. This class must stay runnable without one.
 */
class Seed0MigrationTest {

    private static final Path MIGRATION = Path.of("src/main/resources/db/seed/V6__subscription_seed0.sql");
    private static final Path ASSUMPTIONS = Path.of("../docs/ems-seed0-assumptions.md");
    private static final String SEED_VERSION = "seed-0";

    /** {@code VALUES ( … )} up to the {@code ON CONFLICT} that closes each statement. */
    private static final Pattern VALUES =
            Pattern.compile("VALUES\\s*\\((.*?)\\)\\s*ON CONFLICT", Pattern.DOTALL);

    private static String sql;
    private static List<SubscriptionRow> rows;

    @BeforeAll
    static void parseTheMigration() throws IOException {
        assertThat(MIGRATION).as("the seed migration must live at %s", MIGRATION).exists();
        sql = Files.readString(MIGRATION);
        rows = parseRows(sql);
    }

    @Test
    void theMigrationCarriesExactlySixteenRowsInTheExpectedSplit() {
        assertThat(rows).hasSize(16);
        assertThat(rows).filteredOn(row -> row.stage() == Stage.PERSIST).hasSize(7);
        assertThat(rows).filteredOn(row -> row.stage() == Stage.FORWARD
                && "CAPITAL".equals(row.tenantId())).hasSize(8);
        assertThat(rows).filteredOn(row -> row.stage() == Stage.FORWARD
                && "NSFR".equals(row.tenantId())).hasSize(1);
    }

    /**
     * Compiling exercises the A4 structural guard as a side effect: {@code CelPrograms#compile} rejects
     * a PERSIST rule that references {@code context.*}, so a PERSIST row that reached for enrichment
     * would throw here rather than silently never match in production.
     */
    @Test
    void everyRuleCompilesAndNoPersistRuleReachesForContext() {
        CelPrograms programs = new CelPrograms();

        for (SubscriptionRow row : rows) {
            assertThat(programs.compile(row))
                    .as("%s/%s must compile", row.tenantId(), row.ruleName())
                    .isNotNull();
            if (row.stage() == Stage.PERSIST) {
                assertThat(row.whenCel())
                        .as("%s is a PERSIST rule and must not reference context.*", row.ruleName())
                        .doesNotContain("context.");
            }
        }
    }

    /** A15: one dialect. A stray upper-case literal is a rule that compiles and never matches. */
    @Test
    void everyRuleIsInTheLowercaseDialect() {
        for (SubscriptionRow row : rows) {
            assertThat(row.whenCel())
                    .as("%s must be all-lowercase paths and literals (A15)", row.ruleName())
                    .isEqualTo(row.whenCel().toLowerCase(java.util.Locale.ROOT)
                            // startsWith is the one camelCase token the dialect allows
                            .replace("startswith(", "startsWith("));
        }
    }

    @Test
    void everyRowCarriesTheSeedRegistryVersion() {
        assertThat(rows).allSatisfy(row ->
                assertThat(row.registryVersion()).isEqualTo(SEED_VERSION));
    }

    /**
     * The migration and the test fixture must not disagree. They are two hand-maintained copies of the
     * same 16 rules, and a divergence means every test in this module is validating rules that will not
     * be the ones deployed.
     */
    @Test
    void theMigrationAndTheTestFixtureAreIdentical() {
        List<SubscriptionRow> fixture = SubscriptionFixtures.seed0();

        assertThat(comparable(rows))
                .as("db/seed/V6 and %s must carry the same 16 rules", SubscriptionFixtures.RESOURCE)
                .containsExactlyInAnyOrderElementsOf(comparable(fixture));
    }

    /**
     * ASSUMPTION-8, the load-bearing one. There is no {@code filter.post} property row in the evidence
     * (A12), so an event that matches a FORWARD rule but no PERSIST rule is dropped before enrichment
     * and never routed. Checked here over the sample payloads; the general case needs shadow parity.
     */
    @Test
    void forwardImpliesPersistOverEverySamplePayload() {
        SubscriptionService service = new SubscriptionService(
                () -> rows, new Normalizer(new SimpleMeterRegistry()), new CelPrograms());
        List<JsonNode> contexts = List.of(
                sample("context_merival.json"), sample("context_calc.json"));

        for (String eventFile : List.of("event_merival_ingestion.json", "event_calc_complete.json",
                "event_meg_started.json")) {
            EventRow event = EventRow.of(sampleText(eventFile), new ObjectMapper());
            for (JsonNode context : contexts) {
                assertThat(service.forwardImpliesPersist(event, context))
                        .as("%s forwards but does not persist — it would be dropped before enrichment "
                                + "and never routed (ASSUMPTION-8 / A12)", eventFile)
                        .isTrue();
            }
        }
    }

    /**
     * The register and the migration are two halves of one artifact. If an assumption can be added to
     * the register without the SQL ever mentioning it, the SQL stops being reviewable against it.
     */
    @Test
    void everyAssumptionInTheRegisterIsReferencedFromTheMigration() throws IOException {
        assertThat(ASSUMPTIONS).as("the sign-off register must live at %s", ASSUMPTIONS).exists();
        Set<String> registered = matches(Files.readString(ASSUMPTIONS), "### (ASSUMPTION-\\d+)");
        Set<String> referenced = matches(sql, "(ASSUMPTION-\\d+)");

        assertThat(registered).as("the register must actually contain numbered assumptions").isNotEmpty();
        assertThat(referenced)
                .as("every assumption in the register must be cited by the migration it justifies")
                .containsAll(registered);
    }

    /** Every register entry must still be unticked — a signed-off box here is a review that happened. */
    @Test
    void theRegisterStillHasUntickedSignOffBoxes() throws IOException {
        String register = Files.readString(ASSUMPTIONS);

        assertThat(register)
                .as("the register is a human gate; if this ever fails, seed parity has been signed off "
                        + "and this test should be replaced by a note recording who signed and when")
                .contains("- [ ] Signed off");
    }

    // --- parsing ------------------------------------------------------------------------------------

    /**
     * Splits each {@code VALUES} tuple on top-level commas. Deliberately simple: the migration contains
     * no escaped quotes (every CEL literal uses double quotes), and this test asserts that stays true —
     * a rule that needed an apostrophe would silently mis-parse here.
     */
    private static List<SubscriptionRow> parseRows(String migration) {
        assertThat(migration.replace("''", ""))
                .as("no single-quoted SQL literal may contain an escaped quote; the parser below "
                        + "assumes it and would mis-split the tuple")
                .doesNotContain("\\'");

        List<SubscriptionRow> parsed = new ArrayList<>();
        Matcher statement = VALUES.matcher(migration);
        while (statement.find()) {
            List<String> values = splitTopLevel(statement.group(1));
            assertThat(values).as("each VALUES tuple must carry 8 columns").hasSize(8);
            parsed.add(new SubscriptionRow(
                    0L,
                    unquote(values.get(0)),
                    Stage.valueOf(unquote(values.get(1))),
                    unquote(values.get(2)),
                    unquote(values.get(3)),
                    unquote(values.get(4)),
                    unquote(values.get(5)),
                    "TRUE".equalsIgnoreCase(values.get(6).trim())));
        }
        return parsed;
    }

    private static List<String> splitTopLevel(String tuple) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (char c : tuple.toCharArray()) {
            if (c == '\'') {
                inQuote = !inQuote;
                current.append(c);
            } else if (c == ',' && !inQuote) {
                values.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        values.add(current.toString());
        return values;
    }

    private static String unquote(String value) {
        String trimmed = value.trim();
        if ("NULL".equalsIgnoreCase(trimmed)) {
            return null;
        }
        return trimmed.substring(1, trimmed.length() - 1);
    }

    /** Identity of a rule for comparison purposes — the DB-assigned id is not part of it. */
    private static List<String> comparable(List<SubscriptionRow> subscriptions) {
        return subscriptions.stream()
                .map(row -> String.join("|", row.tenantId(), row.stage().name(), row.ruleName(),
                        String.valueOf(row.controlDagId()), row.whenCel(), row.registryVersion(),
                        String.valueOf(row.enabled())))
                .toList();
    }

    private static Set<String> matches(String text, String regex) {
        Set<String> found = new TreeSet<>();
        Matcher matcher = Pattern.compile(regex).matcher(text);
        while (matcher.find()) {
            found.add(matcher.group(1));
        }
        return found;
    }

    private static JsonNode sample(String name) {
        try {
            return new ObjectMapper().readTree(sampleText(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String sampleText(String name) {
        try (InputStream in = Seed0MigrationTest.class.getResourceAsStream("/samples/" + name)) {
            if (in == null) {
                throw new IllegalStateException("missing sample on the classpath: " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
