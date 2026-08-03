package com.orchestration.ems.perf;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.store.EventQueryRepository;
import com.orchestration.ems.store.RunStatusRepository;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * The ems-design §12 performance gate: 10 M events over 1 M contexts, p95 &lt; 50 ms on the read paths,
 * and {@code EXPLAIN} proof that the plans use indexes rather than sequential scans.
 *
 * <p><b>Opt-in.</b> Tagged {@code perf} and excluded from Failsafe by default (see {@code pom.xml});
 * {@code mvn verify -Pperf} runs this and nothing else. It also self-skips without Docker like every
 * other IT, so a box with no container runtime reports "no tests run" rather than a false pass.
 *
 * <p><b>What this asserts, and what amendment A16 stopped it from asserting.</b> §12 asks for a plan
 * using {@code idx_event_task_id} <em>and</em> {@code idx_context_rep_freq_region} on the canonical §4.3
 * query. That query cannot reach either: under A10 it filters raw JSONB in four locations with no
 * param→column alias map, so no promoted column ever appears in a predicate. So:
 * <ul>
 *   <li>{@code idx_event_task_id} is asserted where it is actually reachable — {@code /run/status}.</li>
 *   <li>The id-qualified §4.3 shape is asserted to plan without a sequential scan.</li>
 *   <li>The <b>un</b>qualified §4.3 shape is measured and written to the report <b>without</b> an
 *       assertion. It is a full scan by construction, and asserting 50 ms on it would be asserting that
 *       A10 does not do what A10 says it does.</li>
 *   <li>{@code idx_context_rep_freq_region} is proven sound against a direct promoted-column query, so
 *       the §14 item 1b decision on §4.3 canonicalization has a measured number to work from rather than
 *       an assumption that the index would help.</li>
 * </ul>
 *
 * <p><b>Not measured here:</b> HTTP overhead. That needs an application context, and standing one up
 * would fold Spring Security, Kafka wiring and Jackson into a number §12 asserts about the query. The
 * report says so explicitly rather than leaving a gap to be misread as zero.
 */
@Tag("perf")
class QueryPerfIT extends AbstractPostgresIT {

    /** The §12 budget. */
    private static final Duration BUDGET = Duration.ofMillis(50);

    private static final int WARMUP = 50;
    private static final int ITERATIONS = 500;
    /** The unqualified shape is a full scan of 10 M rows; three samples is all the evidence needed. */
    private static final int FULL_SCAN_ITERATIONS = 3;

    private static final Path REPORT = Path.of("target", "perf-report.txt");
    private static final List<String> REPORT_LINES = new ArrayList<>();

    private static JdbcClient jdbc;
    private static EventQueryRepository events;
    private static RunStatusRepository runs;

    /**
     * One physical connection for the whole class. {@code AbstractPostgresIT#dataSource()} is a
     * {@code SimpleDriverDataSource}, which opens a fresh connection per query — across 500 iterations
     * that measures TCP and authentication, not the plan. Production runs a HikariCP pool, so a held
     * connection is the closer analogue.
     */
    @BeforeAll
    static void seedAndWire() throws SQLException {
        new PerfSeeder(dataSource()).seed();

        SingleConnectionDataSource held = new SingleConnectionDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(), true);
        jdbc = JdbcClient.create(held);
        events = new EventQueryRepository(jdbc, new ObjectMapper());
        runs = new RunStatusRepository(jdbc);

        record("dataset: %d events over %d contexts (%d events/context, %d events/task)"
                .formatted(PerfSeeder.EVENTS, PerfSeeder.CONTEXTS,
                        PerfSeeder.EVENTS_PER_CONTEXT, PerfSeeder.EVENTS_PER_TASK));
    }

    @AfterAll
    static void writeReport() throws IOException {
        Files.createDirectories(REPORT.getParent());
        Files.write(REPORT, REPORT_LINES);
    }

    // --- the two budgeted paths ---------------------------------------------------------------------

    @Test
    void runStatusByTaskId_meetsTheP95Budget() {
        long p95 = measure("run/status by task_id",
                i -> runs.summarize(Optional.empty(), Optional.of(PerfSeeder.taskId(i))),
                WARMUP, ITERATIONS);

        assertThat(p95)
                .as("§12: /run/status p95 must be under %s ms", BUDGET.toMillis())
                .isLessThan(BUDGET.toMillis() * 1_000);
    }

    @Test
    void canonicalEventQuery_idQualified_meetsTheP95Budget() {
        long p95 = measure("event query, context_id-qualified (§4.3 canonical shape)",
                i -> events.findEvents(Optional.empty(), Optional.of(PerfSeeder.contextId(i)),
                        Optional.empty(), Map.of("STATE", List.of("COMPLETE"))),
                WARMUP, ITERATIONS);

        assertThat(p95)
                .as("§12: the canonical §4.3 query p95 must be under %s ms when an id narrows it",
                        BUDGET.toMillis())
                .isLessThan(BUDGET.toMillis() * 1_000);
    }

    /**
     * The A16 number. No assertion: with no id predicate the four-location OR has nothing to index
     * against, and a budget here would be a budget on A10 being false.
     */
    @Test
    void canonicalEventQuery_unqualified_isMeasuredNotBudgeted() {
        long p95 = measure("event query, UNQUALIFIED (A16: full scan by construction, not budgeted)",
                i -> events.findEvents(Optional.empty(), Optional.empty(), Optional.empty(),
                        Map.of("taskId", List.of(PerfSeeder.taskId(i)))),
                1, FULL_SCAN_ITERATIONS);

        record("  ^ recorded for the §12 before/after evidence; see amendment A16 for why it is not a gate");
        assertThat(p95).as("the measurement itself must have run").isNotNegative();
    }

    // --- plan shape ---------------------------------------------------------------------------------

    @Test
    void runStatusPlanUsesTheTaskIdIndex() {
        JsonNode plan = explain(
                "SELECT event_id, state, task_event_type, created_at FROM event WHERE task_id = '%s'"
                        .formatted(PerfSeeder.taskId(1)));

        assertThat(usesIndex(plan, "idx_event_task_id"))
                .as("§12: /run/status must ride idx_event_task_id, not scan 10 M rows. Plan:%n%s", plan)
                .isTrue();
        assertNoSeqScan(plan, "event");
    }

    @Test
    void idQualifiedEventQueryPlanHasNoSequentialScan() {
        JsonNode plan = explain("""
                SELECT e.json, c.json FROM context c LEFT OUTER JOIN event e
                  ON c.context_id = e.context_id
                WHERE c.context_id = '%s'
                """.formatted(PerfSeeder.contextId(1)));

        assertNoSeqScan(plan, "event");
        assertNoSeqScan(plan, "context");
    }

    /**
     * A16's other half: the composite index is sound, it is simply unreachable from the read path as
     * A10 leaves it. Proving that here means the §14 item 1b decision is made against a measurement.
     */
    @Test
    void theContextCompositeIndexIsSoundEvenThoughNoReadPathReachesIt() {
        JsonNode plan = explain("""
                SELECT context_id FROM context
                WHERE reporting_date = '2026-03-01' AND frequency = 'DAILY'
                """);

        assertThat(usesIndex(plan, "idx_context_rep_freq_region"))
                .as("the promoted-column predicate must ride the composite index. Plan:%n%s", plan)
                .isTrue();
        record("idx_context_rep_freq_region: reachable from promoted columns, unreachable from /event (A16)");
    }

    // --- machinery ----------------------------------------------------------------------------------

    private interface Iteration {
        void run(long i);
    }

    /**
     * Runs {@code warmup} discarded iterations, then {@code count} timed ones; records the distribution
     * and returns p95 in <b>microseconds</b>.
     */
    private static long measure(String label, Iteration iteration, int warmup, int count) {
        for (int i = 0; i < warmup; i++) {
            iteration.run(i);
        }
        long[] micros = new long[count];
        for (int i = 0; i < count; i++) {
            long start = System.nanoTime();
            iteration.run(warmup + i);
            micros[i] = (System.nanoTime() - start) / 1_000;
        }
        Arrays.sort(micros);
        long p50 = micros[percentileIndex(count, 0.50)];
        long p95 = micros[percentileIndex(count, 0.95)];
        long p99 = micros[percentileIndex(count, 0.99)];
        // Microseconds, not milliseconds: a report line reading "p95=0 ms" is not evidence of anything.
        record("%-64s n=%-4d p50=%9d us  p95=%9d us  p99=%9d us"
                .formatted(label, count, p50, p95, p99));
        return p95;
    }

    private static int percentileIndex(int count, double percentile) {
        return Math.min(count - 1, Math.max(0, (int) Math.ceil(percentile * count) - 1));
    }

    /** The root {@code Plan} node of {@code EXPLAIN (FORMAT JSON)} for {@code sql}. */
    private static JsonNode explain(String sql) {
        String json = jdbc.sql("EXPLAIN (FORMAT JSON) " + sql)
                .query(String.class)
                .single();
        try {
            return new ObjectMapper().readTree(json).get(0).get("Plan");
        } catch (IOException e) {
            throw new IllegalStateException("could not parse the EXPLAIN output: " + json, e);
        }
    }

    /**
     * Walks the whole plan tree. A nested sequential scan under a nested loop is exactly the failure this
     * has to catch, and it is invisible to a check on the root node alone.
     */
    private static void assertNoSeqScan(JsonNode plan, String table) {
        assertThat(hasNode(plan, node -> "Seq Scan".equals(text(node, "Node Type"))
                && table.equals(text(node, "Relation Name"))))
                .as("no Seq Scan on '%s' may appear anywhere in the plan tree. Plan:%n%s", table, plan)
                .isFalse();
    }

    private static boolean usesIndex(JsonNode plan, String indexName) {
        return hasNode(plan, node -> indexName.equals(text(node, "Index Name")));
    }

    private static boolean hasNode(JsonNode node, java.util.function.Predicate<JsonNode> match) {
        if (match.test(node)) {
            return true;
        }
        JsonNode children = node.get("Plans");
        if (children != null) {
            for (JsonNode child : children) {
                if (hasNode(child, match)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null ? null : value.asText();
    }

    private static void record(String line) {
        REPORT_LINES.add(line);
        System.out.println("[perf] " + line);
    }
}
