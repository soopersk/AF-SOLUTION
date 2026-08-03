package com.orchestration.ems.perf;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the ems-design §12 dataset: 10 M events over 1 M contexts, generated <b>server-side</b>.
 *
 * <p><b>Why {@code generate_series} and not a JDBC batch.</b> Ten million rows over the wire is a
 * measurement of the client and the network, not of the database — and it takes long enough that nobody
 * would ever run it. One {@code INSERT … SELECT … FROM generate_series} per chunk keeps the data where it
 * is being measured. Chunks of 250 k bound the WAL each statement produces; one 10 M-row statement is a
 * single transaction whose WAL and dead-tuple footprint dwarf the table it is building.
 *
 * <p><b>The payload shapes are not arbitrary.</b> They are cut down from the real samples in
 * {@code src/test/resources/samples/} to exactly the keys the V1 generated columns read — the
 * {@code additionalData.taskId} / {@code .STATE} / {@code .TYPE} nesting from A7, the top-level
 * {@code contextId}, and on contexts the hyphenated {@code data.reporting-date} of A8 plus
 * {@code frequency} and {@code h3Region}. Seeding a shape that misses those would populate the promoted
 * columns with NULLs, and every index in V2 would then be measured against an empty index.
 *
 * <p><b>Distribution.</b> Ten events per context, and two events per {@code taskId} (a STARTED and a
 * terminal) so {@code /run/status} summarizes a realistic run rather than a single row. Task ids are
 * spread uniformly across the whole range, which is the case that makes {@code idx_event_task_id}
 * selective — a skewed seed would let the planner reach the target p95 for the wrong reason.
 *
 * <p>Sizes are overridable ({@code -Dems.perf.events=…}, {@code -Dems.perf.contexts=…}) so the harness can
 * be smoke-run in minutes; the defaults are the §12 numbers and are what a real gate run must use.
 */
final class PerfSeeder {

    private static final Logger log = LoggerFactory.getLogger(PerfSeeder.class);

    static final long EVENTS = Long.getLong("ems.perf.events", 10_000_000L);
    static final long CONTEXTS = Long.getLong("ems.perf.contexts", 1_000_000L);
    static final long EVENTS_PER_CONTEXT = 10;
    static final long EVENTS_PER_TASK = 2;

    /** Rows per statement. Large enough to amortize planning, small enough to bound WAL per commit. */
    private static final long CHUNK = 250_000;

    private final DataSource dataSource;

    PerfSeeder(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** Seeds contexts then events, then {@code ANALYZE}s — §11 stage 1 item 7 does the same before cutover. */
    void seed() throws SQLException {
        long start = System.nanoTime();
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {

            seedChunked(statement, "context", CONTEXTS, PerfSeeder::contextChunk);
            seedChunked(statement, "event", EVENTS, PerfSeeder::eventChunk);

            // Without this the planner is choosing between index and seq scan on default estimates, and
            // the EXPLAIN assertions below would be measuring the absence of statistics.
            log.info("ANALYZE event, context");
            statement.execute("ANALYZE event, context");
        }
        log.info("Seeded {} events over {} contexts in {} s",
                EVENTS, CONTEXTS, (System.nanoTime() - start) / 1_000_000_000L);
    }

    private interface ChunkSql {
        String forRange(long from, long to);
    }

    private static void seedChunked(Statement statement, String table, long total, ChunkSql sql)
            throws SQLException {
        for (long from = 1; from <= total; from += CHUNK) {
            long to = Math.min(from + CHUNK - 1, total);
            statement.executeUpdate(sql.forRange(from, to));
            log.info("seeded {} {} rows ({}/{})", to - from + 1, table, to, total);
        }
    }

    /**
     * Contexts: {@code parentIds} points one decade up so the A9 GIN index has real chains to walk, and
     * the three promoted columns cycle over small domains — a realistic cardinality for a composite index
     * whose leading column is a date.
     */
    private static String contextChunk(long from, long to) {
        return """
                INSERT INTO context (context_id, json)
                SELECT 'ctx-' || g,
                       jsonb_build_object(
                           'id', 'ctx-' || g,
                           'parentIds', jsonb_build_array('ctx-' || GREATEST(1, g / 10)),
                           'data', jsonb_build_object(
                               'reporting-date',
                                   to_char(DATE '2026-01-01' + ((g %% 365)::int), 'YYYY-MM-DD'),
                               'frequency', (ARRAY['D','M','Q'])[1 + (g %% 3)],
                               'h3Region', (ARRAY['AMERICAS','EMEA','APAC'])[1 + (g %% 3)],
                               'logicalBusinessDate',
                                   to_char(DATE '2026-01-01' + ((g %% 365)::int), 'YYYY-MM-DD')))
                FROM generate_series(%d, %d) g
                ON CONFLICT (context_id) DO NOTHING
                """.formatted(from, to);
    }

    /**
     * Events: the MEG-family nesting (A7), because that is the shape whose promoted columns exercise the
     * COALESCE branches. Odd g is the STARTED event of a task, even g its COMPLETE — so a
     * {@code /run/status} lookup by task id returns a two-row run with a terminal, not a bare row.
     */
    private static String eventChunk(long from, long to) {
        return """
                INSERT INTO event (event_id, json)
                SELECT 'evt-' || g,
                       jsonb_build_object(
                           'contextId', 'ctx-' || (1 + ((g - 1) / %d) %% %d),
                           'source', 'PERF',
                           'businessDate', to_char(DATE '2026-01-01' + ((g %% 365)::int), 'YYYY-MM-DD'),
                           'additionalData', jsonb_build_object(
                               'taskId', 'task-' || ((g - 1) / %d),
                               'STATE', CASE WHEN g %% 2 = 1 THEN 'STARTED' ELSE 'COMPLETE' END,
                               'taskEventType', CASE WHEN g %% 2 = 1 THEN 'START' ELSE 'END' END,
                               'successful', CASE WHEN g %% 2 = 1 THEN 'false' ELSE 'true' END,
                               'TYPE', 'CALC_EVENT'))
                FROM generate_series(%d, %d) g
                ON CONFLICT (event_id) DO NOTHING
                """.formatted(EVENTS_PER_CONTEXT, CONTEXTS, EVENTS_PER_TASK, from, to);
    }

    /** A {@code task_id} that exists in the seeded range, for iteration {@code i}. */
    static String taskId(long i) {
        return "task-" + (Math.floorMod(i, EVENTS / EVENTS_PER_TASK));
    }

    /** A {@code context_id} that exists in the seeded range, for iteration {@code i}. */
    static String contextId(long i) {
        return "ctx-" + (1 + Math.floorMod(i, CONTEXTS));
    }
}
