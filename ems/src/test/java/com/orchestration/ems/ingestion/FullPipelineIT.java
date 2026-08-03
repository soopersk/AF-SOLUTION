package com.orchestration.ems.ingestion;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.orchestration.ems.canonical.DagRunId;
import com.orchestration.ems.decisions.RoutingDecisionRepo;
import com.orchestration.ems.dispatch.OutboxRepo;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.model.EnrichedEvent;
import com.orchestration.ems.model.EventRow;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.store.ContextRepository;
import com.orchestration.ems.store.EventRepository;
import com.orchestration.ems.subscription.CelPrograms;
import com.orchestration.ems.subscription.SubscriptionService;
import com.orchestration.ems.support.AbstractPostgresIT;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The Batch-I full-pipeline drill (ems-design §4.2; the §13 Phase-2 exit gate): the real
 * {@link IngestionService} graph — normalizer, CEL {@link SubscriptionService}, tiered
 * {@link ContextResolver} (Caffeine → DB → EDF-via-WireMock), and the four persistence repos over a
 * single transaction manager — driven against a real PostgreSQL. Auto-skips locally
 * ({@code disabledWithoutDocker}); runs in CI.
 *
 * <p>Proven end-to-end for one MERIVAL BATCH event that both persists (stage 1) and forwards (stage 2):
 * <ul>
 *   <li>the raw event + its EDF-fetched context are persisted, and the typed {@code GENERATED} columns
 *       populate from the raw JSONB (A7 nested {@code taskId}, A8 hyphen-spelled {@code reporting-date},
 *       {@code ems_norm_region}/{@code ems_norm_freq} canonicalization);</li>
 *   <li>exactly one {@code FORWARDED} L0 {@code routing_decision} (tier {@code L0_SUBSCRIPTION}, decided
 *       by {@code ems}) and exactly one {@code dag_trigger_outbox} row with the deterministic A6
 *       {@code dag_run_id} are written <b>in the same transaction</b>;</li>
 *   <li>a firehose non-match is dropped at the persist gate — counted, nothing persisted, no EDF call;</li>
 *   <li>the write is atomic: an outbox failure mid-transaction rolls the event insert back (nothing lands).</li>
 * </ul>
 */
class FullPipelineIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CONTROL_DAG = "orchestration_control_dag_capital";
    private static final String MERIVAL_BATCH_CEL =
            "event.source == \"merival\" && event.additionaldata.type == \"ingestion\" "
                    + "&& event.additionaldata.run_type == \"batch\"";

    /** A MERIVAL BATCH event: matches PERSIST + FORWARD; STATE lower-case to prove the upper() generated col. */
    private static final String MATCH_EVENT = """
            {"id":"evt-mer-batch-1","contextId":"ctx-300","source":"MERIVAL",
             "additionalData":{"taskId":"task-9","DATASET_UUID":"ds-uuid-777","TYPE":"INGESTION",
                               "RUN_TYPE":"BATCH","STATE":"finish"},
             "logicalBusinessDate":"2026-07-17"}""";

    /** A firehose event matching no PERSIST rule ⇒ dropped at the gate. */
    private static final String NOISE_EVENT =
            "{\"id\":\"evt-noise-1\",\"source\":\"FIREHOSE\",\"additionalData\":{\"TYPE\":\"HEARTBEAT\"}}";

    private static WireMockServer wm;

    private DataSource ds;
    private JdbcClient jdbc;
    private PlatformTransactionManager txManager;
    private SimpleMeterRegistry meters;
    private OutboxRepo outboxRepo;
    private IngestionService ingestion;

    @BeforeAll
    static void startEdf() {
        wm = new WireMockServer(options().dynamicPort());
        wm.start();
    }

    @AfterAll
    static void stopEdf() {
        wm.stop();
    }

    @BeforeEach
    void setUp() throws IOException {
        ds = dataSource();
        jdbc = JdbcClient.create(ds); // one shared datasource so all repos join the dispatcher's transaction
        jdbc.sql("TRUNCATE event, context, routing_decision, dag_trigger_outbox").update();

        wm.resetAll();
        wm.stubFor(get(urlPathEqualTo("/context/ctx-300"))
                .willReturn(okJson(resource("/samples/context_merival.json"))));

        txManager = new DataSourceTransactionManager(ds);
        meters = new SimpleMeterRegistry();
        outboxRepo = new OutboxRepo(jdbc);
        ingestion = newService(outboxRepo);
    }

    @Test
    void merivalBatch_persistsEventAndContext_writesL0AndOutbox_inOneTransaction() {
        ingestion.process(MATCH_EVENT);

        // event persisted with the typed GENERATED columns populated from the raw JSONB (A7/A8/A9)
        EventCols ev = jdbc.sql("""
                        SELECT source, event_type, state, task_id, dataset_id, context_id, logical_business_date
                        FROM event WHERE event_id = 'evt-mer-batch-1'""")
                .query((rs, n) -> new EventCols(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7)))
                .single();
        assertThat(ev.source()).isEqualTo("MERIVAL");
        assertThat(ev.eventType()).isEqualTo("INGESTION");
        assertThat(ev.state()).isEqualTo("FINISH");        // upper() of "finish"
        assertThat(ev.taskId()).isEqualTo("task-9");        // A7: nested additionalData.taskId
        assertThat(ev.datasetId()).isEqualTo("ds-uuid-777");
        assertThat(ev.contextId()).isEqualTo("ctx-300");
        assertThat(ev.logicalBusinessDate()).isEqualTo("2026-07-17");

        // context save-on-fetch persisted, with canonicalized generated columns
        ContextCols cx = jdbc.sql("""
                        SELECT reporting_date, run_category, h3_region, frequency
                        FROM context WHERE context_id = 'ctx-300'""")
                .query((rs, n) -> new ContextCols(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)))
                .single();
        assertThat(cx.reportingDate()).isEqualTo("2026-07-17"); // A8: hyphen-spelled reporting-date
        assertThat(cx.runCategory()).isEqualTo("TOPSIDE");
        assertThat(cx.h3Region()).isEqualTo("EMEA");            // ems_norm_region(upper)
        assertThat(cx.frequency()).isEqualTo("DAILY");          // ems_norm_freq

        // exactly one FORWARDED L0 verdict for the matched tenant/DAG
        DecisionCols d = jdbc.sql("""
                        SELECT tenant_id, tier, target_dag_id, decision, decided_by, registry_version, engine_version
                        FROM routing_decision WHERE event_id = 'evt-mer-batch-1'""")
                .query((rs, n) -> new DecisionCols(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7)))
                .single();
        assertThat(d.tenantId()).isEqualTo("CAPITAL");
        assertThat(d.tier()).isEqualTo("L0_SUBSCRIPTION");
        assertThat(d.targetDagId()).isEqualTo(CONTROL_DAG);
        assertThat(d.decision()).isEqualTo("FORWARDED");
        assertThat(d.decidedBy()).isEqualTo("ems");
        assertThat(d.registryVersion()).isEqualTo("seed-0");
        assertThat(d.engineVersion()).isEqualTo(CelPrograms.ENGINE_VERSION);

        // exactly one outbox row with the deterministic A6 dag_run_id, carrying the merged conf
        OutboxCols o = jdbc.sql("""
                        SELECT dag_run_id, dag_id, jsonb_exists(conf, 'context') AS has_ctx
                        FROM dag_trigger_outbox""")
                .query((rs, n) -> new OutboxCols(rs.getString(1), rs.getString(2), rs.getBoolean(3)))
                .single();
        assertThat(o.dagId()).isEqualTo(CONTROL_DAG);
        assertThat(o.hasContext()).isTrue();               // conf embeds the resolved context (A5 merge shape)
        assertThat(o.dagRunId()).isEqualTo(expectedDagRunId());
    }

    @Test
    void firehoseNonMatch_isDroppedAtPersistGate_countedAndNothingPersisted() {
        ingestion.process(NOISE_EVENT);

        assertThat(rowCount("event")).isZero();
        assertThat(rowCount("routing_decision")).isZero();
        assertThat(rowCount("dag_trigger_outbox")).isZero();
        assertThat(meters.get("ems_events_dropped_total").tag("source", "FIREHOSE").counter().count())
                .isEqualTo(1.0);
        // the dropped event never reaches context resolution, so EDF is never consulted
        wm.verify(0, com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor(
                urlPathEqualTo("/context/ctx-300")));
    }

    @Test
    void writeIsAtomic_outboxFailureRollsBackTheEventInsert() {
        OutboxRepo exploding = new OutboxRepo(jdbc) {
            @Override
            public int insert(String dagRunId, String dagId, String conf) {
                throw new RuntimeException("outbox insert boom (atomicity drill)");
            }
        };
        IngestionService failing = newService(exploding);

        assertThatThrownBy(() -> failing.process(MATCH_EVENT)).isInstanceOf(RuntimeException.class);

        // the whole §4.2 step-5 write rolled back: no event, no decision, no outbox row survives
        assertThat(rowCount("event")).isZero();
        assertThat(rowCount("routing_decision")).isZero();
        assertThat(rowCount("dag_trigger_outbox")).isZero();
    }

    /** Build the real ingestion graph over the shared datasource, substituting the given outbox repo. */
    private IngestionService newService(OutboxRepo outbox) {
        Normalizer normalizer = new Normalizer(meters);
        SubscriptionService subscriptions =
                new SubscriptionService(FullPipelineIT::fixtures, normalizer, new CelPrograms());

        Cache<String, ContextRow> cache = Caffeine.newBuilder().build();
        ContextRepository contextRepo = new ContextRepository(jdbc, MAPPER);
        RestClient edfClient = RestClient.builder().baseUrl(wm.baseUrl()).build();
        EdfContextClient edf = new EdfContextClient(edfClient, MAPPER, () -> "",
                "/context/{id}", 1, Duration.ZERO);
        ContextResolver resolver = new ContextResolver(cache, contextRepo, edf, meters);

        return new IngestionService(MAPPER, subscriptions, resolver,
                new EventRepository(jdbc), contextRepo, new RoutingDecisionRepo(jdbc), outbox, meters, txManager);
    }

    /** In-memory seed: one PERSIST gate + one FORWARD route, both matching MERIVAL BATCH. */
    private static List<SubscriptionRow> fixtures() {
        return List.of(
                new SubscriptionRow(1L, "PLATFORM", Stage.PERSIST, "persist_merival_batch", null,
                        MERIVAL_BATCH_CEL, "seed-0", true),
                new SubscriptionRow(2L, "CAPITAL", Stage.FORWARD, "cap_merival_batch", CONTROL_DAG,
                        MERIVAL_BATCH_CEL, "seed-0", true));
    }

    /** Recompute the deterministic dag_run_id the same way the pipeline does (A5 conf shape, A6 derivation). */
    private static String expectedDagRunId() {
        try {
            EventRow event = EventRow.of(MATCH_EVENT, MAPPER);
            ContextRow context = ContextRow.of(resource("/samples/context_merival.json"), MAPPER);
            String conf = new EnrichedEvent(event.parsed(), context.parsed()).toConf().toString();
            return DagRunId.derive(CONTROL_DAG, conf);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private int rowCount(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Integer.class).single();
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = FullPipelineIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private record EventCols(String source, String eventType, String state, String taskId,
            String datasetId, String contextId, String logicalBusinessDate) { }

    private record ContextCols(String reportingDate, String runCategory, String h3Region, String frequency) { }

    private record DecisionCols(String tenantId, String tier, String targetDagId, String decision,
            String decidedBy, String registryVersion, String engineVersion) { }

    private record OutboxCols(String dagRunId, String dagId, boolean hasContext) { }
}
