package com.orchestration.ems.ingestion;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

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
import com.orchestration.ems.decisions.RoutingDecisionRepo;
import com.orchestration.ems.dispatch.OutboxRepo;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.store.ContextRepository;
import com.orchestration.ems.store.EventRepository;
import com.orchestration.ems.subscription.CelPrograms;
import com.orchestration.ems.subscription.SubscriptionService;
import com.orchestration.ems.support.AbstractPostgresIT;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The Batch-I redelivery / L0-idempotency drill (§13 exit gate): Kafka may redeliver a record after a
 * crash between {@code process} and the manual ack, so the whole ingest is exercised twice for the same
 * event and must converge to a single durable outcome. Delivering the same MERIVAL BATCH event twice
 * yields exactly one event row, one FORWARDED {@code routing_decision}, and one {@code dag_trigger_outbox}
 * row — no double trigger. Auto-skips locally; runs in CI.
 *
 * <p>The idempotency guard is the event upsert ({@code ON CONFLICT DO NOTHING}, A3): on the second pass it
 * inserts nothing, so {@link IngestionService} short-circuits the decision + outbox writes — the atomic
 * companion of the first pass's committed rows.
 */
class RedeliveryIdempotencyIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CONTROL_DAG = "orchestration_control_dag_capital";
    private static final String MERIVAL_BATCH_CEL =
            "event.source == \"MERIVAL\" && event.additionalData.TYPE == \"INGESTION\" "
                    + "&& event.additionalData.RUN_TYPE == \"BATCH\"";

    private static final String EVENT = """
            {"id":"evt-mer-batch-1","contextId":"ctx-300","source":"MERIVAL",
             "additionalData":{"TYPE":"INGESTION","RUN_TYPE":"BATCH","STATE":"FINISH"},
             "logicalBusinessDate":"2026-07-17"}""";

    private static WireMockServer wm;

    private JdbcClient jdbc;
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
        DataSource ds = dataSource();
        jdbc = JdbcClient.create(ds);
        jdbc.sql("TRUNCATE event, context, routing_decision, dag_trigger_outbox").update();

        wm.resetAll();
        wm.stubFor(get(urlPathEqualTo("/context/ctx-300"))
                .willReturn(okJson(resource("/samples/context_merival.json"))));

        PlatformTransactionManager txManager = new DataSourceTransactionManager(ds);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        Normalizer normalizer = new Normalizer(meters);
        SubscriptionService subscriptions =
                new SubscriptionService(RedeliveryIdempotencyIT::fixtures, normalizer, new CelPrograms());
        Cache<String, ContextRow> cache = Caffeine.newBuilder().build();
        ContextRepository contextRepo = new ContextRepository(jdbc, MAPPER);
        RestClient edfClient = RestClient.builder().baseUrl(wm.baseUrl()).build();
        EdfContextClient edf = new EdfContextClient(edfClient, MAPPER, () -> "",
                "/context/{id}", 1, Duration.ZERO);
        ContextResolver resolver = new ContextResolver(cache, contextRepo, edf, meters);
        ingestion = new IngestionService(MAPPER, subscriptions, resolver, new EventRepository(jdbc),
                contextRepo, new RoutingDecisionRepo(jdbc), new OutboxRepo(jdbc), meters, txManager);
    }

    @Test
    void sameEventDeliveredTwice_convergesToSingleEventDecisionAndTrigger() {
        ingestion.process(EVENT);
        ingestion.process(EVENT); // redelivery

        assertThat(rowCount("event")).isEqualTo(1);
        assertThat(rowCount("context")).isEqualTo(1);
        assertThat(rowCount("routing_decision")).isEqualTo(1);
        assertThat(rowCount("dag_trigger_outbox")).isEqualTo(1);

        // the single verdict is the FORWARD, and the single trigger targets the control DAG (no duplicate)
        assertThat(jdbc.sql("SELECT decision FROM routing_decision").query(String.class).single())
                .isEqualTo("FORWARDED");
        assertThat(jdbc.sql("SELECT dag_id FROM dag_trigger_outbox").query(String.class).single())
                .isEqualTo(CONTROL_DAG);
    }

    private static List<SubscriptionRow> fixtures() {
        return List.of(
                new SubscriptionRow(1L, "PLATFORM", Stage.PERSIST, "persist_merival_batch", null,
                        MERIVAL_BATCH_CEL, "seed-0", true),
                new SubscriptionRow(2L, "CAPITAL", Stage.FORWARD, "cap_merival_batch", CONTROL_DAG,
                        MERIVAL_BATCH_CEL, "seed-0", true));
    }

    private int rowCount(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Integer.class).single();
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = RedeliveryIdempotencyIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
