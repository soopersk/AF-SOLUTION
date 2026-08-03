package com.orchestration.ems.dispatch;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.orchestration.ems.recon.ReconRepository;
import com.orchestration.ems.recon.ReconciliationSweep;
import com.orchestration.ems.support.AbstractPostgresIT;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * The Batch-I kill-Airflow backlog-drain drill (ems-design §12; §13 exit gate): the transactional outbox
 * decouples ingest from Airflow (Amendment A1), so while Airflow is down the outbox simply accumulates and
 * ingestion is unaffected; when Airflow returns, the {@link OutboxDispatcher} drains the whole backlog with
 * zero lost triggers. Driven against a real PostgreSQL with a real {@link AirflowTriggerClient} pointed at
 * WireMock. Auto-skips locally; runs in CI.
 *
 * <p>The dispatcher is configured with a zero backoff so re-eligibility is immediate and the drill needs no
 * wall-clock waits: while Airflow returns 503 every row stays undelivered with a growing {@code attempts},
 * and the {@code ems_outbox_pending_age_seconds} gauge reports a non-empty backlog; once Airflow returns 200
 * a single drain delivers every row and the gauge falls back to zero.
 *
 * <p><b>The gauge is read from {@link ReconciliationSweep}, not from the dispatcher.</b> That is the point
 * of the ownership move: the backlog signal comes from an observer that does not depend on the component
 * under failure. A dispatcher that is wedged — or, in {@code shadow}, absent entirely — still gets its
 * backlog reported.
 */
class KillAirflowDrainIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CONTROL_DAG = "orchestration_control_dag_capital";
    private static final String TRIGGER_URL = "/dags/" + CONTROL_DAG + "/dagRuns";
    private static final int BACKLOG = 5;

    private static WireMockServer airflow;

    private JdbcClient jdbc;
    private OutboxRepo repo;
    private SimpleMeterRegistry meters;
    private OutboxDispatcher dispatcher;
    private ReconciliationSweep sweep;

    @BeforeAll
    static void startAirflow() {
        airflow = new WireMockServer(options().dynamicPort());
        airflow.start();
    }

    @AfterAll
    static void stopAirflow() {
        airflow.stop();
    }

    @BeforeEach
    void setUp() {
        DataSource ds = dataSource();
        jdbc = JdbcClient.create(ds);
        jdbc.sql("TRUNCATE dag_trigger_outbox").update();
        airflow.resetAll();

        repo = new OutboxRepo(jdbc);
        meters = new SimpleMeterRegistry();
        PlatformTransactionManager txManager = new DataSourceTransactionManager(ds);

        // Pin HTTP/1.1 (POST-with-body vs WireMock's cleartext server); zero backoff for a wait-free drill.
        HttpClient http11 = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        RestClient restClient = RestClient.builder().baseUrl(airflow.baseUrl())
                .requestFactory(new JdkClientHttpRequestFactory(http11)).build();
        Supplier<String> noAuth = () -> "";
        AirflowTriggerClient triggerClient =
                new AirflowTriggerClient(restClient, MAPPER, noAuth, "/dags/{dagId}/dagRuns");
        dispatcher = new OutboxDispatcher(repo, triggerClient, txManager, 100, 0, 0);
        // Optional.empty(): this drill is about the outbox, and no broker is involved in it.
        sweep = new ReconciliationSweep(new ReconRepository(jdbc), Optional.empty(), meters,
                Duration.ofHours(6), Duration.ofDays(7));
    }

    @Test
    void airflowDownThenUp_backlogAccumulatesThenFullyDrains() {
        // events ingested while Airflow is down land in the outbox (this is the ingest-side effect)
        for (int i = 0; i < BACKLOG; i++) {
            repo.insert("run-" + i, CONTROL_DAG, "{\"n\":" + i + "}");
        }

        // --- Airflow DOWN: every trigger 503s ⇒ nothing delivered, attempts climb, backlog visible ---
        airflow.stubFor(post(urlPathEqualTo(TRIGGER_URL)).willReturn(aResponse().withStatus(503)));

        dispatcher.drain();
        dispatcher.drain();
        dispatcher.drain();

        assertThat(undeliveredCount()).isEqualTo(BACKLOG);           // ingestion decoupled: nothing lost
        assertThat(maxAttempts()).isGreaterThanOrEqualTo(3);         // retries recorded, no delivery
        airflow.verify(postRequestedFor(urlPathEqualTo(TRIGGER_URL))); // Airflow was actually attempted
        assertThat(gauge()).isGreaterThan(0.0);                      // ems_outbox_pending_age_seconds: backlog

        // --- Airflow UP: one drain delivers the entire backlog, gauge falls to zero ---
        airflow.resetAll();
        airflow.stubFor(post(urlPathEqualTo(TRIGGER_URL)).willReturn(aResponse().withStatus(200)));

        dispatcher.drain();

        assertThat(undeliveredCount()).isZero();                     // zero lost triggers — all delivered
        assertThat(deliveredCount()).isEqualTo(BACKLOG);
        assertThat(gauge()).isEqualTo(0.0);                          // outbox fully drained
    }

    private int undeliveredCount() {
        return jdbc.sql("SELECT count(*) FROM dag_trigger_outbox WHERE delivered_at IS NULL")
                .query(Integer.class).single();
    }

    private int deliveredCount() {
        return jdbc.sql("SELECT count(*) FROM dag_trigger_outbox WHERE delivered_at IS NOT NULL")
                .query(Integer.class).single();
    }

    private int maxAttempts() {
        return jdbc.sql("SELECT coalesce(max(attempts), 0) FROM dag_trigger_outbox")
                .query(Integer.class).single();
    }

    /** One recon tick, then the gauge it published — the operator's view, not the dispatcher's. */
    private double gauge() {
        sweep.sweep();
        return meters.get(ReconciliationSweep.OUTBOX_AGE_METRIC).gauge().value();
    }
}
