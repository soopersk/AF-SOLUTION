package com.orchestration.ems.dispatch;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpClient;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.orchestration.ems.dispatch.AirflowTriggerClient.Outcome;

/**
 * WireMock proof of {@link AirflowTriggerClient}'s outcome classification (runs locally — the real
 * kill-Airflow backlog drain is Batch I's {@code KillAirflowDrainIT}). The client must translate the
 * Airflow REST status into the mark/retain signal the dispatcher acts on:
 * <ul>
 *   <li>200 ⇒ {@link Outcome#DELIVERED}</li>
 *   <li>409 (run already exists, A6) ⇒ {@link Outcome#DELIVERED}</li>
 *   <li>503 ⇒ {@link Outcome#RETRIABLE}</li>
 *   <li>400 ⇒ {@link Outcome#NON_RETRIABLE}</li>
 * </ul>
 */
class AirflowTriggerClientTest {

    private static final String DAG_ID = "control_merival";
    private static final String DAG_RUN_ID = "a1b2c3d4e5f60718";
    private static final String CONF = "{\"eventId\":\"evt-1\",\"tenant\":\"acme\"}";
    private static final String TRIGGER_URL = "/dags/" + DAG_ID + "/dagRuns";

    private static WireMockServer wm;
    private AirflowTriggerClient client;

    @BeforeAll
    static void startServer() {
        wm = new WireMockServer(options().dynamicPort());
        wm.start();
    }

    @AfterAll
    static void stopServer() {
        wm.stop();
    }

    @BeforeEach
    void setUp() {
        wm.resetAll();
        // Pin HTTP/1.1 (mirrors the production airflowRestClient): the JDK client's default h2c upgrade
        // fails a POST-with-body handshake against WireMock's cleartext server (RST_STREAM).
        HttpClient http11 = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        RestClient restClient = RestClient.builder()
                .baseUrl(wm.baseUrl())
                .requestFactory(new JdkClientHttpRequestFactory(http11))
                .build();
        client = new AirflowTriggerClient(restClient, new ObjectMapper(), () -> "", "/dags/{dagId}/dagRuns");
    }

    @Test
    void status200_isDelivered() {
        wm.stubFor(post(urlPathEqualTo(TRIGGER_URL)).willReturn(aResponse().withStatus(200)));

        Outcome outcome = client.trigger(DAG_ID, DAG_RUN_ID, CONF);

        assertThat(outcome).isEqualTo(Outcome.DELIVERED);
        // the body carries the run id and the conf re-embedded verbatim (key order / whitespace ignored)
        wm.verify(postRequestedFor(urlPathEqualTo(TRIGGER_URL)).withRequestBody(
                equalToJson("{\"dag_run_id\":\"" + DAG_RUN_ID + "\",\"conf\":" + CONF + "}")));
    }

    @Test
    void status409_isDeliveredAsAlreadyTriggered() {
        wm.stubFor(post(urlPathEqualTo(TRIGGER_URL)).willReturn(aResponse().withStatus(409)));

        assertThat(client.trigger(DAG_ID, DAG_RUN_ID, CONF)).isEqualTo(Outcome.DELIVERED);
    }

    @Test
    void status503_isRetriable() {
        wm.stubFor(post(urlPathEqualTo(TRIGGER_URL)).willReturn(aResponse().withStatus(503)));

        assertThat(client.trigger(DAG_ID, DAG_RUN_ID, CONF)).isEqualTo(Outcome.RETRIABLE);
    }

    @Test
    void status400_isNonRetriable() {
        wm.stubFor(post(urlPathEqualTo(TRIGGER_URL)).willReturn(aResponse().withStatus(400)));

        assertThat(client.trigger(DAG_ID, DAG_RUN_ID, CONF)).isEqualTo(Outcome.NON_RETRIABLE);
    }
}
