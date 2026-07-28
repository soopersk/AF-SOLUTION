package com.orchestration.ems.config;

import java.net.http.HttpClient;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Outbound HTTP clients (ems-design §10): the EDF context {@code RestClient} (ingest enrichment) and the
 * Airflow trigger {@code RestClient} (outbox dispatch, Batch H).
 *
 * <p>The EDF bearer token is supplied provisionally from a config property ({@code ems.edf.token}) — a
 * placeholder for the real Entra OAuth2 client-credentials flow, which is wired per environment when
 * the EDF contract lands (§14 item 2). It is a {@link Supplier} so token acquisition can later become
 * a live, per-request fetch without touching {@code EdfContextClient}.
 *
 * <p>The Airflow authorization header is likewise a {@link Supplier}: a provisional static value
 * ({@code ems.airflow.auth-header}, e.g. {@code Basic …} or {@code Bearer …}) that a per-profile
 * Basic/JWT source replaces in-environment without touching {@code AirflowTriggerClient}. Blank ⇒ no
 * {@code Authorization} header (local WireMock drills).
 */
@Configuration
public class RestClientConfig {

    /**
     * The EDF-rooted client. Base URL defaults to a local stub; set {@code ems.edf.base-url} per
     * environment. SSL/truststore wiring (legacy {@code itrust-store}) is an environment concern added
     * with the real contract.
     */
    @Bean
    public RestClient edfRestClient(RestClient.Builder builder,
            @Value("${ems.edf.base-url:http://localhost:8081}") String baseUrl) {
        return builder.baseUrl(baseUrl).build();
    }

    /** Provisional EDF bearer-token source (§14 item 2) — a static config value until Entra is wired. */
    @Bean
    public Supplier<String> edfTokenProvider(@Value("${ems.edf.token:}") String token) {
        return () -> token;
    }

    /**
     * The Airflow-rooted client used by the outbox dispatcher (Amendment A1: Airflow is triggered
     * asynchronously off the ingest path). Base URL defaults to a local stub; set
     * {@code ems.airflow.base-url} (including the API prefix, e.g. {@code .../api/v1}) per environment.
     */
    @Bean
    public RestClient airflowRestClient(RestClient.Builder builder,
            @Value("${ems.airflow.base-url:http://localhost:8082/api/v1}") String baseUrl) {
        // Pin HTTP/1.1: the JDK client's default h2c upgrade fails a POST-with-body handshake against a
        // cleartext server (RST_STREAM); Airflow's REST API gains nothing from HTTP/2 for single triggers.
        HttpClient http11 = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
        return builder.baseUrl(baseUrl)
                .requestFactory(new JdkClientHttpRequestFactory(http11))
                .build();
    }

    /** Provisional Airflow {@code Authorization} header source — static config until per-profile auth is wired. */
    @Bean
    public Supplier<String> airflowAuthHeaderProvider(@Value("${ems.airflow.auth-header:}") String header) {
        return () -> header;
    }
}
