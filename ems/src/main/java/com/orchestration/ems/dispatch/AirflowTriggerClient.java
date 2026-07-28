package com.orchestration.ems.dispatch;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * POSTs a trigger to the Airflow REST API — the delivery half of the outbox dispatcher (Amendment A1:
 * Airflow is triggered asynchronously off the ingest path). One call maps a claimed
 * {@link PendingTrigger} to a {@code POST {trigger-path}} with body {@code {dag_run_id, conf}}.
 *
 * <p><b>Outcome classification (drives the dispatcher's mark/retain decision):</b>
 * <ul>
 *   <li><b>2xx</b> → {@link Outcome#DELIVERED}.</li>
 *   <li><b>409 Conflict</b> → {@link Outcome#DELIVERED}: the deterministic {@code dag_run_id} (A6)
 *       already exists, so the run was already triggered — an idempotent success, never an error.</li>
 *   <li><b>429 / 5xx</b>, or a connect/read/I-O failure (Airflow down) → {@link Outcome#RETRIABLE}:
 *       the dispatcher retains the row and retries after backoff.</li>
 *   <li><b>Any other 4xx</b> (a malformed request that will never succeed) → {@link Outcome#NON_RETRIABLE}:
 *       the dispatcher records the failure and alerts; retrying is futile.</li>
 * </ul>
 *
 * <p>The {@code Authorization} header is supplied per request from a provisional static source
 * ({@code RestClientConfig#airflowAuthHeaderProvider}); a blank value sends no header (local WireMock).
 */
@Component
public class AirflowTriggerClient {

    private static final Logger log = LoggerFactory.getLogger(AirflowTriggerClient.class);

    /** The fate of a single trigger POST, as seen by the dispatcher. */
    public enum Outcome {
        /** 2xx or 409 — the DAG run exists; mark the outbox row delivered. */
        DELIVERED,
        /** 429 / 5xx / Airflow unreachable — retain the row and retry after backoff. */
        RETRIABLE,
        /** Other 4xx — a request that can never succeed; record + alert, do not retry. */
        NON_RETRIABLE
    }

    private final RestClient restClient;
    private final ObjectMapper mapper;
    private final Supplier<String> authHeaderProvider;
    private final String triggerPath;

    /**
     * @param restClient         the Airflow-rooted client (base URL in {@code RestClientConfig})
     * @param mapper             Jackson mapper used to build the request body and re-embed the stored conf
     * @param authHeaderProvider supplies the {@code Authorization} header value per request (blank ⇒ none)
     * @param triggerPath        the POST path template carrying the DAG id, e.g. {@code /dags/{dagId}/dagRuns}
     */
    public AirflowTriggerClient(
            @Qualifier("airflowRestClient") RestClient restClient,
            ObjectMapper mapper,
            @Qualifier("airflowAuthHeaderProvider") Supplier<String> authHeaderProvider,
            @Value("${ems.airflow.trigger-path:/dags/{dagId}/dagRuns}") String triggerPath) {
        this.restClient = restClient;
        this.mapper = mapper;
        this.authHeaderProvider = authHeaderProvider;
        this.triggerPath = triggerPath;
    }

    /**
     * Trigger a DAG run.
     *
     * @param dagId    the target Airflow DAG id (path variable)
     * @param dagRunId the deterministic run id (A6) — sent as {@code dag_run_id} so a duplicate is a 409
     * @param confJson the trigger conf, a JSON object serialized as text (embedded verbatim under {@code conf})
     * @return the classified {@link Outcome}; this method does not throw for HTTP or transport failures
     */
    public Outcome trigger(String dagId, String dagRunId, String confJson) {
        ObjectNode body;
        try {
            body = mapper.createObjectNode();
            body.put("dag_run_id", dagRunId);
            body.set("conf", mapper.readTree(confJson));
        } catch (JsonProcessingException e) {
            // The conf is EMS's own serialized JSON object; an unparseable value is a bug, not a transient
            // fault — retrying cannot fix it, so surface it as non-retriable rather than parking the drain.
            log.error("Outbox conf for dag_run_id {} is not valid JSON; treating as non-retriable", dagRunId, e);
            return Outcome.NON_RETRIABLE;
        }

        try {
            return restClient.post()
                    .uri(triggerPath, dagId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .headers(this::applyAuth)
                    .body(body)
                    .exchange((request, response) -> classify(response.getStatusCode(), dagId, dagRunId));
        } catch (ResourceAccessException e) {
            // connect refused / read timeout / I-O — Airflow is unreachable, i.e. transiently down
            log.warn("Airflow unreachable triggering {} (dag_run_id {}): {}", dagId, dagRunId, e.getMessage());
            return Outcome.RETRIABLE;
        }
    }

    private Outcome classify(HttpStatusCode status, String dagId, String dagRunId) {
        if (status.is2xxSuccessful()) {
            return Outcome.DELIVERED;
        }
        int code = status.value();
        if (code == 409) {
            log.debug("DAG run {} already exists for {} (409) — treating as delivered", dagRunId, dagId);
            return Outcome.DELIVERED;
        }
        if (code == 429 || status.is5xxServerError()) {
            log.warn("Airflow returned {} triggering {} (dag_run_id {}) — retriable", code, dagId, dagRunId);
            return Outcome.RETRIABLE;
        }
        log.error("Airflow returned {} triggering {} (dag_run_id {}) — non-retriable", code, dagId, dagRunId);
        return Outcome.NON_RETRIABLE;
    }

    private void applyAuth(HttpHeaders headers) {
        String auth = authHeaderProvider.get();
        if (auth != null && !auth.isBlank()) {
            headers.set(HttpHeaders.AUTHORIZATION, auth);
        }
    }
}
