package com.orchestration.ems.ingestion;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.ContextRow;

/**
 * Fetches a context by id from the EDF Context REST API — the third (miss) tier of the
 * {@code ContextResolver} chain (Caffeine → DB → EDF, ems-design §4.2 step 4).
 *
 * <p><b>Provisional contract (§14 open item 2 — the EDF GET-by-id endpoint/auth/error contract is not
 * yet finalized).</b> This mirrors the legacy shape in {@code old-ems/EventSender.scala:143-164}: a
 * {@code GET {contextPath}} with the id as a path variable and a bearer token, body deserialized as
 * the context payload. WireMock stands in until the real contract lands.
 *
 * <p><b>Error mapping (a deliberate no-loss upgrade over the legacy swallow-all, ems-design §4.2/§10):</b>
 * <ul>
 *   <li><b>2xx</b> → {@code Optional<ContextRow>} of the byte-verbatim body.</li>
 *   <li><b>4xx</b> (incl. 404) → {@code Optional.empty()} — the context is genuinely absent; not an error.</li>
 *   <li><b>5xx / connect-read timeout / I/O</b> → after a short bounded retry, throw
 *       {@link EdfUnavailableException} so the ingest partition <em>parks</em> rather than dropping the
 *       event (legacy returned {@code None} for these, silently losing forwards — corrected here).</li>
 * </ul>
 */
@Component
public class EdfContextClient {

    private static final Logger log = LoggerFactory.getLogger(EdfContextClient.class);

    private final RestClient restClient;
    private final ObjectMapper mapper;
    private final Supplier<String> tokenProvider;
    private final String contextPath;
    private final int maxAttempts;
    private final Duration retryBackoff;

    /**
     * @param restClient    the EDF-rooted {@code RestClient} (base URL configured in {@code RestClientConfig})
     * @param mapper        Jackson mapper used to parse the response into a {@link ContextRow}
     * @param tokenProvider supplies the bearer token per request (provisional — real Entra OAuth2
     *                      client-credentials is wired in-environment; §14 item 2)
     * @param contextPath   the GET path template carrying the id, e.g. {@code /context/{id}}
     * @param maxAttempts   total attempts (including the first) before a transient failure parks; ≥ 1
     * @param retryBackoff  pause between transient retries (kept short — it blocks a partition, §10)
     */
    public EdfContextClient(
            @Qualifier("edfRestClient") RestClient restClient,
            ObjectMapper mapper,
            @Qualifier("edfTokenProvider") Supplier<String> tokenProvider,
            @Value("${ems.edf.context-path:/context/{id}}") String contextPath,
            @Value("${ems.edf.max-attempts:3}") int maxAttempts,
            @Value("${ems.edf.retry-backoff:200ms}") Duration retryBackoff) {
        this.restClient = restClient;
        this.mapper = mapper;
        this.tokenProvider = tokenProvider;
        this.contextPath = contextPath;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.retryBackoff = retryBackoff;
    }

    /**
     * GET the context by id from EDF.
     *
     * @param contextId the context id (must be non-null — the resolver guards null before calling)
     * @return the fetched context, or empty if EDF reports it absent (4xx)
     * @throws EdfUnavailableException if EDF is transiently unavailable after all retries (5xx/timeout/IO)
     */
    public Optional<ContextRow> fetch(String contextId) {
        EdfUnavailableException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return restClient.get()
                        .uri(contextPath, contextId)
                        .headers(headers -> headers.setBearerAuth(tokenProvider.get()))
                        .exchange((request, response) -> {
                            HttpStatusCode status = response.getStatusCode();
                            if (status.is2xxSuccessful()) {
                                String raw = new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8);
                                return Optional.of(ContextRow.of(raw, mapper));
                            }
                            if (status.is4xxClientError()) {
                                log.debug("EDF reports context {} absent (status {})", contextId, status.value());
                                return Optional.<ContextRow>empty();
                            }
                            throw new EdfUnavailableException(
                                    "EDF returned status " + status.value() + " for context id " + contextId);
                        });
            } catch (EdfUnavailableException e) {
                last = e; // 5xx — transient, retry
            } catch (ResourceAccessException e) {
                // connect/read timeout or I/O error (incl. a failed body read wrapped by RestClient)
                last = new EdfUnavailableException("EDF context fetch I/O failure for id " + contextId, e);
            }
            if (attempt < maxAttempts) {
                log.warn("EDF context fetch for {} failed (attempt {}/{}), retrying", contextId, attempt, maxAttempts);
                backoff();
            }
        }
        throw last;
    }

    private void backoff() {
        if (retryBackoff.isZero() || retryBackoff.isNegative()) {
            return;
        }
        try {
            Thread.sleep(retryBackoff.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EdfUnavailableException("interrupted during EDF retry backoff", e);
        }
    }
}
