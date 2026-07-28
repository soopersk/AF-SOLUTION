package com.orchestration.ems.ingestion;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.orchestration.ems.model.ContextRow;

/**
 * WireMock proof of the provisional EDF GET-by-id contract ({@link EdfContextClient}): 200 → present +
 * byte-verbatim with a bearer token; 404 → empty (absent, not an error); 503 → retried then
 * {@link EdfUnavailableException} (the park signal). Runs locally (no Docker).
 */
class EdfContextClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static WireMockServer wm;
    private EdfContextClient client;

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
    void reset() {
        wm.resetAll();
        RestClient rest = RestClient.builder().baseUrl(wm.baseUrl()).build();
        // maxAttempts 3, zero backoff → the 503 retry loop is instant
        client = new EdfContextClient(rest, MAPPER, () -> "test-token", "/context/{id}", 3, Duration.ZERO);
    }

    @Test
    void ok200_returnsPresentContext_verbatim_withBearerToken() {
        String body = "{\"id\":\"ctx-1\",\"data\":{\"frequency\":\"DAILY\"}}";
        wm.stubFor(get(urlEqualTo("/context/ctx-1")).willReturn(okJson(body)));

        Optional<ContextRow> result = client.fetch("ctx-1");

        assertThat(result).isPresent();
        assertThat(result.get().contextId()).isEqualTo("ctx-1");
        assertThat(result.get().rawJson()).isEqualTo(body);
        wm.verify(getRequestedFor(urlEqualTo("/context/ctx-1"))
                .withHeader("Authorization", equalTo("Bearer test-token")));
    }

    @Test
    void notFound404_returnsEmpty_contextAbsent() {
        wm.stubFor(get(urlEqualTo("/context/ctx-missing")).willReturn(aResponse().withStatus(404)));

        assertThat(client.fetch("ctx-missing")).isEmpty();
    }

    @Test
    void serverError503_isRetriedThenThrowsParkSignal() {
        wm.stubFor(get(urlEqualTo("/context/ctx-down")).willReturn(aResponse().withStatus(503)));

        assertThatThrownBy(() -> client.fetch("ctx-down"))
                .isInstanceOf(EdfUnavailableException.class)
                .hasMessageContaining("503");

        wm.verify(3, getRequestedFor(urlEqualTo("/context/ctx-down"))); // maxAttempts exhausted
    }
}
