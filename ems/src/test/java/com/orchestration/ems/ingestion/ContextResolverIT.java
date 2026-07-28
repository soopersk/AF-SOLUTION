package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.store.ContextRepository;
import com.orchestration.ems.support.AbstractPostgresIT;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Integration proof of the {@link ContextResolver} DB tier against a real PostgreSQL: a context
 * previously persisted is resolved from the DB (not EDF), cached, and counted as source {@code db}.
 * {@code disabledWithoutDocker} auto-skips locally; runs in CI.
 */
class ContextResolverIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ContextRepository repo = new ContextRepository(jdbcClient(), MAPPER);
    private final EdfContextClient edf = mock(EdfContextClient.class);
    private final Cache<String, ContextRow> cache =
            Caffeine.newBuilder().expireAfterWrite(Duration.ofHours(24)).maximumSize(10_000).build();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final ContextResolver resolver = new ContextResolver(cache, repo, edf, meters);

    @BeforeEach
    void clean() {
        jdbcClient().sql("TRUNCATE context").update();
        cache.invalidateAll();
    }

    @Test
    void resolve_servesFromDb_withoutCallingEdf_thenCaches() throws IOException {
        repo.upsert(ContextRow.of(resource("/samples/context_merival.json"), MAPPER)); // context_id ctx-300

        Optional<ContextRow> first = resolver.resolve("ctx-300");
        assertThat(first).isPresent();
        assertThat(first.get().contextId()).isEqualTo("ctx-300");

        // second resolve served from cache; EDF never consulted for a DB-resident context
        assertThat(resolver.resolve("ctx-300")).isPresent();
        verify(edf, never()).fetch("ctx-300");

        assertThat(meters.get("ems_context_fetch_total").tag("source", "db").counter().count()).isEqualTo(1.0);
        assertThat(meters.get("ems_context_fetch_total").tag("source", "cache").counter().count()).isEqualTo(1.0);
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = ContextResolverIT.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
