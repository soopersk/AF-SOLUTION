package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.store.ContextRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit proof of the tiered {@link ContextResolver} lookup with a real Caffeine cache and mocked DB/EDF
 * tiers (no Docker): cache dedup (a second resolve touches neither tier), DB→EDF fallthrough with
 * save-on-fetch, null-id guard, EDF-absent (empty, uncached), and EDF-outage propagation (park). Also
 * asserts the {@code ems_context_fetch_total{source}} counters. Runs locally.
 */
class ContextResolverTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Cache<String, ContextRow> cache;
    private ContextRepository repo;
    private EdfContextClient edf;
    private SimpleMeterRegistry meters;
    private ContextResolver resolver;

    @BeforeEach
    void setUp() {
        cache = Caffeine.newBuilder().expireAfterWrite(Duration.ofHours(24)).maximumSize(10_000).build();
        repo = Mockito.mock(ContextRepository.class);
        edf = Mockito.mock(EdfContextClient.class);
        meters = new SimpleMeterRegistry();
        resolver = new ContextResolver(cache, repo, edf, meters);
    }

    @Test
    void dbHit_isCached_secondResolveTouchesNeitherTier() {
        ContextRow row = context("ctx-1");
        when(repo.findById("ctx-1")).thenReturn(Optional.of(row));

        assertThat(resolver.resolve("ctx-1")).containsSame(row);
        assertThat(resolver.resolve("ctx-1")).containsSame(row); // now served from cache

        verify(repo, times(1)).findById("ctx-1");
        verify(edf, never()).fetch("ctx-1");
        assertThat(fetchCount("db")).isEqualTo(1.0);
        assertThat(fetchCount("cache")).isEqualTo(1.0);
    }

    @Test
    void dbMiss_edfHit_persistsFetchedContext_thenCaches() {
        ContextRow row = context("ctx-2");
        when(repo.findById("ctx-2")).thenReturn(Optional.empty());
        when(edf.fetch("ctx-2")).thenReturn(Optional.of(row));

        assertThat(resolver.resolve("ctx-2")).containsSame(row);
        verify(repo).upsert(row); // save-on-fetch (legacy parity)

        assertThat(resolver.resolve("ctx-2")).containsSame(row); // cache hit
        verify(edf, times(1)).fetch("ctx-2");
        verify(repo, times(1)).findById("ctx-2");
        verify(repo, times(1)).upsert(row); // not re-persisted from cache
        assertThat(fetchCount("edf")).isEqualTo(1.0);
        assertThat(fetchCount("cache")).isEqualTo(1.0);
    }

    @Test
    void nullContextId_returnsEmpty_withoutTouchingAnyTier() {
        assertThat(resolver.resolve(null)).isEmpty();
        assertThat(resolver.resolve("  ")).isEmpty();
        verifyNoInteractions(repo, edf);
    }

    @Test
    void edfAbsent_returnsEmpty_andIsNotCached() {
        when(repo.findById("ctx-gone")).thenReturn(Optional.empty());
        when(edf.fetch("ctx-gone")).thenReturn(Optional.empty());

        assertThat(resolver.resolve("ctx-gone")).isEmpty();
        assertThat(resolver.resolve("ctx-gone")).isEmpty();

        // absence is not cached — a later create must be resolvable, so both tiers are re-hit
        verify(repo, times(2)).findById("ctx-gone");
        verify(edf, times(2)).fetch("ctx-gone");
    }

    @Test
    void edfOutage_propagatesParkSignal() {
        when(repo.findById("ctx-down")).thenReturn(Optional.empty());
        when(edf.fetch("ctx-down")).thenThrow(new EdfUnavailableException("EDF returned status 503"));

        assertThatThrownBy(() -> resolver.resolve("ctx-down"))
                .isInstanceOf(EdfUnavailableException.class);
        verify(repo, never()).upsert(Mockito.any());
    }

    private static ContextRow context(String id) {
        return ContextRow.of("{\"id\":\"" + id + "\",\"data\":{\"frequency\":\"DAILY\"}}", MAPPER);
    }

    private double fetchCount(String source) {
        return meters.get("ems_context_fetch_total").tag("source", source).counter().count();
    }
}
