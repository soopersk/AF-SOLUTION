package com.orchestration.ems.ingestion;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.store.ContextRepository;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Resolves an event's context by id through the tiered lookup (ems-design §4.2 step 4, grounded in
 * {@code old-ems/EventSender.scala:108-119}):
 *
 * <ol>
 *   <li><b>Caffeine</b> hit → return (source {@code cache});</li>
 *   <li>else <b>DB</b> PK lookup ({@link ContextRepository#findById}) → cache + return (source {@code db});</li>
 *   <li>else <b>EDF</b> ({@link EdfContextClient#fetch}) → on hit <b>persist the fetched context</b>
 *       (mirrors the legacy {@code repository.save(context)} in the {@code orElse}) then cache + return
 *       (source {@code edf}).</li>
 * </ol>
 *
 * <p>A {@code null}/blank context id yields empty with a warning (legacy: "Unable to fetch context for
 * null context id"). An EDF <em>absent</em> (4xx) yields empty; an EDF <em>outage</em> (5xx/timeout)
 * propagates {@link EdfUnavailableException} to park the partition — resolution never silently drops a
 * forwardable event. Every resolution increments {@code ems_context_fetch_total{source}}.
 */
@Component
public class ContextResolver {

    private static final Logger log = LoggerFactory.getLogger(ContextResolver.class);
    private static final String FETCH_METRIC = "ems_context_fetch_total";

    private final Cache<String, ContextRow> cache;
    private final ContextRepository contextRepository;
    private final EdfContextClient edfClient;
    private final MeterRegistry meterRegistry;

    public ContextResolver(Cache<String, ContextRow> contextCache, ContextRepository contextRepository,
            EdfContextClient edfClient, MeterRegistry meterRegistry) {
        this.cache = contextCache;
        this.contextRepository = contextRepository;
        this.edfClient = edfClient;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Resolve the context for the given id.
     *
     * @param contextId the event's context id (may be {@code null})
     * @return the resolved context, or empty when the id is null/blank or EDF reports it absent
     * @throws EdfUnavailableException if EDF is transiently unavailable (park signal)
     */
    public Optional<ContextRow> resolve(String contextId) {
        if (contextId == null || contextId.isBlank()) {
            log.warn("Unable to fetch context for null/blank context id");
            return Optional.empty();
        }

        ContextRow cached = cache.getIfPresent(contextId);
        if (cached != null) {
            count("cache");
            return Optional.of(cached);
        }

        Optional<ContextRow> fromDb = contextRepository.findById(contextId);
        if (fromDb.isPresent()) {
            cache.put(contextId, fromDb.get());
            count("db");
            return fromDb;
        }

        Optional<ContextRow> fromEdf = edfClient.fetch(contextId);
        if (fromEdf.isPresent()) {
            ContextRow row = fromEdf.get();
            contextRepository.upsert(row); // persist the freshly fetched context (legacy save-on-fetch)
            cache.put(contextId, row);
            count("edf");
            return fromEdf;
        }

        // EDF reported the context absent (4xx). Not cached — a later create should be resolvable.
        return Optional.empty();
    }

    private void count(String source) {
        meterRegistry.counter(FETCH_METRIC, "source", source).increment();
    }
}
