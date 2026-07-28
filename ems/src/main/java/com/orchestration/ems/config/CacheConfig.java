package com.orchestration.ems.config;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.orchestration.ems.model.ContextRow;

/**
 * In-process Caffeine caches (ems-design §4, §8 cache table). Only the context-by-id cache lives here;
 * the compiled-subscription cache is owned by {@code SubscriptionService} (a different refresh policy).
 *
 * <p>Context cache: 24 h / 10 000 entries. Safe because contexts are immutable from EDF
 * (fetch-once semantics — verify per §14 item 5); it deduplicates the EDF call and the DB hit for the
 * many events that share a context.
 */
@Configuration
public class CacheConfig {

    @Bean
    public Cache<String, ContextRow> contextCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofHours(24))
                .maximumSize(10_000)
                .build();
    }
}
