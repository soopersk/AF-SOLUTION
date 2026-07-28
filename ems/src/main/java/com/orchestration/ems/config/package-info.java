/**
 * Cross-cutting Spring configuration: Kafka (manual-ack container factory,
 * ErrorHandlingDeserializer, DefaultErrorHandler with seek backoff + poison-only DLQ),
 * security (Entra JWT + Basic, CI principal), datasource/HikariCP, Caffeine cache,
 * RestClient + retry, Azure passwordless, and feature toggles
 * ({@code ems.dispatch.enabled}, {@code ems.consumer.enabled}).
 *
 * <p>See ems-design.md §10.
 */
package com.orchestration.ems.config;
