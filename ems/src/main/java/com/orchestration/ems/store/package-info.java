/**
 * Persistence via {@code JdbcClient} (no ORM): {@code EventRepository} and
 * {@code ContextRepository}. Write path is byte-verbatim JSONB upsert with
 * {@code ON CONFLICT DO NOTHING} (Amendment A3, unchanged from legacy); the read path is
 * served by typed {@code GENERATED ALWAYS AS ... STORED} columns (Flyway V1). See
 * ems-design.md §5.
 */
package com.orchestration.ems.store;
