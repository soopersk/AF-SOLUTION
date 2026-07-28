package com.orchestration.ems.store;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.orchestration.ems.model.EventRow;

/**
 * Write path for the {@code event} table (ems-design §5). The raw payload is stored in the
 * {@code json} JSONB column exactly as received (content-verbatim; PostgreSQL canonicalizes only
 * JSONB whitespace/key-order, never values) and the typed {@code GENERATED ALWAYS ... STORED}
 * columns populate automatically from it.
 *
 * <p>Constructor takes a {@link JdbcClient} so the same instance works both as a Spring bean and,
 * in integration tests, over the non-pooled {@code AbstractPostgresIT} datasource.
 */
@Repository
public class EventRepository {

    private static final String UPSERT =
            "INSERT INTO event (event_id, json) VALUES (?, ?::jsonb) ON CONFLICT (event_id) DO NOTHING";

    private final JdbcClient jdbc;

    public EventRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Persist one event's raw payload. Idempotent under Kafka redelivery — a duplicate
     * {@code event_id} is a silent no-op (Amendment A3, {@code ON CONFLICT DO NOTHING}).
     *
     * @param row the event id + byte-verbatim raw JSON to store
     * @return 1 when the row was inserted, 0 when an existing row made it a no-op
     */
    public int upsert(EventRow row) {
        return jdbc.sql(UPSERT)
                .param(row.eventId())
                .param(row.rawJson())
                .update();
    }
}
