package com.orchestration.ems.store;

import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.ContextRow;

/**
 * Read/write path for the {@code context} table (ems-design §5). Mirrors {@link EventRepository}: the
 * raw payload is stored content-verbatim in {@code json} JSONB; generated columns
 * ({@code reporting_date}, {@code run_category}, {@code h3_region} via {@code ems_norm_region},
 * {@code frequency} via {@code ems_norm_freq}) populate automatically, and {@code parentIds} is
 * array-containment queried via the GIN index (Amendment A9), not a scalar column.
 *
 * <p>{@link #findById(String)} is the DB tier of the {@code ContextResolver} lookup chain
 * (Caffeine → DB → EDF, ems-design §4.2 step 4).
 */
@Repository
public class ContextRepository {

    private static final String UPSERT =
            "INSERT INTO context (context_id, json) VALUES (?, ?::jsonb) ON CONFLICT (context_id) DO NOTHING";

    private static final String FIND_BY_ID = "SELECT json FROM context WHERE context_id = ?";

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public ContextRepository(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * Persist one context's raw payload. Idempotent — a duplicate {@code context_id} is a silent
     * no-op ({@code ON CONFLICT DO NOTHING}).
     *
     * @param row the context id + byte-verbatim raw JSON to store
     * @return 1 when the row was inserted, 0 when an existing row made it a no-op
     */
    public int upsert(ContextRow row) {
        return jdbc.sql(UPSERT)
                .param(row.contextId())
                .param(row.rawJson())
                .update();
    }

    /**
     * Look up a context by its primary key. The returned {@code rawJson} is the JSONB column's text
     * form as PostgreSQL renders it (whitespace/key-order canonicalized) — <em>not</em> byte-identical
     * to the originally received bytes; byte-verbatim fidelity is a write-time guarantee, and callers
     * on the resolve path use the parsed tree. A re-persist of this row via {@link #upsert} is a no-op.
     *
     * @param contextId the context primary key
     * @return the context row if present, else empty
     */
    public Optional<ContextRow> findById(String contextId) {
        return jdbc.sql(FIND_BY_ID)
                .param(contextId)
                .query((rs, rowNum) -> ContextRow.of(rs.getString("json"), mapper))
                .optional();
    }
}
