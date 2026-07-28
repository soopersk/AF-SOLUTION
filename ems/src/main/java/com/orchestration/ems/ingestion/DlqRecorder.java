package com.orchestration.ems.ingestion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Writes a {@code dlq_record} triage row when a poison event is dead-lettered (ems-design §5, Amendment
 * A1: DLQ is poison-only). Called by the Batch-G {@code DefaultErrorHandler} recoverer <em>after</em> the
 * authoritative verified publish to {@code <topic>.ems.dlq}, so the DLT message — not this row — is the
 * gate: the record is {@link #record best-effort}.
 *
 * <p><b>Best-effort by contract.</b> The offending payload is, by definition, malformed, so correlation
 * keys ({@code event_id}, {@code task_id}, {@code context_id}) are extracted defensively — any parse
 * failure yields {@code null}s rather than throwing. The DB insert itself is wrapped so a storage hiccup
 * never fails the recoverer (which would redeliver an already-DLT'd record and duplicate the DLT message);
 * the row is for {@code /run/status dlq_hint} triage, not for correctness.
 */
@Component
public class DlqRecorder {

    private static final Logger log = LoggerFactory.getLogger(DlqRecorder.class);

    private static final String INSERT = """
            INSERT INTO dlq_record (topic, kafka_partition, kafka_offset, event_id, task_id, context_id, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public DlqRecorder(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * Persist a triage row for a dead-lettered record. Never throws: an extraction or storage failure is
     * logged and swallowed so the recoverer's verified DLT publish stays authoritative.
     *
     * @param record the poison consumer record (its raw value is parsed best-effort for correlation keys)
     * @param ex     the failure that classified the record as poison (its chain is stored as {@code error})
     */
    public void record(ConsumerRecord<?, ?> record, Exception ex) {
        try {
            Correlation c = correlate(record.value());
            jdbc.sql(INSERT)
                    .param(record.topic())
                    .param(record.partition())
                    .param(record.offset())
                    .param(c.eventId())
                    .param(c.taskId())
                    .param(c.contextId())
                    .param(describe(ex))
                    .update();
        } catch (RuntimeException storageFailure) {
            log.warn("dlq_record insert failed for {}-{}@{} (DLT publish already succeeded)",
                    record.topic(), record.partition(), record.offset(), storageFailure);
        }
    }

    /** Best-effort correlation-key extraction from a possibly-malformed payload. */
    private Correlation correlate(Object value) {
        if (!(value instanceof String raw) || raw.isBlank()) {
            return Correlation.EMPTY;
        }
        try {
            JsonNode tree = mapper.readTree(raw);
            return new Correlation(text(tree, "id"), text(tree, "taskId"), text(tree, "contextId"));
        } catch (Exception notJson) {
            return Correlation.EMPTY; // poison is often unparseable — that is expected here
        }
    }

    private static String text(JsonNode tree, String field) {
        return tree.path(field).asText(null);
    }

    /** Render the exception chain compactly for {@code dlq_record.error}. */
    private static String describe(Throwable ex) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = ex; t != null && t != t.getCause(); t = t.getCause()) {
            if (sb.length() > 0) {
                sb.append(" <- ");
            }
            sb.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
        }
        return sb.toString();
    }

    private record Correlation(String eventId, String taskId, String contextId) {
        static final Correlation EMPTY = new Correlation(null, null, null);
    }
}
