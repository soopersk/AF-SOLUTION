package com.orchestration.ems.ingestion;

/**
 * Signals that the EDF Context REST API is <em>transiently</em> unavailable (5xx, connect/read
 * timeout, or an I/O failure reading the response) after the client's short bounded in-call retry
 * was exhausted (ems-design §10).
 *
 * <p>This is the "will succeed later" classification: on the ingest path it must <b>park the
 * partition</b> (seek-based unbounded backoff), never dead-letter — the event is not poison, the
 * dependency is merely down (ems-design §4.2 error taxonomy; A1). Batch G's {@code DefaultErrorHandler}
 * treats this marker as transient. A 4xx (context genuinely absent) is <em>not</em> this exception —
 * that path returns an empty {@code Optional} instead.
 */
public class EdfUnavailableException extends RuntimeException {

    public EdfUnavailableException(String message) {
        super(message);
    }

    public EdfUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
