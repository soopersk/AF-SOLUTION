package com.orchestration.ems.ingestion;

import java.util.Locale;

/**
 * What the §4.2 pipeline did with one consumed record — the {@code outcome} dimension of
 * {@code ems_events_consumed_total{topic,outcome}} (ems-design §10).
 *
 * <p>The three constants are the three ways {@link IngestionService#process(String)} can return
 * normally. There is a fourth outcome on the wire, {@value #POISON}, which this enum deliberately does
 * <em>not</em> model: a poison record never returns from {@code process} at all — it throws, and the
 * container's error handler counts it while dead-lettering (Amendment A1). A record that fails
 * transiently is counted by nobody, which is correct: it has not been consumed, it is parked and will
 * be redelivered.
 *
 * <p>Together the four values partition the topic's traffic, so
 * {@code sum(rate(ems_events_consumed_total[5m])) by (topic)} is the real ingest rate and the
 * {@code outcome} split is the drop/duplicate/poison mix.
 */
public enum IngestOutcome {

    /** Zero PERSIST rules matched — the intended firehose drop (no context fetch, no write). */
    DROPPED,

    /** The event was new: persisted, decided and (if forwarded) enqueued, in one transaction. */
    PERSISTED,

    /** Redelivery of an already-persisted event — the {@code ON CONFLICT DO NOTHING} no-op path. */
    DUPLICATE;

    /** The §10 counter these values tag. */
    public static final String METRIC = "ems_events_consumed_total";

    /** Tag carrying the source topic. */
    public static final String TAG_TOPIC = "topic";

    /** Tag carrying the outcome. */
    public static final String TAG_OUTCOME = "outcome";

    /**
     * The fourth {@code outcome} value, owned by {@code KafkaConfig}'s recoverer rather than by this
     * enum — a poison record has no pipeline outcome because the pipeline threw before producing one.
     */
    public static final String POISON = "poison";

    /** This outcome as a metric tag value (lower-case, matching {@value #POISON}'s style). */
    public String tagValue() {
        return name().toLowerCase(Locale.ROOT);
    }
}
