package com.orchestration.ems.model;

import java.util.List;

/**
 * Wire model for {@code POST /admin/replay} (ems-design §4.5:220) — "re-publish selected DLQ records to
 * the source topic … every invocation writes an audit row; safe end-to-end because every downstream step
 * is idempotent".
 *
 * <p><b>Selection is by {@code dlq_record.id}, deliberately.</b> An operator triages the table (or
 * {@code /run/status}'s {@code dlq_hint}), picks the rows, and names them. Broader selectors
 * ("everything on this topic", "everything since 09:00") are not implemented: they turn a typo into a
 * mass re-drive, and nothing in the design asks for them. The parenthetical second mode in §4.5:220
 * ("or re-emit stored events through the pipeline") is likewise left out — {@code dlq_record} is the
 * poison ledger, and re-emitting healthy stored events is a different operation with a different
 * blast radius.
 *
 * <p>{@code dlq_record.replayed_by} is stamped with the <b>authenticated principal</b>, not with a body
 * field — §4.5:220 requires an elevated JWT group here, and an operator audit that the caller writes for
 * itself would record nothing worth keeping.
 *
 * @param ids the {@code dlq_record} ids to re-publish; required, may be empty (a no-op is not an error),
 *            and de-duplicated before use
 */
public record DlqReplay(List<Long> ids) {

    /** Whether the envelope is usable: a present (possibly empty) id list, with no null entries. */
    public boolean isValid() {
        return ids != null && !ids.contains(null);
    }

    /**
     * The per-invocation outcome: how many distinct ids were considered, how many messages were actually
     * re-published, and precisely why each of the rest was not.
     *
     * <p><b>Why this is a 200 with reasons rather than a 5xx.</b> Unlike {@code POST /decisions} — where
     * swallowing a failure behind a 200 would lose an audit record forever — nothing here is lost when a
     * record cannot be replayed: the {@code dlq_record} row stays unstamped and re-replayable, and the
     * operator reading the response gets a per-id disposition instead of one opaque failure for the batch.
     *
     * @param requested the number of distinct ids acted on
     * @param replayed  the number of messages re-published and stamped
     * @param skipped   one entry per id that was not replayed, with its reason (order follows the request)
     */
    public record Result(int requested, int replayed, List<Skipped> skipped) {
    }

    /**
     * One id that was not replayed.
     *
     * @param id     the requested {@code dlq_record} id
     * @param reason why it was skipped
     */
    public record Skipped(long id, Reason reason) {
    }

    /** Why a selected record was not re-published (serialized by name). */
    public enum Reason {

        /** No {@code dlq_record} row with that id (already purged by the §6 retention job, or a typo). */
        NOT_FOUND,

        /**
         * The row already carries a {@code replayed_at} stamp. Skipped rather than replayed again: a
         * record that poisons a second time produces a <em>new</em> {@code dlq_record} row, so re-posting
         * the same id is a double-submit, and honouring it would both duplicate the message and overwrite
         * the first replay's audit trail.
         */
        ALREADY_REPLAYED,

        /**
         * The original message is no longer readable at its recorded {@code (partition, offset)} — the
         * source topic's retention has passed it, or the offset never existed. {@code dlq_record} keeps
         * triage rows for 13 months (§6) while topic retention is days, so this is the expected outcome
         * for an old row, not a fault.
         */
        PAYLOAD_UNAVAILABLE,

        /** The broker rejected (or did not acknowledge) the re-publish. The row is left unstamped. */
        PUBLISH_FAILED
    }
}
