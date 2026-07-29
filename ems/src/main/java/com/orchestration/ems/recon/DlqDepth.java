package com.orchestration.ems.recon;

/**
 * Unreplayed {@code dlq_record} rows for one source topic — the {@code ems_dlq_depth{topic}} series
 * (ems-design §10).
 *
 * <p><b>Why the table and not the DLT's end offset.</b> Nothing consumes the dead-letter topic, so its
 * end offset is a cumulative total that never falls after a replay; an alert of the form "page when
 * depth &gt; 0 for 5 m" would then fire forever after the first poison message ever seen. The
 * {@code replayed_at IS NULL} count is the actionable triage depth: it returns to zero when an operator
 * has actually replayed the records (§8 runbook).
 *
 * <p><b>Known undercount.</b> The {@code dlq_record} write is best-effort — the DLQ publish is verified,
 * the triage-row insert is not ({@code KafkaConfig}'s recoverer) — so this can read lower than the DLT's
 * true contents. It never reads higher, which keeps it safe as a page.
 */
public record DlqDepth(String topic, long depth) {
}
