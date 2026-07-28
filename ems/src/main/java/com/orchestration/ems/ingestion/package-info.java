/**
 * Kafka ingest pipeline: {@code EventConsumer}, {@code Normalizer} (value canonicalization
 * at the edges only — upper-casing, normFreq, region resolution; the single Java authority
 * mirrored by the SQL {@code ems_norm_*} functions), {@code ContextResolver} (Caffeine +
 * EDF fetch), and {@code EdfContextClient}.
 *
 * <p>Two-stage Level-0 (Amendment A4): PERSIST drop-gate (event-only CEL, pre-enrichment)
 * then FORWARD routing (event+context CEL, post-enrichment), culminating in the single-TX
 * persist of event/context/decision/outbox. See ems-design.md §4.
 */
package com.orchestration.ems.ingestion;
