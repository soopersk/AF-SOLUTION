/**
 * {@code ReconciliationSweep}: a scheduled backstop independent of the Phase-D heartbeat.
 * Publishes {@code ems_dlq_depth}, {@code ems_outbox_pending_age_seconds} and
 * {@code ems_overdue_inflight_runs} (STARTED events with no terminal past a coarse global
 * window) from committed database state, plus {@code ems_consumer_lag} and
 * {@code ems_consumer_retention_headroom_records} from a {@code ConsumerLagProbe} poll of the
 * ingest group's broker-side offsets. See ems-design.md §10.
 */
package com.orchestration.ems.recon;
