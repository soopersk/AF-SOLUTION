/**
 * {@code ReconciliationSweep}: a scheduled backstop independent of the Phase-D heartbeat.
 * Publishes {@code ems_consumer_lag}, {@code ems_dlq_depth},
 * {@code ems_outbox_pending_age_seconds}, and {@code ems_overdue_inflight_runs} (STARTED
 * events with no terminal past a coarse global window). See ems-design.md §10.
 */
package com.orchestration.ems.recon;
