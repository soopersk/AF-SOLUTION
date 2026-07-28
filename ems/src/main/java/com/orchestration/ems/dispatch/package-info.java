/**
 * Transactional-outbox dispatch: {@code OutboxRepo}, {@code OutboxDispatcher} (drains with
 * {@code FOR UPDATE SKIP LOCKED}; safe to run in every pod), and {@code AirflowTriggerClient}
 * (idempotent trigger; 200/409 = delivered). The outbox row is committed in the SAME TX as
 * the event persist, so there is no acked-but-never-forwarded window (Amendment A1).
 *
 * <p>Trigger identity (Amendment A6):
 * {@code dag_run_id = orch_sha1(dag_id + canonical_json(conf))[:16]}, where
 * {@code canonical_json} = RFC 8785 JCS. Derivation lives in
 * {@link com.orchestration.ems.canonical}. See ems-design.md §4.4.
 */
package com.orchestration.ems.dispatch;
