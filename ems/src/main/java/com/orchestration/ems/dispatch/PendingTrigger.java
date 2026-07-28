package com.orchestration.ems.dispatch;

/**
 * A pending outbox row claimed for delivery by the dispatcher. Returned by
 * {@link OutboxRepo#drainPending(int)}.
 *
 * @param dagRunId the deterministic run id (Amendment A6: {@code orch_sha1(dag_id + jcs(conf))[:16]});
 *                 also the value sent to Airflow so 409 = already-triggered = delivered
 * @param dagId    the target Airflow DAG id
 * @param conf     the trigger conf JSON to POST (jsonb read back as text)
 * @param attempts prior failed-delivery count; drives the dispatcher-side 30s–600s backoff (§4.2/§12)
 */
public record PendingTrigger(String dagRunId, String dagId, String conf, int attempts) { }
