-- ============================================================================
-- V5 — transactional outbox + DLQ record (trigger-plan §4.4 / ems-design §5)
-- Outbox row committed in the SAME TX as the event persist — no acked-but-never-
-- forwarded window (Amendment A1: Airflow is off the ingest path). Dispatcher drains
-- with FOR UPDATE SKIP LOCKED; 200/409 = delivered.
--
-- A6: dag_run_id = orch_sha1(dag_id + jcs(conf))[:16] where jcs = RFC 8785 canonical JSON.
--     This is a NEW deterministic scheme (legacy set no run id) — see ems-design §0 A6.
-- ============================================================================

CREATE TABLE dag_trigger_outbox (
    dag_run_id   text PRIMARY KEY,      -- orch_sha1(dag_id + jcs(conf))[:16]  (A6)
    dag_id       text NOT NULL,
    conf         jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz,           -- set on 200/409; also set at cutover for shadow rows (§11)
    attempts     int NOT NULL DEFAULT 0,
    last_error   text
);

-- dispatcher drain + age alert (ems_outbox_pending_age_seconds)
CREATE INDEX ix_outbox_pending ON dag_trigger_outbox (created_at) WHERE delivered_at IS NULL;

CREATE TABLE dlq_record (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic           text NOT NULL,
    kafka_partition int NOT NULL,       -- "partition"/"offset" are reserved words, hence prefixed
    kafka_offset    bigint NOT NULL,
    event_id        text,               -- best-effort extraction from the poison payload
    task_id         text,
    context_id      text,
    error           text NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    replayed_at     timestamptz,
    replayed_by     text
);

-- /run/status dlq_hint lookup + triage
CREATE INDEX ix_dlq_context ON dlq_record (context_id) WHERE context_id IS NOT NULL;
CREATE INDEX ix_dlq_task    ON dlq_record (task_id)    WHERE task_id    IS NOT NULL;
