-- ============================================================================
-- V4 — routing_decision (trigger-plan §4.5 / ems-design §5, verbatim contract)
-- Slim durable audit: L0 verdicts (EMS), L1 summaries/outcomes + gate evals (dispatcher,
-- Phase B+). Retained on the AUDIT horizon (years), not the event horizon — §9/§14.10.
-- ============================================================================

CREATE TABLE routing_decision (
    decision_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),   -- pgcrypto/PG13+ builtin
    event_id         text NOT NULL,
    tenant_id        text,
    tier             text NOT NULL,   -- 'L0_SUBSCRIPTION' | 'L1_SUMMARY' | 'L1_OUTCOME' | 'GATE'
    target_dag_id    text,            -- NULL for L1_SUMMARY
    decision         text NOT NULL,   -- FORWARDED | NOT_SUBSCRIBED | MATCHED | TRIGGERED
                                      -- | ERROR | GATE_OPEN | GATE_WAITING
    detail           jsonb,           -- counts / failing clause / missing set / completing ids
    registry_version text,
    engine_version   text,            -- e.g. 'celpy==x.y.z' / 'cel-java==x.y.z' (recomputability anchor)
    decided_by       text NOT NULL,   -- 'ems' | '<tenant>_control_dag' | '<tenant>_heartbeat'
    decided_at       timestamptz NOT NULL DEFAULT now()
);

-- L0 idempotency under Kafka redelivery: the ingest TX inserts L0 rows with
-- ON CONFLICT DO NOTHING against this partial unique index.
CREATE UNIQUE INDEX ux_rd_l0 ON routing_decision (event_id, tenant_id)
    WHERE tier = 'L0_SUBSCRIPTION';

-- audit + debuggability queries (trigger-plan §7)
CREATE INDEX ix_rd_event  ON routing_decision (event_id);
CREATE INDEX ix_rd_target ON routing_decision (target_dag_id, decided_at);
CREATE INDEX ix_rd_tier   ON routing_decision (tier, decided_at);
