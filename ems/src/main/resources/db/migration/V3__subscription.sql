-- ============================================================================
-- V3 — Level-0 tenant subscriptions (ems-design §5/§7, Amendment A4)
-- Two stages mirroring the legacy PRE/POST split:
--   PERSIST  drop gate  — event.* only CEL, pre-enrichment  (old-ems: filterPersist)
--   FORWARD  routing     — event.* + context.* CEL, post-enrichment (old-ems: filterPostWithDagIds)
--
-- Seed note (A4 + code grounding): legacy effective persist = PERSIST u FORWARD
-- (old-ems/EventListener.scala:20-39). The seed-0 rows must satisfy PERSIST >= FORWARD
-- (the §7 CI invariant) so no FORWARD-only match is dropped by the persist gate.
-- ============================================================================

CREATE TABLE subscription (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id        text NOT NULL,      -- orchestration team (CAPITAL, NSFR, ...) — NOT the
                                         -- upstream additionalData.tenant label (FRCA/MR/ACTL); §7
    stage            text NOT NULL CHECK (stage IN ('PERSIST', 'FORWARD')),
    rule_name        text NOT NULL,      -- carried from legacy filter_name (e.g. 'cap_data_update.MER_batch')
    control_dag_id   text,               -- FORWARD only
    when_cel         text NOT NULL,      -- PERSIST: event.* only (A4; write-rejected otherwise)
                                         -- FORWARD: event.* + context.* allowed
    registry_version text NOT NULL,      -- 'seed-0' until Phase B CI takes over
    enabled          boolean NOT NULL DEFAULT true,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    updated_by       text NOT NULL,      -- 'seed' | 'ci' | break-glass identity (audited)
    CONSTRAINT forward_requires_dag CHECK (stage <> 'FORWARD' OR control_dag_id IS NOT NULL),
    CONSTRAINT uq_subscription UNIQUE (tenant_id, stage, rule_name)
);

-- NOTE: seed-0 rows are inserted by a later repeatable/versioned seed migration once the
-- per-environment inventory is confirmed (§14 item 3). The mechanical translation of
-- old-ems/properties.sql + post_filter_control_dag_map is tracked as a Foundation task,
-- with PERSIST >= FORWARD verified before insert.
