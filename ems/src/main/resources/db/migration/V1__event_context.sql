-- ============================================================================
-- V1 — event / context store  (ems-design §5, with code-grounding amendments)
-- ----------------------------------------------------------------------------
-- Write path is UNCHANGED from legacy (raw JSONB upsert, ON CONFLICT DO NOTHING —
-- Amendment A3). Read path is made millisecond-fast via typed GENERATED columns.
--
-- Corrections applied vs ems-design §5 (verified against old-ems/ + old-orchestration/):
--   A7  MEG family taskId/taskEventType are nested under event.additionalData
--       (sample_MEG_STARTED_context_enriched_event.json:26) — COALESCE to top-level.
--   A8  Cross-family key spelling: reporting-date|reportingDate, run-category|runCategory,
--       h3Region|regionCode — COALESCE both spellings.
--   A9  parentIds is array-contains queried — no scalar first_parent_id column here;
--       the GIN index lives in V2.
--
-- Design rule (ems-design §5): every promoted column is an IMMUTABLE expression with
-- NO casts. A ::uuid/::date cast would make one malformed message poison all inserts,
-- and text::date is not IMMUTABLE. ISO-8601 text sorts/compares correctly. NULLs from
-- absent JSON keys are expected and fine.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Canonicalization functions — pure value maps mirroring the Java Normalizer
-- (ems-design §4.4). Final maps come from the §14 item 8 production inventory;
-- a CI test asserts Java <-> SQL equivalence exhaustively over the value set.
-- Legacy matching is case-insensitive (old-ems/JsonFilterRuleset.scala:14-31),
-- so unmapped values pass through upper-cased.
-- ---------------------------------------------------------------------------
CREATE FUNCTION ems_norm_freq(raw text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURNS NULL ON NULL INPUT AS $$
    SELECT CASE upper(raw)
        WHEN 'D'       THEN 'DAILY'
        WHEN 'DAILY'   THEN 'DAILY'
        WHEN 'M'       THEN 'MONTHLY'
        WHEN 'MONTHLY' THEN 'MONTHLY'
        WHEN 'Q'       THEN 'QUARTERLY'
        WHEN 'QUARTERLY' THEN 'QUARTERLY'
        ELSE upper(raw)              -- unmapped values pass through upper-cased (TODO §14.8)
    END
$$;

CREATE FUNCTION ems_norm_region(raw text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE RETURNS NULL ON NULL INPUT AS $$
    SELECT CASE upper(raw)
        WHEN 'AMERICAS' THEN 'AMER'   -- ...full alias map from the §14 item 8 inventory...
        ELSE upper(raw)               -- unmapped values pass through upper-cased (TODO §14.8)
    END
$$;

-- ---------------------------------------------------------------------------
-- event — raw payload byte-verbatim in `json`; hot filter attributes promoted.
-- PK = application-supplied event id (legacy: INSERT ... VALUES (event.getId, json)).
-- ---------------------------------------------------------------------------
CREATE TABLE event (
    event_id        text PRIMARY KEY,
    json            jsonb NOT NULL,                       -- raw payload, byte-verbatim

    -- A7: MEG family nests taskId/taskEventType in additionalData; COALESCE to top-level.
    task_id         text GENERATED ALWAYS AS (
                        COALESCE(json->'additionalData'->>'taskId', json->>'taskId')) STORED,
    task_event_type text GENERATED ALWAYS AS (
                        upper(COALESCE(json->'additionalData'->>'taskEventType',
                                       json->>'taskEventType'))) STORED,

    dataset_id      text GENERATED ALWAYS AS (
                        COALESCE(json->'additionalData'->>'DATASET_UUID',
                                 json->'additionalData'->>'datasetId')) STORED,

    -- context correlation: top-level contextId (join key, old-ems join on json->>'contextId'),
    -- with additionalData fallback (MEG family also carries additionalData.contextId).
    context_id      text GENERATED ALWAYS AS (
                        COALESCE(json->>'contextId', json->'additionalData'->>'contextId')) STORED,

    source          text GENERATED ALWAYS AS (upper(json->>'source')) STORED,

    -- STATE nested in additionalData (trigger_event_context.json, sample_calculator_COMPLETE_event.json).
    state           text GENERATED ALWAYS AS (upper(json->'additionalData'->>'STATE')) STORED,

    -- BOTH key spellings live in prod: 'TYPE' (INGESTION) and 'type' (CALC_EVENT).
    event_type      text GENERATED ALWAYS AS (
                        upper(COALESCE(json->'additionalData'->>'TYPE',
                                       json->'additionalData'->>'type'))) STORED,

    business_date         text GENERATED ALWAYS AS (json->>'businessDate') STORED,
    logical_business_date text GENERATED ALWAYS AS (json->>'logicalBusinessDate') STORED,
    event_timestamp       text GENERATED ALWAYS AS (json->>'eventTimestamp') STORED,   -- emit time (§14.4)

    created_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- context — raw payload in `json`; PK = application-supplied context id (context.id).
-- ---------------------------------------------------------------------------
CREATE TABLE context (
    context_id            text PRIMARY KEY,               -- from the context payload's 'id'
    json                  jsonb NOT NULL,

    dataset_id            text GENERATED ALWAYS AS (json->>'datasetId') STORED,  -- absent in Merival; §14

    -- A8: hyphen (Merival/B3F) vs camelCase (CALC_EVENT) spelling — COALESCE both.
    reporting_date        text GENERATED ALWAYS AS (
                              COALESCE(json->'data'->>'reporting-date',
                                       json->'data'->>'reportingDate')) STORED,
    run_category          text GENERATED ALWAYS AS (
                              upper(COALESCE(json->'data'->>'run-category',
                                             json->'data'->>'runCategory'))) STORED,
    h3_region             text GENERATED ALWAYS AS (
                              ems_norm_region(COALESCE(json->'data'->>'h3Region',
                                                       json->'data'->>'regionCode'))) STORED,

    logical_business_date text GENERATED ALWAYS AS (json->'data'->>'logicalBusinessDate') STORED,
    frequency             text GENERATED ALWAYS AS (ems_norm_freq(json->'data'->>'frequency')) STORED,

    created_at            timestamptz NOT NULL DEFAULT now()
    -- A9: no scalar first_parent_id — parentIds is array-contains queried (see V2 GIN index).
);
