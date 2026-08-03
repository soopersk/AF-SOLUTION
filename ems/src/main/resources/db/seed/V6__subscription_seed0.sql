-- ============================================================================
-- V6 — seed-0 subscription registry (ems-design §11 stage 0 item 3)
--
-- LOCATION. This file lives in db/seed, NOT db/migration, and only the azure/shadow/live profiles add
-- it to spring.flyway.locations. Schema and data have different lifecycles: a Testcontainers IT wants
-- the schema and its own fixtures, and a developer running against a scratch database should not have
-- 16 production routing rules appear. Putting data in db/migration removes that choice.
--
-- RE-RUNNABLE BY CONSTRUCTION. Every row is an upsert on the V3 natural key
-- (tenant_id, stage, rule_name). Re-running changes nothing except updated_at; correcting a rule is an
-- edit here plus a Flyway repair, or — after cutover — a PUT /admin/subscriptions.
--
-- WHAT THIS IS NOT. Not a fully mechanical port. The map-table condition grammar has no parser in the
-- workspace; it is DERIVED by comparing two rows that use the same construction (A17), which is strong
-- but is still inference. The 7 PERSIST rows carry an invented owner and invented names because the
-- legacy persist property has neither (A12/A15).
-- Every departure from the literal legacy text is numbered in docs/ems-seed0-assumptions.md and each
-- one needs a human tick before cutover. Seed0MigrationTest asserts that every ASSUMPTION-n in that
-- register is referenced from this file, so the two cannot drift.
--
-- RULE DIALECT (A15). All-lowercase paths, all-lowercase literals, plain == / && / startsWith. Nothing
-- else. MatchView lower-cases every key and every string value of the activation tree before CEL runs,
-- which is what supplies the legacy engine's case-insensitivity — once, here, instead of in 16 rules.
--
-- Sources: old-ems/properties.sql:26-35 (persist array), :40-51 (post_filter_control_dag_map),
--          old-ems/JsonFilterRuleset.scala (match semantics), old-ems/EventFilter.SCALA:68-70 (gate).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- PERSIST — the drop gate. Event fields only (A4: a context reference is write-rejected).
-- Source: the eventorchestration.filter.persist JSON array, one row per array element.
-- tenant_id 'PLATFORM' and every rule_name below are INVENTED — the array has neither.
--   ASSUMPTION-5 (owner), ASSUMPTION-6 (names).
-- ---------------------------------------------------------------------------

-- properties.sql:28  {"$.additionalData.tenant":"FRCA"}
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_frca_tenant', NULL,
        'event.additionaldata.tenant == "frca"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:29  {"$.additionalData.tenant":"AQUA_CCR", "$.additionalData.batchtype":"INTRA-MONTH-ADJUSTED"}
-- ASSUMPTION-10: `batchtype` here vs `batchType` in the map table — the fold makes them one key.
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_aqua_ccr_adjusted', NULL,
        'event.additionaldata.tenant == "aqua_ccr" && event.additionaldata.batchtype == "intra-month-adjusted"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:30  {"$.additionalData.tenant":"AQUA_CCR", "$.additionalData.batchtype":"INTRA-MONTH-UNADJUSTED"}
-- ASSUMPTION-10
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_aqua_ccr_unadjusted', NULL,
        'event.additionaldata.tenant == "aqua_ccr" && event.additionaldata.batchtype == "intra-month-unadjusted"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:31  {"$.source":"MERIVAL", "$.additionalData.TYPE":"INGESTION", "$.additionalData.RUN_TYPE":"BATCH"}
-- ASSUMPTION-10: TYPE vs type collide under the fold; document order decides.
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_merival_batch', NULL,
        'event.source == "merival" && event.additionaldata.type == "ingestion" && event.additionaldata.run_type == "batch"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:32  {"$.source":"MERIVAL", "$.additionalData.TYPE":"INGESTION", "$.additionalData.RUN_TYPE":"INTRA"}
-- ASSUMPTION-10
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_merival_intra', NULL,
        'event.source == "merival" && event.additionaldata.type == "ingestion" && event.additionaldata.run_type == "intra"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:33  {"$.source":"RWA", "$.additionalData.tenant":"MR", "$.additionalData.FREQUENCY":"MONTHLY"}
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_rwa_mr_monthly', NULL,
        'event.source == "rwa" && event.additionaldata.tenant == "mr" && event.additionaldata.frequency == "monthly"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:34  {"$.source":"CVA", "$.additionalData.tenant":"MR", "$.additionalData.FREQUENCY":"MONTHLY"}
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('PLATFORM', 'PERSIST', 'persist_cva_mr_monthly', NULL,
        'event.source == "cva" && event.additionaldata.tenant == "mr" && event.additionaldata.frequency == "monthly"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- ---------------------------------------------------------------------------
-- FORWARD — routing fan-out. event.* + context.* allowed.
-- Source: post_filter_control_dag_map. tenant_id = the map's team column, rule_name = filter_name.
-- ---------------------------------------------------------------------------

-- properties.sql:41  '$.additionalData.tenant:FRCA.msgTypeEventType:data-update,
--                      $.additionalData.tenant:FRCA.updateType:CURATION,
--                      $.context.date.run-category:TOPSIDE.*'
-- Grammar (A17): each comma-separated group is `$.<prefix>` followed by one or more `.key:value` pairs,
-- ANDed. cap_data_update.FRCA_CALC below uses the identical construction, which is what makes it
-- readable. The two groups here both restate tenant:FRCA, so the conjunction collapses to four terms.
-- ASSUMPTION-2 (context.date -> context.data), ASSUMPTION-3 (the derivation and its residual risk),
-- ASSUMPTION-4 (resolved — the team column is now explicit), ASSUMPTION-9 (runcategory: the A18 alias
-- resolves camelCase first, hyphenated as fallback).
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_data_update.FRCA_CURATION', 'orchestration_control_dag_capital',
        'event.additionaldata.tenant == "frca" && event.additionaldata.msgtypeeventtype == "data-update" && event.additionaldata.updatetype == "curation" && context.data.runcategory.startsWith("topside")',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:43  '$.additionalData.tenant:FRCA.updateType:CALC_EVENT,$.additionalData.STATE:FINISH'
-- The A17 grammar in its clearest form: `$.additionalData` + `.tenant:FRCA` + `.updateType:CALC_EVENT`,
-- then a second group `$.additionalData.STATE:FINISH`.
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_data_update.FRCA_CALC', 'orchestration_control_dag_capital',
        'event.additionaldata.tenant == "frca" && event.additionaldata.updatetype == "calc_event" && event.additionaldata.state == "finish"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:44  '$.additionalData.tenant:AQUA_CCR,$.additionalData.batchType:INTRA-MONTH-ADJUSTED'
-- ASSUMPTION-10
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_AQUA_CCR_ADJUSTED', 'orchestration_control_dag_capital',
        'event.additionaldata.tenant == "aqua_ccr" && event.additionaldata.batchtype == "intra-month-adjusted"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:45  '$.additionalData.tenant:AQUA_CCR,$.additionalData.batchType:INTRA-MONTH-UNADJUSTED'
-- ASSUMPTION-10
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_AQUA_CCR_UNADJUSTED', 'orchestration_control_dag_capital',
        'event.additionaldata.tenant == "aqua_ccr" && event.additionaldata.batchtype == "intra-month-unadjusted"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:46  '$.source:MERYVAL,$.additionalData.TYPE:INGESTION,$.additionalData.RUN_TYPE:BATCH'
-- ASSUMPTION-1 (MERYVAL -> merival; SIGNED OFF), ASSUMPTION-10
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_data_update.MER_batch', 'orchestration_control_dag_capital',
        'event.source == "merival" && event.additionaldata.type == "ingestion" && event.additionaldata.run_type == "batch"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:47  '$.source:MERYVAL,$.additionalData.TYPE:INGESTION,$.additionalData.RUN_TYPE:INTRA'
-- ASSUMPTION-1 (SIGNED OFF), ASSUMPTION-10
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_data_update.MER_intra', 'orchestration_control_dag_capital',
        'event.source == "merival" && event.additionaldata.type == "ingestion" && event.additionaldata.run_type == "intra"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:48  '$.source:RWA,$.additionalData.tenant:MR,$.additionalData.FREQUENCY:MONTHLY,$.additionalData.STATE:FINISH'
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_RWA', 'orchestration_control_dag_capital',
        'event.source == "rwa" && event.additionaldata.tenant == "mr" && event.additionaldata.frequency == "monthly" && event.additionaldata.state == "finish"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:49  '$.source:CVA,$.additionalData.tenant:MR,$.additionalData.FREQUENCY:MONTHLY,$.additionalData.STATE:FINISH'
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('CAPITAL', 'FORWARD', 'cap_CVA', 'orchestration_control_dag_capital',
        'event.source == "cva" && event.additionaldata.tenant == "mr" && event.additionaldata.frequency == "monthly" && event.additionaldata.state == "finish"',
        'seed-0', TRUE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- properties.sql:50  '$.additionalData.msgTypeEventType:data-update,$.additionalData.tenant:ACTL,$.additionalData.updateType:CURATION'
-- Legacy enabled = FALSE. ASSUMPTION-7: migrated disabled rather than omitted.
INSERT INTO subscription (tenant_id, stage, rule_name, control_dag_id, when_cel, registry_version, enabled, updated_by)
VALUES ('NSFR', 'FORWARD', 'nsfr_data-update', 'orchestration_control_dag_liquidity',
        'event.additionaldata.msgtypeeventtype == "data-update" && event.additionaldata.tenant == "actl" && event.additionaldata.updatetype == "curation"',
        'seed-0', FALSE, 'seed-0-migration')
ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET
    when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id,
    enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version,
    updated_at = now(), updated_by = 'seed-0-migration';

-- ASSUMPTION-8: PERSIST must cover every FORWARD row's event-side conditions, or a forwarded event is
--   dropped before enrichment. There is no filter.post property row in the evidence (A12), so this is a
--   correctness requirement, not a nicety. Checked over the samples by Seed0MigrationTest and over real
--   traffic by the §11 stage-1 shadow parity run.
-- ASSUMPTION-11: registry_version = 'seed-0' throughout; the first CI render replaces it.
-- ASSUMPTION-12: one ruleset for every environment. Per-environment deltas (§14 item 3, unanswered)
--   arrive via PUT /admin/subscriptions after this migration — never by forking V6.
