# subscriptions_seed0.json — provenance

TEST fixtures only (not the production seed migration — that is a later phase). These translate the
legacy subscription inventory to CEL for use by EMS unit/integration tests.

## Sources
- `old-ems/properties.sql` lines 25–52 — authoritative inventory.
  - `eventorchestration.filter.persist` (7 rows) → the 7 `PERSIST` rows here.
  - `post_filter_control_dag_map` INSERT → the 9 `FORWARD` rows (8 `CAPITAL`, 1 `NSFR`).
- `old-ems/JsonFilterRuleset.scala` — legacy match semantics being modernized:
  array-of-objects = OR across rows; keys within one object = AND; matching is CASE-INSENSITIVE;
  a trailing `.*` is a literal-prefix match (`Regex.quote(prefix) + ".*"`), not free regex.

## Row count: 16
- 7 PERSIST (all `enabled: true`, `controlDagId: null`)
- 8 CAPITAL FORWARD → `orchestration_control_dag_capital` (all `enabled: true`)
- 1 NSFR FORWARD → `orchestration_control_dag_liquidity` (`enabled: false`)

## CEL translation convention
- Each legacy `key:value` AND-term becomes a CEL `==` against the **upper-cased** literal.
  The Normalizer canonicalizes enumerated fields to upper-case at eval time; authoring the rule
  literals in upper-case reproduces the legacy case-insensitive compare. (`data-update` → `"DATA-UPDATE"`.)
- The `TOPSIDE.*` prefix clause becomes `context.data["run-category"].startsWith("TOPSIDE")`.
- `event.*` for the payload; `context.*` for enrichment. `run-category` is hyphenated so it uses
  index syntax `context.data["run-category"]`.

## Corrections applied (typos in the legacy sample rows)
- `MERYVAL` → `MERIVAL` (FORWARD rows `cap_data_update.MER_batch`, `cap_data_update.MER_intra`).
- `context.date.run-category` → `context.data.run-category` (FORWARD row `cap_data_update.FRCA_CURATION`).

## Field-spelling note (faithful to each source, case-insensitive at eval time)
- PERSIST AQUA_CCR rows use the persist-property key spelling `batchtype` (lower-case).
- FORWARD AQUA_CCR rows use the map-table key spelling `batchType` (camelCase).
These differ only in case; the legacy engine and the future Normalizer treat them identically.

## tenantId (owning orchestration team — NOT the upstream `additionalData.tenant` label)
- FORWARD rows are owned per the map table: `CAPITAL` or `NSFR`.
- PERSIST rows have no team owner in the legacy persist property (they are the lifecycle-wide drop
  gate). Assigned a single consistent sentinel owner `PLATFORM` for all 7. This is a fixture
  assumption; the `(tenant_id, stage, rule_name)` uniqueness constraint (V3) is satisfied because
  the invented `ruleName`s are distinct.

## Note on `whenCel` for `cap_data_update.FRCA_CURATION`
The legacy `filter_condition` for this row is malformed (uses `.` where the clean serialization
uses `,`, and repeats `tenant:FRCA`). Interpreted per the row name (FRCA + CURATION) and the
`JsonFilterRuleset` AND semantics as the de-duplicated conjunction:
tenant==FRCA, msgTypeEventType==data-update, updateType==CURATION, run-category startsWith TOPSIDE.
