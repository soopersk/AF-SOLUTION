# subscriptions_seed0.json — provenance

Test fixtures. The **production** seed is `ems/src/main/resources/db/seed/V6__subscription_seed0.sql`,
and `Seed0MigrationTest` asserts that this file and that migration carry identical rows — they are two
hand-maintained copies of one ruleset, and a divergence would mean every test in this module validates
rules that are not the ones deployed.

**The sign-off artifact is [`docs/ems-seed0-assumptions.md`](../../../../../docs/ems-seed0-assumptions.md).**
Read that before trusting these rows. This file records where they came from; that one records what was
assumed to get here, with a box per assumption that a human has to tick.

## Sources
- `old-ems/properties.sql:26-35` — `eventorchestration.filter.persist` (7 rows) → the 7 `PERSIST` rows.
- `old-ems/properties.sql:40-51` — `post_filter_control_dag_map` INSERT → the 9 `FORWARD` rows
  (8 `CAPITAL`, 1 `NSFR`).
- `old-ems/JsonFilterRuleset.scala` — legacy match semantics: array-of-objects = OR across rows, keys
  within one object = AND, matching **case-insensitive**, a trailing `.*` is a literal-prefix match
  (`Regex.quote(prefix) + ".*"`), not free regex.

## Row count: 16
- 7 PERSIST (all `enabled: true`, `controlDagId: null`)
- 8 CAPITAL FORWARD → `orchestration_control_dag_capital` (all `enabled: true`)
- 1 NSFR FORWARD → `orchestration_control_dag_liquidity` (`enabled: false`)

## CEL dialect (amendments A13 → A15)

**A13 finding:** CEL is case-sensitive in keys *and* values; the legacy engine was case-insensitive in
both; and the `Normalizer` does **not** compensate — it canonicalizes a short list of enumerated values,
not key spellings.

**A15 remedy:** the fold moved out of the rule text and into the activation view. `MatchView` lower-cases
every object key and every string value of the event/context tree — after normalization, in memory only —
before CEL evaluates. So every rule here is:

> all-lowercase paths, all-lowercase literals, plain `==` / `&&` / `startsWith`. Nothing else.

No `.lowerAscii()`, no `has()` guards, no either-spelling branches. These rules are maintained by DAG
authors, and one dialect is the whole point.

Consequences visible in this file:
- `event.additionalData.TYPE` → `event.additionaldata.type`; `$.source:MERIVAL` → `event.source ==
  "merival"`; `TOPSIDE.*` → `.startsWith("topside")`.
- The persist property's `batchtype` and the map table's `batchType` fold to the same key, so the AQUA_CCR
  persist and forward rules are now **textually identical**. That is the intended outcome, not a copy
  error.
- `run-category` is reached as `context.data.runcategory` (**A18**): the fold also aliases every hyphenated
  key to a hyphen-free name, so `run-category` and `runCategory` are one name to a rule author. Where both
  spellings are present the camelCase value wins; the hyphenated key stays reachable. See ASSUMPTION-9 —
  this **widens** matching versus legacy and is an expected shadow-parity diff.

**The fold is a routing-path view only.** Stored JSONB, promoted columns, `/run/status`, `/gate/groups`
and the outgoing Airflow `conf` never see a folded tree, and `MatchView` is deliberately not part of the
`Normalizer` so `ems_normalization_mutations_total` keeps the reviewed-and-≈-zero meaning §4.4 needs.

## Departures from the literal legacy text

All of these are numbered and awaiting sign-off in `docs/ems-seed0-assumptions.md`; the short version:

| | |
|---|---|
| `MERYVAL` → `merival` (2 FORWARD rows) | ASSUMPTION-1 — **signed off**; starts forwarding traffic legacy never did |
| `$.context.date.run-category` → `context.data["run-category"]` | ASSUMPTION-2 |
| `cap_data_update.FRCA_CURATION` condition collapsed to four AND-terms | ASSUMPTION-3 (grammar derived per A17) |
| ~~That row's team inferred from a malformed column layout~~ | ASSUMPTION-4 — **void, source corrected** |
| `PLATFORM` invented as the owner of all 7 PERSIST rows | ASSUMPTION-5 |
| The 7 PERSIST rule names invented (the legacy array has none) | ASSUMPTION-6 |
| `runCategory` preferred over `run-category` (A18) — widens matching vs legacy | ASSUMPTION-9 |
| Key-collision survivors under the fold (`TYPE`/`type`, `batchtype`/`batchType`) | ASSUMPTION-10 |
