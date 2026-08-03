# `seed-0` subscription migration — assumptions register

**This is a human sign-off gate, not a test.** `Seed0MigrationTest` proves the SQL is well-formed, that
every rule compiles, that no `PERSIST` rule touches `context.*`, that the migration and the test fixture
agree, and that `PERSIST ⊇ FORWARD` holds over the sample payloads. **None of that proves the rows are
correct.** Correctness means "these rules route the same events the legacy service routes", and the only
evidence for that is this document plus the §11 stage-1 shadow parity run.

**Scope.** 16 rows: 7 `PERSIST` + 8 `CAPITAL FORWARD` + 1 `NSFR FORWARD`.

**Sources, both read as code rather than as documentation:**

| Source | What it is |
|---|---|
| [`old-ems/properties.sql:26-35`](../old-ems/properties.sql) | `eventorchestration.filter.persist` — a JSON array, well-formed. The 7 `PERSIST` rows. |
| [`old-ems/properties.sql:40-51`](../old-ems/properties.sql) | `post_filter_control_dag_map` INSERT — 9 rows. The 9 `FORWARD` rows. **Corrected at source 2026-08-02**: the `FRCA_CURATION` row's column layout was malformed and has been fixed. The condition text itself is unchanged. |
| [`old-ems/JsonFilterRuleset.scala`](../old-ems/JsonFilterRuleset.scala) | Match semantics: array-of-objects = OR across rows, keys within one object = AND, matching **case-insensitive**, trailing `.*` = literal-prefix (`Regex.quote(prefix) + ".*"`). |
| [`old-ems/EventFilter.SCALA:68-70`](../old-ems/EventFilter.SCALA) | The Kafka admission gate: `!(filterPersist ‖ filterPost)`. |

**Rule dialect (amendment A15).** Every rule below is all-lowercase paths, all-lowercase literals, plain
`==` / `&&` / `startsWith`. Nothing else. The case-insensitivity of the legacy engine is supplied once by
`MatchView`, which lower-cases every object key and every string value of the activation tree before CEL
sees it, and (amendment A18) also exposes every hyphenated key under a hyphen-free alias so
`run-category` and `runCategory` are one name. Rule authors — DAG authors — never write `.lowerAscii()`,
a `has()` guard, or an either-spelling branch.

---

## Assumptions

Each is numbered, referenced from a SQL comment in `V6__subscription_seed0.sql`, and carries its own
sign-off box. `Seed0MigrationTest` asserts that every `ASSUMPTION-n` here has a matching reference in the
migration, so the two cannot drift apart silently.

### ASSUMPTION-1 — `MERYVAL` is a typo for `MERIVAL`

- [x] **Signed off 2026-08-02 — confirmed a typo.**

**Affects:** `cap_data_update.MER_batch` ([:46](../old-ems/properties.sql)),
`cap_data_update.MER_intra` ([:47](../old-ems/properties.sql)).

The map table says `$.source:MERYVAL`. The persist property, one `INSERT` earlier, says
`"$.source":"MERIVAL"` ([:31-32](../old-ems/properties.sql)), and `MERIVAL` is the spelling used
throughout the sample payloads. Translated as `merival`.

**Carry this into the cutover plan.** Now that it is settled as a typo, it follows that the legacy
service has never been forwarding MERIVAL ingestion events to `orchestration_control_dag_capital` —
those two rows match nothing today. EMS **starts** forwarding them at cutover, so the capital control
DAG sees traffic it has not seen from this path before.

Expect a step change in trigger volume during §11 stage 1, and **do not read it as a shadow-parity
failure** — it is the correction working. Two things follow: the capital DAG must be able to absorb the
additional runs, and the parity report needs this listed as an expected diff before the run, not
explained away after it.

### ASSUMPTION-2 — `$.context.date.run-category` means `context.data["run-category"]`

- [ ] Signed off

**Affects:** `cap_data_update.FRCA_CURATION` ([:42](../old-ems/properties.sql)).

The legacy path segment is `date`. No context payload in `ems/src/test/resources/samples/` has a `date`
object; every one carries the field under `data`. Read as a typo for `data`.

### ASSUMPTION-3 — the `FRCA_CURATION` condition parses to four AND-terms

- [ ] Signed off

**Affects:** `cap_data_update.FRCA_CURATION`. **Re-derived 2026-08-02 against the corrected source.**

Verbatim legacy text ([:41](../old-ems/properties.sql)):

```
'$.additionalData.tenant:FRCA.msgTypeEventType:data-update, $.additionalData.tenant:FRCA.updateType:CURATION,$.context.date.run-category:TOPSIDE.*'
```

**The grammar is now legible**, because `cap_data_update.FRCA_CALC` one row below
([:43](../old-ems/properties.sql)) uses the *identical* construction:

```
'$.additionalData.tenant:FRCA.updateType:CALC_EVENT,$.additionalData.STATE:FINISH'
```

Read together, a condition is a comma-separated list of groups; each group is a `$.`-rooted path prefix
followed by one or more `.key:value` pairs; everything ANDs. So
`$.additionalData.tenant:FRCA.updateType:CALC_EVENT` means *"under `additionalData`: `tenant == FRCA`
and `updateType == CALC_EVENT`"* — which is plainly what the FRCA_CALC row is meant to say. **That
agreement across two rows is what makes the reading trustworthy**, and it is the material change from
the earlier reading, which had to guess from the rule's name.

Applied to FRCA_CURATION, three groups:

| Group | Terms |
|---|---|
| `$.additionalData.tenant:FRCA.msgTypeEventType:data-update` | `tenant == frca`, `msgtypeeventtype == data-update` |
| `$.additionalData.tenant:FRCA.updateType:CURATION` | `tenant == frca`, `updatetype == curation` |
| `$.context.date.run-category:TOPSIDE.*` | `runcategory` startsWith `topside` (ASSUMPTION-2, ASSUMPTION-9) |

`tenant == frca` appears in two groups. ANDing an equality with itself is a no-op, so the conjunction
collapses to four distinct terms — **the repetition is not a defect, and the de-duplication is
information-preserving**. The resulting CEL is unchanged from the first translation; only its
justification has changed, from inference-by-name to derivation-by-grammar.

**Why this still needs a tick.** `PostFilterControlDagMapItem.parseCondition`
([`old-ems/EventFilter.SCALA:86`](../old-ems/EventFilter.SCALA)) remains absent from the workspace, so
the grammar is inferred from two samples rather than read from an implementation. It is consistent with
all nine rows, and `JsonFilterRuleset` independently confirms the trailing `.*` as a literal-prefix
match. Confirm against the real parser if it can be recovered.

### ASSUMPTION-4 — the `FRCA_CURATION` row's team — **RESOLVED, no longer an assumption**

- [x] **Void 2026-08-02 — the source was corrected.**

This row previously carried a malformed column layout in which the `'filter.name'` marker was missing and
the team position held the DAG id, so `CAPITAL` had to be inferred from the DAG id and the `cap_` prefix.
[`properties.sql:41`](../old-ems/properties.sql) now reads
`('CAPITAL', 'cap_data_update.FRCA_CURATION', <condition>, 'orchestration_control_dag_capital', TRUE)`,
matching the other eight rows. **The team is explicit; nothing is inferred.**

Kept in place rather than renumbered so the `ASSUMPTION-n` references in `V6__subscription_seed0.sql`
stay stable.

### ASSUMPTION-5 — `PLATFORM` is the owning tenant for all 7 `PERSIST` rows

- [ ] Signed off

`eventorchestration.filter.persist` is a **single unnamed JSON array**. It has no per-row owner, no
per-row name, and no notion of a team at all — it is the lifecycle-wide drop gate. `subscription`
requires a `tenant_id` and a unique `(tenant_id, stage, rule_name)`.

`PLATFORM` is **invented**. It is not a team that exists in the legacy configuration. The alternative —
fanning the 7 rows out across `CAPITAL`/`NSFR`/others — would assert an ownership the evidence does not
support, and would break the "persist is not per-tenant" property that makes `PERSIST ⊇ FORWARD`
checkable at all.

### ASSUMPTION-6 — the 7 `PERSIST` rule names are invented

- [ ] Signed off

Same root cause as ASSUMPTION-5: the array has no names. `persist_frca_tenant`,
`persist_aqua_ccr_adjusted`, `persist_aqua_ccr_unadjusted`, `persist_merival_batch`,
`persist_merival_intra`, `persist_rwa_mr_monthly`, `persist_cva_mr_monthly` are descriptive names
derived from each row's own conditions. They appear in `routing_decision.rule_name` audit rows, so
renaming them later breaks the readability of historical audit data — **the names are cheap to change
now and expensive to change after cutover.**

### ASSUMPTION-7 — `enabled` mirrors the legacy flag exactly

- [ ] Signed off

15 rows `enabled = true`; `nsfr_data-update` `enabled = false`, matching its `FALSE`
([:50](../old-ems/properties.sql)). The disabled NSFR row is migrated rather than omitted, so that
enabling it later is a one-column update instead of an undocumented insert.

### ASSUMPTION-8 — `PERSIST ⊇ FORWARD` is load-bearing, not a nicety (A12)

- [ ] Signed off

There is **no `eventorchestration.filter.post` property row** anywhere in `properties.sql` — only the
persist array and the map table. The admission gate is `!(filterPersist ‖ filterPost)`
([`EventFilter.SCALA:68-70`](../old-ems/EventFilter.SCALA)), so in the legacy system an event that
matched only the *post* filter was still admitted and persisted.

In EMS the `PERSIST` stage is the sole drop gate. Any event matching a `FORWARD` rule but no `PERSIST`
rule would be **dropped before enrichment and never routed**. The 7 persist rows must therefore cover
every forward row's event-side conditions. `Seed0MigrationTest` checks this over the sample payloads;
the general case is checked by the §11 shadow parity run. **Verify this by inspection too** — a sample
set is not a proof.

### ASSUMPTION-9 — `runCategory` resolves first, `run-category` is the fallback (A18)

- [ ] Signed off

**Affects:** `cap_data_update.FRCA_CURATION`, and every future rule over a hyphenated field.

A8 recorded that context payloads carry both `data.run-category` (B3F/Merival) and `data.runCategory`
(CALC_EVENT). Case folding alone does not reconcile them — it lower-cases but does not remove hyphens —
so a rule had to pick one spelling and silently miss the other.

**Resolved by extending the fold (amendment A18).** `MatchView` now gives every hyphenated key a
hyphen-free alias as well, so the rule names the field once:

```
context.data.runcategory.startsWith("topside")
```

**Precedence, as directed:** a camelCase key folds *directly* onto `runcategory` in the first pass, and
the alias pass never overwrites a key that is already there. So where a payload carries both spellings
the **camelCase value wins** and the hyphenated one is the fallback. That order is deliberately *not*
document order — precedence must not depend on which spelling a particular producer happens to emit
first. `MatchViewTest` pins both orderings.

The hyphenated key stays reachable, so `context.data["run-category"]` in any hand-written rule keeps
working. And the generated column `context.run_category` COALESCEs both spellings (A8), so with A18 the
database and the routing rules now **agree** about this field, where before they did not.

**Why this needs a tick: it widens what routes.** This is a departure from legacy behaviour, not parity.
The legacy engine was case-insensitive but not hyphen-insensitive, so it matched `run-category` only.
Under EMS a CALC_EVENT-family context carrying `runCategory: TOPSIDE_…` now matches
`cap_data_update.FRCA_CURATION` where the legacy service would not have forwarded it. Like
ASSUMPTION-1, the §11 stage-1 shadow parity run will show this as a diff, and it needs to be listed as
an expected diff **before** the run rather than explained after it.

### ASSUMPTION-10 — key-collision survivors under the A15 fold

- [ ] Signed off

Folding merges spellings that differ only in case. Where both live in one payload the **last in document
order** wins (legacy parity with a case-insensitive map lookup), and `MatchView` logs it once. The
collisions the seed rules actually depend on:

| Folded key | Spellings seen in the sources | Rules affected |
|---|---|---|
| `batchtype` | `batchtype` (persist property, [:29-30](../old-ems/properties.sql)), `batchType` (map table, [:44-45](../old-ems/properties.sql)) | the 2 AQUA_CCR persist + 2 AQUA_CCR forward rows |
| `type` | `TYPE` (INGESTION family), `type` (CALC_EVENT family) — A8 | the 2 MERIVAL persist + 2 MER forward rules |

The `batchtype` case is safe: no payload carries both spellings; the two sources simply disagree with
each other, and the fold makes the persist and forward rules textually identical.

**The `type` case is the residual risk.** If any single event payload carries both `TYPE` and `type`, the
document-order rule decides which value the MERIVAL rules see. No sample in
`ems/src/test/resources/samples/` carries both — but the sample set is small. **Confirm against real
traffic during shadow.**

### ASSUMPTION-11 — `registry_version = 'seed-0'` for all 16 rows

- [ ] Signed off

Fixed literal, per §11 stage 0. `ems_registry_version` publishes one series while these are the only
enabled rows; the first `PUT /admin/subscriptions` after cutover replaces it with a CI-rendered version.
Two versions live at once is the `EmsRegistryDivergence` warn condition.

### ASSUMPTION-12 — per-environment deltas are out of scope for this migration

- [ ] Signed off

**§14 item 3 is unanswered.** `properties.sql` is one environment's inventory; whether DEV/UAT/PROD carry
different persist arrays or different map rows is unknown. This migration seeds **one** ruleset into
every environment it runs in.

Any environment-specific rule must arrive through `PUT /admin/subscriptions` **after** the migration,
audited via `updated_by`. Do not fork `V6` per environment — a versioned Flyway migration that differs
between environments makes the checksum diverge and the schema history unreconcilable.

---

## Sign-off

| | |
|---|---|
| Reviewed by | |
| Date | |
| Shadow parity run referenced | |

Every box above must be ticked before `ems.dispatch.enabled` is flipped to `true` (§11 stage 2).
