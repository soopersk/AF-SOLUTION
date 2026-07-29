# EMS Phase 4 — Ops Readiness (Metrics · Recon · Alerts · Helm · Perf · Seed-0) — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task.
> **Working-agreement adaptation (no git — user's standing choice):** every "Commit" step is replaced by a **Checkpoint** — run `mvn -B -ntp -f ems/pom.xml verify` and *report + wait* at each batch boundary. One batch = one review/rollback unit. Execute batches **inline in the main thread** (no subagents — session limit). No `git init`, no commits; `mvn verify` is the only gate.

**Goal:** Make EMS operable — close the ems-design §10 metric table, build the `recon/ReconciliationSweep` loss backstop, ship alerts + dashboards as code inside the Helm chart, finalize the chart and gate it in CI, build the §12 perf harness at 10 M rows, and promote the reviewed `seed-0` subscription translation into a real, reviewable Flyway migration.

**Architecture:** Three independent surfaces plus two artifacts.
*(1) In-process instrumentation* — counters/histograms added at the points that already own the events (`EventConsumer`, `IngestionService`, `SubscriptionService`, `SubscriptionRepo`, the Boot `http.server.requests` timer), extending the four metrics that already exist rather than duplicating them.
*(2) `recon/ReconciliationSweep`* — one `@Scheduled` bean, deliberately **independent of the ingest and dispatch paths**, that answers "is anything quietly stuck?" from two out-of-band sources: SQL (`dlq_record`, `dag_trigger_outbox`, `event`) and a Kafka `AdminClient` (committed vs end vs earliest offsets). It is a *loss backstop*: it must publish even when the consumer is parked and even when `ems.dispatch.enabled=false`.
*(3) Deployment as code* — the Helm chart grows the scrape wiring, the alert rules, a dashboard, a PDB, a real ConfigMap and Vault-agent annotations, gated by a `helm lint` + `helm template` job in CI that also asserts the §11 cutover defaults have not flipped.
*Artifacts:* an opt-in, `@Tag("perf")` Testcontainers harness for the §12 gate, and `V6__subscription_seed0.sql` in a **separate Flyway location** with a companion assumptions register for human sign-off.

**Tech Stack:** Java 17 / Spring Boot 3.5.4, Micrometer (registry decision in Batch A), Kafka `AdminClient` (already on the classpath via `spring-kafka`), `JdbcClient`, Helm 3 + Prometheus Operator CRDs (`ServiceMonitor`, `PrometheusRule`), Grafana dashboard JSON, Testcontainers PG16, Flyway. Tests: `*Test` (Surefire, **local-green**), `*IT` (Failsafe, Testcontainers, **CI-only**), `@Tag("perf")` (**opt-in**, excluded from both), and a shell-level Helm check in CI.

**Scope boundary (do NOT build here):** No §11 shadow/cutover stages — no parity jobs, no backfill, no route flip (Phase 5). No retention/archival DAG (Phase 6). No re-opening of closed design decisions (A1–A10). No attempt to fix local Docker. No `contractVersion` in the conf (A5 — Phase B). No per-environment subscription deltas (§14 item 3 is unanswered; deltas arrive later via `PUT /admin/subscriptions`).

---

## The two gates this phase cannot close on this box (read first — it shapes the reporting)

1. **The perf gate (§12, and §13's Phase-4 exit criterion) cannot be signed off here.** It needs 10 M rows in a real PostgreSQL; Testcontainers cannot reach Docker on this machine (a known dead end since Phase 2). Batch G builds the harness and proves it *compiles and skips*; the p95 and `EXPLAIN` numbers do not exist until someone runs it against a real Postgres. **This plan will not report a pass it did not observe.**
2. **"Seed parity reviewed" is a human gate.** §14 item 3 is still unanswered. Batch H produces a migration whose every interpretive decision is enumerated in a sign-off register; no test can discharge it. A green `mvn verify` on Batch H means *"the SQL is well-formed, its CEL compiles, and it satisfies the §7 `PERSIST ⊇ FORWARD` invariant over the sample payloads"* — it does **not** mean the rows are right.

Both are stated again in the phase-exit report.

---

## Grounding checkpoints (verify against code, not docs)

Every one of these was read before this plan was written; the four contradictions they produced are Batch 0.

| # | What was checked | Source | Result |
|---|---|---|---|
| 1 | Which metrics actually exist | `IngestionService.java:67`, `ContextResolver.java:36`, `Normalizer.java:32`, `OutboxDispatcher.java:50` | 4 of 11 §10 rows exist. **`ems_outbox_pending_age_seconds` is registered inside a bean gated by `@ConditionalOnProperty(ems.dispatch.enabled=true)` (`OutboxDispatcher.java:44`) — so it does not exist in `shadow`, the exact profile where outbox rows accumulate by design.** |
| 2 | The scrape path | `application.yml:47` vs `pom.xml:126-129` | `prometheus` is exposed; only `micrometer-registry-otlp` is on the classpath ⇒ `/actuator/prometheus` **does not exist**. → **A11** |
| 3 | Recon's contract | `recon/package-info.java:1-7`, `EmsApplication.java:16-20` | Package is empty but `@EnableScheduling` is already on and its javadoc already names "the reconciliation sweep". No wiring to undo. |
| 4 | Terminal/started vocabulary for `ems_overdue_inflight_runs` | `RunStatusRepository.java:44-48,98,145-159` | Already defined and framework-grounded: terminal ⇔ `state ∈ {FINISH, FAILED}` **or** `task_event_type = COMPLETED`; `started ⇔ any event exists`. **Reuse verbatim — do not invent a second vocabulary.** |
| 5 | What the legacy Kafka filter actually admits | `old-ems/EventFilter.SCALA:24,26-27,35-50,56-60,68-70` | Admission = `filterPersist ‖ filterPost`, and `filterPost` evaluates the **`eventorchestration.filter.post` property**, *not* the map table. Routing dagIds come from the **map table**. They are two inputs with two jobs. → **A12** |
| 6 | The seed's case-insensitivity claim | `subscriptions_seed0.provenance.md:20-22,31-34` vs `Normalizer.java:152-177` and `JsonFilterRuleset.scala:17,22` | **False.** The Normalizer upper-cases only `additionalData.STATE`, `TYPE\|type`, `taskEventType`. Legacy lower-cased the **whole JSON and the JSONPath**, so both value case *and key case* were irrelevant. Several seed rows as written can never match. → **A13** |
| 7 | Is the FORWARD translation mechanical? | `EventFilter.SCALA:86` → `PostFilterControlDagMapItem.parseConditionString` (**source absent from `old-ems/`**); `old-ems/properties.sql:40-51` | The map rows are malformed pseudo-SQL and the condition-string grammar has no readable parser. The FORWARD translation is an **interpretation**, not a mechanical port. → **A14** |
| 8 | Perf targets and index names | `ems-design.md §12:572`, `V2__indexes.sql:8,15` | `idx_event_task_id`, `idx_context_rep_freq_region` exist with those exact names. |
| 9 | Seed target schema | `V3__subscription.sql` uniqueness `(tenant_id, stage, rule_name)`, per `provenance.md:38-41` | Upsert key confirmed; `registry_version='seed-0'`. |

---

## Testing strategy (governs every task's test placement)

| Layer | Harness | Runs locally? | Suffix / gate |
|---|---|---|---|
| Metric **names, tags and emission points**; recon gauge computation over a mocked repo; recon degradation when Kafka is unreachable; alert-rule YAML shape; seed CEL compilation + `PERSIST ⊇ FORWARD` over the sample payloads | plain JUnit + `SimpleMeterRegistry` / `PrometheusMeterRegistry`, `@WebMvcTest` for the histogram filter | **yes — green** | `*Test` / Surefire |
| Recon gauges over **real rows** (`dlq_record`, `dag_trigger_outbox`, `event`), real consumer-group offsets, the seed migration applied by real Flyway | Testcontainers PG16 (+ Kafka) | no — **auto-skip** | `*IT` / Failsafe |
| §12 perf gate: 10 M events + 1 M contexts, p95, `EXPLAIN` plan assertions | Testcontainers PG16 | no — **excluded by default, opt-in** | `*IT` + `@Tag("perf")`, `-Pperf` |
| `helm lint`, `helm template`, cutover-default guard, `promtool check rules` | shell + CI job | no (helm not installed here) | `.github/workflows/ems-ci.yml` |

**Rule (unchanged from Phase 3):** every logic-shaped assertion goes in a `*Test` so `mvn verify` stays green locally at every checkpoint. Anything needing a real Postgres/Kafka is an `*IT`. The perf harness gets a *third* tier so a normal CI run is not held hostage to a 10 M-row seed.

---

## Open micro-decisions for the review (my recommendation in **bold**)

1. **Scrape path (A11).** **Prometheus pull is primary: add `micrometer-registry-prometheus`, keep the existing `prometheus` actuator exposure (it becomes true), and ship `ServiceMonitor` + `PrometheusRule` in the chart. Keep `micrometer-registry-otlp` on the classpath but set `management.otlp.metrics.export.enabled: false` by default** so an environment with a collector flips one property. *Rationale:* the deliverable this phase is asked for — alert rules as code — is a Prometheus Operator CRD that evaluates over metrics **in Prometheus**; shipping a `ServiceMonitor` while the app only pushes OTLP would be a broken chart. *Alt: OTLP-only + collector-side rules — rejected: it moves the alert definitions out of the chart, which is exactly what item 4 forbids ("ship them in the chart, not in a wiki").*
2. **Metric-name suffix handling.** The four existing meters are named with the `_total` suffix baked in (`ems_events_dropped_total`). Whether `PrometheusMeterRegistry` re-suffixes them is version-dependent and **must not be guessed**: **write `MetricNamingTest` first, scrape a real `PrometheusMeterRegistry`, and assert the exposed names equal the §10 table exactly; rename the meters only if that assertion proves a mismatch.**
3. **`ems_events_consumed_total{topic,outcome}` emission point.** `IngestionService.process(String)` has no topic, and `EventConsumer` deliberately has **no try/catch** (A1 depends on the throw propagating). **Make `process` return an `IngestOutcome` enum (`DROPPED`, `PERSISTED`, `DUPLICATE`); `EventConsumer` counts with the record's topic; the DLQ recoverer in `KafkaConfig` counts `outcome=poison`.** A parked record is not a terminal outcome and is not counted (lag is the signal for that). *Alt: count inside `IngestionService` with a thread-local topic — rejected, hidden coupling.*
4. **Ownership of `ems_outbox_pending_age_seconds`.** **Move the gauge to `ReconciliationSweep` (registered unconditionally) and remove its registration from `OutboxDispatcher`.** This closes the grounding-checkpoint-1 blind spot: in `shadow` the dispatcher bean does not exist, so today the §11-stage-1 backlog is unmonitored. Same metric name, same meaning, one owner.
5. **`ems_dlq_depth{topic}` source.** **Count unreplayed `dlq_record` rows grouped by `topic` (`replayed_at IS NULL`)** — that is the actionable triage depth and needs no AdminClient. *Alt: DLT end-offsets — rejected: nothing consumes the DLT, so its end-offset is a cumulative total that never returns to zero after a replay, which would make "page > 0 for 5 m" fire forever.* Noted caveat: `dlq_record` writes are best-effort (`KafkaConfig.java:118`), so this can undercount relative to the DLT; recorded in the spec.
6. **Consumer lag + retention headroom.** **`ReconciliationSweep` opens one `AdminClient` and publishes `ems_consumer_lag{topic,partition}` = `endOffset − committedOffset`, plus `ems_consumer_retention_headroom_records{topic,partition}` = `committedOffset − earliestOffset` as the "lag age approaching retention" proxy.** §10 asks for "lag age"; a true age needs `offsetsForTimes` round-trips per partition per tick, which is expensive and fragile. Records-of-headroom is the cheap, monotone proxy that trips *before* the committed offset falls off the log. Recorded as a spec note (a proxy for an underspecified metric, not a contradiction — no amendment).
7. **`ems_overdue_inflight_runs` definition.** **Reuse `RunStatusRepository`'s vocabulary verbatim** (grounding checkpoint 4): group `event` by `context_id` over a bounded horizon, count groups whose `max(created_at)` is older than `ems.recon.overdue-window` and that contain **no** terminal event. Bounded by `ems.recon.horizon` so the query rides `idx_event_created_at` instead of scanning history.
8. **`ems_registry_version{component,version}`** (the `_info` spelling is unscrapeable — see A11 / `MetricNamingTest`). **Standard info-metric pattern: a gauge of constant value `1`, one series per distinct `registry_version` currently loaded, tagged `component="ems-subscriptions", version="<registry_version>"`.** The divergence alert then counts distinct series. Phase B adds more components without a code change here.
9. **Per-endpoint latency histograms.** **No controller changes: a `MeterFilter` that enables percentile histograms on `http.server.requests` restricted to `uri ∈ {/event, /run/status, /gate/groups}`.** Boot already times every request; §10 only asks for the histogram buckets on three of them. *Alt: `@Timed` on each controller — rejected, three annotations plus a second metric name to alert on.*
10. **Perf `/run/status` measurement point.** **Measure `RunStatusRepository.summarize` directly** — the §12 target is a database-plan gate and that is where the 50 ms lives. HTTP/serialization overhead is measured once and reported separately, not folded into the gate. *Alt: full `@SpringBootTest` + `TestRestTemplate` — rejected, it makes the number a JVM-warmup artifact.*
11. **Where the seed migration lives.** **A separate Flyway location `classpath:db/seed` holding `V6__subscription_seed0.sql`, added to `spring.flyway.locations` in the `azure`/`shadow`/`live` profiles only.** *Rationale:* it stays a versioned, re-runnable, reviewable Flyway migration in production, while the 84 `*IT`s — which we **cannot execute on this box** — keep the empty `subscription` table they were written against. Putting 16 rows into `db/migration` would silently change the fixture state of every IT, and we would not find out until CI, which is inert. *Alt: single location + truncate in the IT base — rejected as a blind change to 84 untestable tests.*
12. ~~**Seed CEL case handling (A13).** Make case-insensitivity explicit in the rule text: `.lowerAscii()` on both sides … and a `has()`-guarded either-spelling expression for the `batchtype|batchType` key split.~~ **SUPERSEDED at the Batch-0 checkpoint by A15 (§0):** rule text is owned by **DAG authors**, so the fold moves out of the rules and into the **activation view** — `MatchView` lower-cases every key and every string value before CEL sees the tree. Rules become all-lowercase paths + all-lowercase literals + plain `==`. Still no `Normalizer` change, so the §4.4 mutation counter keeps its meaning; and because legacy lower-cased both the JSON *and* the JSONPath, the legacy literals now transfer **verbatim**.
13. **Seed `enabled` flags.** **Mirror legacy exactly — all rows enabled except the NSFR row.** Safe because the real cutover gate is `ems.dispatch.enabled=false`, not the subscription rows; and shadow-stage parity evidence (Phase 5) requires the rows to be live.
14. **Alert thresholds §10 does not specify** (sustained-lag records, headroom floor, drop-rate anomaly shape). **Templated in `values.yaml` with conservative defaults and flagged in the assumptions register** — §14 item 10 (topic volumes) is unanswered, so any hard-coded number would be invented. 

---

## Batch 0 — Amendments A11–A14 in `ems-design.md §0` (docs only, no code)

Nothing in Batches A–H may be built before these are recorded and reviewed — that is the working agreement's amendment protocol.

### Task 0.1: Record A11 — the scrape path
**Files:** Modify `ems-design.md` §0 (append to the code-grounding amendment table).

**Step 1:** Write the row. Amendment: *the observability transport is **Prometheus pull** (`/actuator/prometheus` via `ServiceMonitor`); OTLP export remains available but off by default.* Replaces: §10 "Observability (Micrometer → OpenTelemetry)". Grounding: `ems/src/main/resources/application.yml:47` exposes the `prometheus` endpoint while `ems/pom.xml:126-129` puts only `micrometer-registry-otlp` on the classpath, so the exposed endpoint does not exist; and the §10 alert column is only deliverable as code (a `PrometheusRule`) against a Prometheus that scrapes.
**Step 2:** No test. **Checkpoint:** folded into 0.4.

### Task 0.2: Record A12 — what the legacy filter actually admits
**Files:** Modify `ems-design.md` §0; add a cross-reference note in §7 (the "Redundancy to resolve" bullet) and §14 item 3.

**Step 1:** Write the row. Amendment: *the `filter.post` **property** and the `post_filter_control_dag_map` **table** are not redundant — the property is a second **admission** input, the map is the **routing** authority.* Replaces: §7's "confirm the map is what `EventFilter.scala` actually evaluates, then retire the property" and §14 item 3's phrasing of the same question. Grounding: `old-ems/EventFilter.SCALA:68-70` admits on `filterPersist(...) || filterPost(...)`; `filterPost` (`:56-60`) evaluates `postFilterRuleset` = the **property** (`:24`); dagIds come from `filterPostWithDagIds` (`:35-50`) over `postFilterRulesWithDagId` = the **map** (`:26-27`). Consequence to state: `old-ems/properties.sql` defines **no** `eventorchestration.filter.post` row at all, so on the only inventory evidence available the effective legacy admission set is the 7 persist rows alone — which makes §7's `PERSIST ⊇ FORWARD` invariant a **hard correctness requirement** for the 8 map rows to ever fire, not merely a CI nicety. This **partially answers §14 item 3**; the per-environment deltas remain open.
**Step 2:** No test. **Checkpoint:** folded into 0.4.

### Task 0.3: Record A13 + A14 — the seed translation is not mechanical and its case assumption is false
**Files:** Modify `ems-design.md` §0; annotate §7's "Mechanical translation" heading.

**Step 1 (A13):** Amendment: *CEL is case-sensitive in both keys and values; the legacy engine was case-insensitive in both. Seed rules must therefore make the fold explicit (`lowerAscii()`) and resolve key-spelling splits with a `has()` guard.* Replaces: `subscriptions_seed0.provenance.md:20-22,31-34` ("the Normalizer canonicalizes enumerated fields to upper-case at eval time … These differ only in case; the legacy engine and the future Normalizer treat them identically"). Grounding: `old-ems/JsonFilterRuleset.scala:17` lower-cases the whole event JSON and `:22` lower-cases the JSONPath; `Normalizer.normalizeEvent` (`Normalizer.java:152-177`) upper-cases **only** `additionalData.STATE`, `additionalData.TYPE|type` and `taskEventType` — it never touches `tenant`, `msgTypeEventType`, `updateType`, `batchtype/batchType`, `RUN_TYPE`, `FREQUENCY` or `source`, and it never touches **keys**. Cite the two concrete casualties: the fixture's `msgTypeEventType == "DATA-UPDATE"` cannot match the legacy literal `data-update`, and PERSIST's `batchtype` vs FORWARD's `batchType` cannot both resolve.
**Step 2 (A14):** Amendment: *the FORWARD half of the `seed-0` translation is an **interpretation requiring human sign-off**, not a mechanical port.* Replaces §7's "Mechanical translation" claim for FORWARD rows only (PERSIST rows remain mechanical — they come from a well-formed JSON array). Grounding: `EventFilter.SCALA:86` calls `PostFilterControlDagMapItem.parseConditionString`, whose **source is not present in `old-ems/`**; `old-ems/properties.sql:40-51` is malformed pseudo-SQL with interleaved column/value tuples, and row 1's condition mixes `.` and `,` as separators (`'$.additionalData.tenant:FRCA.msgTypeEventType:data-update,'`).
**Step 3:** No test. **Checkpoint:** folded into 0.4.

### Task 0.4: Update `docs/ems-technical-specification.md` §5 (binding invariants) with A11–A14
**Files:** Modify `docs/ems-technical-specification.md` §5.

**Step 1:** Extend the A1–A10 invariant table to A1–A14 with one line each, linking to `ems-design.md §0`.
**Step 2:** `mvn -B -ntp -f ems/pom.xml verify` → green (docs only; nothing compiled changed).
**Checkpoint:** green + **report and wait**. *These four amendments are the load-bearing decisions of the phase; A12/A13/A14 change what Batch H is allowed to ship. Do not proceed without sign-off.*

---

## Batch A — Make the scrape path real (A11)

### Task A.1: Registry dependency + config agreement
**Files:**
- Modify `ems/pom.xml` — add `io.micrometer:micrometer-registry-prometheus` (version from the Boot BOM; **no explicit version**).
- Modify `ems/src/main/resources/application.yml` — under `management`: keep the `prometheus` exposure; add `otlp.metrics.export.enabled: false`; add `metrics.distribution.percentiles-histogram` wiring is **not** put here (it is the Batch B `MeterFilter`, which is testable).

**Step 1:** Add the dependency and the property.
**Step 2:** `mvn verify` → green.

### Task A.2: `MetricNamingTest` — the exposed names must equal the §10 table
**Files:** Create `ems/src/test/java/com/orchestration/ems/config/MetricNamingTest.java`.

**Step 1 (red):** Build a `PrometheusMeterRegistry`, register one meter for **every** §10 name using the same builders the production code uses (`counter(name, tags)`, `Gauge.builder(name, …)`), call `registry.scrape()`, and assert the scrape output contains each §10 name **verbatim** and contains no `_total_total` / `_seconds_seconds` doubling.
**Step 2:** Run → this is the empirical answer to micro-decision 2. If it fails, the fix is in the *meter names*, and it is applied to the four existing meters in this task (`IngestionService.java:67`, `ContextResolver.java:36`, `Normalizer.java:32`, `OutboxDispatcher.java:50`) plus their existing assertions — **report the rename before doing it.**
**Step 3:** `mvn verify` → green.
**Checkpoint:** green + report (state plainly which naming the registry actually produced).

---

## Batch B — Close the in-process half of the §10 metric table

### Task B.1: `ems_events_consumed_total{topic,outcome}`
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/ingestion/IngestOutcome.java` — `enum { DROPPED, PERSISTED, DUPLICATE }`.
- Modify `IngestionService.java` — `process` returns `IngestOutcome` (`DROPPED` at the persist gate; `DUPLICATE` when `eventRepository.upsert` returns 0; else `PERSISTED`). No behaviour change, no try/catch.
- Modify `EventConsumer.java` — count `ems_events_consumed_total` tagged `topic=record.topic()`, `outcome=<enum, lower-case>` **before** `ack.acknowledge()`; inject `MeterRegistry`.
- Modify `KafkaConfig.kafkaErrorHandler` — in the recoverer, count `outcome="poison"` tagged with the record's topic.
- Tests: `EventConsumerTest` (extend), `IngestionServiceTest` (extend — assert the returned outcome for each of the three paths), `DlqRoutingTest` (extend — poison increments with `outcome=poison`).

**Steps (TDD):** red — assert the counter name, both tags and the value for drop / persist / duplicate / poison; green — implement.

### Task B.2: `ems_subscription_verdicts_total{tenant,decision}`
**Files:**
- Modify `IngestionService.java` — after `forwardMatches`, increment once per match with `tenant=match.tenantId(), decision="FORWARDED"`, and once with `tenant="none", decision="NOT_SUBSCRIBED"` when the match list is empty. This mirrors the `L0Decision` rows written in the same transaction (`IngestionService.java:154-164`) so the counter and `routing_decision` can be reconciled.
- Tests: `IngestionServiceTest` (extend) — fan-out to two tenants yields two series; zero matches yields the `NOT_SUBSCRIBED` series.

**Note:** count **outside** the transaction callback but only on the non-duplicate path, so a redelivery does not double-count (mirrors the `inserted == 0` early return at `IngestionService.java:135-139`). Assert exactly that in the test.

### Task B.3: `ems_registry_version{component,version}`
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/subscription/RegistryVersionMetrics.java` — a component that, on a `@Scheduled` tick (`ems.recon.interval-ms`, shared with recon), reads the distinct `registry_version` values of enabled rows and maintains one constant-`1` gauge per version tagged `component="ems-subscriptions"`. Removes series for versions that disappear.
- Add `SubscriptionRepo.distinctEnabledRegistryVersions()`.
- Tests: `RegistryVersionMetricsTest` (mocked repo — two versions ⇒ two series; one retired ⇒ series removed).

### Task B.4: Per-endpoint latency histograms
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/config/MetricsConfig.java` — a `MeterFilter` bean enabling `percentilesHistogram` + SLO buckets on `http.server.requests` **only** for `uri ∈ {/event, /run/status, /gate/groups}`; leaves every other URI untouched.
- Tests: `MetricsConfigTest` — apply the filter to a `PrometheusMeterRegistry`, record timings on `/event` and on `/context`, scrape, assert `_bucket` series exist for `/event` and do **not** for `/context`.

**Checkpoint:** `mvn verify` → green + report. (Metric table now closed except the four recon-owned rows.)

---

## Batch C — `ReconciliationSweep`, part 1: the DB-sourced backstop

### Task C.1: `ReconRepository`
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/recon/ReconRepository.java` (`JdbcClient`):
  - `List<DlqDepth> dlqDepthByTopic()` → `SELECT topic, count(*) FROM dlq_record WHERE replayed_at IS NULL GROUP BY topic`.
  - `double oldestPendingOutboxAgeSeconds()` → `SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0) FROM dag_trigger_outbox WHERE delivered_at IS NULL` (rides `ix_outbox_pending`, `V5:22`).
  - `long overdueInflightRuns(Duration window, Duration horizon)` → group `event` by `context_id` within the horizon, `HAVING max(created_at) < now() - window AND NOT bool_or(<terminal predicate>)`, with the terminal predicate **copied from `RunStatusRepository`'s constants** (`state IN ('FINISH','FAILED') OR task_event_type = 'COMPLETED'`).
- Tests: `ReconRepositoryIT` (Testcontainers) — seed rows covering each gauge, including a replayed DLQ row (excluded), a delivered outbox row (excluded), a terminated run (excluded) and a genuinely stuck run (counted).

**Step 1 (IT, red):** write the assertions; run → fails (no class). **Step 2 (green):** implement. **Step 3:** `mvn verify` → green locally (IT skips).

### Task C.2: `ReconciliationSweep` + the gauge ownership move
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/recon/ReconciliationSweep.java`:
  - `@Component`, `@ConditionalOnProperty(prefix="ems.recon", name="enabled", havingValue="true", matchIfMissing=true)` — **unconditional by default and independent of both `ems.consumer.enabled` and `ems.dispatch.enabled`** (that independence is the whole point: it is the loss backstop).
  - `@Scheduled(fixedDelayString = "${ems.recon.interval-ms:60000}")` `sweep()` — refreshes volatile fields; multi-gauge registration for `ems_dlq_depth{topic}` uses a `MultiGauge`.
  - Gauges registered in the constructor: `ems_outbox_pending_age_seconds`, `ems_overdue_inflight_runs`; `MultiGauge` for `ems_dlq_depth`.
  - **The whole tick body is wrapped so no exception escapes** — a scheduled method that throws stops being rescheduled in some configurations, which would silently disable the backstop. Log at WARN and leave the previous values.
- Modify `OutboxDispatcher.java` — **delete** the `PENDING_AGE_METRIC` constant, its `Gauge.builder` registration and `oldestPendingAgeSeconds` field; keep `refreshPendingAge()`'s repo call only if it is still used for logging, else delete it too (micro-decision 4).
- Modify `OutboxDispatcherTest` — drop the gauge assertion, add a comment pointing at `ReconciliationSweepTest`.
- Modify `application.yml` — add the `ems.recon` block (`enabled: true`, `interval-ms: 60000`, `overdue-window: 6h`, `horizon: 7d`).
- Tests: `ReconciliationSweepTest` (mocked `ReconRepository` + `SimpleMeterRegistry`) — asserts all four gauge names/tags/values, that a repository throw does **not** propagate and leaves the last-known values, and that the bean is registered with dispatch disabled.

**Checkpoint:** `mvn verify` → green + report. *Explicitly report the ownership move — `ems_outbox_pending_age_seconds` now exists in `shadow`, which it did not before.*

---

## Batch D — `ReconciliationSweep`, part 2: Kafka lag + retention headroom

### Task D.1: `ConsumerLagProbe`
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/recon/ConsumerLagProbe.java` — wraps a Kafka `AdminClient` (built from `spring.kafka.bootstrap-servers` + `ems.consumer.group-id`); one method `List<PartitionLag> probe()` returning `(topic, partition, lag, headroom)` from `listConsumerGroupOffsets` + `listOffsets(latest)` + `listOffsets(earliest)`. Bounded `KafkaFuture.get(timeout)`; **returns an empty list and logs at WARN on any failure** — an unreachable broker must never break the sweep or the other three gauges.
- Create `ems/src/main/java/com/orchestration/ems/recon/PartitionLag.java` (record).
- Modify `ReconciliationSweep` — two more `MultiGauge`s: `ems_consumer_lag{topic,partition}` and `ems_consumer_retention_headroom_records{topic,partition}`.
- Modify `application.yml` — `ems.recon.kafka.enabled` (default `true`), `ems.recon.kafka.timeout-ms` (default `5000`).
- Tests: `ConsumerLagProbeTest` (mocked `AdminClient` — lag/headroom arithmetic, and the failure path returning empty), `ReconciliationSweepTest` (extend — two partitions ⇒ two series each; empty probe ⇒ series cleared, not stale).
- Test: `ConsumerLagProbeIT` (Testcontainers Kafka — real group offsets after producing N records).

**Checkpoint:** `mvn verify` → green + report. (§10 metric table is now fully closed; state the row-by-row coverage in the report.)

---

## Batch E — Alerts + dashboard as code

### Task E.1: `PrometheusRule` template
**Files:**
- Create `ems/deploy/helm/ems/templates/prometheusrule.yaml` — guarded by `.Values.metrics.prometheusRule.enabled`. Groups `ems.page` and `ems.warn`, sourced **row by row from the §10 alert column**:
  - **page** `EmsDlqDepthNonZero` — `max(ems_dlq_depth) > 0`, `for: 5m`.
  - **page** `EmsOutboxBacklogStale` — `ems_outbox_pending_age_seconds > 600`, `for: 5m`.
  - **page** `EmsConsumerLagSustained` — `max(ems_consumer_lag) > {{ .Values.alerts.lagRecords }}`, `for: 15m`.
  - **page** `EmsRetentionHeadroomLow` — `min(ems_consumer_retention_headroom_records) < {{ .Values.alerts.headroomRecords }}`, `for: 5m` (the "lag age approaching retention" early page).
  - **warn** `EmsDropRateAnomaly` — per-source ratio of `rate(ems_events_dropped_total[15m])` against `[15m] offset 1d`, `for: 30m`.
  - **warn** `EmsNormalizationMutations` — `increase(ems_normalization_mutations_total[1h]) > 0`, `for: 5m` (§4.4: any nonzero reviewed before cutover).
  - **warn** `EmsRegistryDivergence` — `count(count by (version) (ems_registry_version)) > 1`, `for: 30m`.
  - **warn** `EmsOverdueInflightRuns` — `ems_overdue_inflight_runs > 0`, `for: 30m`.
  - **warn** `EmsEndpointP95Regression` — `histogram_quantile(0.95, sum by (le,uri) (rate(http_server_requests_seconds_bucket{uri=~"/event|/run/status|/gate/groups"}[10m]))) > {{ .Values.alerts.endpointP95Seconds }}`, `for: 15m`.
  - Every rule carries `annotations.runbook` pointing at the matching `docs/ems-user-guide.md` §8 runbook.
- Modify `ems/deploy/helm/ems/values.yaml` — the `alerts:` block with the four tunables (micro-decision 14) and `metrics.prometheusRule.enabled: true`.

### Task E.2: `ServiceMonitor` + dashboard
**Files:**
- Create `ems/deploy/helm/ems/templates/servicemonitor.yaml` — `path: /actuator/prometheus`, interval from values, guarded by `.Values.metrics.serviceMonitor.enabled`.
- Create `ems/deploy/helm/ems/dashboards/ems-overview.json` — Grafana dashboard, rows: **Ingest** (consumed by outcome, drop rate by source), **Routing** (verdicts by tenant/decision, registry versions), **Delivery** (outbox pending age, dispatch outcomes), **Failure** (DLQ depth, overdue in-flight), **Kafka** (lag, headroom), **API** (p95/p99 per endpoint).
- Create `ems/deploy/helm/ems/templates/dashboard-configmap.yaml` — `.Files.Get` the JSON, label `grafana_dashboard: "1"` (sidecar convention), guarded by `.Values.metrics.dashboard.enabled`.

### Task E.3: Alert-rule shape test (local)
**Files:** Create `ems/src/test/java/com/orchestration/ems/config/AlertRuleCoverageTest.java`.

**Step 1 (red):** parse `ems/deploy/helm/ems/templates/prometheusrule.yaml` as text (it is a Helm template, so parse the `alert:`/`expr:`/`for:` lines rather than the YAML tree) and assert: every §10 alert row has a rule; every rule has a non-empty `expr`, a `for`, a `severity` label and a `runbook` annotation; and **every metric name referenced in an `expr` is a name this codebase actually registers** (cross-checked against the constants — this is the test that catches a typo'd metric name, which is otherwise invisible until an alert silently never fires).
**Step 2 (green):** fix any mismatch found. **Step 3:** `mvn verify` → green.
**Checkpoint:** green + report.

---

## Batch F — Helm finalization + CI gate

### Task F.1: ConfigMap, PDB, Vault annotations
**Files:**
- Create `ems/deploy/helm/ems/templates/configmap.yaml` — renders the **non-secret** wiring: `ems.auth.mode`, `groups-claim`, `principal-claim`, the three `ems.auth.groups.*` ids, `spring.kafka.bootstrap-servers`, `ems.consumer.topics`, `ems.consumer.group-id`, `ems.consumer.park-backoff-ms`, `ems.edf.base-url`, `ems.airflow.base-url` + backoff, `ems.recon.*`, `ems.dispatch.*`, `ems.consumer.enabled`. Secrets stay in Vault (§10) — assert none appear here.
- Create `ems/deploy/helm/ems/templates/poddisruptionbudget.yaml` — `minAvailable: {{ .Values.pdb.minAvailable | default 2 }}`, guarded by `.Values.pdb.enabled`.
- Modify `ems/deploy/helm/ems/templates/deployment.yaml` — mount the ConfigMap via `envFrom`/`configMapRef`; add **real** Vault-agent annotations (`vault.hashicorp.com/agent-inject: "true"`, `role`, and one `agent-inject-secret-*` + `agent-inject-template-*` pair per secret family: Airflow basic auth, Azure OAuth, JKS truststore, EDF API keys — the four families audited in §10/§14 item 7), rendered from `.Values.vault`.
- Modify `ems/deploy/helm/ems/values.yaml` — `pdb`, `metrics`, `alerts`, and expanded `auth`/`kafka`/`edf`/`airflow` blocks. **`dispatch.enabled: false` and `springProfile: shadow` stay exactly as they are.**

### Task F.2: The cutover-default guard + Helm check script
**Files:** Create `ems/deploy/helm/check.sh`.

**Step 1:** `helm lint ems/deploy/helm/ems`; then `helm template ems ems/deploy/helm/ems` into a file and assert, with explicit non-zero exits and named error messages:
  - the rendered Deployment sets `SPRING_PROFILES_ACTIVE=shadow`;
  - it sets the dispatch toggle to `false`;
  - a `PodDisruptionBudget`, a `PrometheusRule`, a `ServiceMonitor` and the dashboard ConfigMap are all present;
  - **no rendered manifest contains a plaintext secret key** (grep for `password|secret|api-key` outside the Vault annotation block).
  Then `helm template … --set dispatch.enabled=true --set springProfile=live` and assert the render still succeeds (the flip is a values change, not a rebuild — §10).
**Step 2:** Run it here → **expected to fail: `helm` is not installed on this box.** Do not install it; the script's contract is that CI runs it. Say so in the report.

### Task F.3: CI wiring
**Files:** Modify `.github/workflows/ems-ci.yml`.

**Step 1:** Add a `helm` job (parallel to `build`): `azure/setup-helm@v4`, run `ems/deploy/helm/check.sh`, then `helm template` the rules into a file and run `promtool check rules` via the `prom/prometheus` image (Docker is available on `ubuntu-latest`). Add `ems/deploy/**` to the `paths:` triggers.
**Step 2:** Add a `perf` job that is **`workflow_dispatch`-only** and runs `mvn -B -ntp -f ems/pom.xml verify -Pperf` (Batch G supplies the profile), so the §12 gate is one manual click rather than a 10 M-row seed on every push.
**Step 3:** `mvn verify` → green (CI YAML does not affect the build). **Note in the report: CI remains inert — there is no git.**
**Checkpoint:** green + report.

---

## Batch G — Perf harness (§12) — opt-in, and honestly unsignable here

### Task G.1: Failsafe `perf` tag exclusion + `-Pperf` profile
**Files:** Modify `ems/pom.xml` — `maven-failsafe-plugin` gets `<excludedGroups>perf</excludedGroups>`; a `<profile><id>perf</id>` flips it to `<groups>perf</groups>` so `-Pperf` runs **only** the perf harness.

**Step 1:** Configure. **Step 2:** `mvn verify` → green, and confirm the existing 84 ITs are still discovered (they carry no tag).

### Task G.2: The seeder
**Files:** Create `ems/src/test/java/com/orchestration/ems/perf/PerfSeeder.java`.

**Step 1:** Server-side generation only — one `INSERT INTO context (context_id, json) SELECT … FROM generate_series(1, 1000000)` and one `INSERT INTO event (event_id, json) SELECT … FROM generate_series(1, 10000000)`, both in chunks of 250 k so the WAL does not blow up. The synthetic payloads are shaped from the real samples (`ems/src/test/resources/samples/*.json`) so the generated columns populate: `additionalData.taskId`, `contextId`, `additionalData.STATE`, and on contexts `data.reporting-date`, `data.frequency`, `data.h3Region`. Distribution: ~10 events per context, task ids uniformly spread so the driving index is selective. Finish with `ANALYZE event, context;` (§11 stage 1 item 7 does the same).
**Step 2:** No assertions yet.

### Task G.3: `QueryPerfIT`
**Files:** Create `ems/src/test/java/com/orchestration/ems/perf/QueryPerfIT.java` — `@Tag("perf")`, `@Testcontainers(disabledWithoutDocker = true)`, extending the existing `AbstractPostgresIT` pattern.

**Step 1:** Seed once per class. Then:
  - **p95 of the canonical §4.3 query** (`EventQueryRepository` DATASET CHECK shape) over ≥ 500 randomized-parameter iterations after a 50-iteration warm-up ⇒ assert **p95 < 50 ms**.
  - **p95 of `RunStatusRepository.summarize`** over ≥ 500 iterations ⇒ assert **< 50 ms** (micro-decision 10); separately measure and *log* the HTTP overhead once, without asserting on it.
  - **`EXPLAIN (FORMAT JSON)` assertions:** the event-side plan contains a node using `idx_event_task_id`; the context-side plan contains a node using `idx_context_rep_freq_region`; and **no `Seq Scan` on `event` or `context` appears anywhere in either plan tree** (walk the JSON recursively — a nested seq scan under a nested loop is exactly the failure this must catch).
  - Write every measured number to `ems/target/perf-report.txt` so a CI run produces the §12 "before/after" evidence artifact.
**Step 2:** `mvn verify` → green locally (**the test is excluded by tag *and* would skip without Docker**).
**Step 3:** `mvn verify -Pperf` → **runs zero tests here** (no Docker). Report that output verbatim.
**Checkpoint:** green + report. **The report must state: the §12 perf gate — and therefore the §13 Phase-4 exit criterion — is NOT signed off; the harness exists and is unexecuted.**

---

## Batch H — Production `seed-0` subscription migration (§11 stage 0 item 3)

Governed by A12/A13/A14/**A15**. **Do not invent rows.** Under A15 the rule dialect is: all-lowercase paths, all-lowercase literals, plain `==`/`&&`/`startsWith`, nothing else. Every departure from the literal legacy text becomes a numbered line in the sign-off register.

### Task H.1: `MatchView` — the case-folded activation view (A15)
**Files:**
- Create `ems/src/main/java/com/orchestration/ems/subscription/MatchView.java` — `static JsonNode fold(JsonNode)`: deep-copy, recursively lower-case every object **key** and every **string value**; numbers/booleans/nulls untouched; arrays recursed element-wise. On a key collision after folding (A8's `TYPE` vs `type`), **last-wins in document order** (legacy parity) and log WARN once with both keys.
- Modify `SubscriptionService#eventMap`/`#contextMap` — apply `MatchView.fold(...)` **after** `normalizer.normalize*`.
- Create `ems/src/test/java/com/orchestration/ems/subscription/MatchViewTest.java` — keys folded, string values folded, non-strings untouched, nested objects + arrays, collision last-wins, input not mutated.
- Modify `ems/src/test/java/com/orchestration/ems/subscription/SubscriptionServiceTest.java` and any rule text in test fixtures to the lower-case dialect.

**Step 1 (red):** write `MatchViewTest`, then `MatchView`.
**Step 2:** wire it into `SubscriptionService`; fix the existing rule text in tests/fixtures (mechanical: lower-case the path and the literal).
**Step 3:** `mvn verify` → green. *Report the count of test rule-strings rewritten.*

### Task H.2: The assumptions register (the sign-off artifact)
**Files:** Create `docs/ems-seed0-assumptions.md`.

**Step 1:** One section per subscription row (7 PERSIST + 8 CAPITAL FORWARD + 1 NSFR FORWARD = 16), each carrying: the verbatim legacy source text with its `old-ems/properties.sql` line number, the CEL it becomes, and an explicit **`ASSUMPTION-n`** entry for every interpretive step. At minimum these must appear as numbered assumptions:
  - `MERYVAL` → `MERIVAL` (2 rows) — a typo correction that changes which events route.
  - `context.date.run-category` → `context.data["run-category"]` (1 row).
  - the FRCA_CURATION condition de-duplication (A14 — the grammar has no readable parser).
  - `PLATFORM` as the invented `tenant_id` for all 7 PERSIST rows (legacy persist rows have no owner).
  - **(A15 removes this class of assumption)** — the case fold is now uniform and mechanical, so instead record the *one* residual: which key survives a fold collision where both spellings are present in a sample payload, per field.
  - `enabled` flags mirroring legacy (micro-decision 13).
  - **the unanswered §14 item 3 per-environment deltas** — stated as out of scope for the migration, arriving via `PUT /admin/subscriptions`.
  - the A12 consequence: no `eventorchestration.filter.post` property row exists in the evidence, so `PERSIST ⊇ FORWARD` is load-bearing.
**Step 2:** Add an unchecked sign-off checkbox per assumption. **No test discharges this.**

### Task H.3: `V6__subscription_seed0.sql`
**Files:**
- Create `ems/src/main/resources/db/seed/V6__subscription_seed0.sql` (micro-decision 11 — the `db/seed` location, **not** `db/migration`).
- Modify `application.yml` — add `spring.flyway.locations: classpath:db/migration,classpath:db/seed` to the `azure`, `shadow` and `live` profile blocks only; leave the default/`local` profile at `classpath:db/migration`.

**Step 1:** Write the migration: 16 `INSERT … ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE SET when_cel = EXCLUDED.when_cel, control_dag_id = EXCLUDED.control_dag_id, enabled = EXCLUDED.enabled, registry_version = EXCLUDED.registry_version, updated_by = 'seed-0-migration'` — **re-runnable by construction**. Every row preceded by a SQL comment giving its `properties.sql` line and its `ASSUMPTION-n` references. `registry_version = 'seed-0'` throughout.
**Step 2:** Update `ems/src/test/resources/fixtures/subscriptions_seed0.json` and `subscriptions_seed0.provenance.md` to the **corrected** A13 translation, and rewrite the provenance's falsified case-insensitivity paragraph (`:20-22,31-34`) to cite A13. The fixture and the migration must not disagree — Task H.4 enforces that.

### Task H.4: `Seed0MigrationTest` (local, green) + `Seed0MigrationIT` (CI)
**Files:**
- Create `ems/src/test/java/com/orchestration/ems/subscription/Seed0MigrationTest.java` — reads `db/seed/V6__subscription_seed0.sql` off the classpath, extracts the 16 rows, and asserts **locally**:
  1. exactly 16 rows, 7 PERSIST / 8 CAPITAL FORWARD / 1 NSFR FORWARD;
  2. every `when_cel` **compiles** via `CelPrograms`, and every PERSIST row is rejected if it references `context.*` (the A4 structural guard);
  3. the migration's rows are **identical** to `subscriptions_seed0.json` (the fixture/migration parity check);
  4. the §7 CI invariant: load the rows into a `SubscriptionService` over the sample payloads in `ems/src/test/resources/samples/` and assert `forwardImpliesPersist` holds for each — the A12 load-bearing check;
  5. every row that Task H.2 marks with an assumption has a matching `ASSUMPTION-n` comment in the SQL (so the register can never silently drift from the migration).
- Create `ems/src/test/java/com/orchestration/ems/subscription/Seed0MigrationIT.java` — Testcontainers: run Flyway with **both** locations, assert 16 rows land, assert **re-running is a clean no-op** (row count and `updated_at` semantics), and assert `SubscriptionRepo.loadEnabled()` returns 15 enabled rows.

**Step 1 (red):** write both. **Step 2 (green):** fix the migration until the local test passes. **Step 3:** `mvn verify` → green (the IT skips).
**Checkpoint:** green + **report and wait**. *State plainly: the tests prove the SQL is well-formed, compiles, and is superset-consistent — they do not prove the rows are correct. Present the assumptions register for sign-off.*

---

## Batch I — Docs, memory, phase-exit report

### Task I.1: `docs/ems-technical-specification.md`
**Files:** Modify.
- §17 Observability — replace the whole table: remove every ⏳, add the emitter for each of the 11 rows, add `ems_consumer_retention_headroom_records` with its proxy rationale (micro-decision 6), record the `ems_dlq_depth` best-effort caveat (micro-decision 5), and record the A11 scrape path.
- §5 — already extended in Batch 0.
- §8.6 / §8.8 — the seed section moves from "fixture, not yet a migration" to the real `V6` migration + the `db/seed` location decision + a link to the assumptions register.
- §19 Deployment topology — the finalized chart inventory (ConfigMap, PDB, ServiceMonitor, PrometheusRule, dashboard, Vault annotations).
- §21.5 — the perf gate stays ⏳ with the harness now named and its opt-in invocation documented.
- §24.2 — remove the gaps this phase closed; add the ones it opened (perf unexecuted, seed sign-off pending, helm check unrun locally).

### Task I.2: `docs/ems-user-guide.md`
**Files:** Modify.
- §9 Monitoring and alerts — replace with the shipped rule names, thresholds, and the `values.yaml` knobs that tune them.
- §8.3 (consumer lag) — add the headroom gauge and the early-page semantics.
- §8.1 (DLQ) — `ems_dlq_depth` now falls to zero after a successful replay; say why.
- New §8.7 — "Overdue in-flight runs" runbook, matching the `EmsOverdueInflightRuns` alert's `runbook` annotation.
- §12.2 — the new `ems.recon.*` knobs.

### Task I.3: Memory + handoff
**Files:** Write `…/memory/ems-phase4-progress.md`; add its one-line pointer to `MEMORY.md`; update `SESSION_HANDOFF.md` (state, amendments A11–A14, the two carried-forward gates plus the two new ones, and the Phase-5 prompt).

**Checkpoint:** `mvn verify` → green + **final report**, which must state without hedging:
- what is code-complete and locally green (unit count, IT skip count);
- that the **§12 perf gate is unexecuted** and therefore the §13 Phase-4 exit criterion is **not** met on this box;
- that **seed parity is awaiting human sign-off** against `docs/ems-seed0-assumptions.md`;
- that the **Helm check and CI jobs have never run** (no git, no helm locally);
- that Phase 5 (shadow/cutover) and Phase 6 (retention) were not started.

---

## What this plan deliberately does NOT do (YAGNI / scope guard)

- No §11 shadow parity jobs, backfill, route flip or rollback drill (Phase 5).
- No retention/archival DAG (Phase 6).
- No `routing_decision` partitioning (§14 item 10 unanswered — the design already defers it).
- No `contractVersion` in the conf (A5 — Phase B).
- No change to `Normalizer`'s field list. Extending it to fold `tenant`/`updateType`/`msgTypeEventType` would make the seed CEL simpler, but it would also make `ems_normalization_mutations_total` fire on ordinary traffic — and §4.4 requires that counter to be reviewed-and-≈-zero *before* cutover. A15's `MatchView` keeps the counter meaningful *and* keeps the rules simple — it folds the CEL activation view only, never the stored payload, the generated columns, or the outgoing `conf`.
- No local Docker remediation, no `git init`, no commits.
- No hard-coded alert thresholds presented as derived — the four tunables are values-file inputs flagged as assumptions until §14 item 10 lands.
