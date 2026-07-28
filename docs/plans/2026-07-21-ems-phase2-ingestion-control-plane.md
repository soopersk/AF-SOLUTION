# EMS Phase 2 — Ingestion + Control-Plane Core — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
> **Working-agreement adaptation (no git — user's standing choice):** every "Commit" step is replaced by a **Checkpoint**: run the stated `mvn verify` and *report + wait* at each batch boundary. One batch = one review/rollback unit.

**Goal:** Build the EMS ingest→persist→forward→dispatch machinery (ems-design §13 phase 2): Kafka consumer, Normalizer, two-stage L0 SubscriptionService, ContextResolver+EDF client, single-TX persist of (event, context, routing_decision, outbox), OutboxDispatcher, and poison-only DLQ — proven by the §12 integration drills.

**Architecture:** Rule-free event backbone. A `@KafkaListener` (manual ack, `ErrorHandlingDeserializer`) hands each `EventResponse` to an `IngestionService` that runs the normative §4.2 pipeline: normalize → **PERSIST gate** (stage-1 CEL, event-only, zero-match ⇒ drop+ack) → resolve context (Caffeine→DB→EDF) → **FORWARD eval** (stage-2 CEL, event+context) → **one `@Transactional`** writing event, context, L0 `routing_decision` rows, and one `dag_trigger_outbox` row per FORWARD match → ack. A separate `OutboxDispatcher` (`FOR UPDATE SKIP LOCKED`, 200/409 = delivered) drains asynchronously, so Airflow is off the ingest path. `DefaultErrorHandler` splits failures: **poison** → verified-publish DLQ + `dlq_record`; **transient infra** → unbounded seek-based backoff (park the partition). No-loss by construction: every step is idempotent under at-least-once redelivery.

**Tech Stack:** Java 17 / Spring Boot 3.5.4, Spring Kafka, `JdbcClient` (no ORM), cel-java (`org.projectnessie.cel:cel-tools:0.4.4`), Caffeine, RFC 8785 JCS (`CanonicalJson`/`DagRunId`, already built). Tests: JUnit 5 + **EmbeddedKafka** (in-JVM, no Docker → runs locally) for consumer/error-handler slices; **Testcontainers** PG16 (+Kafka) for persistence and end-to-end drills, all `@Testcontainers(disabledWithoutDocker = true)` → auto-skip locally, **run green in CI**; WireMock for the EDF stub.

**Scope boundary (do NOT build here — Phase 3):** REST query/control controllers (`/event`, `/context`, `/run/status`, `/gate/groups`), `POST /decisions` ingest controller, `POST /admin/replay`, `PUT /admin/subscriptions`, Spring Security. Phase 2 delivers the **repositories, services, consumer, dispatcher, and DLQ** those endpoints will later sit on. `decisions/` here = `RoutingDecisionRepo` (the L0 writer) only. The production `seed-0` subscription seed is Phase 4 (§11 stage 0); Phase 2 uses **test fixtures**.

---

## Testing strategy (read first — governs every task's test placement)

| Layer | Harness | Runs locally? | Suffix / plugin |
|---|---|---|---|
| model assembly, EnrichedEvent conf canonicalization, Normalizer values, CEL subscription eval, dispatcher backoff logic, error classification | plain JUnit / Mockito / real cel-java | **yes, green** | `*Test` / Surefire |
| consumer manual-ack, `ErrorHandlingDeserializer`, poison→DLQ publish, transient park | **`@EmbeddedKafka`** (in-JVM broker, no Docker) + mocked/real service | **yes, green** | `*Test` / Surefire |
| repositories (JSONB upsert, generated columns, `ON CONFLICT`, `SKIP LOCKED`), Normalizer↔SQL parity, single-TX persist, full pipeline, redelivery/idempotency, kill-Airflow drain | **Testcontainers PG16** (+Kafka where end-to-end) | no — **auto-skip**, CI-only | `*IT` / Failsafe, `@Testcontainers(disabledWithoutDocker=true)` |

**Rule:** maximize the top two rows so `mvn -B -ntp -f ems/pom.xml verify` stays **green locally at every checkpoint**; isolate anything needing a real Postgres/`SKIP LOCKED`/generated columns into `*IT`. The §12 acceptance drills (poison, redelivery, outage, kill-Airflow) are the phase exit and are proven in CI.

**Grounding checkpoints (verify against code, not docs — record any contradiction as amendment A11+ in ems-design §0 before building on it):**
- CEL translation semantics ← `old-ems/JsonFilterRuleset.scala:14-31` (case-insensitive; trailing `.*` = `Regex.quote(prefix)+".*"` literal prefix, **not** free regex) and the A6 CEL-translation note.
- Save-then-forward order & PERSIST∪FORWARD admit ← `old-ems/EventListener.scala:20-39`, `old-ems/EventFilter.SCALA:68-70`.
- Context `data.{key}.value` unwrap ← `old-ems/EventFilter.SCALA:91-103` (`normalizeContextDataValues`).
- Context resolution order (DB→EDF, persist fetched) ← `old-ems/EventSender.scala:108-119`.
- EDF GET-by-id contract is **still open (§14 item 2)** — `EdfContextClient` is written against a *provisional* shape mirroring `old-ems/EventSender.scala:143-164` (GET `{edfPath}/{id}`, bearer token, 200→body / 4xx→absent). Flagged as an assumption, **not** an amendment (it's a known gap, not a contradiction). WireMock stands in until the contract lands.

---

## Open micro-decisions for the review (my recommendation in **bold**)

1. **Kafka test harness for consumer/error-handler slices:** **EmbeddedKafka** (in-JVM, runs locally, fast) for poison/park/ack behavior; Testcontainers Kafka only for the true end-to-end pipeline IT. *Alt: Testcontainers Kafka everywhere (CI-only, slower, no local signal).*
2. **CEL case-insensitivity:** **Normalizer canonicalizes enumerated fields to upper-case; seed CEL rules are authored against upper-case literals; `TOPSIDE.*` prefix → `.startsWith("TOPSIDE")` on the normalized value.** This reproduces `JsonFilterRuleset`'s lower-cased comparison via canonical-upper instead. *Alt: a custom CEL `ci_eq`/`ci_prefix` function library that lower-cases both sides at eval time (closer to legacy, more moving parts).*
3. **EDF client contract:** provisional shape per the grounding note above, WireMock-backed, marked as a §14-item-2 assumption.

---

## Batch 0 — Test harness scaffolding (unit + IT base)

### Task 0.1: Extract a reusable Testcontainers PG base

**Files:**
- Create: `ems/src/test/java/com/orchestration/ems/support/AbstractPostgresIT.java`
- Reference: `ems/src/test/java/com/orchestration/ems/store/FlywayMigrationIT.java` (existing PG16 + Flyway pattern to factor out)

**Step 1:** Write `AbstractPostgresIT` — `@Testcontainers(disabledWithoutDocker = true)`, a static `@Container PostgreSQLContainer<>("postgres:16")`, a `@DynamicPropertySource` wiring `spring.datasource.*` + `spring.flyway` at the container URL, and a `JdbcClient`/`DataSource` accessor. Migrations V1–V5 apply via Flyway on context start.
**Step 2:** Refactor `FlywayMigrationIT` to extend it (behavior unchanged) — run `mvn -B -ntp -f ems/pom.xml verify` → still green locally (IT skips).
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify` green.

### Task 0.2: Subscription seed-0 test fixtures

**Files:**
- Create: `ems/src/test/resources/fixtures/subscriptions_seed0.json` — the §7 inventory translated to CEL: 7 PERSIST rows, 8 CAPITAL FORWARD rows, 1 NSFR FORWARD row (disabled). Source: `old-ems/properties.sql:25-52` (typos `MERYVAL`/`context.date.*` corrected to `MERIVAL`/`context.data.run-category`).
- Create: `ems/src/test/java/com/orchestration/ems/support/SubscriptionFixtures.java` — loads the JSON into `SubscriptionRow` records for unit tests.

**Step 1:** Author the fixture (CEL per §7 mechanical translation; e.g. `event.source == "MERIVAL" && event.additionalData.TYPE == "INGESTION" && event.additionalData.RUN_TYPE == "BATCH"`; the TOPSIDE clause as `context.data["run-category"].startsWith("TOPSIDE")`).
**Step 2:** No test yet — consumed by later tasks.
**Checkpoint:** none (data only) — folded into Batch D checkpoint.

---

## Batch A — `model/` records (unit, local)

### Task A.1: EventRow, ContextRow, SubscriptionRow, SubscriptionMatch

**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/model/EventRow.java` — `record EventRow(String eventId, String rawJson, JsonNode parsed)` (raw byte-verbatim string for storage; `parsed` for CEL/column-free logic). Factory `of(String rawJson, ObjectMapper)` extracting `eventId` from `id`/`eventId` (verify key ← samples).
- Create: `.../model/ContextRow.java` — `record ContextRow(String contextId, String rawJson, JsonNode parsed)`.
- Create: `.../model/SubscriptionRow.java` — `record SubscriptionRow(long id, String tenantId, Stage stage, String ruleName, String controlDagId, String whenCel, String registryVersion, boolean enabled)` + `enum Stage { PERSIST, FORWARD }`.
- Create: `.../model/SubscriptionMatch.java` — `record SubscriptionMatch(String tenantId, String controlDagId, String ruleName)` (a FORWARD hit → one outbox row + one FORWARDED decision).
- Test: `ems/src/test/java/com/orchestration/ems/model/EventRowTest.java`

**Step 1 (test-first):** `EventRowTest` — load `samples/event_meg_started.json`, assert `EventRow.of(...).eventId()` equals the sample's id and `rawJson()` is byte-identical to input.
**Step 2:** Run `mvn -Dtest=EventRowTest -pl ems test` → FAIL (class missing).
**Step 3:** Implement the records.
**Step 4:** Run → PASS.
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`.

### Task A.2: EnrichedEvent + conf canonicalization

**Files:**
- Create: `.../model/EnrichedEvent.java` — `record EnrichedEvent(JsonNode event, JsonNode context)`; method `ObjectNode toConf()` producing the merged shape **identical to legacy** (`old-ems/EventFilter.SCALA:35-43`): event object with a nested `"context"` field (context present ⇒ set; null ⇒ omit). `contractVersion` **deferred to Phase B (A5)** — not added here.
- Test: `.../model/EnrichedEventTest.java`

**Step 1 (test-first):** assert `toConf()` for `(event_calc_complete, context_calc)` merges to event-with-nested-context; assert `CanonicalJson.canonicalize(toConf().toString())` is stable and `DagRunId.derive(dagId, conf)` yields a 16-hex id. (Byte-parity vs the Scala derivation is a shadow-stage §11 check, not unit — noted.)
**Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS.
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch A).**

---

## Batch B — `ingestion/Normalizer` (unit local; parity IT in CI)

### Task B.1: Normalizer value canonicalization

**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/ingestion/Normalizer.java`
- Test: `.../ingestion/NormalizerTest.java`

Behavior (§4.4, the single Java authority mirroring `ems_norm_*`): upper-case enumerated fields (`state`, `type`, `taskEventType`, `frequency`); `normFreq` (`D`/`daily`→`DAILY`, `M`→`MONTHLY`, else upper) mirroring `V1` `ems_norm_freq`; `normRegion` (`AMERICAS`→`AMER`, else upper) mirroring `ems_norm_region`; **context `data.{key}.value` unwrap** (`old-ems/EventFilter.SCALA:91-103`). Applies to the **CEL activation** and the **conf** only — never mutates the stored raw payload. Emits `ems_normalization_mutations_total{field}` (Micrometer counter) on each actual change.

**Step 1 (test-first):** `NormalizerTest` — `normFreq("D")=="DAILY"`, `normFreq("monthly")=="MONTHLY"`, `normRegion("Americas")=="AMER"`, unwrap `{"reporting-date":{"value":"2026-07-17"}}`→`"2026-07-17"`, and mutation counter increments only on change.
**Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS.
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`.

### Task B.2: Java↔SQL Normalizer parity (Testcontainers IT)

**Files:**
- Create: `ems/src/test/java/com/orchestration/ems/ingestion/NormalizerSqlParityIT.java` (extends `AbstractPostgresIT`)

**Step 1 (test-first):** for the full (finite) value inventory, assert `SELECT ems_norm_freq(?)`/`ems_norm_region(?)` equals `Normalizer.normFreq(?)`/`normRegion(?)` exhaustively (§4.4 CI requirement). `@Testcontainers(disabledWithoutDocker=true)`.
**Step 2:** Run `mvn verify` locally → **skips** (no Docker); confirmed CI-only.
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify` green (IT skipped). **Report + wait (end of Batch B).**

---

## Batch C — `store/` + `decisions/` + `dispatch/` repositories (IT, CI)

### Task C.1: EventRepository / ContextRepository (byte-verbatim JSONB upsert — A3)

**Files:**
- Create: `.../store/EventRepository.java`, `.../store/ContextRepository.java`
- Test: `.../store/EventContextRepositoryIT.java` (extends `AbstractPostgresIT`)

Repos use `JdbcClient`. `upsert`: `INSERT INTO event (event_id, json) VALUES (?, ?::jsonb) ON CONFLICT (event_id) DO NOTHING` — raw payload verbatim; generated columns populate automatically. Same for context.

**Step 1 (test-first, IT):** insert `event_meg_started.json` → assert row present, `json` byte-verbatim, generated `task_id` (from `additionalData.taskId` — A7), `state`, `event_type` populated; re-insert same id → **no-op**, no duplicate. Insert `context_merival.json` → assert `first_parent_id`, `frequency=ems_norm_freq(...)`.
**Step 2:** Run → skips locally. **Step 3:** Implement repos. **Step 4:** (CI proves; locally: `mvn verify` green with skip).
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`.

### Task C.2: RoutingDecisionRepo (L0 rows, idempotent) + OutboxRepo

**Files:**
- Create: `.../decisions/RoutingDecisionRepo.java` — batch insert L0 rows `INSERT ... ON CONFLICT DO NOTHING` on `ux_rd_l0 (event_id, tenant_id) WHERE tier='L0_SUBSCRIPTION'`; `decided_by='ems'`, `tier='L0_SUBSCRIPTION'`, decision `FORWARDED|NOT_SUBSCRIBED`.
- Create: `.../dispatch/OutboxRepo.java` — `insert(dagRunId, dagId, conf)` `ON CONFLICT (dag_run_id) DO NOTHING`; `drainPending(batchSize)` = `SELECT ... WHERE delivered_at IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT ?`; `markDelivered(dagRunId)`; `recordAttempt(dagRunId, error, nextEligibleAt)`.
- Test: `.../decisions/RoutingDecisionRepoIT.java`, `.../dispatch/OutboxRepoIT.java` (extend `AbstractPostgresIT`)

**Step 1 (test-first, IT):** L0 idempotency — insert (event, tenant) twice → one row. Outbox — `SKIP LOCKED` drain returns pending only; `markDelivered` sets `delivered_at`; concurrent drain in two TX returns disjoint rows.
**Step 2–4:** implement; local `mvn verify` green (skip).
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch C).**

---

## Batch D — `subscription/SubscriptionService` (unit local; repo load IT)

### Task D.1: SubscriptionRepo (load enabled rows)

**Files:**
- Create: `.../subscription/SubscriptionRepo.java` — `List<SubscriptionRow> loadEnabled()` via `JdbcClient`.
- Test: `.../subscription/SubscriptionRepoIT.java` (seeds a few rows, asserts load).

### Task D.2: SubscriptionService (two-stage A4, cel-java, cached)

**Files:**
- Create: `.../subscription/SubscriptionService.java`
- Create: `.../subscription/CelPrograms.java` (compile+cache cel-java `Program` per row version)
- Test: `.../subscription/SubscriptionServiceTest.java` (real cel-java, in-memory rows from `SubscriptionFixtures` — **runs locally**)

Behavior:
- `persistMatches(EventRow)` — stage 1: OR over enabled PERSIST programs; **event fields only** (activation = `{event: <normalized event map>}`, no `context`). Returns boolean (≥1 match ⇒ persist).
- `forwardMatches(EnrichedEvent)` — stage 2: every enabled FORWARD program over `{event, context}` activation; returns `List<SubscriptionMatch>` (fan-out).
- `context.*` **rejection** on PERSIST rows at load/compile time (A4; mirrors the `PUT /admin/subscriptions` guard) — a PERSIST `when_cel` referencing `context` is rejected.
- Caffeine `refreshAfterWrite(60s)`; programs cached per row version.
- `PERSIST ⊇ FORWARD` invariant helper (§7 CI chain): a check that every FORWARD fixture passes the PERSIST gate.
- CEL semantics grounded in `JsonFilterRuleset` (case-insensitive via Normalizer-upper per micro-decision 2; `.startsWith` for `TOPSIDE.*`).

**Step 1 (test-first):** with seed-0 fixtures — MERIVAL BATCH INGESTION event ⇒ persist=true and CAPITAL forward match; a firehose non-matching event ⇒ persist=false (drop); FRCA CURATION event with `context.data.run-category="TOPSIDE_X"` ⇒ CAPITAL forward match, with `"OTHER"` ⇒ no match; a PERSIST row referencing `context.*` ⇒ rejected; `PERSIST ⊇ FORWARD` holds for all fixtures.
**Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS (locally).
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch D).**

---

## Batch E — `ingestion/` context resolution (unit + WireMock local; DB path IT)

### Task E.1: EdfContextClient (provisional contract, WireMock)

**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/ingestion/EdfContextClient.java` — `RestClient`; GET `{edfPath}/{id}` with bearer token; 200→`Optional<ContextRow>`, 404/4xx→`Optional.empty()`, 5xx/timeout→throw (transient → park). Short bounded in-call retry (§10). **Provisional per §14 item 2** — javadoc flags the assumption.
- Create: `.../config/RestClientConfig.java` (EDF `RestClient` bean; Airflow client added in Batch H).
- Test: `.../ingestion/EdfContextClientTest.java` (**WireMock**, runs locally).

**Step 1 (test-first):** WireMock stub 200 with a context body ⇒ `Optional` present, byte-verbatim; 404 ⇒ empty; 503 ⇒ throws (park signal).
**Step 2–4:** implement; PASS locally.
**Checkpoint:** `mvn verify`.

### Task E.2: ContextResolver (Caffeine → DB → EDF)

**Files:**
- Create: `.../ingestion/ContextResolver.java` — `Optional<ContextRow> resolve(String contextId)`: Caffeine hit → return; else DB PK lookup (`ContextRepository.findById`) → cache+return; else `EdfContextClient` → on hit **persist fetched context** (mirrors `old-ems/EventSender.scala:113-118`) + cache. Null contextId → empty (warn). Emits `ems_context_fetch_total{source}`.
- Create: `.../config/CacheConfig.java` (Caffeine: context 24h/10k; subscription cache lives in SubscriptionService).
- Test: `.../ingestion/ContextResolverTest.java` (cache + EDF-miss paths, mocked repo/client — local); `.../ingestion/ContextResolverIT.java` (real DB-hit path, extends `AbstractPostgresIT`).

**Step 1 (test-first):** second `resolve` of same id ⇒ zero repo/client calls (cache); DB miss + EDF hit ⇒ context persisted then cached.
**Step 2–4:** implement; local unit PASS, IT skips.
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch E).**

---

## Batch F — single-TX ingestion pipeline (`IngestionService`) + consumer

### Task F.1: IngestionService (the normative §4.2 pipeline, one TX)

**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/ingestion/IngestionService.java`
- Test: `.../ingestion/IngestionServiceTest.java` (mocked repos/resolver/subscription — verifies **order + drop semantics**, local); single-TX atomicity proven in Batch I IT.

Flow (`process(EventResponse)`):
1. Parse → `EventRow`; `Normalizer` normalizes the event half for the activation (raw kept).
2. **Stage 1** `subscriptionService.persistMatches(event)` — zero ⇒ `ems_events_dropped_total{source}`++, **return (ack, no persist, no fetch)**.
3. `contextResolver.resolve(event.contextId)` → normalize context half.
4. **Stage 2** `subscriptionService.forwardMatches(enriched)` → `List<SubscriptionMatch>`.
5. **`@Transactional`** `persist(...)`: `EventRepository.upsert` + `ContextRepository.upsert` (if context present); `RoutingDecisionRepo` L0 rows — `FORWARDED` per matched tenant, `NOT_SUBSCRIBED` for the rest; one `OutboxRepo.insert(dagRunId, controlDagId, conf)` per FORWARD match where `conf = enriched.toConf()`, `dagRunId = DagRunId.derive(controlDagId, canonical conf)`.
6. Return normally ⇒ caller acks.

**Step 1 (test-first):** zero-match event ⇒ resolver/repos never called, drop counter++; matching event ⇒ upserts + N outbox rows + correct decision mix, all within a single `@Transactional` (verified via a spy on the tx-bound repo calls); FORWARD-but-no-PERSIST is impossible by construction; PERSIST-but-no-FORWARD ⇒ persisted with all-`NOT_SUBSCRIBED`, zero outbox.
**Step 2:** Run → FAIL. **Step 3:** Implement. **Step 4:** Run → PASS (local).
**Checkpoint:** `mvn verify`.

### Task F.2: EventConsumer (manual ack)

**Files:**
- Create: `.../ingestion/EventConsumer.java` — `@KafkaListener` on the EDF topics, `AckMode.MANUAL_IMMEDIATE`; calls `IngestionService.process`, then `ack.acknowledge()`; throws on failure (no ack → error handler classifies). Gated by `ems.consumer.enabled`.
- Test: `.../ingestion/EventConsumerTest.java` (**EmbeddedKafka**, real listener, mocked `IngestionService` — asserts ack-after-success and no-ack-on-throw, local).

**Step 1 (test-first):** publish a record ⇒ `process` invoked, offset committed; `process` throws ⇒ offset **not** committed (redelivered).
**Step 2–4:** implement; PASS locally (EmbeddedKafka).
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch F).**

---

## Batch G — error handling + poison-only DLQ (A1)

### Task G.1: KafkaConfig — ErrorHandlingDeserializer + DefaultErrorHandler

**Files:**
- Create: `.../config/KafkaConfig.java` — consumer factory with `ErrorHandlingDeserializer` (wrapping the value deserializer), `auto.offset.reset=earliest` (§10 deliberate correction), `enable.auto.commit=false`, `isolation.level=read_committed`; `DefaultErrorHandler` classifying **poison** (deserialization/contract) → `DeadLetterPublishingRecoverer` (`failIfSendResultIsError=true`, DLT producer `acks=all`) to `<topic>.ems.dlq` then ack; **transient** (PG/EDF unavailability marker exceptions) → `SeekToCurrentErrorHandler`-style **unbounded** backoff (no `max.poll.interval` trip). `DlqRecorder` writes a `dlq_record` row (best-effort correlation keys + exception chain) on dead-letter.
- Create: `.../ingestion/DlqRecorder.java`
- Test: `.../ingestion/DlqRoutingTest.java` (**EmbeddedKafka**, local).

**Step 1 (test-first):** a malformed/poison record ⇒ lands on `<topic>.ems.dlq` within the drill window, `dlq_record` row written, partition **not** stalled (next good record processes); a transient-marked failure ⇒ record redelivered (offset uncommitted), nothing on DLQ.
**Step 2:** Run → FAIL. **Step 3:** Implement config + recorder. **Step 4:** Run → PASS (local EmbeddedKafka; the `dlq_record` DB write asserted via mock here, real DB in Batch I).
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch G).**

---

## Batch H — OutboxDispatcher (async trigger drain)

### Task H.1: AirflowTriggerClient

**Files:**
- Create: `.../dispatch/AirflowTriggerClient.java` — `RestClient` `POST /dags/{dagId}/dagRuns` with `{dag_run_id, conf}`; **200 or 409 ⇒ delivered**; 429/5xx ⇒ retriable (backoff); other 4xx ⇒ non-retriable (record + alert). Basic/JWT auth per profile.
- Add its bean to `RestClientConfig`.
- Test: `.../dispatch/AirflowTriggerClientTest.java` (**WireMock**, local): 200⇒delivered, 409⇒delivered, 503⇒retriable, 400⇒non-retriable.

### Task H.2: OutboxDispatcher

**Files:**
- Create: `.../dispatch/OutboxDispatcher.java` — `@Scheduled(fixedDelay=2000)` (gated `ems.dispatch.enabled`); `OutboxRepo.drainPending` (`FOR UPDATE SKIP LOCKED`) in a TX; per row call `AirflowTriggerClient`; delivered ⇒ `markDelivered`; retriable ⇒ `recordAttempt` with 30s→600s jittered backoff (port of `ExponentialBackoffRetryStrategy`). Emits `ems_outbox_pending_age_seconds`.
- Test: `.../dispatch/OutboxDispatcherTest.java` (mocked repo/client — backoff + 200/409 delivered + retriable-retained logic, local).

**Step 1 (test-first):** delivered rows marked; 503 row retained with incremented attempts + future eligibility; 409 treated as delivered; dispatch disabled ⇒ no calls.
**Step 2–4:** implement; PASS locally.
**Checkpoint:** `mvn -B -ntp -f ems/pom.xml verify`. **Report + wait (end of Batch H).**

---

## Batch I — integration drills (Testcontainers, CI) — the Phase-2 exit gate (§12)

All extend `AbstractPostgresIT` (+ Testcontainers/EmbeddedKafka as needed), `@Testcontainers(disabledWithoutDocker=true)` → **auto-skip locally, run green in CI**.

### Task I.1: Full-pipeline IT
**File:** `.../ingestion/FullPipelineIT.java` — publish an `EventResponse` (MERIVAL BATCH) → assert L0 verdict rows, event+context persisted, generated columns populated (A7/A8/A9), **one outbox row written in the same TX**; publish a firehose non-match → dropped, counted, nothing persisted; EDF fetch via WireMock.

### Task I.2: Redelivery / L0 idempotency IT
**File:** `.../ingestion/RedeliveryIdempotencyIT.java` — deliver the same event twice → single event row, single L0 row set, single outbox row, no double trigger.

### Task I.3: Poison → DLQ IT
**File:** `.../ingestion/PoisonDlqIT.java` — malformed payload → verified publish to `<topic>.ems.dlq` < drill window, `dlq_record` correlations populated, partition not stalled; **DLQ-publish-failure drill** (broker rejects DLT send → offset uncommitted → redelivered).

### Task I.4: Transient-outage park IT
**File:** `.../ingestion/TransientOutageIT.java` — DB/EDF down (stop container / WireMock 503) → partition parks (unbounded backoff), **nothing dead-lettered**, record processed after recovery.

### Task I.5: Kill-Airflow backlog-drain IT
**File:** `.../dispatch/KillAirflowDrainIT.java` — Airflow (WireMock) down while events ingest → outbox accumulates, ingestion unaffected; bring Airflow up → backlog **fully drains**, zero lost triggers, `ems_outbox_pending_age_seconds` observed.

**Step per task:** write IT → `mvn verify` locally **skips** → confirm it is CI-wired (`.github/workflows/ems-ci.yml` runs Failsafe with Docker).
**Checkpoint (phase exit):** `mvn -B -ntp -f ems/pom.xml verify` green locally (all ITs skip); **CI run green** on the drills = the §13 Phase-2 exit criterion. **Report + wait.**

---

## Phase-2 exit criteria (record as gate evidence)

- [ ] `mvn -B -ntp -f ems/pom.xml verify` green locally (unit + EmbeddedKafka + WireMock tests pass; Testcontainers ITs auto-skip).
- [ ] **CI green** on all Batch I drills (poison→DLQ, redelivery/idempotency, transient-park, kill-Airflow drain) + repository/parity ITs.
- [ ] Two-stage A4 semantics unit-proven: PERSIST event-only + `context.*` rejection; FORWARD event+context; `PERSIST ⊇ FORWARD` invariant holds for seed-0 fixtures.
- [ ] Single-TX atomicity of (event, context, routing_decision, outbox) demonstrated in `FullPipelineIT`.
- [ ] Any legacy contradiction found while building recorded as amendment **A11+** in `ems-design.md §0` before it was built on.

## Notes carried forward
- Production `seed-0` subscription seed + shadow parity = Phase 4/5 (§11), not here.
- REST controllers, `POST /decisions`, admin endpoints, Spring Security = **Phase 3**.
- EDF contract (§14 item 2) still open — `EdfContextClient` provisional; revisit when the contract lands.
