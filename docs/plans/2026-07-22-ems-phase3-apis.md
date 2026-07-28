# EMS Phase 3 — APIs (Query + Control-Plane + Auth) — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
> **Working-agreement adaptation (no git — user's standing choice):** every "Commit" step is replaced by a **Checkpoint** — run the stated `mvn -B -ntp -f ems/pom.xml verify` and *report + wait* at each batch boundary. One batch = one review/rollback unit. Execute batches **inline in the main thread** (no subagents — session limit).

**Goal:** Put the HTTP surface on top of the Phase-2 backbone (ems-design §13 phase 3): the byte-compatible query controllers (`GET /event`, `/context`, `/parentcontext`, `/childcontext`), the framework-F0-unblocking `GET /run/status`, `GET /gate/groups`, `POST /decisions`, `POST /admin/replay`, `PUT /admin/subscriptions`, and Spring Security (Entra JWT + Basic, CI principal).

**Architecture:** Thin `@RestController`s over new read repositories that query the Phase-2 store. The query path reproduces the **legacy 4-location OR** semantics (Amendment A10) with the redesign's promoted-column *indexed join* (A3) and `parentIds` array-containment (A9) as index accelerators — response bodies are built from the raw `json` columns, so the wire format stays byte-compatible. Control-plane writes reuse Phase-2 machinery: `POST /decisions`→`routing_decision`, `POST /admin/replay`→`dlq_record` + Kafka re-publish, `PUT /admin/subscriptions`→`subscription` upsert gated by the existing `CelPrograms` compile + A4 `context.*` rejection. Security is an OAuth2 resource server (JWT + group claims) with Basic fallback, layered on last so every controller is built and contract-tested permit-all first.

**Tech Stack:** Java 17 / Spring Boot 3.5.4, Spring Web MVC (blocking, already a dep), `JdbcClient` (no ORM), Spring Kafka (`KafkaTemplate` for replay — already present), Spring Security + `spring-boot-starter-oauth2-resource-server` (**new deps, Batch G**), cel-java (`CelPrograms`, already built). Tests: `@WebMvcTest` slices with a mocked repository for controller/contract behavior (**run locally, green**); Testcontainers PG16 `*IT` (`@Testcontainers(disabledWithoutDocker=true)` → auto-skip locally, **run in CI**) for the real query/persistence round-trips; `spring-security-test` for the auth matrix.

**Scope boundary (do NOT build here):** No metrics/alerts/dashboards/Helm (Phase 4). No production `seed-0` seed (Phase 4, §11 stage 0). No shadow/cutover (Phase 5). No retention DAG (Phase 6). This phase adds only the API layer + auth over the existing backbone.

---

## The contract-oracle caveat (read first — it shapes the whole phase)

§13's Phase-3 exit criterion is *"Contract suites green **vs recorded current responses**; F0-unblocking `/run/status` verified."* **We have no recorded production responses** — the legacy Scala service is not running on this box and there is no prod/staging access. What we *do* have, and what this plan uses as the contract oracle, is:

1. **Legacy source semantics** — `old-ems/EventController.scala` + `old-ems/DatabaseEventRepository.scala` define the exact query construction, join-type selection, param handling, and 200/404 rules. These are reproduced 1:1 and asserted by **source-derived golden fixtures**.
2. **Sample payloads** — `old-orchestration/dags/*.json` and `ems/src/test/resources/samples/*.json` (Merival + MEG/CALC families) seed the store in ITs so the round-trip produces a concrete, asserted body.

**Consequence, stated honestly:** Phase-3 "contract green" means *green vs the source-derived golden fixtures*, not vs live prod bytes. **True prod byte-parity is the §11 shadow stage (Phase 5)** — it is not achievable in Phase 3 and is not claimed here. Where the legacy source leaves a param-handling detail genuinely underdetermined (the §14-item-1b sensor-traffic inventory), the plan reproduces the **legacy source behavior verbatim** and flags the residual as an assumption — it does **not** invent canonicalization the legacy path did not perform (see the A10 note below).

---

## The central query-semantics decision (Batch A hinges on this)

Amendment **A10** governs `/event` and **replaces the §4.3 param→single-column alias table**: each non-id param is matched across **four raw-JSON locations** — `e.json->>?`, `e.json->'additionalData'->>?`, `c.json->>?`, `c.json->'data'->>?` — **case-sensitive**, `|`-multivalue OR, with **no** param→column alias map; promoted columns are an **index accelerator, not a semantic change** (`old-ems/DatabaseEventRepository.scala:34-35`).

This is in tension with §4.3's claim that param *values* "pass through the same canonicalization as ingestion" (e.g. `FREQUENCY=DAILY`, compact-date→ISO). The legacy path did **no** such canonicalization — `OTHER_PARAMETERS_TEMPLATE` is a literal `IN (…)`. Since the stored `json` is byte-verbatim, canonicalizing a param value and matching it against **raw** JSONB would *break* byte-compat, not preserve it.

**Resolution adopted by this plan (no new amendment needed — A10 already replaced the §4.3 alias table):**
- The `/event` WHERE is the **literal 4-location OR against raw JSONB** (A10), verbatim from legacy. **No value canonicalization is applied to query params.**
- The two **id filters** that *do* bind to promoted columns are index accelerators with identical result sets: the join `c.context_id = e.context_id` (A3, replaces the legacy `e.json->>'contextId'` join) and `parent_id → c.json->'parentIds' @> to_jsonb(?)` (A9, replaces `c.json->'parentIds' ?? ?` — same JSONB-contains semantics, GIN-indexed).
- The §4.3 canonicalization claim (frequency/region/compact-date) is **deferred, evidence-gated on §14 item 1b** (the sensor-traffic inventory). Until that inventory exists and proves sensors send non-canonical values that the legacy service somehow matched, adding canonicalization would be a *speculative behavior change* against byte-compat. **YAGNI: not built in Phase 3.** Flagged in the plan and in memory.

If, while building, the golden fixtures reveal the legacy source actually *does* transform a value (it does not, per the read), that is a contradiction to record as **amendment A11** in ems-design §0 *before* building on it.

---

## Grounding checkpoints (verify against code, not docs)

- `/event` query construction, join-type table, 4-location OR, `|`-split, 200/404, empty-params→400 ← `old-ems/EventController.scala:37-54`, `old-ems/DatabaseEventRepository.scala:26-104`.
  - joinType: `(eventId,contextId)` = `(Some,Some)`→`INNER`, `(Some,None)`→`RIGHT OUTER`, else→`LEFT OUTER` (`DatabaseEventRepository.scala:67-71`). Base: `FROM context c <join> JOIN event e ON <join key> <where>`.
  - id templates: `e.event_id = ?`, `c.context_id = ?`, `parentIds @>` — only the supplied ids contribute; ANDed with the per-param 4-location OR groups.
- `/context` = single param `context_id`, 200 body / 404 (`EventController.scala:63-74`); `/parentcontext` + `/childcontext` require `initial_context_id`, walk the `parentIds` chain, `/childcontext` honours `limit` default `"1"` (`EventController.scala:76-112`).
- Response body shape = **list of `EnrichedEvent{event, context}`** built from the raw `event_json`/`context_json` columns (`DatabaseEventRepository.scala:37-41`) → byte-compatible.
- `/run/status` response schema ← the framework `SlaAwareHttpTrigger` expectations in `framework_redesign_final_implementation_plan.md` (categorized `NEVER_STARTED`/`STARTED_NO_TERMINAL`/`TERMINAL_IN_DLQ`); ems-design §4.5 fields `{scheduled, started, terminal:{present,successful,event_id}, dlq_hint, last_event_at}`.
- `PUT /admin/subscriptions` A4 guard (`PERSIST` rejects any `context.*` reference; any stage rejects uncompilable CEL) ← existing `CelPrograms` (Phase-2, already enforces this in `SubscriptionService`).
- Auth modes (OAuth2 JWT + group claims, Basic fallback; `/decisions`=dispatcher JWT, `/admin/*`=elevated group, `PUT /admin/subscriptions`=CI principal) ← ems-design §4.3 "Auth" para + §4.5; legacy `AuthorizationManager` modes.

---

## Testing strategy (governs every task's test placement)

| Layer | Harness | Runs locally? | Suffix |
|---|---|---|---|
| controller param handling, join-type selection, 200/404/400, response assembly, `/run/status` categorization, `/gate/groups` grouping, `POST /decisions` mapping, admin validation, **auth matrix** | `@WebMvcTest` + mocked repo (+ `spring-security-test` in Batch G) | **yes, green** | `*Test` / Surefire |
| real query round-trip over seeded store (4-location OR, indexed join, `parentIds @>`, `/run/status` over real rows, replay re-publish, subscription upsert) | Testcontainers PG16 (+EmbeddedKafka for replay) | no — **auto-skip**, CI-only | `*IT` / Failsafe |

**Rule:** put all logic-shaped assertions in `@WebMvcTest` slices so `mvn verify` stays **green locally at every checkpoint**; isolate anything needing real Postgres/GIN/JSONB into `*IT`. The byte-compat golden fixtures live in the `@WebMvcTest` layer (mocked repo returns the fixture body) **and** are re-asserted end-to-end in one `*IT` per controller.

---

## Open micro-decisions for the review (my recommendation in **bold**)

1. **Query repo return type:** **new `EventQueryRepository` in `store/` returning `List<EnrichedEventView>` where `EnrichedEventView` = raw `event` JSON node + raw `context` JSON node** (mirrors legacy `EnrichedEvent{event,context}`), serialized straight to the response so bytes come from stored `json`. *Alt: map to typed DTOs — rejected, risks wire drift.*
2. **`/event` join key:** **`c.context_id = e.context_id` (both promoted, A3-indexed)** — the A3 fix to the legacy `e.json->>'contextId'` join; identical result set, index-eligible. *Alt: keep the JSONB-extraction join — rejected, defeats the whole redesign.*
3. **Controller test harness:** **`@WebMvcTest(controllers=…)` with `@MockBean` repo** for fast local contract tests; one Testcontainers `*IT` per controller for the real round-trip. *Alt: full `@SpringBootTest` everywhere — CI-only, slower, no local signal.*
4. **Auth last:** **build + contract-test all controllers permit-all (no security dep), then add security in Batch G** so each surface is proven before the auth matrix layers on. *Alt: security-first — churns every controller test twice.*
5. **`/run/status` "scheduled":** EMS has no scheduler view, so **`scheduled` = derived-false/absent unless an event evidences it** (documented gap; the framework treats missing terminal as `NEVER_STARTED`/`STARTED_NO_TERMINAL` from `started`+`terminal`, which EMS *can* answer from the event store). Flag as a §14 follow-up if the framework needs a truer `scheduled`.

---

## Batch 0 — API test harness + byte-compat golden fixtures

### Task 0.1: WebMvc contract-test base + security-free web slice
**Files:**
- Create: `ems/src/test/java/com/orchestration/ems/api/support/AbstractWebMvcTest.java` — common `@AutoConfigureMockMvc`, `ObjectMapper`, JSON assert helpers (`assertJsonEquals` byte-normalized).
- Verify: `ems/pom.xml` has `spring-boot-starter-test` (it does); **no** security dep yet (permit-all until Batch G).

**Step 1:** Write the base with a `MockMvc` accessor and a `JsonNode`-equality helper (order-insensitive object compare, array-order-sensitive — matching Jackson serialization of stored JSONB).
**Step 2:** `mvn -B -ntp -f ems/pom.xml verify` → green (no controllers yet; base compiles).
**Checkpoint:** green.

### Task 0.2: Byte-compat golden fixtures (the contract oracle)
**Files:**
- Create: `ems/src/test/resources/contract/event/` — request-param sets + expected response bodies, derived from `old-ems` semantics over the sample payloads. At minimum: DATASET CHECK (`contextId,DATASET_UUID,FREQUENCY,LBD,source,TYPE`), START-EVENT LINK (`triggerContextId,taskEventType`), STATUS CHECK (`parent_id,type,STATE` multi-value `FINISH|FAILED`), single-`event_id`, single-`context_id`, and a **404** case (no match).
- Create: `ems/src/test/resources/contract/context/`, `contract/runstatus/` fixtures likewise.
- Create: `ems/src/test/java/com/orchestration/ems/api/support/ContractFixtures.java` — loader.

**Step 1:** Author fixtures by hand-executing the legacy query semantics against the sample rows (document the derivation in a header comment per fixture — which legacy line produces the shape). Mark any param whose per-family binding is a §14-1b gap with a `// ASSUMPTION §14-1b` note.
**Step 2:** No test yet — consumed by Batches A–C.
**Checkpoint:** none (data only) — folded into Batch A.

---

## Batch A — `GET /event` (byte-compat query, the migration-critical surface)

### Task A.1: `EnrichedEventView` model + `EventQueryRepository`
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/model/EnrichedEventView.java` — `record EnrichedEventView(JsonNode event, JsonNode context)`; Jackson serializes to `{"event":…,"context":…}` (legacy `EnrichedEvent` shape).
- Create: `ems/src/main/java/com/orchestration/ems/store/EventQueryRepository.java`.
- Test: `ems/src/test/java/com/orchestration/ems/store/EventQueryRepositoryIT.java` (Testcontainers, CI-only).

**Step 1 (IT, red):** seed 2 events + 2 contexts (Merival + MEG samples); assert `findEvents(eventId, contextId, parentId, dataMap)` reproduces every grounding-checkpoint case — join-type selection, 4-location OR, `|`-multivalue, `parentIds @>`, empty→all-filters-absent. Run → fails (no class).
**Step 2 (green):** implement the query builder as a 1:1 port of `DatabaseEventRepository.findEvents` with the A3/A9 substitutions:
  - base `SELECT e.json AS event_json, c.json AS context_json FROM context c <JOINTYPE> JOIN event e ON c.context_id = e.context_id <WHERE>`;
  - id templates `e.event_id = ?`, `c.context_id = ?`, `c.json->'parentIds' @> to_jsonb(?::text)`;
  - per-data-param group `(e.json->>? IN (…) OR e.json->'additionalData'->>? IN (…) OR c.json->>? IN (…) OR c.json->'data'->>? IN (…))`, one `?`-list per `|`-split value, params in the legacy `List.fill(4)(k +: v).flatten` order;
  - map rows to `EnrichedEventView` from the raw `json` text columns.
**Step 3:** `mvn verify` → green locally (IT skips).
**Checkpoint:** green.

### Task A.2: `EventController.event(...)`
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/api/EventController.java`.
- Test: `ems/src/test/java/com/orchestration/ems/api/EventControllerTest.java` (`@WebMvcTest`, mocked `EventQueryRepository`).

**Step 1 (red):** golden-fixture tests — empty params → **400**; no-match → **404**; match → **200** with the fixture body byte-equal. Assert the controller removes `event_id`/`context_id`/`parent_id` from the param map, `|`-splits the rest, and calls the repo with the legacy argument shape.
**Step 2 (green):** implement `@GetMapping("/event")` taking `@RequestParam MultiValueMap`/`Map<String,String>`; port `EventController.scala:37-54` control flow (param-name handling is case-sensitive on the *reserved* ids exactly as legacy — `event_id`/`context_id`/`parent_id`); 200 `ResponseEntity.ok(list)` / 404 / 400.
**Step 3:** `mvn verify` → green locally.
**Checkpoint:** green + report (this is the migration-critical contract — pause for review).

---

## Batch B — `GET /context`, `/parentcontext`, `/childcontext`
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/api/ContextController.java`.
- Add to `EventQueryRepository`: `findContextById(String)`, `walkParents(String initialContextId, Map filters)`, `walkChildren(String initialContextId, Map filters, int limit)` (parentIds array-containment via A9 GIN).
- Tests: `ContextControllerTest.java` (`@WebMvcTest`) + `ContextQueryIT.java` (Testcontainers chain-walk).

**Steps (TDD):** red fixture tests (`/context` 200/404 single `context_id`; parent/child require `initial_context_id`→400 otherwise; `/childcontext` `limit` default `"1"`); green by porting `EventController.scala:63-112` + the chain traversal using `json->'parentIds' @> to_jsonb(?)`.
**Checkpoint:** green.

---

## Batch C — `GET /run/status` (framework F0 unblocker — highest-leverage surface)
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/api/RunStatusController.java`.
- Create: `ems/src/main/java/com/orchestration/ems/api/RunStatusRepository.java` (one indexed query over `event` keyed by the `/event` correlation vocab; `dlq_hint` via `dlq_record` correlation-key match).
- Create: `ems/src/main/java/com/orchestration/ems/model/RunStatus.java` — `{scheduled, started, terminal:{present, successful, event_id}, dlq_hint, last_event_at}`.
- Tests: `RunStatusControllerTest.java` (`@WebMvcTest`, schema/categorization vs `SlaAwareHttpTrigger` fixture) + `RunStatusIT.java` (real rows: never-started, started-no-terminal, terminal-present, terminal-in-DLQ).

**Steps (TDD):** red — assert the response schema matches the framework fixture and the three categories resolve from `started`+`terminal`+`dlq_hint`; green — single query (`idx_event_context_id` driver) computing `started` (any event for the criteria), `terminal` (a terminal-state event → `present/successful/event_id`), `last_event_at` (`max(created_at)`), `dlq_hint` (`ix_dlq_context`/`ix_dlq_task` match). Document `scheduled` per micro-decision 5.
**Checkpoint:** green + report (F0 dependency — explicitly note "F0-unblocking `/run/status` verified" against the framework schema fixture).

---

## Batch D — `GET /gate/groups`
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/api/GateGroupsController.java` + `GateGroupsRepository.java`.
- Tests: `GateGroupsControllerTest.java` (`@WebMvcTest`) + `GateGroupsIT.java`.

**Steps (TDD):** generic grouped query — caller supplies `criteria`, `group_by=<json-path>`, `contributor=<json-path>`, `lookback=<dur>`; EMS stays rule-free (all paths from the caller). Implementation: `idx_event_created_at` lookback-window scan + promoted-column residual filters → in-service JSONB path extraction of `group_by`/`contributor` over the small qualifying set → distinct groups with present-contributor sets. red: assert grouping/lookback semantics on fixtures; green: implement.
**Checkpoint:** green.

---

## Batch E — `POST /decisions`
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/decisions/DecisionIngestController.java`.
- Extend `RoutingDecisionRepo` with `insert(DecisionRecord)` for **any** tier (not L0-only) carrying caller-supplied `decided_by`, `tier`, `tenant_id`, `target_dag_id`, `decision`, `detail`, `registry_version`, `engine_version`; batch insert; audit-never-blocks-dispatch semantics (best-effort, returns counts).
- Create: `ems/src/main/java/com/orchestration/ems/model/DecisionRecord.java`.
- Tests: `DecisionIngestControllerTest.java` (`@WebMvcTest`) + `DecisionIngestIT.java` (batch persist, idempotency where applicable).

**Steps (TDD):** red — POST a batch of slim decision records → rows persisted with `decided_by` from the (later JWT) identity, malformed record → 400, partial batch semantics documented; green — map + `insertBatch`.
**Checkpoint:** green.

---

## Batch F — `POST /admin/replay` + `PUT /admin/subscriptions`
**Files:**
- Create: `ems/src/main/java/com/orchestration/ems/api/AdminController.java`.
- Add to `DlqRecorder`/a new `DlqReplayService`: read selected `dlq_record` rows, re-`send` to the source topic via the existing `KafkaTemplate`, stamp `replayed_at`/`replayed_by`.
- Add `SubscriptionRepo.upsert(SubscriptionRow, updatedBy)` (V3 `ON CONFLICT (tenant_id, stage, rule_name) DO UPDATE`); validate via `CelPrograms` (compile; `PERSIST` rejects `context.*` — A4).
- Tests: `AdminControllerTest.java` (`@WebMvcTest` — validation: bad CEL→400, `PERSIST` with `context.*`→400), `DlqReplayIT.java` (EmbeddedKafka + PG: replay re-publishes + audits), `SubscriptionUpsertIT.java`.

**Steps (TDD):** red — replay of a `dlq_record` re-emits the original payload to its source topic and writes `replayed_by`; subscription upsert rejects uncompilable CEL and A4 violations, accepts valid rows and flips `SubscriptionService` cache within refresh; green — implement, reusing `CelPrograms` and `KafkaTemplate`.
**Checkpoint:** green + report (admin write path — pause for review).

---

## Batch G — Security (Entra JWT + Basic, CI principal) + `POST /token`
**Files:**
- Modify: `ems/pom.xml` — add `spring-boot-starter-security` + `spring-boot-starter-oauth2-resource-server`; test-scope `spring-security-test`.
- Create: `ems/src/main/java/com/orchestration/ems/config/SecurityConfig.java` — OAuth2 resource server (JWT, group-claim→authority mapping) with HTTP Basic fallback; request matchers: query endpoints = authenticated; `POST /decisions` = dispatcher/heartbeat authority; `/admin/**` = elevated group; `PUT /admin/subscriptions` = CI-principal authority. Local/test profile: a permissive/mock-JWT decoder so ITs and `@WebMvcTest` slices keep running without a live IdP.
- Create: `ems/src/main/java/com/orchestration/ems/api/TokenController.java` — legacy dev/test `POST /token` (mirror `AuthorizationManager` token issuance for non-Entra environments).
- Tests: `SecurityMatrixTest.java` (`@WebMvcTest` + `spring-security-test`: each endpoint × {anonymous, wrong-group, right-group} → 401/403/2xx); update existing controller `@WebMvcTest`s to `@WithMockUser`/`jwt()` where a principal is now required.

**Steps (TDD):** red — the auth matrix asserts 401/403/2xx per endpoint/role; green — `SecurityFilterChain` with the matchers above; wire `decided_by`/`replayed_by`/`updated_by` from the authenticated principal (closing the Batch E/F "from the JWT identity" placeholders).
**Checkpoint:** green + report.

---

## Phase-exit checkpoint (§13 Phase-3 exit)
- `mvn -B -ntp -f ems/pom.xml verify` **green locally** (all `*Test` pass; all `*IT` auto-skip without Docker).
- **Contract suites green vs the source-derived golden fixtures** (the achievable oracle — see the caveat; prod byte-parity remains Phase 5 / §11 shadow).
- **`/run/status` verified** against the framework `SlaAwareHttpTrigger` schema fixture (F0 unblock evidenced).
- **CI/Docker caveat (unchanged from Phase 2):** the `*IT`s only compile-and-skip locally; a real green run against Postgres/Kafka requires CI, which is still inert (no git). This carries the same open exit-gate as Phase 2 — the user chose to "settle the CI run later."
- Update memory (`ems-phase3-progress.md` + index); **report + wait** for review. Do **not** start Phase 4.

---

## What this plan deliberately does NOT do (YAGNI / scope guard)
- No query-param value canonicalization (the A10 vs §4.3 tension — deferred, evidence-gated on §14 item 1b; adding it now would break byte-compat).
- No metrics/alerts/dashboards/Helm, no perf @10M rows (Phase 4).
- No production `seed-0` subscription seed (Phase 4).
- No `/listen`, `/listencontext`, `/statuschange` dev endpoints unless a contract fixture proves an Airflow caller needs them (legacy marks them dev/test — §4.3; add only on evidence).
