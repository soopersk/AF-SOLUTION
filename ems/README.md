# EMS — Event Micro Service (Java 17 / Spring Boot)

**Workstream:** EMS rewrite = trigger-plan **Phase A**.
**Authoritative plan:** [`../ems-design.md`](../ems-design.md) (build-ready). Where silent, the
[trigger plan](../trigger_redesign_final_implementation_plan.md) governs; the five recorded
amendments **A1–A5** (ems-design §0) win over the trigger plan.

A from-scratch Java 17 + Spring Boot 3.5.x rewrite of the Scala event-orchestration service against
Azure Database for PostgreSQL. Fixes the 10+ min sensor queries (typed generated columns + indexes)
and ships the Phase-A control-plane surfaces the later phases depend on.

## The binding amendments

Design-level (A1–A5) and code-grounding (A6–A10, derived from `old-ems/` + `old-orchestration/`).
Full text + evidence in [ems-design §0](../ems-design.md).

| # | Amendment |
|---|---|
| A1 | Transient-infra failures **park the partition** (unbounded seek-based backoff), never dead-letter. DLQ is **poison-only**. Airflow is off the ingest path via the outbox. |
| A2 | Cutover = **shadow-consume then big-bang route flip** with version-controlled rollback (§11). |
| A3 | Event store is **not** "as-is": typed `GENERATED ALWAYS … STORED` columns + composite indexes over the JSONB (write path unchanged). |
| A4 | Level-0 is **two-stage**: `PERSIST` gate (event-only CEL, pre-enrichment; zero-match = drop-without-persist) + `FORWARD` conditions (event+context CEL, post-enrichment, in-TX). |
| A5 | `contractVersion` enters the conf at **Phase B**, not Phase A (protects `dag_run_id` dedup across the cutover overlap — Phase A conf stays byte-identical to today). |
| A6 | `dag_run_id` is a **new** deterministic scheme — legacy set no run id (`EventSender.scala:86-103`, `dag_utils.py:28-37`). `dag_run_id = orch_sha1(dag_id + jcs(conf))[:16]`, jcs = RFC 8785. Cross-engine parity locked by [`../shared/canonical-conformance/`](../shared/canonical-conformance/). |
| A7 | MEG `taskId`/`taskEventType` are nested under `additionalData` — generated columns COALESCE to top level. |
| A8 | Cross-family key spellings (`reporting-date`\|`reportingDate`, `run-category`\|`runCategory`, `h3Region`\|`regionCode`) — COALESCE both. |
| A9 | `parentIds` is **array-contains** queried (`c.json->'parentIds' ?? ?`) — GIN `jsonb_path_ops` index, not a scalar first-parent column. |
| A10 | `/event` param matching is a **4-location OR**, not a single-column alias map (`DatabaseEventRepository` OTHER_PARAMETERS_TEMPLATE). |

## Target package layout (ems-design §10)

Chosen org root: `com.orchestration.ems`. Packages below exist as documented `package-info.java`
stubs (skeleton); classes are filled in Phases 2–3.

```
ems/
  pom.xml                                # Spring Boot 3.5.4 parent, Java 17
  src/main/java/com/orchestration/ems/
    EmsApplication.java
    canonical/    # CanonicalJson (RFC 8785 JCS) + DagRunId (A6) — DONE
    config/       # kafka, security, datasource, cache, retry, azure, toggles
    ingestion/    # EventConsumer, Normalizer, ContextResolver, EdfContextClient
    subscription/ # SubscriptionService (cel-java, cached programs), SubscriptionRepo
    store/        # EventRepository, ContextRepository (JdbcClient, JSONB upsert)
    decisions/    # RoutingDecisionRepo, DecisionIngestController (POST /decisions)
    dispatch/     # OutboxRepo, OutboxDispatcher, AirflowTriggerClient
    api/          # Event/Context/RunStatus/GateGroups/Admin/Token controllers
    recon/        # ReconciliationSweep (lag, DLQ depth, outbox age, overdue runs)
    model/        # records: EventRow, ContextRow, EnrichedEvent, SubscriptionMatch
  src/main/resources/
    application.yml                      # profiles: local | azure | shadow | live
    db/migration/                        # Flyway V1__..V5__ (ems-design §5–§6, A7–A10 applied)
  src/test/java/com/orchestration/ems/
    canonical/CanonicalConformanceTest.java   # A6 cross-engine lock
    store/FlywayMigrationIT.java              # Testcontainers PG16 — flyway migrate + gen cols
  src/test/resources/samples/           # MEG / CALC / Merival event+context fixtures
  deploy/helm/ems/                      # Chart, values, Deployment/Service/HPA
../shared/canonical-conformance/        # authoritative dag_run_id vectors (both engines load)
../.github/workflows/ems-ci.yml         # mvn verify (unit + Testcontainers IT)
```

## Phase board (ems-design §13)

| Phase | Scope | Exit criterion | Status |
|---|---|---|---|
| **0. Verification spike** | Resolve all §14 open items vs production data & the EDF team | Open-items table fully answered; DDL paths + seed inventory finalized | ⛔ blocked — needs external inputs (see below) |
| **1. Foundation** | Repo scaffold, CI, Flyway V1–V5, Testcontainers harness, cel-java pin + cross-engine conformance fixtures, Helm skeleton | `flyway migrate` + integration harness + conformance suite green in CI | 🟢 **`mvn verify` green locally** — build compiles; **`CanonicalConformanceTest` 7/7 pass** (A6 JCS + `dag_run_id` verified for real). `FlywayMigrationIT` is written but **`disabledWithoutDocker`** — it self-skipped here (this box's Docker Desktop engine is on the `dockerDesktopLinuxEngine` pipe and docker-java's probe 400s; CLI works, Testcontainers doesn't). **It must be run green in CI (Docker present) to sign off Foundation.** Seed translation + value inventory still gated on §14 items 3,8. |
| **2. Ingestion + control-plane core** | Consumer, Normalizer, SubscriptionService, ContextResolver + EDF client, single-TX persist, OutboxDispatcher, DLQ | Integration tests incl. poison/redelivery/outage/kill-Airflow drills green | ⏳ |
| **3. APIs** | Query controllers (byte-compat), `/run/status`, `/gate/groups`, `POST /decisions`, `POST /admin/replay`, `PUT /admin/subscriptions`, auth | Contract suites green; F0-unblocking `/run/status` verified | ⏳ |
| **4. Ops readiness** | Metrics/alerts, dashboards, Helm, perf test @10 M rows, subscription seed | Perf gate (<50 ms p95) green; seed parity reviewed | ⏳ |
| **5. Shadow + cutover** | §11 stages 1–2: shadow-consume, parity jobs, backfill, flip; rollback drill rehearsed | §12 acceptance checklist fully ticked; observation window entered | ⏳ |
| **6. Lifecycle** | Retention/archival DAG + staging dry-run | Retention gate ticked | ⏳ |

## What this build supplies to later phases

- **Phase B:** `PUT /admin/subscriptions`, `routing_decision` store, `POST /decisions`.
- **Phase D:** `GET /gate/groups`, fast indexed contribution queries.
- **Framework F0 (hard blocker):** `GET /run/status` in production.
- **Framework F1+ / unmigrated DAGs:** `GET /event` byte-compatible 200/404 contract.

## Current blockers

- **Phase-0 spike (ems §14)** still needs production data + the EDF team for the items code cannot
  answer: EDF Context REST API contract (item 2), per-environment seed deltas (item 3),
  normalization value inventory (item 8), `routing_decision` retention horizon + topic volumes
  (items 6, 10). These gate the *seed* and *perf* work, not the Foundation scaffold.
- **Legacy source now in-workspace** (`old-ems/`, `old-orchestration/`): the "verify against code,
  not docs" pass is done for the code that exists here — it produced amendments **A6–A10** and the
  corrected V1 DDL. `EventSender.scala`/`dag_utils.py` proved A6 (no legacy run id → new scheme);
  `EventFilter`/`JsonFilterRuleset`/`EventListener` grounded A4 + the CEL translation;
  `DatabaseEventRepository`/`EventController` grounded A7–A10. Remaining code-parity gap: no live
  Airflow/EDF to run shadow parity against (that is Phase 5, not Foundation).
- **Environment (this workspace):** no Docker daemon and no network, so `mvn verify` /
  `flyway migrate` / the Testcontainers IT have **not been executed** — the SQL and harness are
  reviewed for PG16 correctness but their green run is pending CI or a local Docker host.
