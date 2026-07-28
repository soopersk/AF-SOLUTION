# Session handoff — Orchestration platform modernization

Paste the **"Prompt for the next session"** block at the bottom into a fresh session to continue.
Everything above it is the context that prompt refers to.

---

## What this program is

A 3-workstream modernization of an event-orchestration platform, run under a strict working
agreement. All high-level design decisions are **closed — do not re-open them.**

- **Kickoff / precedence:** [`phase2_dag_framework_planning_prompt.md`](phase2_dag_framework_planning_prompt.md)
- **Workstream 1 — EMS rewrite** (Scala → Java 17 / Spring Boot) = trigger-plan **Phase A**.
  Authoritative: [`ems-design.md`](ems-design.md); folder [`ems/`](ems/).
- **Workstream 2 — Control plane** (trigger semantics, Phases B–E). Authoritative:
  [`trigger_redesign_final_implementation_plan.md`](trigger_redesign_final_implementation_plan.md);
  folder [`control-plane/`](control-plane/).
- **Workstream 3 — Calculator DAG framework** (steps F0–F4). Authoritative:
  [`framework_redesign_final_implementation_plan.md`](framework_redesign_final_implementation_plan.md);
  folder [`framework/`](framework/).
- **Legacy reference (read-only, in-workspace):** `old-ems/` (Scala), `old-orchestration/`
  (Python + sample DAGs, registry, sample events). Design principle: **verify against code, not docs.**

## Working agreement (must follow)

1. **Per-step plan, reviewed with the requester before executing.** One step = one plan = one
   rollback unit. Use the `superpowers:executing-plans` skill discipline: batch related work,
   **checkpoint between batches, report and wait.**
2. **Gate discipline** — record evidence before advancing a phase.
3. **Amendment protocol** — when reality (esp. legacy code) contradicts a plan, record a numbered
   amendment **in the owning doc** (the A1–A10 pattern in `ems-design.md §0`) with file:line
   evidence, before building on it.
4. **No git yet** (user's standing choice). Do not `git init` unless asked.
5. **Canonical JSON = RFC 8785 (JCS).** Locked.

## Amendments recorded so far

- **A1–A5** (design-level) and **A6–A10** (code-grounding, from `old-ems/` + `old-orchestration/`),
  all in [`ems-design.md §0`](ems-design.md). Key ones the build depends on:
  - **A6** — `dag_run_id` is a NEW deterministic scheme (legacy set no run id):
    `dag_run_id = orch_sha1(dag_id + jcs(conf))[:16]`, jcs = RFC 8785, `orch_sha1` = lowercase-hex
    SHA-1 over UTF-8. Cross-engine parity is locked by
    [`shared/canonical-conformance/`](shared/canonical-conformance/).
  - **A7** taskId/taskEventType nested under `additionalData` → COALESCE. **A8** cross-family key
    spellings (`reporting-date|reportingDate`, etc.) → COALESCE. **A9** `parentIds` is
    array-contains → GIN `jsonb_path_ops`, not a scalar column. **A10** `/event` param matching is
    a 4-location OR.

## DONE — EMS Phases 1–3 (ems-design §13)

`mvn -B -ntp -f ems/pom.xml verify` is **green locally**: **151 unit tests pass, 84 `*IT`s skip**
(JDK 17 + Maven 3.9.12 are on this Windows box).

- **Phase 1 — Foundation.** `ems/pom.xml` (Spring Boot 3.5.4, Java 17; pinned cel-tools 0.4.4,
  JCS 1.1, Testcontainers 1.20.4, WireMock 3.9.2), `application.yml` (profiles local|azure|shadow|live),
  Flyway **V1–V5** with A7–A10 applied, the **A6 anchor** (`canonical/CanonicalJson`, `canonical/DagRunId`,
  `shared/canonical-conformance/canonical_vectors.json`, `CanonicalConformanceTest` **7/7**),
  `FlywayMigrationIT`, Helm **skeleton** `ems/deploy/helm/ems/`, CI `.github/workflows/ems-ci.yml`.
- **Phase 2 — Ingestion + control plane core.** `EventConsumer` (manual ack, `ErrorHandlingDeserializer`),
  `Normalizer` (Java authority mirroring the SQL `ems_norm_*` functions), `ContextResolver` +
  `EdfContextClient` (Caffeine), `SubscriptionService` + `CelPrograms` (A4 enforced structurally —
  `context` is undeclared for PERSIST), single-TX persist (event/context/L0 decision/outbox),
  `OutboxDispatcher` (`FOR UPDATE SKIP LOCKED`, 200/409 = delivered), poison-only DLQ + `dlq_record` (A1).
- **Phase 3 — APIs.** `GET /event` (A10 4-location OR, A3 indexed join, A9 `parentIds @>`),
  `/context` `/parentcontext` `/childcontext`, **`GET /run/status`** (framework F0 unblocker),
  `GET /gate/groups`, `POST /decisions`, `POST /admin/replay`, `PUT /admin/subscriptions`, and
  **security** (`SecurityConfig` + `AuthProperties` + `GroupAuthorities` + `POST /token`).
  Plan: [`docs/plans/2026-07-22-ems-phase3-apis.md`](docs/plans/2026-07-22-ems-phase3-apis.md).
- **Docs (outside the phase board):** [`docs/ems-technical-specification.md`](docs/ems-technical-specification.md)
  (24 sections; §23 traceability design§→code→test, §24 gap list) and
  [`docs/ems-user-guide.md`](docs/ems-user-guide.md) (API cookbook, CEL authoring, ops runbooks).
  Both are written against **the code as read** — every unbuilt surface is marked ⏳ inline.

### Auth shape landed in Phase 3 (Phase 4 inherits it)

`ems.auth.mode=local|entra`. **`local` is the default** — it self-issues HS256 tokens via
`POST /token` and decodes them with the same key, so slices/ITs/dev boxes need no IdP; a shared
environment must set `entra`. Startup logs a WARN; recorded as a spec §24.2 gap. Matrix: actuator
health/info permitAll · `PUT /admin/subscriptions` = `EMS_ADMIN` **and** `EMS_CI` · `/admin/**` =
`EMS_ADMIN` · `POST /decisions` = `EMS_DISPATCHER` · everything else authenticated.
`decided_by`/`replayed_by`/`updated_by` come from the authenticated principal, **not** the body.

### The two open gates carried forward

1. **No `*IT` has ever run for real.** Testcontainers can't reach Docker on this machine (engine on
   the `dockerDesktopLinuxEngine` pipe; docker-java's probe 400s; the CLI works, Testcontainers
   doesn't). All 84 ITs are `@Testcontainers(disabledWithoutDocker = true)` → they compile-and-skip
   locally and **must run green in CI**. Do not burn time re-diagnosing the local Docker pipe — that
   path is a dead end here. **Because there is no git, CI is inert**; this gate has been deliberately
   deferred since Phase 2 ("settle the CI run later").
2. **Still blocked on external inputs (ems-design §14):** EDF Context REST contract (item 2),
   per-environment subscription seed deltas (item 3), normalization value inventory (item 8),
   `routing_decision` retention + topic volumes (items 6, 10). These gate the **seed** and **perf**
   work — i.e. two of Phase 4's five workstreams.

## NEXT — EMS Phase 4 (Ops readiness), ems-design §13

**Scope:** metrics/alerts, dashboards, Helm finalization, perf test @10 M rows, subscription seed
translation. **Exit criterion:** perf gate (< 50 ms p95) green; seed parity reviewed.

State of each, verified against the code (not the docs):

- **Metrics — partial.** Implemented: `ems_events_dropped_total{source}` (`IngestionService`),
  `ems_context_fetch_total{source}` (`ContextResolver`), `ems_normalization_mutations_total{field}`
  (`Normalizer`), `ems_outbox_pending_age_seconds` (gauge, `OutboxDispatcher`). **Missing from the
  §10 table:** `ems_events_consumed_total{topic,outcome}`, `ems_subscription_verdicts_total{tenant,decision}`,
  `ems_dlq_depth{topic}`, `ems_consumer_lag{topic,partition}`, `ems_registry_version_info{component}`,
  `ems_overdue_inflight_runs`, and per-endpoint latency histograms for `/event`, `/run/status`,
  `/gate/groups`.
- **`recon/` is empty** — only `package-info.java`. `ReconciliationSweep` owns four of those six
  missing metrics (lag, DLQ depth, outbox age, overdue in-flight runs), so it is the largest single
  piece of Phase 4 code.
- **A live contradiction to resolve first:** `application.yml` exposes `management.endpoints.web.exposure.include: … prometheus`,
  but the only registry on the classpath is `micrometer-registry-otlp` — so `/actuator/prometheus`
  does not exist. Decide (and record) whether the scrape path is OTLP-push or Prometheus-pull, then
  make the config and the dependency agree.
- **Helm — skeleton only.** `Chart.yaml`, `deployment.yaml`, `service.yaml`, `hpa.yaml`,
  `_helpers.tpl`, `values.yaml` exist. Absent: alert rules / `PrometheusRule`, `ServiceMonitor`,
  `PodDisruptionBudget`, ConfigMap-rendered `ems.auth.*` + Kafka/EDF wiring, real Vault-agent
  annotations, and any `helm lint`/`helm template` check in CI.
- **Seed — fixture only, no migration.** `ems/src/test/resources/fixtures/subscriptions_seed0.json`
  (16 rows: 7 PERSIST + 8 CAPITAL FORWARD + 1 disabled NSFR) with a full provenance file. Its own
  header says it is **not** the production seed. The production seed is §11 stage 0 item 3 and needs
  the §14-item-3 per-environment deltas that are still outstanding.
- **Perf — nothing exists.** §12 wants 10 M synthetic events + 1 M contexts, canonical §4.3 query
  p95 **< 50 ms**, `/run/status` p95 < 50 ms, and an `EXPLAIN` assertion that the plan uses
  `idx_event_task_id` / `idx_context_rep_freq_region` with no seq scans. **This needs a real
  Postgres, so it cannot be run on this box** — same Docker wall as gate 1 above.

## Environment notes

- Windows 11, primary shell PowerShell 5.1; a Bash (Git Bash) tool is also available. Working dir
  `d:\GitRepo\Orchestration-Solution`.
- JDK 17 (Liberica) + Maven 3.9.12 present. Build: `mvn -B -ntp -f ems/pom.xml verify`.
- Docker CLI works but Testcontainers does not (see above) — keep ITs `disabledWithoutDocker`.
- Maven output is large: redirect it to a file and grep the file. Piping it through `Select-String`
  has broken the pipe and returned a misleading exit 255 on a build that actually succeeded.

---

## Prompt for the next session

> Continue the Orchestration platform modernization — **EMS Phase 4 (Ops readiness)**, ems-design
> §13. Read `SESSION_HANDOFF.md` first, then `ems-design.md §0` (amendments A1–A10), §10
> (config/deployment/observability — the metric table is the spec for this phase), §11 stage 0
> (the seed), §12 (the perf acceptance criteria), and `docs/plans/2026-07-22-ems-phase3-apis.md`
> for the format and discipline the previous phase plans used.
>
> **State:** Phases 1–3 are code-complete. `mvn -B -ntp -f ems/pom.xml verify` is green locally —
> 151 unit tests pass, 84 `*IT`s skip (no Docker). **Do not try to fix local Docker**; that is a
> known dead end, ITs are CI-only. There is **no git** — do not `git init`, do not commit; `mvn
> verify` is the only gate.
>
> **First, do not write code — write the Phase-4 plan and wait for my approval**, in the same shape
> as the Phase-3 plan: a goal/architecture/tech-stack header, an explicit scope guard, the
> grounding checkpoints, a testing-strategy table, the open micro-decisions with your recommendation
> in bold, then batches of bite-sized tasks with a **Checkpoint (run verify, report + wait)** at
> each batch boundary instead of a commit. Save it to
> `docs/plans/YYYY-MM-DD-ems-phase4-ops-readiness.md`. Then execute it **one batch at a time,
> inline in the main thread (no subagents — I have hit the session limit before), reporting and
> waiting at every checkpoint.**
>
> **Scope to plan** (§13 Phase 4 = metrics/alerts, dashboards, Helm finalization, perf @10 M rows,
> subscription seed translation):
>   1. **Close the §10 metric table.** Add `ems_events_consumed_total{topic,outcome}`,
>      `ems_subscription_verdicts_total{tenant,decision}`, `ems_registry_version_info{component}`,
>      and per-endpoint latency histograms for `/event`, `/run/status`, `/gate/groups`. Four metrics
>      already exist (`ems_events_dropped_total`, `ems_context_fetch_total`,
>      `ems_normalization_mutations_total`, `ems_outbox_pending_age_seconds`) — extend, don't
>      duplicate.
>   2. **Build `recon/ReconciliationSweep`** (the package exists but is empty — its `package-info`
>      states the contract): a scheduled backstop publishing `ems_consumer_lag{topic,partition}`
>      (with retention headroom), `ems_dlq_depth{topic}`, `ems_outbox_pending_age_seconds`, and
>      `ems_overdue_inflight_runs` (STARTED events with no terminal past a coarse global window).
>      It must be a loss backstop independent of the Phase-D heartbeat.
>   3. **Resolve the scrape-path contradiction before building on it:** `application.yml` exposes
>      the `prometheus` actuator endpoint but only `micrometer-registry-otlp` is on the classpath,
>      so that endpoint does not exist. Pick one (OTLP push vs Prometheus pull), make the pom and
>      the config agree, and record the decision. If it contradicts §10, record it as **amendment
>      A11** in `ems-design.md §0` first.
>   4. **Alerts + dashboards as code**, sourced from the §10 alert column (page: `ems_dlq_depth>0`
>      for 5 m, outbox age > 10 m, sustained consumer lag / lag-age approaching retention; warn:
>      drop-rate anomaly, nonzero normalization mutations, registry divergence > 30 m, overdue
>      in-flight runs, endpoint p95 regression). Ship them in the chart, not in a wiki.
>   5. **Finalize the Helm chart** `ems/deploy/helm/ems/` (today: Chart/deployment/service/hpa/
>      _helpers/values only): add the alert rules + scrape wiring from item 4, a PodDisruptionBudget,
>      ConfigMap-rendered `ems.auth.*` / Kafka / EDF / Airflow wiring, real Vault-agent annotations,
>      and a `helm lint` + `helm template` check wired into `.github/workflows/ems-ci.yml`. Keep
>      `dispatch.enabled=false` and `springProfile: shadow` as the defaults — that toggle is the
>      §11 cutover switch and must not flip by accident.
>   6. **Perf harness (§12):** seed 10 M synthetic events + 1 M contexts, assert the canonical §4.3
>      query p95 < 50 ms and `/run/status` p95 < 50 ms, plus an `EXPLAIN` assertion that the plan
>      uses `idx_event_task_id` / `idx_context_rep_freq_region` with **no seq scans**. This needs a
>      real Postgres, so build it as an opt-in, tagged harness that skips without Docker like every
>      other `*IT` — and **state plainly in the report that the perf exit gate cannot be signed off
>      on this box.** Do not fake a pass.
>   7. **Production `seed-0` subscription migration** (§11 stage 0 item 3): promote the reviewed
>      translation in `ems/src/test/resources/fixtures/subscriptions_seed0.json` (+ its provenance
>      file) into a real Flyway migration with `registry_version='seed-0'`. §14 item 3 (per-environment
>      row deltas, and confirmation that the map table rather than the `filter.post` property is what
>      `EventFilter.scala` evaluates) is **still unanswered** — so make the migration reviewable and
>      re-runnable, surface every assumption for my sign-off, and do not silently invent rows.
>      "Seed parity reviewed" is a human gate, not a green test.
>
> **Explicitly out of scope:** the §11 shadow/cutover stages (Phase 5) and the retention/archival
> DAG (Phase 6). Do not start them.
>
> Ground every ambiguous behavior against `old-ems/` and `old-orchestration/` (**verify against
> code, not docs**) and record any contradiction as a new amendment (A11+) in `ems-design.md §0`
> **before** building on it. Do not re-open closed design decisions. Update
> `docs/ems-technical-specification.md` and `docs/ems-user-guide.md` wherever this phase falsifies
> them (Phase 3 did, and they were refreshed). Update memory at the phase boundary. No git.
