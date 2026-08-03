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

## Amendments recorded so far — **A1–A18**

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
- **A11–A18** were added during Phase 4. The four that constrain future work:
  - **A11** — observability is **Prometheus pull** (`/actuator/prometheus` + `ServiceMonitor`), not
    OTLP push. The alert set ships as a `PrometheusRule` **in the Helm chart**.
  - **A12** — the legacy `filter.post` *property* and the `post_filter_control_dag_map` *table* are
    two different inputs (admission vs routing). Therefore **`PERSIST ⊇ FORWARD` is a hard
    correctness requirement** of `seed-0`, not a CI nicety.
  - **A15 + A18** — subscription CEL evaluates against a **case-folded, hyphen-aliased matching
    view** (`subscription/MatchView`). Rule dialect is *one* spelling: lowercase paths, lowercase
    literals, plain `==`/`&&`/`startsWith`. No `.lowerAscii()`, no `has()` guards, no either-spelling
    branches — because **DAG authors maintain these rules**. A18 makes `run-category` and
    `runCategory` one name and **widens matching vs legacy** (an expected shadow-parity diff).
  - **A16** — the §12 `EXPLAIN` criterion names two indexes the canonical §4.3 query cannot reach
    under A10. The perf harness asserts an index only where one is reachable and records the
    unqualified shape **without a budget**. A10 was **not** re-opened.
  - **A14, narrowed by A17** — the FORWARD half of the seed is an *interpretation*: the map-table
    condition grammar was derived from two consistent rows (`$.<prefix>` + `.key:value` pairs,
    comma-separated groups, all ANDed), but `parseCondition` is still absent from `old-ems/`. It
    stays a **human sign-off item**.

## DONE — EMS Phases 1–4 (ems-design §13)

`mvn -B -ntp -f ems/pom.xml verify` is **green locally**: **207 unit tests pass, 99 `*IT`s skip**
(JDK 17 + Maven 3.9.12 are on this Windows box). `bash ems/deploy/helm/check.sh` also **passes** —
`helm` v3.2.4 is installed at `/c/VMWorkspace/helm3.2.4/helm`.

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
- **Phase 4 — Ops readiness.** Plan:
  [`docs/plans/2026-07-28-ems-phase4-ops-readiness.md`](docs/plans/2026-07-28-ems-phase4-ops-readiness.md);
  batches 0, A–I all executed and green.
  - **§10 metric table closed.** Every row now has a real emitter: `ems_events_consumed_total`,
    `ems_subscription_verdicts_total`, `ems_registry_version` (`RegistryVersionMetrics`), and
    histogram buckets for `/event` `/run/status` `/gate/groups` (`config/MetricsConfig` — buckets,
    **not** client-side percentiles, so `histogram_quantile` works fleet-wide).
  - **`recon/ReconciliationSweep` + `ConsumerLagProbe` built.** Publishes `ems_dlq_depth`,
    `ems_outbox_pending_age_seconds`, `ems_overdue_inflight_runs`, `ems_consumer_lag`,
    `ems_consumer_retention_headroom_records`. **Deliberately ungated by consumer/dispatch state** —
    it must report on a pod doing nothing. Note `ems_outbox_pending_age_seconds` **moved off
    `OutboxDispatcher`**: that bean doesn't exist in `shadow`, which is the one profile where the
    backlog grows by design.
  - **Alerts + dashboard as code.** 11 rules in `deploy/helm/ems/templates/prometheusrule.yaml`
    (groups `ems.page` ×4, `ems.warn` ×7) + a Grafana dashboard ConfigMap. `AlertRuleCoverageTest`
    ties rule ↔ metric-literal-in-`src/main/java` ↔ user-guide runbook heading, so a typo'd metric
    name can't ship silently.
  - **Helm chart finalized** — ConfigMap, PDB, ServiceMonitor, PrometheusRule, dashboard, Vault
    file-injection, config checksum. `check.sh` = 16 assertions, runs green. Two real defects fixed:
    the old `values.yaml` `env:` keys bound to **non-existent** properties (Spring **removes hyphens**
    — `EMS_EDF_BASE_URL` ≠ `ems.edf.base-url`; use `EMS_EDF_BASEURL`), so pods would have silently
    used `localhost`; those settings are now `required` so the chart refuses to render without them.
  - **Perf harness built** (`perf/PerfSeeder`, `perf/QueryPerfIT`, `-Pperf`, `workflow_dispatch` CI
    job) — **never executed**, see gates below.
  - **`seed-0` promoted to a real migration**: `db/seed/V6__subscription_seed0.sql`, 16 upserts on
    `(tenant_id, stage, rule_name)`, `registry_version='seed-0'`. Loaded via a **separate Flyway
    location** listed only by `shadow`/`live`/`azure` — `default`/`local` stay schema-only.
    Sign-off register: [`docs/ems-seed0-assumptions.md`](docs/ems-seed0-assumptions.md).
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

### The open gates carried forward

1. **No `*IT` has ever run for real.** Testcontainers can't reach Docker on this machine (engine on
   the `dockerDesktopLinuxEngine` pipe; docker-java's probe 400s; the CLI works, Testcontainers
   doesn't). All 99 ITs are `@Testcontainers(disabledWithoutDocker = true)` → they compile-and-skip
   locally and **must run green in CI**. Do not burn time re-diagnosing the local Docker pipe — that
   path is a dead end here. **Because there is no git, CI is inert**; this gate has been deliberately
   deferred since Phase 2 ("settle the CI run later").
2. 🔴 **The §12 perf gate is UNEXECUTED, so the §13 Phase-4 exit criterion is NOT met.** The harness
   exists and is wired (`mvn verify -Pperf`; a `workflow_dispatch` CI job uploading
   `target/perf-report.txt`) but has never run once — it needs Docker *and* CI. **Every "< 50 ms
   p95" statement in the repo is an unmeasured design target.** Do not let it be read as a pass.
3. 🔴 **`seed-0` parity is awaiting human sign-off** — a human gate, not a green test.
   [`docs/ems-seed0-assumptions.md`](docs/ems-seed0-assumptions.md) holds 12 numbered assumptions:
   **1 signed off, 1 void (ASSUMPTION-4, source corrected), 10 open.** Two are known behaviour
   changes vs legacy that **must be declared as expected diffs before the shadow run**:
   - **ASSUMPTION-1** (`MERYVAL` → `merival`, signed off): legacy never forwarded MERIVAL ingestion
     events to `orchestration_control_dag_capital` because the typo matched nothing. EMS will —
     **the capital DAG must absorb the extra runs.**
   - **ASSUMPTION-9** (A18): a context spelling `runCategory` now matches `cap_data_update.FRCA_CURATION`
     where legacy required `run-category`.
   - Worth deciding early: **ASSUMPTION-5/6** — `PLATFORM` as owner and the **seven invented PERSIST
     rule names**, which land permanently in `routing_decision.rule_name`. Cheap to change now.
4. **Still blocked on external inputs (ems-design §14):** EDF Context REST contract (item 2),
   per-environment subscription seed deltas (item 3 — now expressible as *which Flyway locations a
   profile lists*), normalization value inventory (item 8), `routing_decision` retention + topic
   volumes (items 6, 10).
5. **No dispatch-outcome metric.** §10 has no counter for Airflow trigger success/failure by status;
   delivery health is inferred from `ems_outbox_pending_age_seconds` + `last_error`. Only becomes
   visible once `dispatch.enabled=true` — i.e. Phase 5.

## NEXT — EMS Phase 5 (Shadow + cutover), ems-design §11

**Scope:** run EMS alongside the legacy Scala service on live traffic, prove parity, then flip.
**Exit criterion:** the §11 stage gates, ending in the big-bang route flip with a rollback path.

**Phase 5 cannot start on this box.** It needs live Kafka traffic, a real Postgres, a deployed
chart and the legacy service running for comparison — none of which exist here. What *can* be done
without those is preparation: the parity-diff tooling, the expected-diff register, the stage
checklists and the rollback drill script.

Three things must be true before stage 1 begins, and none is true today:

1. **The perf gate has actually run** (gate 2 above). Phase 4's exit criterion is still open.
2. **The seed assumptions are signed off** (gate 3 above), and ASSUMPTION-1 + ASSUMPTION-9 are
   written into the parity report as *expected* diffs **in advance**. If they surface as surprises
   during the shadow comparison they will read as EMS bugs and cost a day each.
3. **CI has run green at least once** — which needs a git repository.

Carry into the plan: `ems.dispatch.enabled=false` and `springProfile: shadow` are the defaults and
**the flip is the cutover switch** (A2). The Helm chart already gates `EmsOutboxBacklogStale` on
`dispatch.enabled`, so alerting arms itself correctly when the flip happens — verify that rather
than re-deriving it.

## Environment notes

- Windows 11, primary shell PowerShell 5.1; a Bash (Git Bash) tool is also available. Working dir
  `d:\GitRepo\Orchestration-Solution`.
- JDK 17 (Liberica) + Maven 3.9.12 present. Build: `mvn -B -ntp -f ems/pom.xml verify`.
- Docker CLI works but Testcontainers does not (see above) — keep ITs `disabledWithoutDocker`.
- Maven output is large: redirect it to a file and grep the file. Piping it through `Select-String`
  has broken the pipe and returned a misleading exit 255 on a build that actually succeeded.

---

## Prompt for the next session

> Continue the Orchestration platform modernization. Read `SESSION_HANDOFF.md` first, then
> `ems-design.md §0` (amendments **A1–A18**), §11 (shadow + cutover), §13 (the phase board), and
> `docs/ems-seed0-assumptions.md`.
>
> **State:** EMS Phases 1–4 are **code-complete**. `mvn -B -ntp -f ems/pom.xml verify` is green
> locally — **207 unit tests pass, 99 `*IT`s skip** (no Docker) — and `bash ems/deploy/helm/check.sh`
> passes (`helm` v3.2.4 is installed). **Do not try to fix local Docker**; that is a known dead end,
> ITs are CI-only. There is **no git** — do not `git init`, do not commit; `mvn verify` is the only
> gate.
>
> **Read these three gates before proposing anything, because they change what is worth doing next:**
>   1. **The §12 perf gate has never been executed**, so the §13 **Phase-4 exit criterion is not met**.
>      The harness is built and wired (`mvn verify -Pperf`, plus a `workflow_dispatch` CI job) but
>      needs Docker *and* a git repo. Every "< 50 ms p95" claim in the repo is an unmeasured target.
>   2. **`seed-0` parity awaits human sign-off** — 12 assumptions in `docs/ems-seed0-assumptions.md`,
>      1 signed off, 1 void, **10 open**. This is a human gate, not a test.
>   3. **No CI job has ever run and no `*IT` has ever been observed green**, because there is no git.
>
> **So decide with me first, do not assume:** Phase 5 (§11 shadow + cutover) **cannot run on this
> box** — it needs live Kafka, a real Postgres, a deployed chart and the legacy service running for
> comparison. The genuinely available options are (a) `git init` + a first CI run, which unblocks
> gates 1 and 3 at once; (b) work the seed sign-off with me interactively; (c) build the Phase-5
> *preparation* that needs no live environment — parity-diff tooling, the expected-diff register,
> stage checklists, the rollback drill; or (d) start Phase 6 (retention/archival DAG), which is
> independent. **Ask which, with your recommendation in bold, before writing a plan.**
>
> When we do pick a phase: **first write the plan and wait for my approval**, in the same shape as
> `docs/plans/2026-07-28-ems-phase4-ops-readiness.md` — goal/architecture header, explicit scope
> guard, grounding checkpoints, testing-strategy table, open micro-decisions with your
> recommendation in bold, then batches of bite-sized tasks with a **Checkpoint (run verify, report +
> wait)** at each boundary instead of a commit. Save it to `docs/plans/YYYY-MM-DD-<name>.md`. Then
> execute it **one batch at a time, inline in the main thread (no subagents — I have hit the session
> limit before), reporting and waiting at every checkpoint.**
>
> **Two things must not flip by accident:** `ems.dispatch.enabled=false` and `springProfile: shadow`
> are the §11 cutover switch. And if Phase 5 does start, **ASSUMPTION-1 and ASSUMPTION-9 must be
> written into the parity report as *expected* diffs in advance** — legacy never forwarded MERIVAL
> ingestion events to the capital DAG (typo), and A18 now matches `runCategory` where legacy needed
> `run-category`. Surfacing them as surprises mid-comparison will read as EMS bugs.
>
> Ground every ambiguous behavior against `old-ems/` and `old-orchestration/` (**verify against
> code, not docs**) and record any contradiction as a new amendment (**A19+**) in `ems-design.md §0`
> **before** building on it. Do not re-open closed design decisions. Update
> `docs/ems-technical-specification.md` and `docs/ems-user-guide.md` wherever the work falsifies
> them. Update memory at the phase boundary. No git unless I say so.
