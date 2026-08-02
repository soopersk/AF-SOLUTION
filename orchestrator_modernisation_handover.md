# Orchestrator Modernisation — Session Handover Document

> **Purpose:** Complete context for continuing this work in a new session. Contains the full problem statement, current architecture discovery findings, redesign decisions, and component-level enhancement plans with enough detail to resume implementation planning without re-reading source code.

---

## 0. Quick Reference

| Item | Value |
|---|---|
| System name | Regulatory Capital Calculator Orchestrator |
| Owner team | Capital Engineering / Orchestration Platform |
| Primary driver for change | Architectural inefficiency — skip-churn waste + fragile state, not correctness failures |
| System scale | Moderate: 20–100 concurrent calculator runs at peak |
| System constraint | **Correctness/efficiency-bound**, not throughput-bound — SLA risk matters more than raw speed |
| External fixed components | **MEG** (Databricks compute, black-box internals) · **EDF** (third-party event bus, transport only) |
| Components we own & can change | `event-orchestration` (Scala/Spring Boot) · `orchestration` (Python Airflow framework lib) · `orchestration-dags` (Python DAG definitions) · PostgreSQL schema |
| Target Airflow version | 2.10.5 → **3.3.x** |
| Documents produced so far | Architecture Modernisation Strategy (.docx, 12pp) · Executive Enhancement Plan slides (v1, v2, themed) |

---

## 1. Current System — What It Is

### 1.1 The Three Components

#### A) `event-orchestration` — Scala / Spring Boot microservice

The **ingestion and routing layer**. Lives between the Kafka event stream and Airflow.

- **Stack:** Scala 2.13.18, JDK 17, Spring Boot 3.5.7, Spring Kafka 3.3.11, Kafka client 3.9.2, Confluent JSON Schema Serializer 7.9.0, PostgreSQL JDBC 42.7.3 with HikariCP (max 40 connections), Apache HC5, Scaffeine 5.3.0 cache
- **What it does:**
  1. Consumes `EventResponse` events from EDF via Kafka (`SASL_SSL/PLAIN`, `read_committed`, manual ack)
  2. Calls the external **EDF Context Service** via REST to enrich each event with a `contextId` (h3Region, frequency, calcType, runType, companyCode, etc.)
  3. Persists raw event JSON + context JSON into PostgreSQL JSONB tables (`event` and `context`)
  4. Reads `post_filter_control_dag_map` DB table (Tier 1 routing) to find the target Control DAG
  5. Fires a REST `POST` to Airflow (`/dags/{controlDagId}/dagRuns`) to trigger the Control DAG
  6. Exposes a `GET /event` query API that Airflow deferrable sensors poll to detect calculator completion
- **Config:** Loaded at runtime from `system_properties` DB table via `DbConfigurationPostProcessor`
- **Deployment:** Docker on `openjdk:17-jdk-slim`; Helm with HPA 3–10 pods (CPU 50% / mem 80%), RollingUpdate, Istio + Vault + Workload Identity

**Key internal classes:**
- `EventListener.scala` — `@KafkaListener` entry point
- `EventFilter.scala` — reads `post_filter_control_dag_map`, decides which Control DAG to trigger
- `DatabaseEventRepository.scala` — `JdbcTemplate`, upserts with `ON CONFLICT DO NOTHING`
- `CachedContextEventRepository.scala` — Scaffeine cache (24h TTL, 10k entries) for context lookups
- `EventSender.scala` — fires REST call to Airflow
- `EventController.scala` — exposes `GET /event`, `/context`, `/parentcontext`, `/childcontext`
- `ExponentialBackoffRetryStrategy.scala` — 5 retries, 30s–600s, jitter, on 429/5xx

---

#### B) `orchestration` — Python framework library (v5.3.0)

The **reusable Airflow building blocks**. A Poetry-packaged library imported by `orchestration-dags`.

- **Stack:** Python 3.10–3.13, Airflow 2.10.5, deps: `cicommon 3.0.1`, `msal`, `azure-identity`, `opencensus-ext-azure`, `requests`
- **What it does:** Provides all shared DAG primitives — condition evaluation, task groups, sensors, triggers, EDF integration, auth

**Key internal modules:**
- `common/dag_utils.py` → `create_control_tasks()` — builds one `DagTriggerWithConditionTaskGroup` per registered DAG (~85). This is the **root cause** of skip-churn.
- `common/trigger_conditions.py` → `TriggerCondition` namedtuple, `get_trigger_condition()` parses `"TRIGGER_CONDITION[REGION]"` strings (e.g. `SOURCE.MERIVAL.AMER`, `CALC.CAPITALCALC`)
- `common/base_calculator.py`, `common/generic_calculator.py` → `SimpleCalculator`, `TaskTriggeringCalculator`, `DisplayCalcRunDetails`
- `common/group.py` → `CalculatorTaskGroup`, `DatasetMegEventCriteriaTaskGroup`, dynamic mapped task groups
- `sensors/http_deferrable_completion_sensor.py` → `CalcRunCompletionSensor`, defers to `Mv1HttpTrigger`, polls `/FRCA/event-orchestration/event`, 300s interval, 3h timeout
- `common/edf_util.py` → `trigger_calculator_task()`, `create_edf_context()`, `publish_edf_event()` with MSAL Bearer auth
- `common/af_utils.py` → Airflow Variable get/set helpers (used for portfolio fan-in tracking — a core fragility)
- `trigger_dag()` → `POST /dags/{dag_id}/dagRuns`, idempotent `dag_run_id = orch_sha1(dag_id+conf)[:16]`, 409 treated as success

**Unused but relevant:** `evaluate_dag_start_criteria` + `DagTriggerer` also exist in `dag_utils.py` — these evaluate all conditions in plain Python and return only the matching DAG list **without creating any skipped tasks**. This is the seed of the redesign.

---

#### C) `orchestration-dags` — Python DAG definitions (v5.1.3)

The **DAG inventory**. Packaged via Maven (`pom.xml` + assembly descriptor) into a `tar.gz` for Nexus, then deployed to Airflow. Depends on framework `orchestration 5.2.4` (note: **version skew** — framework is 5.3.0 but DAGs depend on 5.2.4).

**DAG inventory (85 registered):**
- 2 control/meta DAGs: `orchestration_control_dag.py`, `orchestration_control_dag_capital.py`
- 20 regional B3F DAGs (daily + monthly, all H3 regions)
- IHC: `usrg_ihc_dag`
- 7 portfolio DAGs: `portfolio_daily_dag`, `portfolio_fed`, `groupreporting_*`
- Floors: `output_floor_monthly`, `sectoral_floor_monthly`
- Others: consensus, validations, LRD SFT, Gemini, market-risk RWA, SACVA, SCCL, adjustments, SACCR-QA, archive, snapshot, opcontrol, modelled-exposure, lenses-controls

**Routing registry:** `dags/dag_trigger_criteria_map.json` maps `dag_id → trigger condition(s)` (e.g. `SOURCE.MEG.AMER`, `CALC.CAPITALCALC`, `DATASET.OUTPUTPOSTING`). Custom condition: `EventRateCriteria` in `dags/logic/capital/custom_trigger_conditions.py`.

**Calculator DAG shape** (e.g. `b3f/daily/amer_d_b3f_dag.py`):
```
calc_start
  → CAPITALCALC_CALC group
      → OTHER_PRE_CONDITIONS          ← re-checks frequency, region, etc. — causes Level 2 skip
          → CALC_INIT
              → MAPPED_GROUP
                  → CREATE_CONTEXT
                  → PUBLISH_EDF_EVENT
                  → DISPLAY
                  → CHECK_STATUS      ← deferrable sensor polling GET /event
      → RESULT
  → calc_end
```

**Portfolio DAG extras:** adds `CHECK_IS_CAPITALCALC_RUNS_READY_CONDITION` fan-in pre-condition (reads from mutable Airflow Variables), `max_active_runs=1`.

**Config priority:** DAG-run `conf` → Airflow Variables (portfolio tracking, floors, BOD) → `@dag` params → framework constants

---

### 1.2 End-to-End Event Flow (Current State)

```
MEG (Databricks)
  → emits lifecycle events (scheduled/started/finished/failed) to EDF
  → EDF delivers via Kafka to event-orchestration

event-orchestration
  → enriches event with context via EDF Context Service REST call
  → persists event + context as JSONB to PostgreSQL
  → reads post_filter_control_dag_map → determines Control DAG
  → POST /dags/{controlDag}/dagRuns to Airflow

Airflow Control DAG (per event run)
  → materialises ~85 DagTriggerWithConditionTaskGroup instances
  → each group evaluates its condition → all-but-one raise AirflowSkipException
  → matching group fires trigger_dag() → calculator DAG triggered

Calculator DAG
  → runs OTHER_PRE_CONDITIONS (frequency, region, run-type checks)
  → if not ready → AirflowSkipException → whole DAG run skipped
  → if ready → CREATE_CONTEXT → PUBLISH_EDF_EVENT → MEG starts running
  → deferrable sensor polls GET /event every 300s up to 3h
  → on COMPLETED event → success/fail downstream
```

**Enriched event payload shape:**
```json
{
  "event": {
    "source": "...", "businessDate": "...", "STATE": "...",
    "pipelineId": "...", "taskId": "...", "taskEventType": "...",
    "updateType": "...", "megdpEventType": "...",
    "additionalData": { "type": "...", "datasetId": "..." }
  },
  "context": {
    "data": {
      "frequency": "...", "reporting-date": "...", "h3Region": "...",
      "region": "...", "run-category": "...", "runType": "...",
      "companyCode": "...", "calcType": "...", "batchType": "..."
    }
  }
}
```

---

## 2. The Problems — Root Causes and Business Impact

### Problem 1 — Scheduler Skip-Churn (PRIMARY DRIVER)

**Root cause:** The Control DAG uses `create_control_tasks()` to statically build one `DagTriggerWithConditionTaskGroup` per registered DAG (~85). Per incoming event, the scheduler must materialise, queue, run, and state-track **~150–300+ task instances** — all to produce ~1 useful trigger. Non-matching tasks raise `AirflowSkipException`, which is still a full task lifecycle in Airflow's metadata DB.

**Level 1 — Control DAG:** ~85 task groups × 2–4 tasks each = 150–300+ task instances scheduled per event. Only 1–2 are meaningful.

**Level 2 — Calculator self-skip:** Calculator DAGs triggered by the Control DAG immediately re-run `OTHER_PRE_CONDITIONS` (frequency, region, run-type, company-code checks). If not ready, the entire DAG run is skipped — another set of wasted Airflow metadata entries.

**Worst case — Portfolio fan-in:** Every regional completion event (e.g. AMER B3F DAILY) triggers `portfolio_daily_dag`. The DAG checks if all regional dependencies are ready via mutable Airflow Variables. It self-skips 9 out of 10 times, waiting for the last region. This is 9× wasted DAG lifecycle events per portfolio run.

**Business impact:**
- Airflow metadata DB write volume: ~150–300 state writes per event (scheduled → queued → running → skipped, repeated for each wasted task)
- Scheduler queue depth grows under load → p99 time-to-trigger degrades under busy periods
- Infrastructure cost: wasted scheduler CPU/memory on tasks that will never do real work
- SLA risk: degraded scheduler response directly delays calculator starts on regulatory reporting deadlines

---

### Problem 2 — Event Store Query Latency

**Root cause:** The `event` and `context` PostgreSQL tables store raw JSONB only. There are no top-level indexed columns for high-frequency query predicates (`business_date`, `calc_type`, `h3_region`, `task_event_type`, `successful`). No table partitioning. The Scaffeine cache is scoped only to context lookups (24h TTL, 10k entries) — not to the sensor polling query pattern.

**Business impact:**
- `GET /event` queries from deferrable sensors scan unindexed JSONB → **query latency in tens of minutes** under concurrent load
- HikariCP connection pool cap is 40 connections → simultaneous sensor polling from 20–100 concurrent DAG runs exhausts the pool
- Pool exhaustion → sensor timeouts → false-negative "not found" responses → sensors sleep again → calculators appear stuck
- Under worst case: entire batch appears stalled with no alerts

---

### Problem 3 — Dual Routing Registry (Two Sources of Truth)

**Root cause:** Routing logic is split across two systems that must be kept in sync manually:
- **Tier 1 (Scala):** `post_filter_control_dag_map` DB table in `event-orchestration` — determines which Control DAG to call
- **Tier 2 (Python):** `dags/dag_trigger_criteria_map.json` flat file in `orchestration-dags` — determines which Calculator DAG to trigger

**Business impact:**
- Every routing rule change requires coordinated deployment across two codebases and two CI pipelines
- Mis-synchronisation between Tier 1 and Tier 2 causes **silent routing failures** — a calculator simply does not run with no observable alert
- No structured audit trail: "why was DAG X not triggered for event Y?" requires manual log trawling

---

### Problem 4 — Fragile Portfolio Fan-In State

**Root cause:** Portfolio DAG readiness (waiting for all regional B3F/IHC/SACVA completions before running) is tracked by **accumulating events into mutable Airflow Variables** via `common/af_utils.py`. The accumulator is built by each portfolio DAG run reading and writing the Variable.

**Failure modes:**
- A single lost Kafka event permanently corrupts the accumulator state for that business date — no automated recovery
- Concurrent Airflow Variable reads/writes carry race conditions under concurrent DAG runs
- The 3h deferrable sensor timeout is the **only signal** that something is wrong — no proactive alerting

---

### Problem 5 — Framework/DAG Version Skew

**Root cause:** `orchestration` framework is at v5.3.0 but `orchestration-dags` depends on `5.2.4`. Two completion schemes coexist: legacy `STATE` field vs current MEG `successful` flag. These need to be reconciled before the Airflow 3.x upgrade.

---

## 3. Redesign Vision

### 3.1 Core Architectural Shift

**From:** Static Imperative Fan-Out
> The system routes by exhaustive enumeration (materialise all 85 task groups) and exception-based elimination (skip all-but-one with `AirflowSkipException`). State is accumulated in mutable Airflow Variables.

**To:** Declarative Dispatcher with Reconciled Queries
> Routing logic is expressed as compound CEL predicates in a versioned registry. No DAG or task instance is ever materialised unless its predicate evaluates `true` against the incoming event. Fan-in readiness is resolved by a deterministic query against the durable event store — not accumulated mutable state.

### 3.2 Core Principle

> **Evaluate everything before materialising anything.** A calculator DAG is triggered only when it will actually execute — eliminating both the Level 1 control-DAG skip-churn and the Level 2 calculator self-skip in a single architectural intervention.

### 3.3 Target Flow

```
MEG → EDF → Kafka
  → event-orchestration [L0]
      → enrich + persist to INDEXED event store
      → route to tenant Dispatcher DAG (ONE call per tenant, never per-calculator)

Dispatcher DAG [L1 — new, replaces Control DAG]
  → single evaluate_and_dispatch task
  → loads compiled CEL rule cache (in-process, no DB round-trip)
  → iterates all rules in-memory
  → for rules with trigger.when = true AND no await clause:
      → trigger_dag() immediately (idempotent)
  → for rules with trigger.when = true AND await.all_of clause:
      → run reconciled readiness query against event store
      → if all dependencies COMPLETED: trigger_dag()
      → else: no-op (next event will re-evaluate)

Calculator DAG [L2 — simplified]
  → NO OTHER_PRE_CONDITIONS (all absorbed into CEL trigger.when)
  → starts directly at CALC_INIT
  → CREATE_CONTEXT → PUBLISH_EDF_EVENT → deferrable sensor

Reconciliation Heartbeat [safety net]
  → runs every 30–60 min during business window
  → re-evaluates all open business dates against await.all_of rules
  → triggers any portfolios whose dependencies completed out-of-order
```

### 3.4 CEL Registry Schema (illustrative)

```yaml
# Direct trigger (no fan-in)
dag_id: amer_d_b3f_dag
version: "2.1"
trigger:
  when: >-
    event.source == "MEG" &&
    event.taskEventType == "COMPLETED" &&
    context.h3Region == "AMER" &&
    context.frequency == "DAILY" &&
    context.calcType == "B3F"
  await: null

# Portfolio fan-in
dag_id: portfolio_daily_dag
trigger:
  when: >-
    event.taskEventType == "COMPLETED" &&
    context.calcType in ["B3F","IHC","SACVA"]
  await:
    all_of:
      - { calcType: B3F,   regions: [AMER, EMEA, APAC], frequency: DAILY }
      - { calcType: IHC,   regions: [AMER],              frequency: DAILY }
      - { calcType: SACVA, regions: [AMER, EMEA],        frequency: DAILY }
```

---

## 4. Component Enhancement Plans

### 4.1 Event Micro Service (`event-orchestration`) — Enhancement Plan

#### 4.1.1 Schema Redesign & Database Optimisation

**Problem:** JSONB-only tables with no indexed top-level columns. Queries by `business_date`, `task_event_type`, `calc_type`, `h3_region` perform full JSONB scans. No partitioning.

**Target changes:**

1. **Add materialized top-level columns** to the `event` table via additive `ALTER TABLE ... ADD COLUMN`:
   ```sql
   ALTER TABLE event ADD COLUMN business_date   DATE         GENERATED ALWAYS AS ((json->>'businessDate')::DATE) STORED;
   ALTER TABLE event ADD COLUMN task_event_type VARCHAR(32)  GENERATED ALWAYS AS (json->>'taskEventType') STORED;
   ALTER TABLE event ADD COLUMN calc_type       VARCHAR(32)  GENERATED ALWAYS AS (json->'context'->'data'->>'calcType') STORED;
   ALTER TABLE event ADD COLUMN h3_region       VARCHAR(16)  GENERATED ALWAYS AS (json->'context'->'data'->>'h3Region') STORED;
   ALTER TABLE event ADD COLUMN frequency       VARCHAR(16)  GENERATED ALWAYS AS (json->'context'->'data'->>'frequency') STORED;
   ALTER TABLE event ADD COLUMN successful      BOOLEAN      GENERATED ALWAYS AS ((json->>'successful')::BOOLEAN) STORED;
   ```
   > Or via explicit backfill + trigger if generated column expressions are not suitable for the JSONB path format in prod PostgreSQL version.

2. **Range partition on `business_date`** using `pg_partman` or equivalent:
   - Monthly partitions (or weekly during BAU window evaluation)
   - Sensor queries and readiness queries for a given `business_date` prune to a single partition
   - Attach existing data to `DEFAULT` partition; migrate during low-volume window (weekend)

3. **Composite covering index** (created `CONCURRENTLY` — no table lock, zero downtime):
   ```sql
   CREATE INDEX CONCURRENTLY idx_event_routing
   ON event (business_date, task_event_type, calc_type, h3_region);
   ```
   This index covers both the deferrable sensor polling query pattern and the dispatcher readiness query.

4. **Validation gate:** Run `EXPLAIN ANALYZE` on representative sensor poll queries before and after. Gate Month 2 work on achieving **p99 < 100ms** for all event store queries.

#### 4.1.2 Introduce Caching

**Problem:** The existing Scaffeine cache in `CachedContextEventRepository.scala` covers context enrichment lookups only (24h TTL, 10k entries). There is no cache for the sensor polling query (`GET /event` → "is this run COMPLETED?") or the dispatcher readiness query (fan-in state per business date).

**Target changes:**

1. **Extend `CachedContextEventRepository`** (or introduce a new `ReadinessQueryCache`):
   - Key: `(business_date, rule_id)` → readiness result (boolean + last-evaluated timestamp)
   - TTL: 60–120 seconds
   - Invalidation: on any new `COMPLETED` event ingested for the matching `business_date`, evict the affected entries
   - This prevents the dispatcher from issuing a DB query on every incoming event when the readiness state has not changed

2. **Sensor poll response caching** (optional, lower priority):
   - `GET /event?taskId=X&taskEventType=COMPLETED` — once a COMPLETED event exists, the answer is immutable
   - Cache key: `(taskId, taskEventType=COMPLETED)` → 200 + event body
   - TTL: until business date rolls over (or explicit eviction on event ingestion)
   - **Benefit:** Eliminates repeated DB reads for the same sensor polling the same completed event across 300s sleep cycles

3. **Cache size calibration:** With 20–100 concurrent runs and 85 DAGs, the 10k entry cap is adequate. Document the calculation and add an alert if cache eviction rate rises above a threshold (Micrometer/Actuator metric).

#### 4.1.3 Housekeeping

**Problem:** No automated partition retention, no dead-letter handling for unprocessable Kafka events, no backfill tooling for when migrations add new columns to existing data.

**Target changes:**

1. **Automated partition retention policy:**
   - Use `pg_partman` retention policies to drop partitions older than N months (configure per business requirement — regulatory data retention may require 7 years, so "drop" means move to cold storage or archive table, not delete)
   - Alert on partition management failures

2. **Kafka Dead-Letter Queue (DLQ):**
   - Implement DLQ in `KafkaErrorHandlingConfig.scala`: events that fail deserialization or throw unhandled exceptions after max retries → routed to a `${edf.topic}.dlq` topic with full payload + error metadata + original offset
   - Add Prometheus alert: `dlq_depth > 0` → PagerDuty
   - Add a replay tool: allows re-processing DLQ messages after the root cause is fixed without Kafka replay

3. **Backfill validation tooling:**
   - A one-off `MigrationValidator` tool that queries a sample of events post-column-addition and confirms generated column values match what `json->>'...'` returns directly
   - Run as a CI job after each schema migration before promoting to production

4. **Routing to new Dispatcher (replaces Tier 1 `post_filter_control_dag_map` read):**
   - Replace `EventFilter.scala`'s DB table read with a static route to the tenant dispatcher DAG (the CEL registry takes over all Tier 2 routing decisions)
   - `post_filter_control_dag_map` → renamed to `_deprecated`, read removed from `EventFilter.scala`
   - Configuration of dispatcher DAG ID moved to `system_properties`

---

### 4.2 Smart Router (Dispatcher DAG + CEL Registry) — Enhancement Plan

#### 4.2.1 CEL Rule Registry

**Problem:** `dag_trigger_criteria_map.json` is a flat `dag_id → "TRIGGER_CONDITION[REGION]"` string map. Cannot express compound predicates. Lives only in `orchestration-dags` Git repo as a flat file. No versioning, no validation pipeline, no audit trail.

**Target changes:**

1. **New registry format:** YAML files, one per DAG or one per tenant, stored in `orchestration-dags/dags/registry/`. Each entry declares:
   - `dag_id` — the calculator DAG to trigger
   - `version` — semantic version of the rule
   - `trigger.when` — CEL expression evaluated against the enriched event JSON (both `event.*` and `context.*` fields accessible)
   - `trigger.await` — optional `all_of` block specifying fan-in dependencies (replaces Airflow Variable accumulator for portfolio DAGs)

2. **CEL implementation:** Use `celpy` (Pure Python CEL, no JVM dependency). CEL expressions compiled once at Dispatcher DAG parse time → stored as executable ASTs in an in-process dict keyed by `rule_id + version_hash`. Per-event evaluation = dict lookup + CEL runtime evaluation (microsecond-scale, no DB round-trip).

3. **CI validation pipeline:**
   - On every push to `orchestration-dags`: parse all registry YAML files, compile CEL expressions, fail on syntax errors
   - Schema validation against the registry JSON schema
   - Semantic regression: a test suite of known `(event, context)` fixtures asserts that each rule produces the expected match/no-match result
   - **New rules require peer review by the DAG author of that calculator before merge**

4. **Migration from v1 to v2:**
   - Translate all 85 entries in `dag_trigger_criteria_map.json` into CEL YAML rules
   - Absorb `FrequencyCriteria`, `H3RegionCriteria`, `RunTypeCriteria`, `CompanyCodeCriteria` from `OTHER_PRE_CONDITIONS` into `trigger.when`
   - `EventRateCriteria` (custom, in `dags/logic/capital/custom_trigger_conditions.py`) → expressed as a CEL function or pre-computed field in the enriched event

#### 4.2.2 Dispatcher DAG (replaces Control DAG)

**Problem:** `create_control_tasks()` builds ~85 task groups per DAG run. Replacing it with a single Python function call.

**Target changes:**

1. **New `orchestration_dispatcher_dag.py`:**
   - A single `PythonOperator` task: `evaluate_and_dispatch`
   - Receives enriched event JSON from `dag_run.conf`
   - Loads CEL registry from compiled cache (populated at module import time)
   - Iterates all rules: evaluates `trigger.when` → if false, skip; if true + no `await`, call `trigger_dag()`; if true + `await`, call reconciled readiness query
   - Emits structured routing decision log for every evaluated rule: `{dag_id, matched: bool, reason, latency_ms}` to Airflow task logs and to `routing_audit_log` PostgreSQL table

2. **Zero skipped task instances:** The entire dispatch evaluation is a single Python function call producing O(1) Airflow task instance per dispatcher run, regardless of how many rules are evaluated.

3. **Idempotency:** Existing `trigger_dag()` mechanism preserved: `dag_run_id = orch_sha1(dag_id+conf)[:16]`, HTTP 409 treated as success.

4. **Pluggable trigger-source strategy (future-proof):** Registry entries can declare `trigger_source: condition_engine` (default, CEL) or `trigger_source: correlation_service` (opt-in, routes to external correlation service instead of CEL evaluation). Both converge on the same `trigger_dag()` call. No re-architecture needed to add the correlation service later.

#### 4.2.3 Massive Reduction in Scheduler Churn

**Quantified impact:**

| Metric | Current | Target |
|---|---|---|
| Airflow task instances per incoming event | ~150–300+ | 1 |
| Metadata DB state writes per event | ~150–300 | ~3–5 |
| Wasted DAG runs (portfolio) | 9 of 10 | 0 |
| Time-to-trigger p99 | Highly variable (tens of mins under load) | < 30s |

#### 4.2.4 Shadow Testing (Validation Before Cut-Over)

1. **Shadow mode harness:** Both the legacy Control DAG and the new Dispatcher DAG write their routing decisions to a `shadow_routing_comparison` PostgreSQL table.
2. **Nightly reconciliation job:** Diffs the two decision sets for the prior business day. Alerts on any divergence.
3. **Gate:** 100% routing agreement over **minimum 15 business days** (covering at least two weekly reporting cycles and one month-end close) required before cut-over is approved.
4. **Cut-over:** Once validated, `event-orchestration` routes all events to the Dispatcher DAG. Legacy Control DAG is **paused, not deleted** — kept as emergency fallback for 90 days.

#### 4.2.5 Reconciled Readiness Query (replaces Portfolio Fan-In Variables)

**Problem:** Mutable Airflow Variable accumulator is permanently corrupted by a single lost Kafka event.

**Target implementation in `event-orchestration`:**

```python
def is_portfolio_ready(business_date: str, rule: RegistryEntry) -> bool:
    """
    Returns True only if ALL dependencies in rule.await.all_of have
    a COMPLETED event in the event store for the given business_date.
    """
    for dep in rule.await_.all_of:
        count = db.query(
            """
            SELECT COUNT(DISTINCT h3_region)
            FROM event
            WHERE business_date = %s
              AND task_event_type = 'COMPLETED'
              AND calc_type = %s
              AND frequency = %s
              AND h3_region = ANY(%s)
              AND successful = true
            """,
            (business_date, dep.calc_type, dep.frequency, dep.regions)
        )
        if count < len(dep.regions):
            return False
    return True
```

**Properties:**
- **Idempotent:** Re-runs on every incoming COMPLETED event. No state to corrupt.
- **Crash-safe:** Reads durable PostgreSQL — immune to Airflow Variable loss, application restarts, Kafka redelivery
- **Self-healing via heartbeat:** A CronJob (`*/30 * * * 1-5` during business window) re-evaluates all open business dates. If a completion event was delivered late or out-of-order, the heartbeat detects readiness and triggers the portfolio DAG without operator intervention.
- **Observability:** Every readiness check is logged with `{business_date, rule_id, deps_ready, deps_pending}` → queryable via `routing_audit_log`

---

### 4.3 DAG Framework (`orchestration` + `orchestration-dags`) — Enhancement Plan

#### 4.3.1 Type-Safe Framework (Modern Python + Airflow 3.x)

**Problem:** Framework uses Python 3.10 minimum, Airflow 2.10.5. DAG config passed as freeform `dict` via Airflow Variables — no type safety, no IDE support, runtime errors only.

**Target changes:**

1. **Upgrade to Python 3.12+:** Required for Airflow 3.x and modern type annotation features
2. **Upgrade Airflow 2.10.5 → 3.3.x:** Significant changes in Airflow 3.x — see Section 4.3.4
3. **Typed config model:** Replace freeform Airflow Variable dicts with `dataclasses` or `pydantic` models:
   ```python
   @dataclass
   class CalcRunConfig:
       dag_id: str
       business_date: date
       calc_type: str
       h3_region: str
       frequency: Literal["DAILY", "MONTHLY", "WEEKLY"]
       run_type: str
       company_code: str | None = None
   ```
4. **Remove `common/af_utils.py` Variable accumulator pattern** — replace with the reconciled readiness query (see 4.2.5)
5. **Resolve version skew:** Standardize `orchestration-dags` dependency on `orchestration 5.3.0`. Remove dual-completion `STATE` legacy path — use `successful` flag only.

#### 4.3.2 Clean Authoring Model

**Problem:** DAG authors must understand `OTHER_PRE_CONDITIONS`, `DagTriggerWithConditionTaskGroup`, the `dag_trigger_criteria_map.json` format, and Airflow Variable fan-in state — four separate systems to configure one calculator DAG.

**Target changes:**

1. **Single-file DAG declaration:** A new DAG author needs to specify only:
   - Which CEL registry rule triggers this DAG (or author the rule inline)
   - Which `CalcRunConfig` fields they expect
   - The `CalculatorTaskGroup` they want to execute
   - Nothing else — no routing config, no pre-condition logic, no Variable management

2. **New base class:** `DeclarativeCalculatorDag` wrapping `@dag` + `CalculatorTaskGroup` with defaults:
   ```python
   class AmerDailyB3FDag(DeclarativeCalculatorDag):
       dag_id = "amer_d_b3f_dag"
       registry_rule = "dags/registry/b3f/amer_d_b3f.yaml"
       # No OTHER_PRE_CONDITIONS needed — absorbed into registry rule
   ```

3. **DAG author workflow:** Create registry YAML → create DAG file inheriting `DeclarativeCalculatorDag` → CI validates CEL syntax and DAG import → peer review → merge.

4. **Eliminate `create_control_tasks()`:** This function and `DagTriggerWithConditionTaskGroup` are removed from `common/dag_utils.py` once all DAGs migrate (or archived in a `_legacy` module during transition).

#### 4.3.3 Simplified DAGs

**Problem:** All 85 calculator DAGs contain `OTHER_PRE_CONDITIONS` task groups that duplicate logic already expressed in the routing registry. Portfolio DAGs contain `CHECK_IS_CAPITALCALC_RUNS_READY_CONDITION` that reads mutable Variables.

**Target changes:**

1. **Remove `OTHER_PRE_CONDITIONS` from all 85 calculator DAGs:**
   - `FrequencyCriteria` → absorbed into `trigger.when` CEL expression
   - `H3RegionCriteria` → absorbed into `trigger.when`
   - `RunTypeCriteria` → absorbed into `trigger.when`
   - `CompanyCodeCriteria` → absorbed into `trigger.when`
   - `CheckCalcRunCriteria` → absorbed into `trigger.when`
   - `EventRateCriteria` → expressed as CEL function

2. **New calculator DAG shape (target):**
   ```
   calc_start
     → CALC_INIT               ← starts immediately (no pre-condition tasks)
         → MAPPED_GROUP
             → CREATE_CONTEXT
             → PUBLISH_EDF_EVENT
             → DISPLAY
             → CHECK_STATUS    ← deferrable sensor
     → calc_end
   ```

3. **Portfolio DAG simplification:**
   - Remove `CHECK_IS_CAPITALCALC_RUNS_READY_CONDITION`
   - The Dispatcher handles readiness via `await.all_of` reconciled query
   - Portfolio DAG is only triggered when all regional dependencies are already confirmed COMPLETED → executes immediately with no pre-check

4. **Rollback path:** Framework library version pin. If a Calculator DAG has issues post-migration, rolling back the `orchestration` library version in `orchestration-dags/pyproject.toml` reverts all DAGs to the previous behaviour — no DAG file changes needed.

#### 4.3.4 Airflow 2.10.5 → 3.3.x Upgrade

**Why now:** Airflow 3.x introduces significant API changes that are best co-delivered with the DAG Framework redesign rather than as a separate project. Doing them together means DAGs are written once in Airflow 3.x style, not twice.

**Key Airflow 3.x changes to plan for:**

1. **Task SDK separation:** In Airflow 3.x, the Task SDK (`airflow.sdk`) is a separate lightweight package from the Airflow scheduler/webserver. DAG authors import from `airflow.sdk` — this needs updating in `orchestration` framework imports.

2. **DAG authoring API changes:** `@dag` decorator patterns remain broadly compatible but some `TaskGroup` and `BranchOperator` APIs have changed. Audit all `common/*.py` modules against the Airflow 3.3 changelog.

3. **Deferrable operators:** The `Triggerer` architecture is stable and unchanged. `CalcRunCompletionSensor` + `Mv1HttpTrigger` should be compatible with minor import path updates.

4. **REST API v2:** Airflow 3.x uses REST API v2 (`/api/v2/`). Both `event-orchestration`'s `EventSender.scala` (`POST /dags/{dag}/dagRuns`) and `orchestration`'s `trigger_dag()` must update endpoint paths. The payload shape is compatible.

5. **Metadata DB changes:** Airflow 3.x has schema migrations. The `alembic` upgrade must be run on the metadata DB as part of the deployment. Test in UAT first — non-reversible.

6. **Deployment:** The upgrade is co-delivered with the DAG Framework go-live (Production Month 4+). Airflow 3.3 is adopted in the Build phase (Months 1–2) for development, validated in UAT on Airflow 3.3, and deployed to production in the same rollout window as the DAG Framework.

7. **Rollback:** Airflow container image pinned and tagged pre-upgrade. If post-upgrade issues arise, the previous container image is redeployed in under 5 minutes. Metadata DB schema rollback is more complex — the pre-upgrade DB snapshot is the rollback artifact. Document the snapshot/restore procedure before go-live.

---

## 5. Delivery Roadmap Summary

### Phases

| Phase | Timing | Description | Risk |
|---|---|---|---|
| **Q1 — Build** | Months 1–2 | All three workstreams built and internally tested. Shadow mode active for Smart Router. Airflow 3.x SDK adopted for development. | Low — no production routing changes |
| **UAT** | Month 3 | All three workstreams validated in UAT. 15+ business days shadow run for Smart Router. Airflow 3.3 regression suite. Sign-off gate — no cut-over without explicit Chief Architect approval. | Low — no production impact |
| **Production** | Month 4+ | Full cut-over per component (not gradual traffic split). Each workstream deployed independently. Legacy Control DAG paused but kept. | Medium — managed by rehearsed rollback SOPs |

### Workstream Independence

Each of the three workstreams can be deployed and rolled back **independently**:

| Workstream | Rollback mechanism |
|---|---|
| Event Micro Service | Re-point queries to unpartitioned view via config flag. Additive DDL — no data loss possible. |
| Smart Router | Re-enable legacy Control DAG. Single `system_properties` DB row update — takes effect without redeploy. Legacy Control DAG retained 90 days post go-live. |
| DAG Framework | Pin `orchestration` library to v1 in `pyproject.toml`. One-line dependency change — no DAG file edits needed. |

### No Gradual Traffic Split

Production cut-over is **all-or-nothing per component**, not a percentage ramp. This was an explicit decision:
- The UAT sign-off gate with 15+ business days shadow run provides the same confidence that a percentage ramp would, without the operational complexity of running two routing paths simultaneously
- The single-DB-property rollback is instant if issues arise post cut-over

---

## 6. Key Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| CEL rule does not precisely reproduce legacy routing condition → calculator under/over-triggered | HIGH | Shadow mode with 100% agreement gate. Peer review by original DAG author. CI semantic test suite. |
| PostgreSQL partition migration causes write latency spike during backfill | MEDIUM | Execute during lowest-volume weekend window. `pg_partman` attach approach (no full table rewrite). Rollback: view abstraction re-points queries. |
| Reconciliation heartbeat triggers portfolio DAG for stale business date | MEDIUM | Heartbeat scoped to `business_date >= today - 2`. Idempotent `dag_run_id` prevents duplicate execution. |
| Airflow 3.x metadata DB schema migration is non-reversible | MEDIUM | Pre-upgrade DB snapshot is rollback artifact. Procedure documented and rehearsed before go-live. |
| Framework version skew between `orchestration 5.2.4` (DAGs) and `5.3.0` (framework) causes incompatibility during transition | LOW | Standardise on 5.3.0 in Month 1. CI matrix tests against both versions until DAGs migrate. |

---

## 7. KPIs — Current State vs Target State

| KPI | Current State | Target State | Improvement |
|---|---|---|---|
| Airflow task instances per incoming event | ~150–300+ (all-but-one skipped) | 1 (single dispatch task) | 95–99% reduction |
| Wasted DAG runs — portfolio fan-in | 9 of 10 runs wasted | 0 wasted runs | ~90% reduction |
| Metadata DB state writes per event | ~150–300 | ~3–5 | ~98% reduction |
| Event store query latency p99 (sensor poll) | Tens of minutes (unindexed JSONB scan) | < 100ms | 10–100× faster |
| Time-to-trigger p99 | Highly variable (degrades under load) | < 30s | Order-of-magnitude |
| Configuration change deployment cycle | Multi-system coordinated deploy (Scala + Python) | Single versioned registry push | 2 systems → 1 file |
| Routing failure observability | Opaque (no structured log, manual triage) | Fully auditable (routing_audit_log, 1-second lookup) | None → Full |
| Fan-in correctness | Event-accumulator (permanently corrupted by single lost event) | Reconciled query (immune to event loss, self-healing) | Fragile → Durable |
| Sources of routing truth | 2 (Scala DB table + Python JSON file) | 1 (versioned CEL registry) | 2 → 1 |

---

## 8. Architecture Decisions (ADRs)

### ADR-001: CEL over Drools/Rete for rule evaluation
**Decision:** Use `celpy` (Pure Python CEL) for dispatcher rule evaluation.  
**Rationale:** The rule surface is event-field predicate matching — exactly what CEL was designed for. `celpy` requires no JVM dependency, integrates natively into Python Airflow operators, and compiles to cacheable ASTs. Drools/Rete is appropriate for stateful forward-chaining production rule systems — unnecessary complexity for this use case.

### ADR-002: Reconciled query over Airflow Variable accumulator for fan-in
**Decision:** Replace mutable Airflow Variable accumulator with a parameterised query against the event store.  
**Rationale:** The event store is the authoritative source of truth for calculator completion events. Deriving fan-in readiness from a query against this store is idempotent, crash-recoverable, and immune to Kafka redelivery. Variable accumulators introduce a secondary state machine that can permanently diverge from reality on any delivery failure.

### ADR-003: Full cut-over (per component) over gradual traffic split
**Decision:** Each workstream cuts over fully in one go after UAT sign-off, rather than a percentage ramp.  
**Rationale:** The 15+ business day shadow mode provides equivalent confidence to a percentage ramp. A full cut-over with instant single-property rollback is operationally simpler than managing dual routing paths simultaneously in production.

### ADR-004: Strangler Fig — legacy preserved until 90 days post go-live
**Decision:** Legacy Control DAG is paused (not deleted) for 90 days after Smart Router go-live.  
**Rationale:** The orchestrator is on the critical path for regulatory capital reporting. Any routing failure is a potential regulatory breach. Keeping the legacy DAG as a paused emergency fallback (re-enabled by a single DB property update) gives the team a hard rollback path with zero code deployment.

### ADR-005: Airflow 3.x upgrade co-delivered with DAG Framework
**Decision:** Do not upgrade Airflow independently; co-deliver with DAG Framework modernisation.  
**Rationale:** Airflow 3.x requires DAG import path changes that are best done once, in the new-style DAGs. Doing the upgrade separately would require touching all DAG files twice. Co-delivery also means the UAT sign-off validates both the new framework and the new Airflow version together — a single integrated regression, not two.

---

## 9. Work Still To Do (Continuation Items)

The following items were identified but not fully designed in this session:

1. **Detailed CEL registry migration:** Translate all 85 `dag_trigger_criteria_map.json` entries into CEL YAML. Requires DAG author review of each rule to confirm correct condition absorption.

2. **`EventRateCriteria` CEL representation:** `EventRateCriteria` is a custom condition in `dags/logic/capital/custom_trigger_conditions.py`. Its exact logic must be read and expressed as a CEL predicate or pre-computed field.

3. **Correlation Service integration spec:** The architecture supports a `trigger_source: correlation_service` pluggable strategy but the interface between the Dispatcher and the external Correlation Service has not been defined.

4. **Airflow 3.x API audit:** A detailed diff of all `orchestration/common/*.py` module imports and API calls against Airflow 3.3 changelog is needed to scope the migration effort.

5. **Sensor polling optimisation:** Whether to implement response caching for `GET /event` (immutable once COMPLETED) has been identified but not fully designed (see §4.1.2).

6. **Multi-tenant routing at L0:** The architecture describes `event-orchestration` routing to "the owning tenant's dispatcher" but the multi-tenancy model (how tenant is determined from event payload) has not been fully specified.

7. **Routing audit log schema:** `routing_audit_log` is referenced throughout but its exact schema (`event_id`, `rule_id`, `dag_id`, `matched`, `reason`, `evaluated_at`, `triggered_at`) needs to be formalised as a DDL statement.

8. **Observability dashboard spec:** Prometheus counter names, Grafana dashboard panels, and PagerDuty alert thresholds for time-to-trigger, DLQ depth, readiness heartbeat, and routing divergence have been named but not specified in detail.

---

*Document generated from four-session design series. Sufficient for a new session to resume implementation planning from §4 (Component Enhancement Plans) or §9 (Continuation Items) without re-reading source code or prior conversation.*
