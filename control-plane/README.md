# Control Plane — Trigger Semantics (Airflow-resident condition engine)

**Workstream:** control plane = trigger-plan **Phases B–E**.
**Authoritative plan:** [`../trigger_redesign_final_implementation_plan.md`](../trigger_redesign_final_implementation_plan.md)
(program backbone). This plan governs wherever the other documents are silent.

All calculator trigger rules — definition *and* evaluation — live in the Airflow layer. One
per-tenant control DAG with a single `EVALUATE_AND_DISPATCH` task replaces the ~85-TaskGroup
fan-out; verdicts are visible in the task log, XCom, and the grid. EMS stays rule-free.

## Two deliverable surfaces

1. **Registry v2** — tenant-scoped YAML in the DAG repo, shipped in the DAG bundle. `trigger.when`
   as a list of CEL clauses (implicit AND); gates via `trigger.await`; CI renders the Level-0
   subscription slice and upserts it to EMS via `PUT /admin/subscriptions`.
2. **Airflow condition engine** — the `orchestration/conditions/` package + per-tenant control DAG
   and gate-heartbeat DAG.

## Target layout (trigger plan §3.1, §5.1)

```
control-plane/
  orchestration/
    conditions/
      cel_engine.py    # pinned celpy behind a seam + custom pure functions
      registry.py      # RuleSet load / validate / compile / cache
      models.py        # ClauseVerdict, RuleVerdict, EvaluationSummary
      evaluator.py     # evaluate_rules(), per-rule isolation, clause diagnostics
      gates.py         # gate readiness + canonical conf + dispatch
      decisions.py     # slim routing_decision batch POST client
      verdict_log.py   # human verdict-table renderer
  registry/            # ships in the DAG bundle (as today)
    schema/event_context_schema.yaml
    capital/registry.yaml + fixtures/
    nsfr/registry.yaml   (enabled: false until NSFR activates)
  dags/control/
    capital_control_dag.py
    capital_gate_heartbeat_dag.py
  tests/               # CEL conformance suite (shared with EMS cel-java), fixture execution, CI checks
```

## Phase board (trigger plan §10)

| Phase | Scope | Proves it worked | Status |
|---|---|---|---|
| **B. Dispatcher v2 in shadow** | Registry v2 authored (mechanical translation of the criteria map + routing rows); `capital_control_dag` (v2) evaluates every dual-routed event, records **full per-rule verdicts**, triggers nothing | `parity_mismatch_total == 0` over a window **spanning month-end** | ⏳ needs EMS Phase A live + registry v2 authored |
| **C. Per-(tenant, event-class) cutover** | Flip subscription rows one at a time, lowest-stakes first (`control_dag_id` v1 → v2) | Zero missed/extra triggers vs decision records; TIs/event ~150–300 → 1 | ⏳ |
| **D. Gates + heartbeat** | `await` rules authored; shadow ≥1 month-end, then per-portfolio cutover; stateless gate recompute + heartbeat | Portfolio runs per reporting date ~10 → 1; lost-final-event drill pages naming the missing contributor | ⏳ needs §12.1 (gate composition identity) resolved |
| **E. Precondition retirement + deletions** | Per-DAG: classify checks (taxonomy §2.3), fold class A → `trigger.when`, class B → `await`, delete `OTHER_PRE_CONDITIONS`; then delete control DAG v1, legacy criteria map, portfolio Variables, the Scala service | `af_calc_runs_skipped_total` → ~0; deletions with metrics steady 2 weeks | ⏳ coordinated with framework F4 |

Ordering: A first; B before C; D after A (may shadow ∥ B); E last, per-DAG, never in bulk.

## Cross-workstream dependencies

- **From EMS (Phase A):** `PUT /admin/subscriptions`, `routing_decision`, `POST /decisions` (Phase B);
  `GET /gate/groups` + fast contribution queries (Phase D).
- **To framework:** pilot DAG's `trigger.when` live (Phase C) → F1; gates live (Phase D) → F3.
- **Shared CI asset:** CEL conformance fixtures must be green on **both** pinned engines
  (cel-java in EMS + celpy here). Ownership = ems §14 item 9.

## Known landmines (do NOT repair — scheduled for deletion at Phase E; kickoff § landmines)

1. `trigger_conditions.py:165` — dormant evaluator can't parse the current registry.
2. `trigger_conditions.py:218-222` + `dag_utils.py:157` — malformed registry entry raises `AirflowSkipException` during build.
3. `http_deferrable_sensor.py:101-124` — `_rehydrate_calculator_state` (never port it).
4. `group.py:403` — placeholder `create_pre_condition_tasks` stub.

## Current blockers

- **EMS Phase A must be live** before Phase B shadow can begin.
- **Legacy framework repo (`orchestration/` package) absent** from the workspace: `dag_utils.py`
  (`trigger_dag` idempotent `dag_run_id` derivation — the invariant to reproduce byte-exactly),
  `dag_trigger_criteria_map.json` (mechanical-translation source for registry v2), and
  `trigger_conditions.py`. Registry v2 authoring (Phase B) needs these as inputs.
- **Open question §12.1** (gate composition identity) blocks Phase D schema.
