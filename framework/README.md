# Calculator DAG Framework (Phase 2)

**Workstream:** framework = steps **F0–F4**.
**Authoritative plan:** [`../framework_redesign_final_implementation_plan.md`](../framework_redesign_final_implementation_plan.md)
(requester-accepted 2026-07-19). Where it conflicts with the trigger plan, the **trigger plan wins**.

One factory, one hook: a calculator DAG is `calculator_dag(spec=CalculatorSpec(...), plan=<function>)`.
The author writes a single pure `plan(input, services) -> CalculatorPlan` (business logic only);
everything else — conf normalization, EDF submission, completion waiting, aggregation, observability,
XCom — is framework-owned.

## Locked decisions (framework plan §1)

- **D1** Option A (lean typed planner). **D2** direct per-DAG rewrite, no shim.
- **D3** `CREATE_CONTEXT` + `PUBLISH_EDF_EVENT` merge into one `SUBMIT`.
- **D4** split-at-submit deferred (rationale §13). **D5** completion interpretation made structural.
- Added: thoughtful naming (§3.2) and a framework DAG-chaining mechanism (§7).

## Fixed, obs-invariant anatomy

```
CALC_INIT → RUN[i]: SUBMIT → AWAIT_COMPLETION → RESULT (→ TRIGGER_<dag>…)
```

## Target layout (framework plan §3.1)

```
framework/
  orchestration/
    framework/               # the entire Phase 2 authoring + runtime surface
      __init__.py            # public API (the only import authors need)
      spec.py                # CalculatorSpec, DownstreamDag, DataWait
      models.py              # CalculatorInput, CalculatorPlan, Invocation, InvocationState, ...
      factory.py             # calculator_dag() — the ONLY task-graph builder
      tasks.py               # CALC_INIT / SUBMIT / RESULT / TRIGGER_* callables
      completion.py          # CompletionScheme (MEG_TASK_EVENT | MERIVAL_CALC_EVENT) + interpret()
      sensors.py             # CalculatorCompletionSensor
      triggers.py            # SlaAwareHttpTrigger (async; trigger plan §5.8.1 rules)
      services.py            # CalculatorServices bundle passed to plan()
      clients/               # edf.py, ems.py (thin re-export of conditions/ clients), obs.py
      testing.py             # author-facing test kit (§9)
  tests/                     # plan unit, spec-contract, DagBag, trigger-schedule suites
```

`orchestration/conditions/` (control-plane workstream) is a **sibling**: framework imports its EMS
clients one-way; `conditions/` never imports `framework/` (Handoff 5).

## Phase board (framework plan §12)

| Step | Scope | Depends on | Proves it worked | Status |
|---|---|---|---|---|
| **F0. Framework lands** | `orchestration/framework/` complete (factory, models, wait stack, chaining, testing kit); no DAG migrated | EMS Phase A in prod (`GET /run/status`); `CalculatorMetadata` SLA verified (OQ-2) | Unit + contract + schedule suites green; testing kit runs in DAG-repo CI | ⏳ **hard-blocked** on `GET /run/status` in prod |
| **F1. Pilot: `usrg_ihc_dag`** | Full rewrite (plan + spec) + classification table | F0; its registry rule live (Phase C) | ≥1 month-end green; support sign-off from grid/XCom/logs only | ⏳ |
| **F2. Family waves** | B3F regional (~11) → IHC/floors/validations/market-risk → singles; one DAG per PR | F1 lessons | Per wave: zero `af_calc_runs_skipped_total`; verdict tables in RESULT logs | ⏳ |
| **F3. Gated/portfolio DAGs** | Portfolio planners consume `GATED` input + evidence via `services.events`; payload relays | Control-plane Phase D | Portfolio runs 10→1 per reporting date | ⏳ |
| **F4. Deletion** | §11 deletion list; framework 6.0.0 | All 85 on `framework:v2`; control-plane Phase E close | Grep-zero legacy imports; metrics steady 2 weeks | ⏳ coordinated with Phase E |

## Open questions (framework plan §15)

| # | Question | Blocks |
|---|---|---|
| OQ-1 | EDF context/event native idempotency? | F0 (§5.4 stays framework-side regardless) |
| OQ-2 | Prod `CalculatorMetadata` SLA fields + coverage across all 85 | F0 |
| OQ-3 | Obs sink contract (endpoint, payload, auth) | F0 |
| OQ-4 | Implicit relays today (hand-rolled `trigger_dag`) | F1/F2 |
| OQ-5 | Legacy `STATE` completion-scheme users | F4 / Phase E |
| OQ-6 | Mid-run `jobLink` in XCom vs triggerer-log line | F1 pilot |

## Current blockers

- **F0 is hard-blocked** on EMS Phase A `GET /run/status` being live in production.
- **OQ-2/OQ-3** need the prod `CalculatorMetadata` object and `orchestration/observability/` — both
  absent from this workspace (framework plan notes the `CalculatorMetadata` file is absent).
- **Pilot inputs absent:** `usrg_ihc_dag.py` + `usrg_ihc_calculator.py` (the F1 pilot) and
  `group.py`/`dag_utils.py` are needed to derive the plan/spec and the classification table.
