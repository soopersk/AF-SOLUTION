---
name: new-calculator-dag
description: Author a new calculator DAG for the current orchestration framework — thin DAG file, calculator logic module, registry entry, unit tests and event fixture — via a structured interview, annotated golden exemplars and a deterministic validator gate. Use when someone needs to onboard a new calculator, add a regional/frequency variant of an existing one, or build a DAG that relays to downstream DAGs.
---

# Authoring a new calculator DAG

## 1. Mission and when to use this

**Mission:** produce a production-correct calculator DAG for the **current**
orchestration framework, complete and consistent across all five artifacts, with every
cross-artifact identity mechanically verified before you hand it over.

**Use this for:**

- onboarding a brand-new calculator;
- adding a regional / frequency variant of an existing calculator;
- a calculator that waits on datasets or on another calculator;
- a calculator that relays to a fixed set of downstream DAGs (exemplar 04).

**Refuse, and say why:**

| Request | Response |
|---|---|
| Migrating a DAG to the Phase-2 `calculator_dag(spec, plan)` framework | Out of scope — that is a separate skill. This one targets the current framework only. |
| Changing anything under `orchestration/common/`, `orchestration/sensors/`, `orchestration/observability/` | Framework change. Route to the framework team; a local edit is a global regression. |
| A DAG shape none of the four exemplars covers | **Stop.** Name the framework mechanism that would be needed and hand back to a human. Do not improvise novel framework usage. |
| A custom `CalcCriteria` subclass | Stop. Name `CalcCriteria` / `CalcCriteriaTask` as the base classes and require human review. Never invent one. |

**The value of this skill is not typing speed.** It is (a) the tribal knowledge in
`reference/`, which is written down nowhere else, and (b) the validator, which enforces
cross-artifact consistency the framework never checks. Do not skip either.

---

## 2. Read first

Read these, in order, before asking the first interview question:

1. `reference/authoring-contract.md` — the framework contract, each rule with its runtime consequence
2. `reference/registry-rules.md` — condition grammar and the full Tier-1 → Tier-2 routing chain
3. `reference/trigger-conditions.md` — pre-condition criteria catalogue and allowed value sets
4. `reference/relay-triggering.md` — relay pattern, payload sources, boundary rule, idempotency
5. `reference/pitfalls.md` — the observed failure modes and what guards each one

Then skim the exemplar READMEs under `examples/` to know what each one covers.

---

## 3. The interview

Fill the spec in five batches. The spec is **ephemeral** — it exists for the duration of
this session and is never committed. Ask a batch at a time; offer the default from the
nearest exemplar; only ask what cannot be derived.

### Hard rules — apply verbatim

- **Never invent domain values.** Component lists, company codes, dataset names,
  calculator catalogue names, connection ids, tag constants: the author supplies them or
  points at the existing module that defines them. A plausible-looking invented value is
  the worst possible output of this skill.
- **Stop on un-expressible shapes.** If the requested shape has no exemplar, name the
  framework path that would be needed and stop. Never improvise novel framework usage.
- **Confirm the narrowest condition.** Every registry entry adds `CHECK` task instances
  to every control-DAG run forever. Read back the proposed condition and confirm it is as
  narrow as the grammar allows.

### Batch 1 — Identity

| Question | Fills | Default |
|---|---|---|
| Calculator name (the MEG catalogue key, lowercase, e.g. `capitalcalc`) | `calculator_name` | — (must be supplied) |
| `dag_id` | `dag_id` | `<region>_<freq>_<calc>_dag` if regional; else `<calc>_<freq>_dag`. Check against the existing DAG inventory naming. |
| Description | `description` | — |
| Tags | `tags` | `CAPITAL_TAGS.CAPITAL`, or the tenant's tag constant |
| Domain / logic package | `logic_module_path` | `dags/logic/<domain>/<name>_calculator.py` |

Confirm: `dag_id` == the DAG filename stem == the registry key. State this back explicitly.

### Batch 2 — Triggering

| Question | Fills | Default |
|---|---|---|
| Registry condition: `SOURCE.` / `CALC.` / `DATASET.` / `PIPELINEID.` + identifier [+ region] | `registry_conditions` | — |
| Is this event source already routed by Tier 1? | `needs_tier1_row` | assume **yes** for existing sources; a new source needs a Tier-1 DB row (manual touchpoint) |
| Pre-conditions: frequency (`D`/`M`), run types (`BATCH`/`INTRA`), h3 region, event-date window | `pre_conditions` | frequency + run types if regional/periodic; otherwise none |
| Dataset waits: name + `MERIVAL` or `MEGDP` source | `datasets` | none |
| Waits on another calculator's completion? | `calc_deferrable_preconditions` | none |

Confirm the narrowest condition here. Also confirm the gating placement out loud:
skip-or-run facts go in `pre_conditions`, readiness goes in `datasets` /
`calc_deferrable_preconditions` — getting this backwards permanently loses runs.

### Batch 3 — Execution

| Question | Fills | Default |
|---|---|---|
| Run params to set (`enabledComponents`, static flags) | `run_params` | — (author supplies; never invent) |
| Timeout in hours | `timeout_hours` | **5 hours** |
| Completion scheme: legacy CALC_EVENT, or MEG task-event | `run_completion_check_by_meg_event` | **legacy CALC_EVENT** (`False`) unless the calculator is on the MEG task-event scheme |
| Observability integration | `obs_enabled` | **enabled** |

### Batch 4 — Fan-out

| Question | Fills | Default |
|---|---|---|
| Shape: single invocation / static list / company-group derived | `fanout_shape` | single invocation |
| Per-unit rules, **in the author's own words** | `fanout_rules` | none |

`fanout_rules` is the only free-form part of the spec, and it becomes the `__call__`
body. Ask for it in prose, read it back as pseudocode, and get agreement before writing
code. If it references domain data, get the module that provides it.

### Batch 5 — Downstream triggering

| Question | Fills | Default |
|---|---|---|
| Does this DAG relay to other DAGs? | `has_relay` | **no** |
| Target dag_ids | `relay_targets` | — |
| **Per target: is it event-routable?** | `relay_justifications` | — (see below) |
| Payload source: `XCOM_RESULT` / `FORWARD_CONF` / `CUSTOM_FUNCTION` | `payload_source` | **`XCOM_RESULT`** |
| `wait_for_completion` | `wait_for_completion` | `False` (fire-and-forget) |
| Run-id choice | `trigger_run_id` | **deterministic** (`sha1(target + canonical conf)[:16]`) |

**The boundary rule — enforce this, do not soften it.** For each relay target, ask
whether that DAG is event-routable (could a registry entry trigger it from this
calculator's completion event?).

- **If yes** → recommend a registry entry instead, and require an **explicit written
  justification** before generating a relay to it.
- **If no** → record why (payload not event-derivable / target not event-addressable).

Record every justification in the run summary **and** in the comment block above
`DOWNSTREAM_DAG_IDS` in the generated DAG file. A relay generated without a recorded
justification is a defect. If a target is also present in the registry, say so plainly:
that is the live double-trigger defect (`reference/pitfalls.md` #8).

---

## 4. Exemplar selection

| Interview answer | Exemplar |
|---|---|
| fan-out = single invocation | `examples/01-single-invocation/` |
| fan-out = per-unit / company-group derived, or per-unit rules | `examples/02-parametrized-fanout/` |
| dataset waits, or waits on another calculator | `examples/03-dataset-gated/` |
| relays to downstream DAGs | `examples/04-relay-fanout/` |

**These combine.** Pick the closest as the base, then borrow the missing feature from the
other's annotated section — a dataset-gated calculator that also relays takes 03 as its
base and borrows 04's relay block. Relay is always an *addition* to a calculator, never
an alternative shape.

Read the chosen exemplar's `README.md` and both code files before writing anything.

**Never copy from the raw snapshot files.** `dags/logic/capital_calculator.py` does not
even import (syntax error, `NameError`, `TypeError`, de-indented protocol methods) and
`dags/hdl_process_dag.py` is missing its imports and relay helpers. Exemplars 02 and 04
are the corrected versions of exactly those two. See `reference/pitfalls.md` #7.

---

## 5. Generation workflow

Run these six steps in order. Replace `<...>` with the interview's values; all commands
run from the repository root.

**Step 1 — Write the logic module** → `dags/logic/<domain>/<name>_calculator.py`

Model it on the chosen exemplar. Non-negotiable:
`*Init` class with `__call__` / `get_calc_name` / `get_calculator` **on the class**; the
full five-parameter `__call__` signature; `push_xcom(context, value={CALC_RUN_PARAMS:
run_params})` before returning; return type matching the fan-out shape;
`group_id=f"{calc_name.upper()}_CALC"`; `timeout=int(timedelta(hours=N).total_seconds())`.

**Step 2 — Instantiate the templates**

| Template | Write to |
|---|---|
| `templates/dag_file.py.tmpl` | `dags/<dag_id>.py` |
| `templates/test_calculator.py.tmpl` | next to the logic module, `test_<name>_calculator.py` |
| `templates/fixture_event.json.tmpl` | next to the test, `fixture_<...>.json` |

Substitute **every** `{{placeholder}}` and delete each template's header block. Replace
the test's assertion placeholders with the interview's real values — a test that asserts
a placeholder is worse than no test.

**Step 3 — Register the DAG**

```
python ai-skills/new-calculator-dag/scripts/update_registry.py \
    --registry dags/dag_trigger_criteria_map.json \
    --dag-id <dag_id> \
    --condition <TYPE.IDENT[.REGION]> [--condition <...>]
```

Refuses duplicates and bad grammar; writes `dag_trigger_criteria_map.json.bak` first.

**Step 4 — Validate**

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
    --dag-id       <dag_id> \
    --dag-file     dags/<dag_id>.py \
    --logic-module dags/logic/<domain>/<name>_calculator.py \
    --registry     dags/dag_trigger_criteria_map.json \
    --test-file    <path to the generated test> \
    --fixture      <path to the generated fixture>
```

Exit 0 = pass (warnings allowed), 1 = at least one ERROR, 2 = a file is missing.

**Step 5 — Fix and re-run until green.** Every ERROR must be fixed. Read every WARN and
either fix it or state in the summary why it is accepted. If the loop cannot converge,
**restore the registry from `dag_trigger_criteria_map.json.bak`** and hand back — never
leave the registry pointing at a DAG that does not validate.

**Step 6 — Run the generated unit test**, then write the run summary (§6).

`--with-dagbag` additionally runs a real Airflow import check (`airflow dags
list-import-errors`). It is CI-only: dev boxes do not have Airflow installed, and every
other check is AST-based precisely so it works without it. If `airflow` is not on PATH
the flag degrades to an explicit WARN — it never silently passes.

---

## 6. Run summary template

Report all of the following. The manual touchpoints are **deliberately not automated** —
they are outside this repository's reach — so they must be stated every time.

```
## Files written
  dags/<dag_id>.py                                    (new)
  dags/logic/<domain>/<name>_calculator.py            (new)
  <test path>                                         (new)
  <fixture path>                                      (new)

## Registry diff
  dags/dag_trigger_criteria_map.json
  + "<dag_id>": "<condition>"
  backup: dags/dag_trigger_criteria_map.json.bak

## Validation
  <verbatim validator output>
  <for each accepted WARN: why it is accepted>

## Unit tests
  <pytest output>

## Relay justifications          (only when has_relay)
  <target dag_id> - <why the registry cannot route this>

## MANUAL TOUCHPOINTS — not automated, required before this DAG works
  [ ] Calculator catalogue: add "<calculator_name>" to the CALCULATOR_CATALOGUE
      Airflow Variable (with its SLA field)
  [ ] Tier-1 routing row in post_filter_control_dag_map
      -> REQUIRED ONLY IF this is a new event source. <state which applies here>
      Without it the registry entry never fires; the liquidity tenant is the live
      example of a valid registry that is completely dormant.
  [ ] Company-group / company-region mapping for <region>, if this calculator
      resolves companies
  [ ] Airflow connection ids used by the calculator, if new
```

---

## 7. Guardrails

- **Never modify framework code** — `orchestration/common/`, `orchestration/sensors/`,
  `orchestration/observability/`. A local fix is a global regression.
- **Never leave the registry updated with validation failing.** Restore from the `.bak`
  if the fix-and-rerun loop cannot converge.
- **Never copy from the raw snapshot files** `dags/logic/capital_calculator.py` or
  `dags/hdl_process_dag.py` — both carry defects (`reference/pitfalls.md` #7). Imitate
  exemplars 02 and 04 instead.
- **Never generate a relay whose targets duplicate registry routing** without a recorded
  justification (the boundary rule, §3 batch 5).
- **Relay target lists must be static module-level constants.** The validator errors on a
  computed target expression.
- **Never invent domain values** — components, company codes, dataset names, catalogue
  names, connection ids.
- **Never invent a custom `CalcCriteria`.** Stop and require human review.
- **Never hand over without a green validator run**, and never report success without
  showing its actual output.
