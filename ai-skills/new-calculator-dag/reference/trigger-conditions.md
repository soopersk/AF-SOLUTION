# Trigger conditions — pre-condition criteria catalogue

Two different things are called "conditions" in this framework. Keep them apart:

| | Registry condition | Pre-condition criterion |
|---|---|---|
| Lives in | `dag_trigger_criteria_map.json` | `pre_conditions=[...]` in your logic module |
| Evaluated by | the **control DAG**, before your DAG is triggered | a task **inside** your DAG, after it is triggered |
| Cost when it fails | one `CHECK` task instance in the control DAG | a whole calculator-DAG run, created and abandoned |
| Documented in | `registry-rules.md` | this file |

**Consequence:** anything expressible as a registry condition belongs there, not here.
Pre-conditions are for facts the registry grammar cannot express (a day-of-month
window, a region the condition already narrowed to a source).

---

## Catalogue

Signatures below are taken from the **live call sites** — the criteria classes
themselves are not part of the analysis snapshot, so treat the signatures as
observed-usage evidence and check the installed framework before using an argument
form not listed here.

| Class | Signature (observed) | Inspects | Allowed values |
|---|---|---|---|
| `FrequencyCriteria` | `FrequencyCriteria(frequency="M")` | run frequency in the incoming run params (`context.data.frequency`) | `"D"`, `"M"` |
| `RunTypeCriteria` | `RunTypeCriteria(["BATCH", "INTRA"])` | run type in the incoming run params (`context.data.runType`) | `"BATCH"`, `"INTRA"` |
| `H3RegionCriteria` | `H3RegionCriteria(region)` | `context.data.h3Region` (discovery §6.6 cites `trigger_conditions.py:807`) | H3 region codes — see `registry-rules.md` |
| `EventDateCriteria` | `EventDateCriteria(day_of_month_from=5, day_of_month_to=31)` | the event's business/logical date | `1`–`31`, `from <= to` |

Evidence: `usrg_ihc_calculator.py:35-39` (`FrequencyCriteria`, `RunTypeCriteria`,
`EventDateCriteria`), `capital_calculator.py:64` (`FrequencyCriteria`,
`H3RegionCriteria`, `RunTypeCriteria`).

The validator (`scripts/validate_calculator_dag.py`, `check_logic_module`) hard-fails
on a `FrequencyCriteria` frequency outside `{D, M}` and a `RunTypeCriteria` value
outside `{BATCH, INTRA}`. **Why an error and not a warning:** an unmatched criterion
does not fail loudly — `create_pre_condition_tasks` raises `AirflowSkipException`
(`trigger_conditions.py:101-104`), so a typo like `frequency="W"` produces a DAG that
skips every single run and looks healthy on the Airflow grid.

---

## How pre-conditions execute

`common/trigger_conditions.py:73-110` builds one task per criterion, task id
`{NAME}_CONDITION`, inside the `OTHER_PRE_CONDITIONS` group:

1. Run params come from `context["dag_run"].conf`, falling back to `context["params"]`.
2. **Manual-run bypass:** if `is_manual_run(run_params)` and the params do not ask for
   pre-condition checking, the task returns `True` immediately without evaluating
   anything (`:91-93`). A manual trigger therefore ignores frequency/region gating by
   design — do not rely on a pre-condition to protect against an operator mistake.
3. `criteria.check(params=run_params)` runs. An **exception** becomes
   `AirflowFailException` (the run fails, visibly). A **`False`** becomes
   `AirflowSkipException` (the run skips, quietly).

That asymmetry is the thing to remember: a broken criterion is loud, a non-matching
criterion is silent.

---

## When to use which mechanism

```
Is the fact already carried by the event, and does a mismatch mean "not for me"?
    └─ yes → can the registry grammar express it (TYPE.IDENT[.REGION])?
              ├─ yes → put it in the registry condition.  Cheapest. Preferred.
              └─ no  → pre_conditions=[...]
    └─ no, the fact is "some input isn't ready yet"
              ├─ a dataset          → datasets=[...]                      (waits)
              └─ another calculator → calc_deferrable_preconditions=[...]  (waits)
```

Never use `pre_conditions` for readiness. A pre-condition that is not yet true skips
the run permanently; the event will not be redelivered.

---

## Waiting criteria (not pre-conditions)

**`CalcEventCriteria`** — a dataclass (`trigger_conditions.py:21-27`) describing another
calculator whose completion this DAG waits for:

```python
CalcEventCriteria(
    calc_identifier="CONSENRICHMENTCALC",   # the MEG calculator identifier
    calculator=SimpleCalculator("consenrichmentcalc"),
    static_params={...},                    # optional: fixed params to match on
    dynamic_params_keys=[...],              # optional: params copied from this run
)
```

Passed as `calc_deferrable_preconditions=[...]`; the framework wraps it in
`CalcEventCompletionCriteriaTask` with task id
`WAIT_FOR_{calc_identifier}_COMPLETION` (`group.py:116-130`). It **defers** until the
completion event arrives or `timeout` expires — see the timeout rule in
`authoring-contract.md` §8.

**`DatasetEventCriteria`** — a namedtuple (`trigger_conditions.py:30-34`) with fields
`dataset_name, event_source, unique_identifier, static_params_map, context_param_keys`.
Use it in `datasets=[...]` only when the plain string or dict form is not enough; it is
passed through as `**dataset._asdict()` (`group.py:343-348`).

---

## The custom-criteria escape hatch

Any object satisfying the `CalcTriggerCriteria` protocol (`trigger_conditions.py:59-70`:
a `name` property, a `uuid` property, and `check(params, **kwargs) -> bool`) can be a
pre-condition. The supported base classes are:

- `CalcCriteria` (ABC, `trigger_conditions.py:225-237`) — implement `check`; `name` and
  `config` come from the base.
- `CalcCriteriaTask` (`trigger_conditions.py:240-243`) — additionally implement
  `task(task_group)` when the criterion needs to build its own Airflow task rather than
  be wrapped in the generic `{NAME}_CONDITION` task.

**Rule for this skill: never invent a custom criterion.** Custom criteria are business
logic in the routing layer, run on every event, and are outside the validator's reach.
If the interview surfaces a gating rule that none of the catalogued criteria express:

1. Stop.
2. Report that a custom `CalcCriteria` subclass is required and name the two base
   classes above.
3. Require a human decision and a human review before the code is written.

Do not improvise one to keep the workflow moving.
