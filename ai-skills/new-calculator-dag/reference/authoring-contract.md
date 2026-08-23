# Authoring contract — current calculator-DAG framework

The framework never validates any of this. Every rule below is enforced only by
convention, and each one has been broken at least once in the live repo. Each rule
carries the **why** — the runtime consequence of breaking it — and a citation into
the framework source so a maintainer can re-verify it.

Citations are to the `orchestration` framework snapshot (`old-orchestration/` in the
analysis workspace, `orchestration/common/...` when installed as a package) and to
`system_discovery_updated.md`.

---

## 1. The three artifacts and their relationship

Authoring one calculator DAG means writing **three** things. They are wired to each
other by *string identity only* — nothing imports across the boundary.

| # | Artifact | Path | Contains |
|---|---|---|---|
| 1 | Thin DAG file | `dags/<dag_id>.py` | `@dag(...)` boilerplate + `calc_start() >> group >> calc_end()` |
| 2 | Logic module | `dags/logic/<domain>/<name>_calculator.py` | the `*Init` callback class + `get_*_calc_group()` |
| 3 | Registry entry | `dags/dag_trigger_criteria_map.json` | `"<dag_id>": "<TYPE.IDENT[.REGION]>"` |

The three are joined by two string identities that nothing checks:

```
registry key  ==  @dag(dag_id=...)  ==  <dag file name>.py stem
group_id      ==  f"{calc_name.upper()}_CALC"   (drives every derived task id)
```

Break the first and the DAG exists but is unreachable by events — this is live today
(`dag_trigger_criteria_map.json:3` registers `amer_b3f_dag`, the DAG declares
`amer_d_b3f_dag`). Break the second and XCom pulls silently return `None`.

### Runtime anatomy the three artifacts produce

Per `system_discovery_updated.md` §4.4 and `common/group.py:242-336`:

```
calc_start
   └─> {NAME}_CALC                       (task group, mapped_calc_task_group_component)
         ├─ OTHER_PRE_CONDITIONS         (pre_conditions=[...]  — synchronous check-or-skip)
         ├─ WAIT_FOR_{DATASET}_INGESTION (datasets=[...]        — deferrable sensors)
         ├─ WAIT_FOR_{CALC}_COMPLETION   (calc_deferrable_preconditions=[...])
         ├─ {NAME}_CALC_INIT             (your *Init.__call__ runs here)
         ├─ {NAME}_MAPPED_GROUP          (dynamic — one lane per element you returned)
         │     CREATE_CONTEXT
         │       └─> PUBLISH_EDF_EVENT
         │             └─> DISPLAY_{CALC}_CALCULATOR_DETAILS
         │                   ├─> OBS_POST_START           (only if obs enabled)
         │                   └─> CHECK_{CALC}_CALCULATOR_STATUS   (deferrable, poll to FINISH)
         └─ {NAME}_RESULT                (aggregates the mapped lanes)
   └─> {NAME}_OBS_POST_COMPLETE          (only if obs enabled; group.py:324-334)
   └─> calc_end
```

You author **only** the `*Init` callback body and the `get_*_calc_group()` wiring call.
Everything else in that diagram is framework-owned and must not be reimplemented.

---

## 2. The callback protocol

Source: `common/base_task.py:12-34` (`TaskCallbackFunction` Protocol), invoked at
`common/base_task.py:106-112`.

The calc-init callback MUST be a **class** with all three members defined **on the
class**:

```python
class MyCalcInit:
    def __call__(
        self,
        context: Context,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs: Any,
    ) -> Union[dict, list[dict]]:
        ...

    def get_calc_name(self) -> str:
        """The calculator's MEG-registered catalogue name."""

    def get_calculator(self) -> BaseCalculator:
        """A BaseCalculator instance — usually SimpleCalculator(self.get_calc_name())."""
```

**Why a class, not a function:** `generic_task` (`base_task.py:66-123`) receives the
*instance* and calls `callback_function(context=..., config=..., calc_run_params=...,
xcom_data=..., **extra_args)`; `mapped_calc_task_group_component` separately calls
`calc_init.get_calculator()` and `calc_init.get_calc_name()` on the same object. A
module-level `def get_calc_name()` next to the class *looks* right and is never found —
this is the exact defect in `dags/logic/capital_calculator.py:56-60`, where both
protocol methods sit outside `CapitalInit` (note their stray `self` parameter, the
tell-tale of a botched de-indent).

**Why the full signature:** `generic_task` passes all four parameters by keyword plus
`**extra_args`. Omitting `xcom_data` or `**kwargs` raises `TypeError` at task runtime,
not at parse time — the DAG imports cleanly and fails on the first real event.

---

## 3. Fan-out by return type

`__call__`'s **return type decides the DAG's shape**:

| Return | Result |
|---|---|
| `dict` | one lane in `{NAME}_MAPPED_GROUP` |
| `list[dict]` | one dynamically-mapped lane **per element** |

**Why:** `create_dynamic_calc_task_group(..., dynamic_args=calc_initialize)`
(`group.py:293-299`) expands over the return value —
`calculator_mapped_task_group.partial().expand(dynamic_args=dynamic_args)`
(`group.py:239`). Each element becomes one lane's `mapped_args`, which
`generic_task` then uses verbatim as that lane's `calc_run_params`
(`base_task.py:84-87`).

Consequence: returning a single-element list and returning a bare dict produce
different task graphs. Choose deliberately, and keep the shape stable across runs —
Airflow's dynamic mapping tolerates width changes, but downstream `map_indexes`
lookups (`base_task.py:89-94`) assume element *i* means the same thing run to run.

---

## 4. The `CALC_RUN_PARAMS` envelope and the `push_xcom` obligation

`__call__` MUST push the shaped run params into XCom before returning:

```python
push_xcom(context, value={CALC_RUN_PARAMS: run_params})
return comp_groups
```

**Why:** downstream tasks resolve their params from the envelope. `generic_task`
reads `xcom_data.get(CALC_RUN_PARAMS)` when no mapped args are present
(`base_task.py:99-103`), and `BaseCalculator.pre_process` pulls the upstream XCom into
`self.xcom_data` (`base_calculator.py:47-71`). Skip the push and `CREATE_CONTEXT`
receives whatever `get_runtime_params(context)` produces — the raw incoming event
params, not your enriched ones. The calculator then runs with the wrong
`enabledComponents`, silently, and succeeds.

Note the *belt-and-braces* behaviour in `base_task.py:114-116`: if your return value is a
plain `dict` without a `CALC_RUN_PARAMS` key, the framework injects the *inbound*
`calc_run_params` into it. That backfill hides a missing `push_xcom` in the
single-dict case and does nothing in the `list[dict]` case — do not rely on it.

`push_xcom` takes the value as its **second positional or `value=` keyword** argument.
`push_xcom(context, value: {...})` is a syntax error, not an annotation
(`capital_calculator.py:51` — one more reason that file is not copy-safe).

---

## 5. `SKIP_TASK`

Returning `{"SKIP_TASK": True}` from `__call__` makes `generic_task` raise
`AirflowSkipException`, skipping the rest of the calculator run
(`base_task.py:117-120`).

Use it only for legacy parity with an existing calculator that already does this.
**Prefer a narrower registry condition or a pre-condition criterion**: a `SKIP_TASK`
return means the event was routed, the control DAG spent a `CHECK`+`TRIGGER` pair,
the calculator DAG run was created, and only then did anything decide not to work.
Registry-level narrowing avoids all of that (§6.2 skip-churn).

Only the `dict` return path is inspected for `SKIP_TASK` — returning
`[{"SKIP_TASK": True}]` does **not** skip; it creates one lane that proceeds.

---

## 6. `**kwargs` behaviour toggles

Extra kwargs passed to `mapped_calc_task_group_component(**extra_kwargs)` reach the
group builder and are read by string-constant key:

| Constant | Type | Default | Effect |
|---|---|---|---|
| `OBS_INTEGRATION_ENABLED` | bool | resolved by `resolve_obs_enabled(kwargs)` | adds `OBS_POST_START` in each lane and `{NAME}_OBS_POST_COMPLETE` after `RESULT` (`group.py:298, 303, 324-334`); also switches `{NAME}_RESULT`'s trigger rule to `NONE_FAILED_MIN_ONE_SUCCESS` (`group.py:307`) |
| `RUN_COMPLETION_CHECK_BY_MEG_EVENT` | bool | `False` | selects the completion-detection scheme (`group.py:255`) |
| `TRIGGER_RULE_KEY` | `TriggerRule` | `ALL_SUCCESS` | trigger rule for the first task in the group (`group.py:254`) |

**The completion-scheme toggle matters.** With the default (`False`),
`SimpleCalculator.process` asks the sensor to poll for the legacy CALC event
(`{"type": "CALC_EVENT", "STATE": "FINISH|FAILED"}`) and `post_process` reads
`event.additionalData.STATE` (`generic_calculator.py:50-55, 57-80`). With `True` it
polls MEG task-events (`{"taskEventType": "COMPLETED", "successful": "true|false"}`)
and maps `successful=true → FINISH`. **Pick the one the calculator actually emits** —
choosing wrong makes `CHECK_*_CALCULATOR_STATUS` poll until its 5-hour timeout while
the calculator has long since finished.

Pass toggles as a dict so the constant is used, never a hand-typed string:

```python
extra_kwargs = {OBS_INTEGRATION_ENABLED: False}
return mapped_calc_task_group_component(..., **extra_kwargs)
```

---

## 7. Naming rules

```python
group_id=f"{calc_init.get_calc_name().upper()}_CALC"
```

**The `_CALC` suffix is mandatory.** `normalize_group_id` (`group.py:397-399`) strips
exactly one trailing `_CALC` and uppercases the rest; the result is the stem for every
derived task id (`group.py:284-301`):

| Derived name | Formula |
|---|---|
| `{NAME}_CALC_INIT` | `f"{group_id_upper}_CALC_INIT"` |
| `{NAME}_MAPPED_GROUP` | `f"{group_id_upper}_MAPPED_GROUP"` |
| `{NAME}_RESULT` | `f"{group_id_upper}_RESULT"` |
| sensor task | `f"{group_id_upper}_MAPPED_GROUP.CHECK_{calculator.name.upper()}_CALCULATOR_STATUS"` |
| obs start task | `f"{group_id_upper}_MAPPED_GROUP.OBS_POST_START"` |

**Why it breaks silently:** `{NAME}_RESULT` is created by
`calc_run_aggregator_task(task_id=result_task_name, upstream_task_id=sensor_task_name)`
— a *hand-composed string* naming its upstream. If `group_id` ends in `_GROUP`
instead of `_CALC`, the strip is a no-op, the aggregator's `upstream_task_id` points at
a task id that does not exist, `xcom_pull` returns `None`, and `{NAME}_RESULT`
succeeds with an empty result. No exception anywhere.

Same class of failure applies to `AIRFLOW_UPSTREAM_TASK_ID` wiring
(`base_task.py:89-94`, `base_calculator.py:48-56`), which resolves upstream task ids by
string too.

Also required: `prefix_group_id=False` is set by the framework
(`group.py:252`), so task ids inside the group are **not** namespaced by the group id —
two calc groups in one DAG with the same `calc_name` collide.

---

## 8. Timeout rule

```python
timeout=int(datetime.timedelta(hours=5).total_seconds())     # correct
timeout=datetime.timedelta(hours=5).seconds                  # WRONG
```

**Why:** `timedelta.seconds` is the *seconds component* of the duration, not the
duration — `timedelta(days=1, hours=5).seconds` is `18000`, silently dropping a whole
day. The framework converts back with `timedelta(seconds=timeout)` (`group.py:263`)
and applies it as the `execution_timeout` of every dataset sensor and the calculator
status sensor.

The two production samples disagree on this: `usrg_ihc_calculator.py:52` is correct,
`capital_calculator.py:74` is not. Always use `int(...total_seconds())`.

---

## 9. Pre-conditions vs dataset waits vs deferrable calc waits

`mapped_calc_task_group_component` has three distinct gating mechanisms with very
different runtime behaviour. Choosing the wrong one is a correctness bug, not a style
preference.

| Kwarg | Mechanism | Behaviour when unmet | Use for |
|---|---|---|---|
| `pre_conditions=[...]` | synchronous task per criterion (`trigger_conditions.py:73-110`) | raises `AirflowSkipException` — **the run is abandoned** | facts already true in the incoming event: frequency, region, run type, date window |
| `datasets=[...]` | deferrable ingestion sensors (`group.py:268-273, 339-394`) | **waits** (deferred) until the dataset completion event arrives, up to `timeout` | input data that will arrive later |
| `calc_deferrable_preconditions=[...]` | `CalcEventCompletionCriteriaTask` (`group.py:116-130, 275-282`) | **waits** for another calculator's FINISH event | ordering behind another calculator |

Rule of thumb: **skip-or-run → `pre_conditions`; wait-then-run → the other two.**
Putting "the upstream dataset is not ready yet" in `pre_conditions` turns a normal
wait into a lost run.

### Dataset forms

`_create_dataset_task` (`group.py:339-380`) accepts three forms:

```python
datasets=[
    "INTERIMCOLLATERAL",                                   # str → MERIVAL ingestion sensor
    {"name": "SOMEDATASET", "event_source": EventSource.MEGDP},   # dict → MEGDP task group
    DatasetEventCriteria(dataset_name=..., event_source=...),      # namedtuple → full control
]
```

- **str** → `create_merival_ingestion_task` (`group.py:383-394`): a MERIVAL ingestion
  sensor with `static_params_map={"source": "MERIVAL", "TYPE": "INGESTION"}` and
  `context_param_keys=["FREQUENCY", "contextId", "LBD"]`. Task id
  `WAIT_FOR_{NAME}_INGESTION`.
- **dict** → requires the `event_source` key or it raises `ValueError` at DAG-parse
  time; the name is read from `dataset_name` **or** `name` (`any_match`, `group.py:354`).
  `EventSource.MEGDP` builds the two-step `CHECK_{NAME}` group
  (`CHECK_DATASET_SCHEDULED → CHECK_DATASET_CURATION`, `group.py:49-91`);
  `EventSource.MERIVAL` is equivalent to the string form.
- Anything else raises `ValueError`.

### Pre-condition criteria

See `reference/trigger-conditions.md` for the catalogue and allowed value sets. Note
that **all pre-conditions are bypassed on a manual run** unless the params ask for
them (`trigger_conditions.py:91-93`) — a manual trigger deliberately ignores frequency
and region gating.

---

## 10. DAG-file boilerplate contract

The DAG file is pure boilerplate and contains **no business logic**. Model:
`old-orchestration/dags/usrg_ihc_dag.py`.

```python
import pendulum
from airflow.decorators import dag
from orchestration.common.base_task import calc_end, calc_start
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from dags.control.dag_constants import CAPITAL_TAGS
from dags.logic.<domain>.<name>_calculator import get_<name>_calc_group


@dag(
    dag_id="<dag_id>",                                   # == filename stem == registry key
    description="...",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.<TAG>],
    schedule=None,                                       # event-triggered, never scheduled
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="Europe/Zurich"),
    catchup=False,
)
def <dag_id>():
    start = calc_start()
    calc_group = get_<name>_calc_group()
    finish = calc_end()

    start >> calc_group >> finish


<dag_id>()                                               # module-level invocation
```

Rules and why:

- **`schedule=None`** — routing is the registry's job. A schedule here creates a second,
  uncoordinated trigger channel. Time-based runs belong in `prod/*_trigger_dag_prod.py`
  (discovery §6.4).
- **`catchup=False` and the fixed 2021 `start_date`** — with `schedule=None` the start
  date is inert; every existing DAG uses this exact value, so deviating only invites the
  question. Never use a dynamic `start_date`.
- **Module-level invocation** — the decorated function must be *called* at import time or
  Airflow never registers the DAG. Easy to lose in a refactor; the file then parses
  cleanly and the DAG simply does not exist. Enforced by
  `scripts/validate_calculator_dag.py` (a missing invocation is an ERROR).
- **`dag_id` == filename stem** — one DAG per file, file named after it. Enforced by
  `scripts/validate_calculator_dag.py`.
- The decorated function's *own* name is free (`amer_d_b3f_dag.py` names it
  `b3f_context`), but naming it after the `dag_id` is the majority convention and
  removes one thing to check.
- `params=DAG_DEFAULT_PARAMETERS` is added when the DAG must be manually triggerable with
  a params form (`hdl_process_dag.py:25`).

---

## 11. What the author must NEVER touch

| Never | Why |
|---|---|
| Anything under `orchestration/common/`, `orchestration/sensors/`, `orchestration/observability/` | shared by every tenant's DAGs; a local fix is a global regression. Needed changes go to the framework team as a framework release |
| Envelope keys `CALC_RUN_PARAMS`, `PREVIOUS_CONTEXT_ID` (`calculator_task.py:14`), `CALC_TRIGGER_CONTEXT_ID` (`calculator_task.py:16`), `SKIP_TASK` | read and written by framework tasks by name; renaming or repurposing one silently detaches the mapped lanes from their contexts |
| The completion-polling internals — `CalcRunCompletionSensor`, `DatasetIngestionCompletionSensor`, `HttpDeferrableRunCriteriaSensor`, `post_process` response parsing | deferrable-operator lifecycle; getting it wrong strands triggers in the triggerer |
| Task ids inside the group | derived from `group_id`; the only supported way to change them is to change `calc_name` |
| The `enabledComponents` / company-code catalogues | domain data owned by the calculator team — the skill must never invent values |

---

## Quick self-check before validating

- [ ] registry key == `@dag(dag_id=…)` == filename stem
- [ ] `*Init` class defines `__call__`, `get_calc_name`, `get_calculator` — **all indented into the class**
- [ ] `__call__` signature has `context, config, calc_run_params, xcom_data, **kwargs`
- [ ] `__call__` calls `push_xcom(context, value={CALC_RUN_PARAMS: run_params})` before returning
- [ ] return type matches the intended fan-out (`dict` = 1 lane, `list[dict]` = N lanes)
- [ ] `group_id` ends with `_CALC`
- [ ] `timeout=int(timedelta(...).total_seconds())`
- [ ] gating placed correctly: skip-or-run in `pre_conditions`, wait-then-run in `datasets` / `calc_deferrable_preconditions`
- [ ] `schedule=None`, `catchup=False`, module-level DAG invocation present
- [ ] no framework file modified

Then run `scripts/validate_calculator_dag.py` — it mechanically enforces most of the
above.
