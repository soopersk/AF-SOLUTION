# Exemplar 03 — dataset-gated (`output_floor_monthly_dag`)

**Imitate me when:** the calculator must **wait** for dataset ingestion, or for another
calculator's completion, before it can submit. Interview answers that map here —
*dataset waits: yes*, or *runs behind calculator X*.

Registry entry is the live one from `dag_trigger_criteria_map.json:34-37`.

## Files

| File | Role |
|---|---|
| `output_floor_monthly_dag.py` | thin DAG file — identical in shape to exemplar 01's |
| `floors_calculator.py` | logic module — **all three gating mechanisms in one place** |
| `test_floors_calculator.py` | unit tests, parametrized over both trigger channels |
| `fixture_floors_calc.json` | sample event for the `CALC.FLOORSCALC` channel |
| `fixture_outputposting_dataset.json` | sample event for the `DATASET.OUTPUTPOSTING` channel |
| `registry.json` | the two-condition (list-form) entry |

## What this exemplar teaches

### 1. The three gating mechanisms, and why the choice matters

| Kwarg | When unmet | Use for |
|---|---|---|
| `pre_conditions=[...]` | raises `AirflowSkipException` — **the run is abandoned** | facts already in the event: frequency, region, run type |
| `datasets=[...]` | **defers and waits** for the ingestion event | input data arriving later |
| `calc_deferrable_preconditions=[...]` | **defers and waits** for a calculator's FINISH | ordering behind another calculator |

Encoding readiness as a `pre_condition` converts a normal wait into a permanently lost
run — the event is not redelivered. Nothing in the framework catches this and no static
check can; it is the highest-consequence authoring decision in the system.

### 2. Both dataset forms, side by side

```python
datasets = [
    "INTERIMCOLLATERAL",                                        # str  -> MERIVAL
    {"name": "OUTPUTPOSTING", "event_source": EventSource.MEGDP},  # dict -> MEGDP
]
```

- **str** → a MERIVAL ingestion sensor, task id `WAIT_FOR_INTERIMCOLLATERAL_INGESTION`,
  with `static_params_map` and context keys applied for you (`group.py:383-394`).
- **dict** → `event_source` is **mandatory**; without it `_create_dataset_task` raises
  `ValueError` at DAG-parse time. `EventSource.MEGDP` builds the two-step group
  `CHECK_OUTPUTPOSTING` (`CHECK_DATASET_SCHEDULED → CHECK_DATASET_CURATION`,
  `group.py:49-91`) — a MEGDP dataset is usable only once *curated*, not merely
  scheduled. The name may be given as `name` or `dataset_name` (`group.py:354`).

### 3. `calc_deferrable_preconditions` vs a `CALC.*` registry condition

Both express "after calculator X". They are not interchangeable:

- **`CALC.X` in the registry** = *a reason to start a run*. X's completion triggers the DAG.
- **`CalcEventCriteria(calc_identifier="X")`** = *a dependency of a run already started*.
  The DAG was triggered by something else and holds until X finishes.

Here the DAG is triggered by FLOORSCALC or OUTPUTPOSTING and *then* waits for
CONSENRICHMENTCALC — so the wait belongs in `calc_deferrable_preconditions`.

### 4. `dict` return = one lane

`OutputFloorInit.__call__` returns a bare `dict`, so `FLOORSCALC_MAPPED_GROUP` has
exactly one lane. Contrast exemplar 02's `list[dict]`. `[run_params]` would give the
same single lane — use the form that states the intent.

### 5. Multi-condition registry entries are OR, not AND

`["CALC.FLOORSCALC", "DATASET.OUTPUTPOSTING"]` triggers on **either** event. There is no
AND in the registry grammar; conjunction is what `datasets=` and
`calc_deferrable_preconditions=` are for. Note also that each extra condition adds a
permanent `CHECK` task instance to **every** control-DAG run (`registry-rules.md`,
skip-churn).

The tests are parametrized over both fixtures precisely because of this: whichever event
arrives first must produce identical calculator parameters.

## Validate it

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
  --dag-id output_floor_monthly_dag \
  --dag-file      ai-skills/new-calculator-dag/examples/03-dataset-gated/output_floor_monthly_dag.py \
  --logic-module  ai-skills/new-calculator-dag/examples/03-dataset-gated/floors_calculator.py \
  --registry      ai-skills/new-calculator-dag/examples/03-dataset-gated/registry.json \
  --test-file     ai-skills/new-calculator-dag/examples/03-dataset-gated/test_floors_calculator.py \
  --fixture       ai-skills/new-calculator-dag/examples/03-dataset-gated/fixture_floors_calc.json
```

Expected: **`0 error(s), 1 warning(s)`**.

That warning is expected and correct:

```
WARN: fixture lacks event.additionalData.DATASET_NAME, which condition
      'DATASET.OUTPUTPOSTING' inspects
```

`--fixture` takes one file, and one event is either a calc-completion event or a
dataset-curation event — never both. The validator checks the passed fixture against
*every* registered condition, so a two-condition entry always warns about the channel
the fixture does not represent. Passing `--fixture fixture_outputposting_dataset.json`
produces the mirror-image warning about `CALC.FLOORSCALC`.

**Do not "fix" this by inventing a fixture carrying both event shapes** — no such event
exists, and a fabricated one would make the test lie. Both channels are covered by
their own fixture and both are exercised by the parametrized tests. Treat the warning as
a prompt to confirm the second fixture exists, which
`test_fixtures_carry_the_fields_their_conditions_inspect` asserts.
