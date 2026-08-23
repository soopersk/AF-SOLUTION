# Exemplar 01 — single invocation (`usrg_ihc_dag`)

**Imitate me when:** one calculator, one company / one fixed param set, no dynamic
fan-out. Interview answers that map here — *fan-out shape = single invocation*.

Corrected and annotated from `old-orchestration/dags/usrg_ihc_dag.py` +
`usrg_ihc_calculator.py`. This is the simplest complete shape: everything in it is
required, nothing in it is optional.

## Files

| File | Role |
|---|---|
| `usrg_ihc_dag.py` | thin DAG file — `dags/usrg_ihc_dag.py` |
| `usrg_ihc_calculator.py` | logic module — `dags/logic/capital/usrg_ihc_calculator.py` |
| `test_usrg_ihc_calculator.py` | unit tests (instantiated from `templates/test_calculator.py.tmpl`) |
| `fixture_merival_usrg.json` | sample enriched event this DAG triggers on |
| `registry.json` | the one entry to merge into `dags/dag_trigger_criteria_map.json` |

## What this exemplar teaches

- The **three-artifact identity chain**: `registry key == @dag(dag_id=…) == filename stem`.
- The **protocol on the class** — `__call__` / `get_calc_name` / `get_calculator`, all
  indented into `CapitalUsrgIhcInit`.
- The **envelope push** — `push_xcom(context, value={CALC_RUN_PARAMS: run_params})`
  before returning.
- **`_CALC` group-id suffix** and why the whole task graph depends on it.
- **`int(timedelta(...).total_seconds())`** for the timeout.
- **Gating placed correctly**: frequency / run-type / date window in `pre_conditions`
  (which skip), dataset readiness in `datasets` (which wait).

## Differences from the production original

The prod file is not copy-safe as-is; the corrections here are:

| Original | Here | Why |
|---|---|---|
| `# Assuming global context elements … are imported` | real import block | the module cannot run without them; the comment hides which package owns what |
| `Any` used, `typing.Any` not imported | imported | `NameError` at import |
| `__call__(self, context, config, calc_run_params)` | full five-parameter signature | `generic_task` passes `xcom_data` and `**extra_args` too (`base_task.py:106-112`) |

Everything else — including `timeout=int(...total_seconds())` and the protocol methods
being on the class — the original already got right. This is the *good* prod sample.

## Not shown here

- Dynamic fan-out over company groups → **exemplar 02**
- MEGDP dataset waits, `calc_deferrable_preconditions`, multi-condition registry
  entries → **exemplar 03**
- Triggering downstream DAGs → **exemplar 04**

## Validate it

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
  --dag-id usrg_ihc_dag \
  --dag-file      ai-skills/new-calculator-dag/examples/01-single-invocation/usrg_ihc_dag.py \
  --logic-module  ai-skills/new-calculator-dag/examples/01-single-invocation/usrg_ihc_calculator.py \
  --registry      ai-skills/new-calculator-dag/examples/01-single-invocation/registry.json \
  --test-file     ai-skills/new-calculator-dag/examples/01-single-invocation/test_usrg_ihc_calculator.py \
  --fixture       ai-skills/new-calculator-dag/examples/01-single-invocation/fixture_merival_usrg.json
```

Expected: `0 error(s), 0 warning(s)`.
