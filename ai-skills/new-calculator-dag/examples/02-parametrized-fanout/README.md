# Exemplar 02 — parametrized fan-out (`amer_d_b3f_dag`)

**Imitate me when:** one calculator must run once per unit (company group, portfolio,
book) resolved at runtime, and some units need different parameters from others.
Interview answers that map here — *fan-out shape = company-group derived*, or *static
list with per-unit rules*.

> ⚠ **Corrected reconstruction.** `old-orchestration/dags/logic/capital_calculator.py`
> does not import — it has a syntax error plus a `NameError`, a `TypeError`, and the
> protocol methods de-indented out of the class. **Never copy from it.** This file
> reconstructs its intent, written to the contract. See "Defects corrected" below.

## Files

| File | Role |
|---|---|
| `amer_d_b3f_dag.py` | thin DAG file — `dags/amer_d_b3f_dag.py` |
| `capital_calculator.py` | logic module — `dags/logic/capital/capital_calculator.py` |
| `test_capital_calculator.py` | unit tests, incl. the per-unit-rule assertions |
| `fixture_merival_amer.json` | sample enriched event (`SOURCE.MERIVAL.AMER`) |
| `registry.json` | the corrected entry (`amer_d_b3f_dag`, not the live `amer_b3f_dag`) |

## What this exemplar teaches

- **Fan-out by return type.** `__call__` returns `list[dict]` with one element per
  company group; `create_dynamic_calc_task_group` expands over it (`group.py:293-299`),
  giving one mapped lane per element.
- **One logic module, many DAG files.** `get_capital_calc_group(region, freq)` is
  parametrized, so eleven regional B3F DAG files share it. Adding a region = a DAG file
  + a registry entry, never new logic.
- **Per-unit business rules** — the WM component swap. This is the exemplar's whole
  reason for existing: templates and placeholders cannot express *"Wealth-Management
  regions get a different component list, and the group containing the region's
  flagship company gets a different one again."* Real calculators are full of these.
- **Lane isolation.** Each lane is built as a fresh `{**run_params, **company_groups}`
  dict, so a per-lane override cannot leak into its neighbours. Mutating the shared
  `run_params` inside the loop — as the production original does — makes every lane's
  components depend on iteration order.
- **Defence in depth on routing.** `H3RegionCriteria(region)` rejects other regions'
  events even if the registry condition is broader than intended.

## Defects corrected from the production original

| Line | Defect | Correction |
|---|---|---|
| `:51` | `push_xcom(context, value: {…})` — annotation where an argument belongs. **Syntax error; the module does not import** | `push_xcom(context, value={…})` |
| `:17`, `:49`, `:53` | initialises `return_comp_groups`, appends to `run_comp_groups` (undefined), returns the empty one | one list, built and returned |
| `:22`, `:28`, `:44` | initialises `h3_region_company_code`, writes/reads `h3_region__company_code` → `NameError` on the ordinary path | one name, initialised before use |
| `:32` | `calc_run_params.get('isManual', default=False)` — `dict.get` has no `default` kwarg → `TypeError` | positional default |
| `:44-49` | mutates the shared `run_params["enabledComponents"]` inside the loop → order-dependent leakage across lanes | per-lane dict |
| `:56-60` | `get_calc_name` / `get_calculator` at module level with a stray `self` → `AttributeError` at DAG-parse time | methods on `CapitalInit` |
| `:74` | `timeout=datetime.timedelta(hours=5).seconds` | `int(datetime.timedelta(hours=5).total_seconds())` |
| — | no imports at all | explicit import block |

## Domain values

`IB_B3F_COMPONENTS`, `WM_B3F_COMP_COMPONENTS`, `WM_G6L_B3F_COMPONENTS`, `COMP_GROUPS`
and the company-region mapping are **owned by the calculator team**. They are imported,
never inlined, and never invented by the skill — the interview must obtain them or a
pointer to the module that defines them.

## Combining with other exemplars

Need dataset waits as well? Borrow the `datasets=` / `calc_deferrable_preconditions=`
block from exemplar 03. Need to trigger downstream DAGs? Borrow exemplar 04's relay
block — after applying its boundary rule.

## Validate it

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
  --dag-id amer_d_b3f_dag \
  --dag-file      ai-skills/new-calculator-dag/examples/02-parametrized-fanout/amer_d_b3f_dag.py \
  --logic-module  ai-skills/new-calculator-dag/examples/02-parametrized-fanout/capital_calculator.py \
  --registry      ai-skills/new-calculator-dag/examples/02-parametrized-fanout/registry.json \
  --test-file     ai-skills/new-calculator-dag/examples/02-parametrized-fanout/test_capital_calculator.py \
  --fixture       ai-skills/new-calculator-dag/examples/02-parametrized-fanout/fixture_merival_amer.json
```

Expected: `0 error(s), 0 warning(s)`.

Run the same command with `--registry ai-skills/new-calculator-dag/tests/fixtures/registry_drift.json`
(a verbatim copy of the live registry) to see the drift being caught: exit 1, naming
`amer_b3f_dag` as the near-miss key.
