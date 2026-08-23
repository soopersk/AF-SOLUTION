# Pitfalls — observed failure modes, with evidence

Every entry below is a defect that **exists in the repository today** or that the two
production samples disagree about. None is hypothetical. Each names the guard that now
catches it.

The common thread: this framework fails **quietly**. Nearly every pitfall here produces
a DAG that parses cleanly, runs green, and does the wrong thing (or nothing).

---

## Summary table

| # | Pitfall | Evidence | Guard |
|---|---|---|---|
| 1 | Registry key ≠ `dag_id` | `dag_trigger_criteria_map.json:3` registers `amer_b3f_dag`; the DAG declares `dag_id="amer_d_b3f_dag"` — unreachable via events | validator `check_registry` near-miss detection |
| 2 | Dots vs underscores in conditions | `SOURCE.MERIVAL.AMER` (registry) vs `SOURCE.MERIVAL_AMER` (discovery §6.3) | grammar underscore-drift guard (`condition_grammar.check_condition`) |
| 3 | `timedelta(...).seconds` truncation | `capital_calculator.py:74` vs correct `usrg_ihc_calculator.py:52` | validator AST check |
| 4 | Protocol methods left at module level | `capital_calculator.py:56-60` defines `get_calc_name` / `get_calculator` outside the class | validator protocol check |
| 5 | Missing `push_xcom` | contract §4 consequence: empty params at `CREATE_CONTEXT` | validator `check_logic_module` |
| 6 | Region-code drift | `ldnl` vs `lonl`, `wmde` vs `wmge` across artifacts | `KNOWN_REGIONS` warning |
| 7 | Snapshot code is NOT copy-safe | `capital_calculator.py` and `hdl_process_dag.py` carry transcription defects / omissions | rule: imitate the exemplars, never the raw snapshot files |
| 8 | Relay + registry dual-channel double-trigger | `hdl_process_dag.py:5-12` relays to the six `nsfr_*_cals_dag`s also registered under `CALC.LNFHDLPROCESSCALC` (discovery §6.5) | validator `check_relay` registry-overlap WARN + interview boundary-rule gate |
| 9 | Non-idempotent relay (no `trigger_run_id`) | `hdl_process_dag.py:56-62` — a retried trigger task creates duplicate downstream runs (discovery Risk 3) | validator WARN; deterministic run-id is the interview default |
| 10 | `group_id` without the `_CALC` suffix | `normalize_group_id` (`group.py:397-399`) strip is a no-op → derived task ids miss | validator `_CALC` check |
| 11 | Wrong completion scheme | `RUN_COMPLETION_CHECK_BY_MEG_EVENT` selects between two mutually exclusive polling shapes (`generic_calculator.py:50-55`) | interview question; not statically checkable |
| 12 | Readiness expressed as a pre-condition | `pre_conditions` skip; `datasets` wait (`trigger_conditions.py:101-104` vs `group.py:268-273`) | contract §9; not statically checkable |

---

## 1. Registry key ≠ `dag_id`

**The live bug.** `dags/dag_trigger_criteria_map.json` line 3:

```json
"amer_b3f_dag": "SOURCE.MERIVAL.AMER",
```

`dags/amer_d_b3f_dag.py:10` declares `dag_id="amer_d_b3f_dag"`. Every other regional B3F
DAG follows `<region>_d_b3f_dag`; only AMER's registry key drops the `_d_`.

**Consequence:** the control DAG's `TRIGGER` task targets a `dag_id` that does not
exist. As registered, AMER B3F is unreachable through the event path — whatever runs it
in production runs it some other way.

**Why it is easy to make:** the key is a string in a JSON file; nothing imports the DAG,
nothing resolves the name, and the control DAG builds the CHECK/TRIGGER group happily
for a non-existent target.

**Guard:** the validator reports absence *and* names the near-miss key
(`difflib.get_close_matches`), so the author sees "you probably meant this existing
key" rather than just "not registered". This exact case is a regression test
(`tests/test_golden_exemplars.py::test_real_drift_registry_fails_for_amer`).

---

## 2. Dots vs underscores in conditions

The same condition appears in three forms across artifacts:

| Artifact | Form |
|---|---|
| `dag_trigger_criteria_map.json` | `SOURCE.MERIVAL.AMER` |
| `system_discovery_updated.md` §6.3 | `SOURCE.MERIVAL_AMER` |
| `trigger_conditions.py:165` parser | splits on `", "` |

The live registry file is canonical. **Guard:** `check_condition` errors on
`TYPE.IDENT_REGION` when `IDENT` is an identifier already used with a region elsewhere
in the registry and the tail is a known region, with the message
`did you mean 'TYPE.IDENT.REGION'?`.

---

## 3. `timedelta(...).seconds` truncation

```python
timeout=datetime.timedelta(hours=5).seconds                 # capital_calculator.py:74
timeout=int(datetime.timedelta(hours=5).total_seconds())    # usrg_ihc_calculator.py:52
```

`.seconds` is the *seconds component*, not the duration — `timedelta(days=1, hours=5).seconds`
is `18000`, a whole day silently discarded. At 5 hours the two happen to agree, which is
exactly why the bug survives: it is correct until someone writes `days=1`.

**Consequence:** `execution_timeout` on every dataset sensor and on
`CHECK_*_CALCULATOR_STATUS` (`group.py:263`) becomes far shorter than intended; long
runs fail on timeout with no obvious cause.

**Guard:** the validator errors on **any** `timedelta(...).seconds` attribute access in
the logic module, regardless of the argument.

---

## 4. Protocol methods left at module level

`capital_calculator.py:56-60`:

```python
        return return_comp_groups          # end of CapitalInit.__call__


def get_calc_name(self) -> str:            # module level — NOT on the class
    return get_regional_capital_calc_name()

def get_calculator(self) -> BaseCalculator:
    return SimpleCalculator(self.get_calc_name(), region_comp_group=COMP_GROUPS)
```

The stray `self` parameter on a module-level function is the tell — this is a
de-indentation accident.

**Consequence:** `calc_init.get_calc_name()` raises `AttributeError` when
`get_*_calc_group()` runs, i.e. **at DAG-parse time**. That makes this one of the few
loud failures in this list — but only if it reaches an Airflow parse. It looks fine in
review, and unit tests that call the module-level function directly still pass.

**Guard:** the validator checks the three protocol members are in the `*Init` class body,
not merely present in the file.

---

## 5. Missing `push_xcom`

**Consequence:** downstream mapped tasks resolve `calc_run_params` from
`get_runtime_params(context)` instead of your envelope (`base_task.py:99-103`), so
`CREATE_CONTEXT` submits the raw inbound event params — wrong `enabledComponents`, wrong
flags. The run **succeeds**; the calculator computes the wrong thing.

Made worse by a partial safety net: `base_task.py:114-116` backfills `CALC_RUN_PARAMS`
into a returned `dict`, so single-lane DAGs sometimes appear to work anyway. It does
nothing for the `list[dict]` case. Do not rely on it.

**Guard:** the validator requires a `push_xcom` reference inside `__call__`.

---

## 6. Region-code drift

| Registry (`dag_trigger_criteria_map.json`) | Discovery §6.3 | Discovery §6.6 (`PORTFOLIO_CALC_REGIONS`) |
|---|---|---|
| `LDNL` | `LONL` | `LDN1` |
| `WMDE` | `WMGE` | `WMDE` |
| `ZURI` | `ZURI` | `ZUR1` |

Three artifacts, three spellings, for the same regions. A wrong region code in a registry
condition produces a condition that never matches — a DAG that never triggers, with no
error anywhere.

**Guard:** `KNOWN_REGIONS` (the registry's set) — an unknown region is a **warning**, not
an error, since new regions do exist. Treat any such warning as "verify against the
live company-region mapping before merging".

---

## 7. Snapshot code is NOT copy-safe

The analysis snapshot under `old-orchestration/` is a **reconstruction**, not a source
export. Two files are actively broken:

**`dags/logic/capital_calculator.py`** —
- `:17` initialises `return_comp_groups = []` but `:49` appends to `run_comp_groups`
  (undefined) and `:53` returns the empty `return_comp_groups`. A `NameError` at
  runtime, and had it not been, the DAG would fan out to zero lanes.
- `:22` initialises `h3_region_company_code` (single underscore) but `:28` writes and
  `:44` reads `h3_region__company_code` (double). The initialiser is dead, so when the
  `:23-29` branch does not run — the ordinary case — `:44` raises `NameError`.
- `:32` `calc_run_params.get('isManual', default=False)` — `dict.get` takes no `default`
  keyword. `TypeError`.
- `:51` `push_xcom(context, value: {CALC_RUN_PARAMS: run_params})` — an annotation where
  an argument belongs. **Syntax error**; the file does not import.
- `:56-60` the protocol methods at module level (pitfall 4).

**`dags/hdl_process_dag.py`** — has no imports at all, and its relay helpers
(`_resolve_trigger_conf_from_source`, `get_trigger_payload_source`,
`TRIGGER_PAYLOAD_SOURCE_CONF_KEY`, `log_trigger_request`, `log_trigger_success`,
`get_calc_result_task_id`) are absent from the snapshot.

**Rule: imitate `examples/`, never the raw snapshot files.** Exemplars 02 and 04 are
corrected reconstructions of exactly these two, written to the contract and validated.
`usrg_ihc_calculator.py` is comparatively clean but still carries an "assuming global
context elements are imported" comment in place of its import block, and uses `Any`
without importing it.

---

## 8. Relay + registry dual-channel double-trigger

`hdl_process_dag.py:5-12` relays to six DAGs; all six are also registered under
`CALC.LNFHDLPROCESSCALC` in the liquidity registry (discovery §6.3.1). Enabling the
Tier-1 `mfr_data_update_ACTL` row triggers each of them twice per event.

Discovery §6.5 inferred the affected DAGs were `aldop_process_dag` and
`nsfr_version_control_cals_dag`; the relay's actual target list corrects that — see
`relay-triggering.md` §5.

**Guard:** `check_relay` WARNs on every relay target that is also a registry key, plus
the interview's boundary rule, which requires a written justification per target before
a relay is generated at all.

---

## 9. Non-idempotent relay

`hdl_process_dag.py:56-62` constructs `TriggerDagRunOperator.partial(...)` with no
`trigger_run_id`, so every execution mints a fresh run id. A retried trigger task —
Airflow's normal behaviour on transient failure — creates a **second** downstream run
(discovery Risk 3).

**Guard:** validator WARN naming Risk 3; the interview defaults to the deterministic
`sha1(target + canonical conf)[:16]` run id (`relay-triggering.md` §4).

---

## 10. `group_id` without the `_CALC` suffix

`normalize_group_id` (`group.py:397-399`) strips exactly one trailing `_CALC`. Without
that suffix the strip is a no-op, and every derived id
(`{NAME}_CALC_INIT`, `{NAME}_MAPPED_GROUP`, `{NAME}_RESULT`, the sensor task) shifts.
The result aggregator names its upstream by hand-composed string
(`group.py:300-308`), so it ends up pointing at a task that does not exist: `xcom_pull`
returns `None` and `{NAME}_RESULT` succeeds empty. **Guard:** validator error.

---

## 11. Wrong completion scheme

`RUN_COMPLETION_CHECK_BY_MEG_EVENT` picks between two mutually exclusive polling shapes
(`generic_calculator.py:50-55` / `:57-80`): legacy CALC events
(`type=CALC_EVENT`, `STATE=FINISH|FAILED`) or MEG task-events
(`taskEventType=COMPLETED`, `successful=true|false`).

**Consequence of choosing wrong:** `CHECK_*_CALCULATOR_STATUS` polls for an event shape
the calculator never emits and defers until its timeout — a 5-hour hang for a calculator
that finished in minutes.

**No static guard is possible** — the correct value depends on what the calculator emits.
It is an explicit interview question; the default is the legacy scheme (`False`), matching
the framework default at `group.py:255`.

---

## 12. Readiness expressed as a pre-condition

`pre_conditions` **skip** on mismatch (`trigger_conditions.py:101-104`); `datasets` and
`calc_deferrable_preconditions` **wait** (`group.py:268-282`). Encoding "the input isn't
ready yet" as a pre-condition converts a normal wait into a permanently lost run — the
event will not be redelivered.

**No static guard is possible.** See `authoring-contract.md` §9 for the decision table.

---

## Two meta-lessons

1. **Loud failures are rare here.** Of the twelve entries above, only #4 and part of #7
   raise anything. The rest produce green runs. Assume that "it ran fine" carries no
   information, and rely on the validator plus the unit test.
2. **String identity is the weak joint.** Pitfalls 1, 2, 6 and 10 are all the same
   failure: two artifacts that must agree on a string, with nothing checking that they
   do. Whenever you write a name that appears somewhere else, run the validator.
