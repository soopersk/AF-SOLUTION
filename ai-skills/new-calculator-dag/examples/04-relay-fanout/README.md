# Exemplar 04 — relay fan-out (`hdl_process_dag`)

**Imitate me when:** this DAG must directly trigger a fixed set of downstream DAGs with
a shaped payload. Interview answers that map here — batch 5, *downstream triggering:
yes*.

> **First check the boundary rule in `reference/relay-triggering.md` §3.** If the
> targets are event-routable, use a registry entry instead. A relay is a routing edge
> invisible to both the registry and the Tier-1 table; the system already has four
> routing channels and each new relay makes the map less true. The interview requires a
> written justification per target before a relay is generated at all.

> ⚠ **Corrected, self-contained reconstruction.** `old-orchestration/dags/hdl_process_dag.py`
> has no imports and its relay helpers (`_resolve_trigger_conf_from_source`,
> `get_trigger_payload_source`, `TRIGGER_PAYLOAD_SOURCE_CONF_KEY`, the logging
> callbacks, `get_calc_result_task_id`) are absent from the snapshot. They are
> reconstructed here so the exemplar teaches the whole mechanism. **Never copy from the
> raw snapshot file.** In the real repo these helpers live in a shared module — import
> them, do not re-copy them.

## Files

| File | Role |
|---|---|
| `hdl_process_dag.py` | DAG file — calc group + `BUILD_TRIGGER_CONF` + mapped relay |
| `hdl_calculator.py` | logic module — the calculator **and** the reconstructed relay mechanism |
| `test_hdl_calculator.py` | unit tests: Init fan-out, all three payload sources, run-id determinism |
| `fixture_hdl_ingest.json` | sample event (`CALC.LNFHDLINGESTCALC`) |
| `registry.json` | **only** `hdl_process_dag` — deliberately contains no relay target |

## The anatomy

```
calc_start >> LNFHDLPROCESSCALC_CALC >> BUILD_TRIGGER_CONF
                                     >> BUILD_RELAY_PAYLOAD_DIGEST
                                     >> TRIGGER_DEPENDENT_DAGS (mapped)
                                     >> calc_end
```

`TriggerDagRunOperator.partial(conf=…).expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)` —
**one** mapped task, one instance per target, sharing one payload.

## What this exemplar teaches

### 1. The chain must be explicit

`conf=trigger_conf` is an XComArg, so Airflow infers the *payload task → trigger* edge.
It does **not** infer the *calc group → payload task* edge. Without writing it, the
payload builder can run before the calculator finishes, pull an empty result XCom, and
the relay fires an empty conf — successfully.

### 2. Targets are a static module-level constant

The validator rejects a computed target expression as an **error**. A dynamic list is
not reviewable and defeats any audit of the chain graph. The boundary-rule
justifications live in a comment block directly above the list, where a reviewer will
actually see them.

`test_relay_targets_are_not_also_registry_routed` reads that constant out of the DAG
file **by AST** rather than re-declaring it — re-declaring would recreate the exact
drift class this skill exists to prevent, and the DAG file cannot be imported without
Airflow.

### 3. Three payload sources, one resolver

| Source | Behaviour | Use when |
|---|---|---|
| `PAYLOAD_SOURCE_XCOM` (default) | pulls the `{NAME}_RESULT` XCom | downstream needs what this calculator produced |
| `PAYLOAD_SOURCE_CONF` | forwards the incoming `dag_run.conf` | downstream needs the same *event*, not this DAG's output |
| `PAYLOAD_SOURCE_FUNCTION` | calls an author-written builder | the payload needs shaping neither of the above gives |

`get_trigger_payload_source` allows a per-run override from `dag_run.conf`. **Hazard:**
an operator re-running with a conf carrying that key silently changes what downstream
DAGs receive. That is why the resolved source is logged on every run, and why the
override is not documented as a routine operational lever.

The resolver never returns `None` — a `None` conf starts the downstream DAG with no
params at all, which looks like success.

Because the mechanism is pure functions in a module rather than inline logic in the
`@dag` body, all of it is unit-testable without Airflow. A relay written inline cannot
be tested at all.

### 4. Deterministic run id (the recommended default, S7)

Without `trigger_run_id`, `TriggerDagRunOperator` mints a fresh run id per execution, so
an ordinary Airflow **retry** of the trigger task creates a **second** downstream run —
discovery Risk 3, live in the production DAG.

This exemplar renders

```
trigger_run_id="relay_{{ task.trigger_dag_id }}_{{ ti.xcom_pull(task_ids='BUILD_RELAY_PAYLOAD_DIGEST') }}"
```

`{{ task.trigger_dag_id }}` resolves to *this mapped instance's own* target, so one
templated string yields a distinct id per target. The digest comes from its own task
because Jinja cannot compute a hash. This is the renderable equivalent of
`hdl_calculator.deterministic_relay_run_id(target, conf)`, which is the canonical form
from `relay-triggering.md` §4 and is used on the per-target `build_payloads` path.

Semantics: a retry is a deliberate no-op — "run already exists" is the mechanism
**working**. The payload is part of the key, so this deduplicates *retries*, not
*reruns*.

The **faithful production variant** (no `trigger_run_id` at all) is shown in a marked
comment block in the DAG file with its Risk-3 consequence named. It is a legitimate
parity choice with an existing DAG; the validator WARNs rather than errors so that it
can never be an accident.

### 5. `wait_for_completion` is explicit, and the description says why

With `False`, `CALC_END` means "the relays were fired", **not** "the downstream work
finished". Anything reading this DAG's success as end-to-end completion will be wrong,
so the DAG description states it where operators read it.

### 6. Logging callbacks

All six targets share one task id under `.expand()`, so a failed target shows as one
failed map index. `on_execute_callback` / `on_success_callback` log the map index —
without them, the only record that a downstream run was *requested* is the downstream
run itself, which is exactly what you are looking for when it is missing.

## The double-trigger defect — and why this exemplar is clean

In production, the six `nsfr_*_cals_dag` relay targets are **also** registered under
`CALC.LNFHDLPROCESSCALC` in the liquidity registry. Two channels, same targets; they
fire twice per event the moment the Tier-1 `mfr_data_update_ACTL` row is enabled
(discovery §6.5, corrected in `relay-triggering.md` §5).

This exemplar's `registry.json` deliberately contains only `hdl_process_dag`, so the
exemplar is overlap-clean and validates with zero warnings. The overlap case is covered
as a negative test (`tests/test_relay_checks.py::test_registry_overlap_warns_double_trigger`).
Run the validator against the real liquidity registry and `check_relay` WARNs on all six.

## Combining with other exemplars

Relay is an *addition* to a calculator, not an alternative shape. Take the calculator
body from exemplar 01 / 02 / 03 as appropriate, then borrow this file's relay block.

## Validate it

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
  --dag-id hdl_process_dag \
  --dag-file      ai-skills/new-calculator-dag/examples/04-relay-fanout/hdl_process_dag.py \
  --logic-module  ai-skills/new-calculator-dag/examples/04-relay-fanout/hdl_calculator.py \
  --registry      ai-skills/new-calculator-dag/examples/04-relay-fanout/registry.json \
  --test-file     ai-skills/new-calculator-dag/examples/04-relay-fanout/test_hdl_calculator.py \
  --fixture       ai-skills/new-calculator-dag/examples/04-relay-fanout/fixture_hdl_ingest.json
```

Expected: `0 error(s), 0 warning(s)`. Any relay WARN here is a regression.
