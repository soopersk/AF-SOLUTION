# Relay triggering — in-DAG `TriggerDagRunOperator` fan-out

A **relay** is a calculator DAG that directly triggers other DAGs when its own work
finishes, instead of letting the registry route the completion event. It is the fourth
routing channel in the system (discovery §6.1) and the one with the least oversight.

Working exemplar: `examples/04-relay-fanout/`. Read the boundary rule (§3) **before**
generating one.

---

## 1. The pattern

Anatomy, from `old-orchestration/dags/hdl_process_dag.py`:

```
calc_start  >>  {NAME}_CALC group  >>  BUILD_TRIGGER_CONF  >>  TRIGGER_DEPENDENT_DAGS  >>  calc_end
                                        (@task, returns dict)   (mapped TriggerDagRunOperator)
```

```python
DOWNSTREAM_DAG_IDS = [                      # module-level, static, reviewable
    "nsfr_deposit_cals_dag",
    "nsfr_loans_cals_dag",
    ...
]

HDL_RESULT_TASK_ID = get_calc_result_task_id()   # derived from the Init's get_calc_name()


@dag(...)
def hdl_process_context():
    start = calc_start()
    hdl_process_group = get_hdl_process_group()
    finish = calc_end()

    @task(task_id="BUILD_TRIGGER_CONF")
    def build_downstream_trigger_conf(**context) -> dict[str, Any]:
        payload_source = get_trigger_payload_source(
            cast(Context, context), payload_source_conf_key=TRIGGER_PAYLOAD_SOURCE_CONF_KEY
        )
        return build_trigger_conf(
            context=cast(Context, context),
            source_task_id=HDL_RESULT_TASK_ID,
            payload_source=payload_source,
        )

    trigger_conf = build_downstream_trigger_conf()

    trigger_dags = TriggerDagRunOperator.partial(
        task_id="TRIGGER_DEPENDENT_DAGS",
        conf=trigger_conf,
        wait_for_completion=False,
        on_execute_callback=log_trigger_request,
        on_success_callback=log_trigger_success,
    ).expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)

    start >> hdl_process_group >> trigger_conf >> trigger_dags >> finish
```

Three structural requirements, each with a runtime consequence:

- **The chain must be explicit.** `... >> trigger_conf >> trigger_dags >> ...` — the
  payload task must be *ordered* before the trigger, not merely defined before it.
  `conf=trigger_conf` is an XComArg; Airflow does infer a dependency from it, but the
  calc group → payload-task edge is **not** inferred and must be written. Without it the
  payload builder can run before the calculator finishes and pull an empty result XCom.
- **Targets must be a static module-level constant** (or a literal list). A computed
  target list is not reviewable, defeats any chain-graph audit, and is rejected by the
  validator as an error.
- **One relay task, mapped.** `.partial(...).expand(trigger_dag_id=...)` produces one
  mapped task instance per target with a shared conf — not N hand-written operators.

---

## 2. The three payload sources

Per the builders in `old-orchestration/dags/logic/nsfr_calculator.py`. Exemplar 04
reconstructs the resolver (`_resolve_trigger_conf_from_source`) that the snapshot omits.

| Source | Constant | Behaviour | Use when |
|---|---|---|---|
| **XCom result** (default) | `PAYLOAD_SOURCE_XCOM = "xcom"` | pulls the calc group's `{NAME}_RESULT` XCom via `source_task_id` and passes it through as the downstream `conf` | the downstream DAGs need what this calculator produced |
| **Forward conf** | `PAYLOAD_SOURCE_CONF = "conf"` | passes this run's incoming `dag_run.conf` through unchanged | the downstream DAGs need the same *event* this DAG got, not its output |
| **Custom function** | `PAYLOAD_SOURCE_FUNCTION = "function"` | calls an author-written `build_<name>_trigger_conf(context) -> dict` | the payload needs shaping neither of the above provides |

```python
def build_trigger_conf(context, source_task_id, payload_source=PAYLOAD_SOURCE_XCOM,
                       custom_builder=None) -> dict:
    return _resolve_trigger_conf_from_source(
        context=context, source_task_id=source_task_id,
        payload_source=payload_source, custom_builder=custom_builder,
    )
```

For per-target payloads (each downstream DAG gets a *different* conf), use the
`build_payloads` shape instead and expand over the pairs:

```python
def build_payloads(context, source_task_id, downstream_dag_ids,
                   payload_source=PAYLOAD_SOURCE_XCOM) -> list[dict]:
    trigger_conf = _resolve_trigger_conf_from_source(...)
    return [{"trigger_dag_id": dag_id, "conf": dict(trigger_conf)}
            for dag_id in downstream_dag_ids]
```

### Runtime override — and its hazard

`get_trigger_payload_source(context, payload_source_conf_key)` reads the source from
`dag_run.conf[TRIGGER_PAYLOAD_SOURCE_CONF_KEY]`, defaulting to `PAYLOAD_SOURCE_XCOM`.
That makes the payload source overridable per run.

**Hazard:** an operator re-running the DAG manually with a conf that happens to carry
that key silently changes what downstream DAGs receive — no error, no warning, just a
different payload. Log the resolved source on every run (the prod DAG does), and never
document the override as a routine operational lever.

---

## 3. The boundary rule (adopted from the Phase-2 plan §7.1)

> **Relay only when the payload is not event-derivable, or the target is not
> event-addressable. Otherwise the chain belongs in the registry.**

Decision procedure — run this **per target**, before writing any relay code:

```
For each downstream DAG D:
  1. Does D already have (or could it have) a registry entry matching this
     calculator's completion event?
       └─ yes → USE THE REGISTRY. Do not relay to D.
  2. Does D need a payload that cannot be derived from the completion event?
       └─ yes → relay is justified. Record the reason.
  3. Is D unreachable by the event stream (no Tier-1 route, different tenant)?
       └─ yes → relay is justified. Record the reason.
  4. Otherwise → relay is NOT justified. Recommend the registry entry.
```

Why this is a hard gate rather than advice: every relay is a routing edge invisible to
both the registry and the Tier-1 table. Someone auditing "what triggers this DAG?"
reads two JSON files and a DB table and gets the wrong answer. The system already has
four routing channels; each new relay makes the map less true.

The interview (SKILL.md, batch 5) records an explicit justification per target. A relay
generated without one is a defect, not a shortcut.

---

## 4. Run-id idempotency

`TriggerDagRunOperator` without a `trigger_run_id` mints a fresh run id on every
execution. **A retried trigger task therefore creates a second downstream run** —
discovery Risk 3, live in `hdl_process_dag.py:56-62`.

The default this skill uses (design decision S7) is a deterministic id derived from the
target and the payload:

```python
import hashlib
import json


def deterministic_relay_run_id(target_dag_id: str, conf: dict) -> str:
    """Same (target, payload) => same run id => a retry re-uses the existing run."""
    canonical = json.dumps(conf, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha1(f"{target_dag_id}:{canonical}".encode("utf-8")).hexdigest()
    return f"relay_{digest[:16]}"
```

Semantics to understand before adopting it:

- **A retry is a no-op, by design.** The second attempt asks Airflow to create a run id
  that already exists; Airflow refuses. `"run already exists"` on a retry is the
  mechanism **working** — treat it as success, not as an error to be swallowed blindly.
  If you catch it, catch precisely that condition, and log it.
- **The payload is part of the key.** A genuinely different payload produces a different
  id and a legitimately new run. Deterministic ids deduplicate *retries*, not *reruns*.
- **A deliberate re-run needs a different id.** Add a discriminator (business date, run
  id of this DAG) to the hashed string when a same-payload re-run must be possible.

The faithful production variant — no `trigger_run_id` at all — is allowed for parity
with existing DAGs, but carries the duplicate-run defect. The validator WARNs when it
is absent, naming Risk 3. Choose deliberately; do not leave it out by accident.

---

## 5. The double-trigger pitfall — live evidence

`hdl_process_dag.py:5-12` relays to six DAGs:

```
nsfr_deposit_cals_dag, nsfr_loans_cals_dag, nsfr_derivatives_cals_dag,
nsfr_sft_cals_dag, nsfr_debt_cals_dag, nsfr_others_cals_dag
```

Every one of those six is **also** registered in the liquidity registry under
`CALC.LNFHDLPROCESSCALC` (discovery §6.3.1). Two channels, same targets. The only
reason it is not firing twice today is that the Tier-1 row `mfr_data_update_ACTL` is
`enabled=FALSE` — enabling the NSFR flow double-triggers all six.

**Correction to the discovery document:** §6.5 inferred the affected DAGs were
`aldop_process_dag` and `nsfr_version_control_cals_dag`. The actual relay target list in
`hdl_process_dag.py:5-12` is the six `nsfr_*_cals_dag`s above. `aldop_process_dag` and
`nsfr_version_control_cals_dag` are in the registry but are *not* relay targets. Evidence
beats inference.

The validator catches this class mechanically: `check_relay` WARNs when a relay target
also appears as a registry key. If you see that warning, the answer is almost always to
delete the relay target, not to justify it.

---

## 6. Semantics notes

**`wait_for_completion` must be explicit.** The validator WARNs when it is absent,
because the default determines what your DAG's success *means*:

- `wait_for_completion=False` (fire-and-forget, what prod uses): `CALC_END` means
  "the relays were fired", **not** "the downstream work finished". Anything reading this
  DAG's success as end-to-end completion — an SLA dashboard, a downstream wait, an
  operator — will be wrong. Say so in the DAG description.
- `wait_for_completion=True`: this DAG's run occupies a worker slot for the entire
  downstream duration and inherits every downstream failure. Set `poke_interval` and an
  `execution_timeout` if you choose it.

**Logging callbacks are recommended.** `on_execute_callback=log_trigger_request` and
`on_success_callback=log_trigger_success` (`hdl_process_dag.py:60-61`) are what make a
fire-and-forget relay traceable afterwards. Without them, the only record that a
downstream run was requested is the downstream run itself — which is exactly what you
are trying to debug when it is missing.

**Mapped-task visibility.** With `.expand(...)`, all targets share one task id; a single
failed target shows as one failed map index. Name the target in the callback log line, or
finding which one failed means reading map indices against `DOWNSTREAM_DAG_IDS`.

---

## Checklist before shipping a relay

- [ ] boundary rule run per target; justification recorded for each
- [ ] no target also present in the registry (validator WARN is clean)
- [ ] targets in a static module-level list constant
- [ ] payload source chosen deliberately and logged at runtime
- [ ] `BUILD_TRIGGER_CONF`-style payload task exists and is ordered after the calc group
- [ ] `trigger_run_id` set (deterministic default) or its absence deliberately accepted
- [ ] `wait_for_completion` explicit, and the DAG description says what `CALC_END` means
- [ ] `on_execute_callback` / `on_success_callback` wired
