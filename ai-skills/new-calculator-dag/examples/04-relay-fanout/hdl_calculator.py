"""Exemplar 04 — logic module for a relay DAG, plus the relay helper mechanism.

⚠ CORRECTED, SELF-CONTAINED RECONSTRUCTION. `old-orchestration/dags/hdl_process_dag.py`
has no imports at all and its relay helpers (`_resolve_trigger_conf_from_source`,
`get_trigger_payload_source`, `TRIGGER_PAYLOAD_SOURCE_CONF_KEY`, the logging callbacks,
`get_calc_result_task_id`) are absent from the snapshot. They are reconstructed here so
the exemplar teaches the whole mechanism rather than a call into a black box. In the
real repo these helpers live in a shared module (cf. `build_trigger_conf` /
`build_payloads` in `dags/logic/nsfr_calculator.py`) — import them, do not re-copy them.

Read `reference/relay-triggering.md` §3 (the boundary rule) BEFORE imitating this.
"""
import datetime
import hashlib
import json
import logging
from typing import Any, Callable, List, Optional, Sequence, Union

from orchestration.common.base_calculator import BaseCalculator
from orchestration.common.constants import CALC_RUN_PARAMS, OBS_INTEGRATION_ENABLED
from orchestration.common.generic_calculator import SimpleCalculator
from orchestration.common.group import mapped_calc_task_group_component
from orchestration.common.trigger_conditions import FrequencyCriteria, RunTypeCriteria
from orchestration.common.xcom_utils import get_xcom_key, push_xcom

from dags.logic.liquidity.liquidity_commons import (
    HDL_PROCESS_COMPONENTS,
    get_calc_run_params,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# Part 1 — the calculator itself (ordinary, exemplar-01 shaped)
# ===========================================================================

class HdlProcessInit:
    """Calc-init callback for the HDL process calculator."""

    def __call__(
        self,
        context: Any,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs: Any,
    ) -> Union[dict, List[dict]]:
        logger.info("HDL Process Calculator INIT called")

        run_params = get_calc_run_params(calc_run_params)
        run_params["enabledComponents"] = HDL_PROCESS_COMPONENTS
        run_params["calcType"] = "HDL"

        push_xcom(context, value={CALC_RUN_PARAMS: run_params})

        return [run_params]

    def get_calc_name(self) -> str:
        return "lnfhdlprocesscalc"

    def get_calculator(self) -> BaseCalculator:
        return SimpleCalculator(self.get_calc_name())


def get_hdl_process_group():
    pre_conditions = [
        FrequencyCriteria(frequency="D"),
        RunTypeCriteria(["BATCH"]),
    ]

    calc_init = HdlProcessInit()
    extra_kwargs = {OBS_INTEGRATION_ENABLED: False}

    return mapped_calc_task_group_component(
        group_id=f"{calc_init.get_calc_name().upper()}_CALC",
        calculator=calc_init.get_calculator(),
        pre_conditions=pre_conditions,
        calc_init_func=calc_init,
        timeout=int(datetime.timedelta(hours=5).total_seconds()),
        **extra_kwargs,
    )


def get_calc_result_task_id(calc_init: "HdlProcessInit | None" = None) -> str:
    """The `{NAME}_RESULT` task id the relay pulls its payload from.

    Derived exactly as the framework derives it (group.py:284-301): the group id
    with its `_CALC` suffix stripped, uppercased, plus `_RESULT` — and from the SAME
    `get_calc_name()` the group builder uses, so renaming the calculator cannot
    silently detach the relay's XCom pull.
    """
    calc_name = (calc_init or HdlProcessInit()).get_calc_name()
    return f"{calc_name.upper()}_RESULT"


# ===========================================================================
# Part 2 — the relay mechanism
# ===========================================================================

# Payload sources. `xcom` is the default: the downstream DAGs receive what THIS
# calculator produced. See reference/relay-triggering.md §2.
PAYLOAD_SOURCE_XCOM = "xcom"        # pull the {NAME}_RESULT XCom
PAYLOAD_SOURCE_CONF = "conf"        # forward this run's incoming dag_run.conf
PAYLOAD_SOURCE_FUNCTION = "function"  # call an author-supplied builder

VALID_PAYLOAD_SOURCES = frozenset(
    {PAYLOAD_SOURCE_XCOM, PAYLOAD_SOURCE_CONF, PAYLOAD_SOURCE_FUNCTION}
)

# dag_run.conf key that overrides the payload source for a single run.
TRIGGER_PAYLOAD_SOURCE_CONF_KEY = "TRIGGER_PAYLOAD_SOURCE"


def get_trigger_payload_source(
    context: Any, payload_source_conf_key: str = TRIGGER_PAYLOAD_SOURCE_CONF_KEY
) -> str:
    """Resolve the payload source, honouring a per-run override in dag_run.conf.

    HAZARD: an operator re-running this DAG with a conf that happens to carry this key
    silently changes what downstream DAGs receive — no error, no warning. That is why
    the resolved value is logged on every run and why the override is not documented as
    a routine operational lever.
    """
    dag_run = context.get("dag_run") if isinstance(context, dict) else None
    conf = getattr(dag_run, "conf", None) or {}
    source = conf.get(payload_source_conf_key, PAYLOAD_SOURCE_XCOM)

    if source not in VALID_PAYLOAD_SOURCES:
        logger.warning(
            f"Unknown payload source [{source}] in dag_run.conf[{payload_source_conf_key}]; "
            f"falling back to {PAYLOAD_SOURCE_XCOM}. Valid: {sorted(VALID_PAYLOAD_SOURCES)}"
        )
        return PAYLOAD_SOURCE_XCOM

    return source


def _resolve_trigger_conf_from_source(
    context: Any,
    source_task_id: str,
    payload_source: str = PAYLOAD_SOURCE_XCOM,
    custom_builder: Optional[Callable[[Any], dict]] = None,
) -> dict:
    """The three-branch resolver. Always returns a plain dict — never None, because a
    None conf makes TriggerDagRunOperator start the downstream DAG with no params at
    all, which looks like success."""
    if payload_source == PAYLOAD_SOURCE_CONF:
        dag_run = context.get("dag_run") if isinstance(context, dict) else None
        return dict(getattr(dag_run, "conf", None) or {})

    if payload_source == PAYLOAD_SOURCE_FUNCTION:
        if custom_builder is None:
            raise ValueError(
                f"payload_source={PAYLOAD_SOURCE_FUNCTION!r} requires a custom_builder"
            )
        return dict(custom_builder(context) or {})

    # PAYLOAD_SOURCE_XCOM (default): pull the calc group's result.
    pulled = context["ti"].xcom_pull(
        task_ids=source_task_id, key=get_xcom_key(context)
    )
    if isinstance(pulled, list):
        pulled = pulled[0] if pulled else {}
    return dict(pulled or {})


def build_trigger_conf(
    context: Any,
    source_task_id: str,
    payload_source: str = PAYLOAD_SOURCE_XCOM,
    custom_builder: Optional[Callable[[Any], dict]] = None,
) -> dict:
    """One shared conf for every downstream DAG."""
    return _resolve_trigger_conf_from_source(
        context=context,
        source_task_id=source_task_id,
        payload_source=payload_source,
        custom_builder=custom_builder,
    )


def build_payloads(
    context: Any,
    source_task_id: str,
    downstream_dag_ids: Sequence[str],
    payload_source: str = PAYLOAD_SOURCE_XCOM,
    custom_builder: Optional[Callable[[Any], dict]] = None,
) -> List[dict]:
    """Per-target payloads — use instead of `build_trigger_conf` when each downstream
    DAG needs a DIFFERENT conf. Each target gets its own copy, so a downstream-specific
    mutation cannot leak across targets."""
    trigger_conf = _resolve_trigger_conf_from_source(
        context=context,
        source_task_id=source_task_id,
        payload_source=payload_source,
        custom_builder=custom_builder,
    )
    return [
        {"trigger_dag_id": dag_id, "conf": dict(trigger_conf)}
        for dag_id in downstream_dag_ids
    ]


def _canonical_json(conf: dict) -> str:
    """Stable serialisation — sorted keys, no incidental whitespace — so that an
    equal payload always hashes equal regardless of dict ordering."""
    return json.dumps(conf, sort_keys=True, separators=(",", ":"), default=str)


def relay_payload_digest(conf: dict) -> str:
    """Digest of the payload alone.

    Exists because Jinja cannot compute a hash: the DAG file pairs this digest with
    `{{ task.trigger_dag_id }}` to build a per-target run id inside a templated
    string. Payload-only digest + target-in-the-clear has the same dedup key as
    `deterministic_relay_run_id` below, just with the target readable in the run id.
    """
    return hashlib.sha1(_canonical_json(conf).encode("utf-8")).hexdigest()[:16]


def deterministic_relay_run_id(target_dag_id: str, conf: dict) -> str:
    """Same (target, payload) => same run id => a retry re-uses the existing run.

    This is the canonical form from reference/relay-triggering.md §4. Use it on the
    per-target payload path (`build_payloads`), where Python — not Jinja — builds the
    run id. The DAG file's templated string is its renderable equivalent.

    Without a trigger_run_id, TriggerDagRunOperator mints a fresh id per execution, so
    a RETRIED trigger task creates a SECOND downstream run — discovery Risk 3, live in
    the production hdl_process_dag.

    Semantics to understand before adopting this (relay-triggering.md §4):
      * a retry is a deliberate no-op: Airflow refuses the duplicate run id, and
        "run already exists" is the mechanism WORKING, not an error;
      * the payload is part of the key, so a genuinely different payload still gets a
        new run — this deduplicates retries, not reruns;
      * a deliberate same-payload rerun needs a discriminator (business date, this
        run's id) added to the hashed string.
    """
    digest = hashlib.sha1(
        f"{target_dag_id}:{_canonical_json(conf)}".encode("utf-8")
    ).hexdigest()
    return f"relay_{digest[:16]}"


def log_trigger_request(context: Any) -> None:
    """on_execute_callback. With wait_for_completion=False the only record that a
    downstream run was ASKED for is this line — which is exactly what you need when
    the downstream run is the thing that is missing."""
    task_instance = context.get("ti") if isinstance(context, dict) else None
    logger.info(
        "RELAY REQUEST [source_dag=hdl_process_dag, map_index=%s]",
        getattr(task_instance, "map_index", "-"),
    )


def log_trigger_success(context: Any) -> None:
    """on_success_callback. All targets share one task id under `.expand(...)`, so
    without the map index in the log, identifying WHICH target fired means counting
    map indices against DOWNSTREAM_DAG_IDS."""
    task_instance = context.get("ti") if isinstance(context, dict) else None
    logger.info(
        "RELAY FIRED [source_dag=hdl_process_dag, map_index=%s]",
        getattr(task_instance, "map_index", "-"),
    )
