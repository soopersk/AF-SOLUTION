"""Exemplar 04 — a calculator DAG that relays to downstream DAGs.

⚠ Corrected reconstruction of `old-orchestration/dags/hdl_process_dag.py`, whose
snapshot has no imports and whose relay helpers are absent. Those helpers are
reconstructed in `hdl_calculator.py` alongside this file.

⚠ Read `reference/relay-triggering.md` §3 (the boundary rule) BEFORE imitating this.
A relay is a routing edge invisible to both the registry and the Tier-1 table: anyone
asking "what triggers this DAG?" reads two JSON files and a DB table and gets the wrong
answer. Relay ONLY when the payload is not event-derivable or the target is not
event-addressable. Otherwise the chain belongs in the registry.
"""
from typing import Any, cast

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import Context

from orchestration.common.base_task import calc_end, calc_start
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS, DAG_DEFAULT_PARAMETERS
from orchestration.common.logging_utils import decorated_message
from dags.control.dag_constants import LIQUIDITY_TAGS
from dags.logic.liquidity.hdl_calculator import (
    TRIGGER_PAYLOAD_SOURCE_CONF_KEY,
    build_trigger_conf,
    get_calc_result_task_id,
    get_hdl_process_group,
    get_trigger_payload_source,
    log_trigger_request,
    log_trigger_success,
    relay_payload_digest,
)

import logging

logger = logging.getLogger(__name__)

# Relay targets: a STATIC module-level constant. The validator rejects a computed
# target expression as an error — a dynamic list is not reviewable and defeats any
# audit of the chain graph.
#
# BOUNDARY-RULE RECORD (one line per target, filled in during interview batch 5):
#   nsfr_deposit_cals_dag     - justification: <why the registry cannot route this>
#   nsfr_loans_cals_dag       - justification: <...>
#   nsfr_derivatives_cals_dag - justification: <...>
#   nsfr_sft_cals_dag         - justification: <...>
#   nsfr_debt_cals_dag        - justification: <...>
#   nsfr_others_cals_dag      - justification: <...>
#
# ⚠ IN PRODUCTION these six ARE also registered under CALC.LNFHDLPROCESSCALC in the
# liquidity registry — two channels, same targets. They fire twice per event the
# moment the Tier-1 mfr_data_update_ACTL row is enabled (discovery §6.5, corrected in
# reference/relay-triggering.md §5). This exemplar's registry.json deliberately
# contains only hdl_process_dag, so the exemplar itself is overlap-clean; run the
# validator against the real liquidity registry and check_relay WARNs on all six.
DOWNSTREAM_DAG_IDS = [
    "nsfr_deposit_cals_dag",
    "nsfr_loans_cals_dag",
    "nsfr_derivatives_cals_dag",
    "nsfr_sft_cals_dag",
    "nsfr_debt_cals_dag",
    "nsfr_others_cals_dag",
]

# Derived from the calculator's own get_calc_name() rather than hard-coded, so
# renaming the calculator cannot silently detach the relay's XCom pull.
HDL_RESULT_TASK_ID = get_calc_result_task_id()


@dag(
    dag_id="hdl_process_dag",
    # With wait_for_completion=False, CALC_END means "the relays were fired", NOT
    # "the downstream work finished". Say so where operators will read it.
    description="HDL PROCESS Dag - relays to the NSFR calculator DAGs (fire-and-forget)",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[LIQUIDITY_TAGS.LIQUIDITY, LIQUIDITY_TAGS.HDL],
    schedule=None,
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="Europe/Zurich"),
    catchup=False,
    # A params form so the DAG can be triggered manually with an explicit payload
    # source; see the hazard note on get_trigger_payload_source.
    params=DAG_DEFAULT_PARAMETERS,
)
def hdl_process_dag():
    start = calc_start()
    hdl_process_group = get_hdl_process_group()
    finish = calc_end()

    @task(task_id="BUILD_TRIGGER_CONF")
    def build_downstream_trigger_conf(**context: Any) -> dict[str, Any]:
        """The payload every downstream DAG receives.

        Default source is the calc group's {NAME}_RESULT XCom, i.e. what this
        calculator produced. The resolved source is logged on every run because a
        manual rerun carrying the override key silently changes it.
        """
        airflow_context = cast(Context, context)

        payload_source = get_trigger_payload_source(
            airflow_context, payload_source_conf_key=TRIGGER_PAYLOAD_SOURCE_CONF_KEY
        )
        logger.info(
            decorated_message(
                "Building downstream trigger conf "
                f"[payload_source={payload_source}, source_task_id={HDL_RESULT_TASK_ID}]"
            )
        )

        return build_trigger_conf(
            context=airflow_context,
            source_task_id=HDL_RESULT_TASK_ID,
            payload_source=payload_source,
        )

    @task(task_id="BUILD_RELAY_PAYLOAD_DIGEST")
    def build_relay_payload_digest(conf: dict[str, Any]) -> str:
        """Digest of the shared payload, used to build a deterministic run id.

        A separate task because Jinja cannot compute a hash — the templated
        trigger_run_id below pairs this digest with the mapped target.
        """
        return relay_payload_digest(conf)

    trigger_conf = build_downstream_trigger_conf()
    payload_digest = build_relay_payload_digest(trigger_conf)

    trigger_dags = TriggerDagRunOperator.partial(
        task_id="TRIGGER_DEPENDENT_DAGS",
        conf=trigger_conf,
        # --- DETERMINISTIC RUN ID (the default this skill recommends, S7) ---------
        # Same (target, payload) => same run id, so a RETRIED trigger task re-uses the
        # existing downstream run instead of creating a second one. "Run already
        # exists" on a retry is this mechanism working, not a failure.
        #
        # `{{ task.trigger_dag_id }}` renders to THIS mapped instance's own target,
        # so one templated string yields a distinct id per target. Equivalent to
        # hdl_calculator.deterministic_relay_run_id(target, conf), with the target
        # readable in the id instead of folded into the hash.
        #
        # ⚠ CI-VERIFY: this templated form has not been executed against a live
        # Airflow (no Airflow in the authoring workspace). Confirm on the first CI
        # DagBag run that `task.trigger_dag_id` renders per mapped instance; the
        # Python-side deterministic_relay_run_id (build_payloads path) is the
        # fallback if it does not.
        trigger_run_id=(
            "relay_{{ task.trigger_dag_id }}_"
            "{{ ti.xcom_pull(task_ids='BUILD_RELAY_PAYLOAD_DIGEST') }}"
        ),
        # --- FAITHFUL PRODUCTION VARIANT (allowed, but understand the cost) -------
        # The production DAG omits trigger_run_id entirely:
        #
        #     TriggerDagRunOperator.partial(
        #         task_id="TRIGGER_DEPENDENT_DAGS",
        #         conf=trigger_conf,
        #         wait_for_completion=False,
        #         ...
        #     ).expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)
        #
        # Every execution then mints a fresh run id, so an ordinary Airflow retry of
        # the trigger task creates a SECOND downstream run — discovery Risk 3,
        # pitfalls.md #9. The validator WARNs (not errors) on this: it is a legitimate
        # parity choice with an existing DAG, never an accident.
        # -------------------------------------------------------------------------
        #
        # Explicit fire-and-forget. Declaring it deliberately is required — see the
        # DAG description for what CALC_END then means. wait_for_completion=True would
        # hold a worker slot for the whole downstream duration and inherit every
        # downstream failure.
        wait_for_completion=False,
        # All six targets share one task id under .expand(), so a failed target shows
        # as one failed map index. These callbacks are what make a fire-and-forget
        # relay traceable after the fact.
        on_execute_callback=log_trigger_request,
        on_success_callback=log_trigger_success,
    ).expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)

    # The chain must be EXPLICIT. conf=trigger_conf is an XComArg, so Airflow infers
    # the payload-task -> trigger edge, but it does NOT infer the calc group -> payload
    # task edge. Without writing it, the payload builder can run before the calculator
    # finishes and pull an empty result XCom — and the relay fires an empty conf.
    start >> hdl_process_group >> trigger_conf >> payload_digest >> trigger_dags >> finish


hdl_process_dag()
