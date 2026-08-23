"""Exemplar 03 — dataset waits and deferrable calculator-completion preconditions.

Shape: ONE lane (`__call__` returns a bare `dict`, not a list), but the group does not
start until three separate readiness conditions are met — two dataset ingestions and
one upstream calculator's completion.

The point of this exemplar is the DIFFERENCE between the three gating mechanisms:

    pre_conditions=[...]                  SKIP the run when unmet   (synchronous)
    datasets=[...]                        WAIT for the data         (deferrable)
    calc_deferrable_preconditions=[...]   WAIT for a calculator     (deferrable)

Putting readiness in `pre_conditions` turns a normal wait into a permanently lost run:
the event is not redelivered. This is the single most consequential authoring choice in
the framework and nothing checks it — see authoring-contract.md §9.
"""
import datetime
import logging
from typing import Any, List, Union

from orchestration.common.base_calculator import BaseCalculator
from orchestration.common.constants import (
    CALC_RUN_PARAMS,
    OBS_INTEGRATION_ENABLED,
    EventSource,
)
from orchestration.common.generic_calculator import SimpleCalculator
from orchestration.common.group import mapped_calc_task_group_component
from orchestration.common.trigger_conditions import (
    CalcEventCriteria,
    FrequencyCriteria,
    RunTypeCriteria,
)
from orchestration.common.xcom_utils import push_xcom

from dags.logic.capital.capital_commons import (
    OUTPUT_FLOOR_COMPONENTS,
    enrich_region,
    get_calc_run_params,
)

logger = logging.getLogger(__name__)

# The upstream calculator this one runs behind. Named as a constant so the DAG's
# position in the calculator chain is greppable rather than buried in a call.
UPSTREAM_CALC_IDENTIFIER = "CONSENRICHMENTCALC"


class OutputFloorInit:
    """Calc-init callback for the monthly output-floor calculation."""

    def __call__(
        self,
        context: Any,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs: Any,
    ) -> Union[dict, List[dict]]:
        logger.info("Output Floor Calculator INIT called")

        run_params = get_calc_run_params(calc_run_params)
        enrich_region(run_params)

        run_params["enabledComponents"] = OUTPUT_FLOOR_COMPONENTS
        run_params["calcType"] = "FLOORS"

        push_xcom(context, value={CALC_RUN_PARAMS: run_params})

        # A bare dict => exactly ONE lane in the mapped group. Contrast exemplar 02,
        # which returns list[dict] for N lanes. Returning `[run_params]` here would
        # produce the same single lane by a different route — pick the form that says
        # what you mean: dict for "this calculation runs once", list for "one per unit".
        return run_params

    def get_calc_name(self) -> str:
        return "floorscalc"

    def get_calculator(self) -> BaseCalculator:
        return SimpleCalculator(self.get_calc_name())


def get_output_floor_calc_group():
    """Builds the calc group with all three gating mechanisms in play."""

    # ---- SKIP-OR-RUN gating -------------------------------------------------
    # Facts already carried by the incoming event. A mismatch means "this event is
    # not for me" — abandoning the run is correct. Note these are bypassed entirely
    # on a manual trigger (trigger_conditions.py:91-93).
    pre_conditions = [
        FrequencyCriteria(frequency="M"),
        RunTypeCriteria(["BATCH"]),
    ]

    # ---- WAIT-THEN-RUN gating: datasets -------------------------------------
    # Both dataset forms, side by side (group.py:339-380):
    #
    #   str  -> a MERIVAL ingestion sensor, task id WAIT_FOR_INTERIMCOLLATERAL_INGESTION,
    #           with static_params_map {"source": "MERIVAL", "TYPE": "INGESTION"} and
    #           context keys FREQUENCY / contextId / LBD applied for you.
    #
    #   dict -> requires the "event_source" key or _create_dataset_task raises
    #           ValueError at DAG-PARSE time. With EventSource.MEGDP it builds a
    #           two-step task group CHECK_OUTPUTPOSTING:
    #               CHECK_DATASET_SCHEDULED -> CHECK_DATASET_CURATION
    #           because a MEGDP dataset is only usable once curated, not merely
    #           scheduled. The name may be given as "name" or "dataset_name".
    datasets = [
        "INTERIMCOLLATERAL",
        {"name": "OUTPUTPOSTING", "event_source": EventSource.MEGDP},
    ]

    # ---- WAIT-THEN-RUN gating: another calculator ---------------------------
    # Deferrable wait for CONSENRICHMENTCALC to publish its completion event.
    # Becomes the task WAIT_FOR_CONSENRICHMENTCALC_COMPLETION (group.py:116-130).
    #
    # Use this rather than a CALC.* registry condition when the ordering is a
    # *dependency of this run*, not a *reason to start a run* — here the DAG is
    # already triggered (by CALC.FLOORSCALC or DATASET.OUTPUTPOSTING) and must then
    # hold until the enrichment finishes.
    calc_deferrable_preconditions = [
        CalcEventCriteria(
            calc_identifier=UPSTREAM_CALC_IDENTIFIER,
            calculator=SimpleCalculator(UPSTREAM_CALC_IDENTIFIER.lower()),
        )
    ]

    calc_init = OutputFloorInit()
    extra_kwargs = {OBS_INTEGRATION_ENABLED: True}

    return mapped_calc_task_group_component(
        group_id=f"{calc_init.get_calc_name().upper()}_CALC",
        calculator=calc_init.get_calculator(),
        datasets=datasets,
        pre_conditions=pre_conditions,
        calc_deferrable_preconditions=calc_deferrable_preconditions,
        calc_init_func=calc_init,
        # This timeout is the execution_timeout of EVERY sensor above, not just the
        # calculator status check (group.py:263). With three deferrable waits in
        # series, size it for the slowest realistic arrival, not for the calc itself.
        # And always int(...total_seconds()) — .seconds would silently drop days.
        timeout=int(datetime.timedelta(hours=12).total_seconds()),
        **extra_kwargs,
    )
