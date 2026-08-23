"""Exemplar 01 — logic module for a single-invocation calculator.

Corrected and annotated from old-orchestration/dags/usrg_ihc_calculator.py. The
original substitutes a comment ("assuming global context elements are imported")
for its import block and uses `Any` without importing it; both are fixed here.

Shape: ONE calculator, ONE fixed company, no dynamic fan-out. `__call__` returns a
single-element list, so CAPITALUSRGIHCCALC_MAPPED_GROUP has exactly one lane.
"""
import datetime
import logging
from typing import Any, List, Union

from orchestration.common.base_calculator import BaseCalculator
from orchestration.common.constants import CALC_RUN_PARAMS, OBS_INTEGRATION_ENABLED
from orchestration.common.generic_calculator import SimpleCalculator
from orchestration.common.group import mapped_calc_task_group_component
from orchestration.common.trigger_conditions import (
    EventDateCriteria,
    FrequencyCriteria,
    RunTypeCriteria,
)
from orchestration.common.xcom_utils import push_xcom

from dags.logic.capital.capital_commons import (
    IHC_COMPONENTS,
    enrich_region,
    get_calc_run_params,
    get_regional_capital_calc_name,
)

logger = logging.getLogger(__name__)


class CapitalUsrgIhcInit:
    """Calc-init callback: shapes the run params and declares the fan-out.

    The three members below ARE the TaskCallbackFunction protocol
    (base_task.py:12-34). All three must live INSIDE the class — a module-level
    `def get_calc_name(self)` next to the class looks right and is never found
    (pitfalls.md #4).
    """

    def __call__(
        self,
        context: Any,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs: Any,
    ) -> Union[dict, List[dict]]:
        # The full five-parameter signature is mandatory: generic_task passes all of
        # context/config/calc_run_params/xcom_data by keyword plus **extra_args
        # (base_task.py:106-112). A short signature raises TypeError at task runtime,
        # not at parse time — the DAG imports cleanly and dies on the first event.
        logger.info("USRG IHC Capital Calculator INIT called")

        run_params = get_calc_run_params(calc_run_params)
        enrich_region(run_params)

        # Domain values (component lists, company codes) come from the calculator
        # team's catalogue module. Never invent them.
        run_params["enabledComponents"] = IHC_COMPONENTS
        run_params["DerivedAttributesAdvancedApproachOff"] = "true"
        run_params["cumulus"] = "false"

        comp_groups = [{**run_params, "company-code-1": "B615"}]

        # The envelope push. Downstream mapped tasks read CALC_RUN_PARAMS from XCom
        # (base_task.py:99-103); without this push CREATE_CONTEXT submits the raw
        # inbound event params instead of these shaped ones — and SUCCEEDS, having
        # run the calculator with the wrong components (authoring-contract.md §4).
        push_xcom(context, value={CALC_RUN_PARAMS: run_params})

        # Return type decides the DAG shape: list[dict] => one mapped lane per
        # element. One element here = one lane, deliberately.
        return comp_groups

    def get_calc_name(self) -> str:
        # This name drives group_id, and group_id drives every derived task id.
        return get_regional_capital_calc_name()

    def get_calculator(self) -> BaseCalculator:
        return SimpleCalculator(self.get_calc_name())


def get_usrg_calc_group():
    """Builds the calculator task group. Called from the DAG file."""
    # pre_conditions SKIP the run when unmet (trigger_conditions.py:101-104). Only
    # put facts already carried by the event here — never readiness. "Input not
    # ready yet" belongs in datasets=[...] below, which WAITS instead of skipping.
    pre_conditions = [
        FrequencyCriteria(frequency="M"),
        RunTypeCriteria(["BATCH", "INTRA"]),
        EventDateCriteria(day_of_month_from=5, day_of_month_to=31),
    ]

    calc_init = CapitalUsrgIhcInit()

    # Toggles are passed by framework constant, never as hand-typed strings.
    extra_kwargs = {OBS_INTEGRATION_ENABLED: False}

    return mapped_calc_task_group_component(
        # The _CALC suffix is mandatory: normalize_group_id (group.py:397-399)
        # strips it to derive CAPITALUSRGIHCCALC_CALC_INIT / _MAPPED_GROUP /
        # _RESULT. Without it the strip is a no-op, the result aggregator's
        # hand-composed upstream task id misses, and _RESULT succeeds EMPTY.
        group_id=f"{calc_init.get_calc_name().upper()}_CALC",
        calculator=calc_init.get_calculator(),
        # str form => a MERIVAL ingestion sensor per dataset, task id
        # WAIT_FOR_<NAME>_INGESTION. These WAIT (deferrable), they do not skip.
        datasets=["INTERIMCOLLATERAL", "PARENTRATING"],
        pre_conditions=pre_conditions,
        calc_init_func=calc_init,
        # int(...total_seconds()), NEVER .seconds — .seconds is the seconds
        # COMPONENT and silently discards whole days (pitfalls.md #3).
        timeout=int(datetime.timedelta(hours=5).total_seconds()),
        **extra_kwargs,
    )
