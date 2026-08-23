"""Minimal contract-correct logic module — AST fixture for the validator tests.

Modelled on old-orchestration/dags/usrg_ihc_calculator.py. This file is only
ever parsed with `ast`, never imported, so the framework imports below do not
need to resolve; they are present because a real module must have them.
"""
import datetime
import logging
from typing import Any, List, Union

from orchestration.common.base_calculator import BaseCalculator, SimpleCalculator
from orchestration.common.constants import CALC_RUN_PARAMS, OBS_INTEGRATION_ENABLED
from orchestration.common.group import mapped_calc_task_group_component
from orchestration.common.trigger_conditions import (
    EventDateCriteria,
    FrequencyCriteria,
    RunTypeCriteria,
)
from orchestration.common.xcom_utils import push_xcom

logger = logging.getLogger(__name__)

CAPITAL_X_COMPONENTS = "CapitalPreFilter,SyntheticPosting"


class CapitalXInit:
    """Calc-init callback implementing the TaskCallbackFunction protocol."""

    def __call__(
        self,
        context: Any,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs: Any,
    ) -> Union[dict, List[dict]]:
        logger.info("Capital X Calculator INIT called")

        run_params = dict(calc_run_params)
        run_params["enabledComponents"] = CAPITAL_X_COMPONENTS
        comp_groups = [{**run_params, "company-code-1": "B615"}]

        push_xcom(context, value={CALC_RUN_PARAMS: run_params})

        return comp_groups

    def get_calc_name(self) -> str:
        return "capitalxcalc"

    def get_calculator(self) -> BaseCalculator:
        return SimpleCalculator(self.get_calc_name())


def get_capital_x_calc_group():
    pre_conditions = [
        FrequencyCriteria(frequency="M"),
        RunTypeCriteria(["BATCH", "INTRA"]),
        EventDateCriteria(day_of_month_from=5, day_of_month_to=31),
    ]

    calc_init = CapitalXInit()
    extra_kwargs = {OBS_INTEGRATION_ENABLED: False}

    return mapped_calc_task_group_component(
        group_id=f"{calc_init.get_calc_name().upper()}_CALC",
        calculator=calc_init.get_calculator(),
        datasets=["INTERIMCOLLATERAL", "PARENTRATING"],
        pre_conditions=pre_conditions,
        calc_init_func=calc_init,
        timeout=datetime.timedelta(hours=5).seconds,
        **extra_kwargs,
    )
