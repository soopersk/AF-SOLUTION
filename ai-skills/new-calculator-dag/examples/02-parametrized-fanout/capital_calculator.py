"""Exemplar 02 — parametrized fan-out over company groups.

⚠ CORRECTED RECONSTRUCTION. `old-orchestration/dags/logic/capital_calculator.py` is
NOT copy-safe — it carries a syntax error, a NameError, a TypeError and the protocol
methods de-indented out of the class (pitfalls.md #7). This file reconstructs the
INTENT, written to the contract. Imitate this; never that.

Shape: one calculator, N mapped lanes — one per company group resolved for the
(region, environment) pair, with a per-unit rule that swaps the component list for
Wealth-Management regions. This is the exemplar that exists because a template cannot
express arbitrary per-unit business logic.
"""
import datetime
import logging
from typing import Any, List, Union

from orchestration.common.base_calculator import BaseCalculator
from orchestration.common.constants import CALC_RUN_PARAMS
from orchestration.common.generic_calculator import SimpleCalculator
from orchestration.common.group import mapped_calc_task_group_component
from orchestration.common.trigger_conditions import (
    FrequencyCriteria,
    H3RegionCriteria,
    RunTypeCriteria,
)
from orchestration.common.xcom_utils import push_xcom

from dags.logic.capital.capital_commons import (
    COMP_GROUPS,
    DYNAMIC_CAPITAL_CALC_PARAMS,
    IB_B3F_COMPONENTS,
    WM_B3F_COMP_COMPONENTS,
    WM_G6L_B3F_COMPONENTS,
    create_company_groups_by_region,
    enrich_region,
    generate_tracking_id,
    get_calc_run_params,
    get_companycodes_by_runtype,
    get_env,
    get_regional_capital_calc_name,
    insert_dynamic_parameters,
    search_dict,
)

logger = logging.getLogger(__name__)


class CapitalInit:
    """Calc-init callback: resolves company groups and fans out one lane per group."""

    def __call__(
        self,
        context: Any,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs: Any,
    ) -> Union[dict, List[dict]]:
        logger.info("B3F Capital Calculator INIT called")

        # ---- 1. Base params shared by every lane -------------------------------
        run_params = get_calc_run_params(calc_run_params)
        enrich_region(run_params)

        run_params["enabledComponents"] = IB_B3F_COMPONENTS
        run_params["cumulus"] = "true"
        run_params["calcType"] = "B3F"
        run_params["lombardSwitch"] = "true"
        run_params["trackingId"] = generate_tracking_id()
        insert_dynamic_parameters(DYNAMIC_CAPITAL_CALC_PARAMS, run_params)

        env = get_env()
        h3_region = search_dict(run_params, key="h3Region")

        # ---- 2. Resolve the region's "flagship" company code -------------------
        # Used below to decide which WM component list a lane gets. Initialised
        # once, under the SAME name it is read by — the production file initialises
        # `h3_region_company_code` and then writes/reads `h3_region__company_code`,
        # so the ordinary path raises NameError (pitfalls.md #7).
        h3_region_company_code = ""
        comp_groups_by_env = COMP_GROUPS.get(env) or COMP_GROUPS.get("DEFAULT")
        if comp_groups_by_env:
            comp_groups_by_region = (
                comp_groups_by_env.get(h3_region) or comp_groups_by_env.get("DEFAULT")
            )
            if comp_groups_by_region:
                for comp_groups_by_size in comp_groups_by_region.values():
                    for comp_group in comp_groups_by_size:
                        if comp_group[0] != "*":
                            h3_region_company_code = comp_group[0]
                            break

        # ---- 3. Which companies participate in this run ------------------------
        # Monthly runs arrive with isManual=True and carry their own companies;
        # scheduled runs take the region's companies from the mapping table.
        # NOTE: dict.get takes its default POSITIONALLY — `.get(k, default=False)`
        # is a TypeError (the production file's bug).
        is_manual_run = calc_run_params.get("isManual", False)
        run_params_for_company = calc_run_params
        if not is_manual_run:
            all_comps = get_companycodes_by_runtype(calc_run_params)
            logger.info(
                f"**For non-manual run[isManual={is_manual_run}] using companies from "
                f"company-region table. companies for region '{h3_region}': {all_comps}**"
            )
            run_params_for_company = {"h3Region": h3_region, "companyCodes": all_comps}

        # ---- 4. One lane per company group -------------------------------------
        # THE PER-UNIT RULE. Wealth-Management regions run a different component
        # list, and the group holding the region's flagship company runs a different
        # one again. This kind of rule is why exemplars exist: no template
        # placeholder can express it, and it must not be invented — the component
        # constants come from the calculator team's catalogue module.
        #
        # Note each lane gets its OWN dict: `{**run_params, **company_groups}` copies
        # before mutating, so per-lane component swaps cannot leak into other lanes.
        comp_groups = []
        for company_groups in create_company_groups_by_region(
            run_params_for_company, COMP_GROUPS
        ):
            lane_params = {**run_params, **company_groups}

            if h3_region.startswith("WM"):
                if h3_region_company_code in company_groups.values():
                    lane_params["enabledComponents"] = WM_B3F_COMP_COMPONENTS
                else:
                    lane_params["enabledComponents"] = WM_G6L_B3F_COMPONENTS

            comp_groups.append(lane_params)

        # The envelope carries the SHARED params; per-lane overrides travel as the
        # mapped args (base_task.py:84-87). Push before returning, always.
        push_xcom(context, value={CALC_RUN_PARAMS: run_params})

        # list[dict] => one dynamically-mapped lane per company group.
        return comp_groups

    def get_calc_name(self) -> str:
        # ON THE CLASS. The production file has these two at module level with a
        # stray `self` — a de-indentation accident that raises AttributeError at
        # DAG-parse time (pitfalls.md #4).
        return get_regional_capital_calc_name()

    def get_calculator(self) -> BaseCalculator:
        # region_comp_group lets BaseCalculator.get_derived_name pick a size-specific
        # calculator name (capitalcalcsmall / …medium / …) per lane.
        return SimpleCalculator(self.get_calc_name(), region_comp_group=COMP_GROUPS)


def get_capital_calc_group(region: str, freq: str):
    """Builds the calc group for one (region, frequency) pair.

    Parametrizing here is what lets eleven regional DAG files share one logic module.
    """
    pre_conditions = [
        FrequencyCriteria(frequency=freq),
        # Rejects events for other regions even if the registry condition is broader
        # than it should be — defence in depth against a mis-registered condition.
        H3RegionCriteria(region),
        RunTypeCriteria(["BATCH", "INTRA"]),
    ]

    calc_init = CapitalInit()

    return mapped_calc_task_group_component(
        group_id=f"{calc_init.get_calc_name().upper()}_CALC",
        calculator=calc_init.get_calculator(),
        datasets=["INTERIMCOLLATERAL", "PARENTRATING"],
        pre_conditions=pre_conditions,
        calc_init_func=calc_init,
        # int(...total_seconds()). The production file uses `.seconds` here — which
        # happens to agree at 5 hours and would silently drop a day at 29 (pitfalls.md #3).
        timeout=int(datetime.timedelta(hours=5).total_seconds()),
    )
