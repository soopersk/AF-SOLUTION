import datetime
import logging
from typing import List, Union

# Assuming global context elements like Context, BaseCalculator, SimpleCalculator, 
# IHC_COMPONENTS, CALC_RUN_PARAMS, and OBS_INTEGRATION_ENABLED are imported.
logger = logging.getLogger(__name__)


class CapitalUsrgIhcInit:
    def __call__(
        self, context: Any, config: dict, calc_run_params: dict,
    ) -> Union[dict, List[dict]]:
        logger.info('USRG IHC Capital Calculator INIT called')

        run_params = get_calc_run_params(calc_run_params)
        enrich_region(run_params)
        run_params['enabledComponents'] = IHC_COMPONENTS
        run_params['DerivedAttributesAdvancedApproachOff'] = 'true'
        run_params['cumulus'] = 'false'
        comp_groups = [{**run_params, "company-code-1": "B615"}]

        push_xcom(context, value={CALC_RUN_PARAMS: run_params})

        return comp_groups

    def get_calc_name(self) -> str:
        return get_regional_capital_calc_name()

    def get_calculator(self) -> BaseCalculator:
        return SimpleCalculator(self.get_calc_name())


def get_usrg_calc_group():
    pre_conditions = [
        FrequencyCriteria(frequency="M"),
        RunTypeCriteria(["BATCH", "INTRA"]),
        EventDateCriteria(day_of_month_from=5, day_of_month_to=31),
    ]

    calc_init = CapitalUsrgIhcInit()
    
    # Resolving keyword arguments directly inside the mapping function
    extra_kwargs = {OBS_INTEGRATION_ENABLED: False}
    
    return mapped_calc_task_group_component(
        group_id=f"{calc_init.get_calc_name().upper()}_CALC",
        calculator=calc_init.get_calculator(),
        datasets=["INTERIMCOLLATERAL", "PARENTRATING"],
        pre_conditions=pre_conditions,
        calc_init_func=calc_init,
        timeout=int(datetime.timedelta(hours=5).total_seconds()),
        **extra_kwargs
    )