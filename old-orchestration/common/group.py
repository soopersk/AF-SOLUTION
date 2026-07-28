import logging
from datetime import timedelta
from typing import List, Union

from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from orchestration.common.base_calculator import BaseCalculator
from orchestration.common.base_task import TaskCallbackFunction, generic_task
from orchestration.common.af_utils import resolve_obs_enabled
from orchestration.common.calculator_metadata import calculator_catalogue_provider
from orchestration.common.calculator_task import (
    CalculatorTaskManager,
    calc_run_aggregator_task,
    get_parent_source_context,
)
from orchestration.common.collection_utils import any_match
from orchestration.common.constants import (
    AIRFLOW_IGNORE_API_CALL_ERRORS,
    AIRFLOW_PUSH_UPSTREAM_XCOM_DATA,
    AIRFLOW_UPSTREAM_TASK_ID,
    DEFAULT_POKE_INTERVAL,
    DEFAULT_TRIGGER_CRITERIA_SENSOR_TIMEOUT,
    ES_EVENT_ENDPOINT,
    EVENT_SOURCE,
    FRCA_MEGDP_SERVICE_CONN_KEY,
    RUN_COMPLETION_CHECK_BY_MEG_EVENT,
    TRIGGER_RULE_KEY,
    EventSource,
)
from orchestration.common.generic_calculator import DisplayCalcRunDetails
from orchestration.common.trigger_conditions import (
    CalcEventCompletionCriteria,
    CalcEventCriteria,
    CalcTriggerCriteria,
    DatasetEventCompletionCriteria,
)
from orchestration.observability.obs_run_tasks import create_obs_complete_task, create_obs_start_task 
from orchestration.sensors.http_async_run_condition_sensor import HttpDeferrableRunCriteriaSensor 
from orchestration.sensors.http_deferrable_completion_sensor import (
    CalcRunCompletionSensor, 
    DatasetIngestionCompletionSensor,
)
from orchestration.sensors.http_deferrable_sensor import HttpDeferrableSensor

logger = logging.getLogger(__name__)


class DatasetMegEventCriteriaTaskGroup(TaskGroup):

    def __init__(
        self,
        group_id: str,
        dataset_name: str,
        timeout: float = DEFAULT_TRIGGER_CRITERIA_SENSOR_TIMEOUT,
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        super().__init__(group_id=group_id, ui_color="#46cae4", **kwargs)

        source_context = get_parent_source_context(task_group=self, trigger_rule=trigger_rule)
        run_criteria = DatasetEventCompletionCriteria(dataset_name)

        dataset_scheduled = HttpDeferrableRunCriteriaSensor(
            task_group=self,
            run_criteria=run_criteria,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=ES_EVENT_ENDPOINT,
            task_id="CHECK_DATASET_SCHEDULED",
            trigger_rule=trigger_rule,
            request_params={'taskEventType': 'SCHEDULED', 'context_key': 'parent_id'},
            extra_args={
                AIRFLOW_UPSTREAM_TASK_ID: f"{group_id}.GET_SOURCE_CONTEXT",
            },
            execution_timeout=timedelta(seconds=timeout),
        )

        dataset_curated = HttpDeferrableRunCriteriaSensor(
            task_group=self,
            run_criteria=run_criteria,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=ES_EVENT_ENDPOINT,
            task_id="CHECK_DATASET_CURATION",
            trigger_rule=trigger_rule,
            request_params={'updateType': 'CURATION', 'context_key': 'triggerContextId'},
            extra_args={
                AIRFLOW_UPSTREAM_TASK_ID: f"{group_id}.CHECK_DATASET_SCHEDULED",
            },
            execution_timeout=timedelta(seconds=timeout),
        )

        source_context >> dataset_scheduled >> dataset_curated


class BasicDatasetEventCriteriaTask(DatasetIngestionCompletionSensor):

    def __init__(
        self, 
        dataset_name: str, 
        event_source: EventSource, 
        unique_identifier: str | None = None, 
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        **kwargs
    ):
        name = unique_identifier if unique_identifier else dataset_name
        super().__init__(
            run_criteria=DatasetEventCompletionCriteria(dataset_name),
            task_id=f"WAIT_FOR_{name.upper()}_INGESTION",
            trigger_rule=trigger_rule,
            event_source=event_source,
            **kwargs,
        )


class CalcEventCompletionCriteriaTask(CalcRunCompletionSensor):

    def __init__(
        self, 
        calc_event_criteria: CalcEventCriteria, 
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        **kwargs
    ):
        super().__init__(
            run_criteria=CalcEventCompletionCriteria(calc_event_criteria.calc_identifier),
            task_id=f"WAIT_FOR_{calc_event_criteria.calc_identifier}_COMPLETION",
            calc_event_criteria=calc_event_criteria,
            trigger_rule=trigger_rule,
            **kwargs,
        )


class GenericCriteriaTaskGroup(TaskGroup):

    def __init__(
        self,
        pre_conditions: List[CalcTriggerCriteria],
        group_id: str = 'OTHER_PRE_CONDITIONS',
        trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        super().__init__(group_id=group_id, add_suffix_on_collision=True, ui_color="#46caed", **kwargs)
        # Assuming `create_pre_condition_tasks` is defined elsewhere globally or imported
        self.cond_tasks = create_pre_condition_tasks(pre_conditions, trigger_rule, self)


class CalculatorTaskGroup(TaskGroup):

    def __init__(
        self,
        group_id: str,
        calculator: BaseCalculator,
        callback_handler: TaskCallbackFunction,
        timeout: float,
        **kwargs,
    ):
        super().__init__(group_id=group_id, ui_color="#46cae4", **kwargs)

        group_id_upper = normalize_group_id(group_id)
        calc_initialize = generic_task.override(
            task_id=f"{group_id_upper}_CALC_INIT", task_group=self
        )(callback_handler)

        task_manager = CalculatorTaskManager(calculator)

        ctx_task = task_manager.create_context_task(task_group=self)
        edf_task = task_manager.publish_edf_event_task(task_group=self)

        calc_run_task = HttpDeferrableSensor(
            calculator=calculator,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=ES_EVENT_ENDPOINT,
            task_id=f"CHECK_{calculator.name.upper()}_CALCULATOR_STATUS",
            task_group=self,
            poke_interval=kwargs.get("poke_interval", DEFAULT_POKE_INTERVAL),
            execution_timeout=timedelta(seconds=timeout),
        )

        calc_initialize >> ctx_task >> edf_task >> calc_run_task


def create_dynamic_calc_task_group(
    calculator: BaseCalculator,
    timeout: float,
    group_id: str = "CALC_GROUP",
    ui_color: str = "#46cae4",
    dynamic_args: list[dict[str, str]] | None = None,
    obs_enabled: bool = True,
):
    dynamic_args = dynamic_args or []
    calculator_metadata = calculator_catalogue_provider.get_metadata(calculator.name)
    calculator.metadata = calculator_metadata

    @task_group(group_id=group_id, ui_color=ui_color)
    def calculator_mapped_task_group(dynamic_args_mapped: dict | None = None):
        task_manager = CalculatorTaskManager(calculator)
        generate_context = task_manager.create_context_task(mapped_args=dynamic_args_mapped or {})
        send_edf_event = task_manager.publish_edf_event_task(
            extra_args={AIRFLOW_UPSTREAM_TASK_ID: f"{group_id}.CREATE_CONTEXT"}
        )

        display_calc_run_details_id = f"DISPLAY_{calculator.name.upper()}_CALCULATOR_DETAILS"
        display_calc_run_details = HttpDeferrableSensor(
            calculator=DisplayCalcRunDetails(calculator.name),
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=ES_EVENT_ENDPOINT,
            task_id=display_calc_run_details_id,
            poke_interval=50,
            extra_args={
                AIRFLOW_UPSTREAM_TASK_ID: f"{group_id}.PUBLISH_EDF_EVENT",
                AIRFLOW_PUSH_UPSTREAM_XCOM_DATA: True,
                AIRFLOW_IGNORE_API_CALL_ERRORS: True,
            },
            execution_timeout=timedelta(seconds=timeout),
        )

        calc_trigger_wait = HttpDeferrableSensor(
            calculator=calculator,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=ES_EVENT_ENDPOINT,
            task_id=f"CHECK_{calculator.name.upper()}_CALCULATOR_STATUS",
            poke_interval=DEFAULT_POKE_INTERVAL,
            extra_args={
                AIRFLOW_UPSTREAM_TASK_ID: f"{group_id}.{display_calc_run_details_id}"
            },
            execution_timeout=timedelta(seconds=timeout),
        )

        if obs_enabled:
            obs_start_run = create_obs_start_task(
                group_id=group_id,
                display_calc_run_details_id=display_calc_run_details_id,
                calculator=calculator,
            )
            generate_context >> send_edf_event >> display_calc_run_details >> obs_start_run >> calc_trigger_wait
        else:
            generate_context >> send_edf_event >> display_calc_run_details >> calc_trigger_wait

    return calculator_mapped_task_group.partial().expand(dynamic_args=dynamic_args)


def mapped_calc_task_group_component(
    group_id: str,
    calculator: BaseCalculator,
    calc_init_func: TaskCallbackFunction,
    timeout: float,
    datasets: Union[list, None] = None,
    pre_conditions: Union[list, None] = None,
    calc_deferrable_preconditions: Union[list[CalcTriggerCriteria], None] = None,
    **kwargs
):
    @task_group(group_id=group_id, ui_color="#46cae4", prefix_group_id=False)
    def calc_group():
        trigger_rule = kwargs.get(TRIGGER_RULE_KEY, TriggerRule.ALL_SUCCESS)
        calculator.config[RUN_COMPLETION_CHECK_BY_MEG_EVENT] = kwargs.get(RUN_COMPLETION_CHECK_BY_MEG_EVENT, False)

        pre_conditions_task_groups = None
        if pre_conditions is not None:
            pre_conditions_task_groups = GenericCriteriaTaskGroup(
                pre_conditions=pre_conditions, trigger_rule=trigger_rule
            )

        execution_timeout = timedelta(seconds=timeout)
        deferrable_sensor_args = dict()
        if not pre_conditions_task_groups:
            deferrable_sensor_args["trigger_rule"] = trigger_rule

        dataset_task_groups = []
        if datasets is not None:
            for dataset in datasets:
                dataset_task = _create_dataset_task(dataset, execution_timeout, deferrable_sensor_args)
                if dataset_task:
                    dataset_task_groups.append(dataset_task)

        if calc_deferrable_preconditions:
            for calc_event_criteria in calc_deferrable_preconditions:
                calc_task_group = CalcEventCompletionCriteriaTask(
                    calc_event_criteria=calc_event_criteria,
                    execution_timeout=execution_timeout,
                    **deferrable_sensor_args,
                )
                dataset_task_groups.append(calc_task_group)

        group_id_upper = normalize_group_id(group_id)

        if pre_conditions_task_groups or dataset_task_groups:
            calc_initialize = generic_task.override(task_id=f"{group_id_upper}_CALC_INIT")(calc_init_func)
        else:
            calc_initialize = generic_task.override(task_id=f"{group_id_upper}_CALC_INIT", trigger_rule=trigger_rule)(
                calc_init_func
            )

        dynamic_group_task = create_dynamic_calc_task_group(
            calculator=calculator,
            group_id=f"{group_id_upper}_MAPPED_GROUP",
            timeout=timeout,
            dynamic_args=calc_initialize,
            obs_enabled=resolve_obs_enabled(kwargs),
        )
        sensor_task_name = f"{group_id_upper}_MAPPED_GROUP.CHECK_{calculator.name.upper()}_CALCULATOR_STATUS"
        result_task_name = f"{group_id_upper}_RESULT"

        obs_enabled = resolve_obs_enabled(kwargs)
        calc_result = calc_run_aggregator_task(
            task_id=result_task_name,
            upstream_task_id=sensor_task_name,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS if obs_enabled else TriggerRule.ALL_SUCCESS,
        )

        # Build the upstream chain, then wire the sequential post-processing tasks.
        if pre_conditions_task_groups and dataset_task_groups:
            pre_conditions_task_groups >> dataset_task_groups >> calc_initialize >> dynamic_group_task
        elif pre_conditions_task_groups:
            pre_conditions_task_groups >> calc_initialize >> dynamic_group_task
        elif dataset_task_groups:
            dataset_task_groups >> calc_initialize >> dynamic_group_task
        else:
            calc_initialize >> dynamic_group_task

        dynamic_group_task >> calc_result

    group = calc_group()

    if resolve_obs_enabled(kwargs):
        group_id_upper = normalize_group_id(group_id)
        result_task_name = f"{group_id_upper}_RESULT"
        start_task_name = f"{group_id_upper}_MAPPED_GROUP.OBS_POST_START"

        obs_complete_task = create_obs_complete_task(
            task_id=f"{group_id_upper}_OBS_POST_COMPLETE",
            result_task_name=result_task_name,
            start_task_name=start_task_name,
        )
        group >> obs_complete_task

    return group


def _create_dataset_task(dataset, execution_timeout, deferrable_sensor_args):
    """Helper function to create dataset tasks based on input type"""
    try:
        # Assuming DatasetEventCriteria definition exists elsewhere in dependencies
        if hasattr(dataset, '_asdict'): 
            return BasicDatasetEventCriteriaTask(
                **dataset._asdict(),
                execution_timeout=execution_timeout,
                **deferrable_sensor_args
            )

        elif isinstance(dataset, dict):
            if EVENT_SOURCE not in dataset:
                raise ValueError(f"Dictionary dataset missing required '{EVENT_SOURCE}' dataset: {dataset}")

            dataset_name = any_match(dataset, keys=["dataset_name", "name"])
            if not dataset_name:
                raise ValueError(f"Dictionary dataset missing dataset_name in: {dataset}")

            event_source = dataset[EVENT_SOURCE]

            if event_source == EventSource.MEGDP:
                return DatasetMegEventCriteriaTaskGroup(
                    group_id=f"CHECK_{dataset_name}",
                    dataset_name=dataset_name,
                    timeout=execution_timeout,
                    **deferrable_sensor_args,
                )
            elif event_source == EventSource.MERIVAL:
                return create_merival_ingestion_task(dataset_name, execution_timeout, **deferrable_sensor_args)
            else:
                raise ValueError(f"Unsupported event_source: {event_source} passed in dataset: {dataset}")

        elif isinstance(dataset, str):
            return create_merival_ingestion_task(dataset, execution_timeout, **deferrable_sensor_args)

        else:
            raise ValueError(f"Unsupported dataset type: {type(dataset)} for dataset: {dataset}")

    except Exception as e:
        logger.exception(f"Error creating dataset tasks for {dataset}: {e}")
        raise


def create_merival_ingestion_task(
    dataset_name: str, execution_timeout, trigger_rule=TriggerRule.ALL_SUCCESS, **kwargs
):
    return BasicDatasetEventCriteriaTask(
        dataset_name=dataset_name,
        static_params_map={"source": EventSource.MERIVAL.value, "TYPE": "INGESTION"},
        context_param_keys=["FREQUENCY", "contextId", "LBD"],
        event_source=EventSource.MERIVAL,
        execution_timeout=execution_timeout,
        trigger_rule=trigger_rule,
        **kwargs,
    )


def normalize_group_id(group_id: str) -> str:
    normalized_group_id = group_id.rsplit("_CALC", maxsplit=1)[0] if group_id.endswith("_CALC") else group_id
    return normalized_group_id.upper()


# Place holder for reference within class scopes
def create_pre_condition_tasks(pre_conditions, trigger_rule, task_group):
    pass