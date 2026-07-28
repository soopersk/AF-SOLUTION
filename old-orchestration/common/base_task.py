from typing import Any, Optional, Union, Protocol, cast, runtime_checkable
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

# Assuming these classes and helpers are imported in your project
# from airflow.models.xcom import LazyXComSelectSequence
# from your_utils import BaseCalculator, get_xcom_key, decorated_message, push_xcom, ...

@runtime_checkable
class TaskCallbackFunction(Protocol):
    """A callback protocol for generic airflow task."""
    
    def __init__(self):
        pass

    def __call__(
        self, 
        context: Context, 
        config: dict, 
        calc_run_params: dict, 
        xcom_data: Any, 
        **kwargs: Any
    ) -> Union[dict, list[dict]]:  # Fixed Union/list brackets
        pass

    def get_calc_name(self) -> str:
        """Provides the calculator's Meg registered name"""
        pass

    def get_calculator(self) -> BaseCalculator:
        """Provides a calculator of type BaseCalculator"""
        pass


@task(task_id="CALC_START") 
def calc_start(**context):
    logger.info(decorated_message("Calculator orchestration STARTING"))

    run_params = context["dag_run"].conf
    xcom_key = get_xcom_key(context)

    # Fixed missing parenthesis
    logger.info(decorated_message(f"DAG started with xcom_key=[{xcom_key}], run_params=[{run_params}]"))

    logger.info("Validating dataset pre-condition check for Manual Run")
    check_dataset_ingestion_pre_condition(run_params)
    
    if not is_manual_run(run_params):
        context_id_list = get_context_id_list(run_params, context)
        manual_run_params_log_message = create_manual_trigger_log(
            {**run_params, MANUAL_RUN_PARAMS.CONTEXT_ID: context_id_list}
        )
        # Fixed broken f-string and logger format
        logger.info(decorated_message(f"CalcManualRunParams for running DAG manual: {manual_run_params_log_message}"))


@task(task_id="CALC_END") 
def calc_end(**context):
    # Standardized 'Logger' to 'logger' if matching your logging setup
    logger.info(decorated_message("Calculator orchestration FINISHED"))


@task  
def generic_task(
    callback_function: TaskCallbackFunction,
    trigger=TriggerRule.ALL_SUCCESS,
    mapped_args=None,
    extra_args: Optional[dict] = None,
):
    # Cleaned up redundant/broken definitions and nested conditions
    if extra_args is None:
        extra_args = {}
        
    context = get_current_context()
    config = get_config()
    xcom_key = get_xcom_key(context)
    xcom_data = context["ti"].xcom_pull(key=xcom_key)  # Fixed bracket typo
    mapped_index = context['ti'].map_index
    calc_run_params = {}

    if mapped_args is not None:
        calc_run_params = mapped_args
        if isinstance(xcom_data, list):
            xcom_data = xcom_data[mapped_index]
    elif mapped_index != -1:
        upstream_task_id = get_full_task_id(context['ti'].task_id, str(extra_args.get(AIRFLOW_UPSTREAM_TASK_ID)))
        xcom_data = context["ti"].xcom_pull(
            task_ids=upstream_task_id,
            map_indexes=mapped_index if mapped_index else None,  # Fixed '-' to '='
            key=xcom_key,
        ) 

    if isinstance(xcom_data, LazyXComSelectSequence):
        xcom_data = xcom_data[0]

    if not calc_run_params:
        if xcom_data and not isinstance(xcom_data, list) and xcom_data.get(CALC_RUN_PARAMS):
            calc_run_params = xcom_data.get(CALC_RUN_PARAMS)
        else:
            calc_run_params = get_runtime_params(context)

    # Corrected method execution brackets
    task_out = callback_function(
        context=context, 
        config=config, 
        calc_run_params=calc_run_params, 
        xcom_data=xcom_data, 
        **extra_args
    )

    if task_out is not None and isinstance(task_out, dict):
        if not task_out.get("CALC_RUN_PARAMS"):
            task_out["CALC_RUN_PARAMS"] = calc_run_params
        if task_out.get("SKIP_TASK", False):
            logger.warning(f"Task: {context['task'].task_id} is marked for skipping.")
            logger.info(decorated_message(f"Task: {context['task'].task_id} SKIPPED"))
            raise AirflowSkipException

    context["ti"].xcom_push(key=xcom_key, value=task_out)
    return task_out


def create_dynamic_tasks(
    callback_function: TaskCallbackFunction,
    task_id="GENERIC_MAPPED_TASK",
    trigger=TriggerRule.ALL_SUCCESS,  # Fixed '-' to '='
    dynamic_args: list[dict[str, str]] = None,  # Fixed list syntax and default initialization
):
    if dynamic_args is None:
        dynamic_args = []

    # Closed multi-line task execution call correctly
    task_out_list = (
        generic_task.override(task_id=task_id, trigger_rule=trigger)
        .partial(callback_function=callback_function)
        .expand(extra_args=dynamic_args)
    )
    return task_out_list


def start_task(task_id: str = "START"):
    callback_function = cast(
        TaskCallbackFunction, 
        lambda context, config, calc_run_params, xcom_data, **kwargs: kwargs.get(AIRFLOW_XCOM_DATA, {})
    )
    return generic_task.override(task_id=task_id)(callback_function)


def xcom_aggregator_task(task_id: str, task_group=None, upstream_task_id=None, mapped_index=None):

    @task(task_id=task_id, task_group=task_group)
    def xcom_aggregator(**context):
        pulled_xcom = context["ti"].xcom_pull(
            task_ids=upstream_task_id,
            map_indexes=mapped_index,
            key=get_xcom_key(context),
        )

        if isinstance(pulled_xcom, LazyXComSelectSequence):  # Added missing ':'
            pulled_xcom = list(pulled_xcom)

        push_xcom(context, pulled_xcom)
        return pulled_xcom

    return xcom_aggregator()