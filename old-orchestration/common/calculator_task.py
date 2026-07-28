import datetime
import json
import logging
import time
from datetime import timedelta
from typing import Any, Dict, List, Union

# Assuming appropriate imports exist from airflow and local context frameworks
# from airflow.utils.context import Context

logger = logging.getLogger(__name__)

# Mock placeholders for constants and types used throughout the snippet
PREVIOUS_CONTEXT_ID = "PREVIOUS_CONTEXT_ID"
INCLUDE_DAG_INFO = "INCLUDE_DAG_INFO"
CALC_TRIGGER_CONTEXT_ID = "CALC_TRIGGER_CONTEXT_ID"
MANUAL_RUN_PARAMS = type("MANUAL_RUN_PARAMS", (), {"CONTEXT_ID": "CONTEXT_ID"})
FRCA_MEGDP_SERVICE_CONN_KEY = "FRCA_MEGDP_SERVICE_CONN_KEY"
ES_EVENT_ENDPOINT = "ES_EVENT_ENDPOINT"
DEFAULT_POKE_INTERVAL = 60

class BaseCalculator:
    name: str = "Base"
    calc_type: str = "Generic"
    config: dict = {}
    def get_derived_name(self, calc_run_params: dict) -> str: return self.name


class CreateContextHandler:
    """A Callback handler for EDF context creation. It's auto injected with:
    `context`: Airflow context object
    `config`: Airflow variable config
    `calc_run_params`: runtime parameters for the calculator trigger
    `xcom_data`: Previous task's xcom data
    `kwargs`: Extra named arguments

    If callback_function returns `skip_task = True`, then the task will be skipped.

    Args:
        calculator (BaseCalculator): The calculator instance.

    Returns:
        dict: A dictionary containing created `context_id`
    """

    def __init__(self, calculator: BaseCalculator) -> None:
        self.calculator = calculator
        self.calc_name = calculator.name

    def _derive_run_number(self, config: dict, calc_run_params: dict, dag_run_conf: dict) -> Union[int, None]:
        """Derives the run number utilizing the parent run lookup workflow."""
        return self._get_parent_run_number(config, calc_run_params, dag_run_conf)

    def _get_parent_run_number(self, config: dict, calc_run_params: dict, dag_run_conf: dict) -> Union[int, None]:
        # Try to resolve the triggering context ID from calc_run_params first,
        # then fall back to the full dag_run.conf which still has the nested context dict
        context_id = get_trigger_context_id(calc_run_params) or get_trigger_context_id(dag_run_conf)

        if not context_id:
            logger.info(
                f"run_number parent fallback: no trigger context_id found in calc_run_params "
                f"keys={list(calc_run_params.keys()) if calc_run_params else []} "
                f"or dag_run_conf keys={list(dag_run_conf.keys()) if dag_run_conf else []}, skipping"
            )
            return None

        try:
            # full=True (default) is required so that parentids are included in the response
            current_context = get_context(config=config, context_id=context_id)
        except Exception:
            logger.exception(f"Error fetching context for run_number fallback: [context_id={context_id}]")
            return None

        # Check the trigger context's own data first
        trigger_data = current_context.get("data") or {}
        logger.info(
            f"run_number fallback: checking trigger context_id={context_id} data keys={list(trigger_data.keys())}"
        )
        
        run_number = _extract_run_number(trigger_data)
        parent_id = current_context.get("parentId")
        
        if run_number:
            logger.info(f"run_number={run_number} found in parent context.data [context_id={parent_id}]")
            return run_number

        logger.info(f"run_number not found in parent context.data [context_id={parent_id}]")
        logger.info(f"run_number not found in any parent context for context_id={context_id}, defaulting to None")
        return None

    def __call__(
        self, context: Any, config: dict, calc_run_params: dict, xcom_data: Any, **kwargs
    ) -> Union[dict, list[dict]]:
        xcom_data = xcom_data if xcom_data is not None and isinstance(xcom_data, dict) else {}
        dag_run = context["dag_run"]
        context_id_list = get_context_id_list(calc_run_params, context)

        merged_args = {**calc_run_params, **kwargs}
        merged_args[PREVIOUS_CONTEXT_ID] = context_id_list
        
        if merged_args.get(INCLUDE_DAG_INFO, "True") == "True":
            merged_args["dag_id"] = dag_run.dag_id
            merged_args["run_id"] = dag_run.run_id

        clear_metadata_info(merged_args)

        self.calc_name = self.calculator.get_derived_name(calc_run_params=calc_run_params)
        
        # _derive_run_number must be called after get_derived_name so self.calc_name is fully resolved
        # dag_run.conf preserves the original nested event structure needed for parent context lookup
        dag_run_conf = dag_run.conf or {}
        merged_args["run_number"] = self._derive_run_number(config, calc_run_params, dag_run_conf)
        logger.info(f"create_context_handler: run_number={merged_args['run_number']} set for calc={self.calc_name}")

        payload = create_edf_context_payload(config, self.calc_name, merged_args)
        context_id = create_edf_context(config, payload)
        logger.info(f"context_id: {context_id} created for calculator: {self.calc_name} in the create_context_handler")

        if not is_manual_run(calc_run_params):
            manual_run_params_log_message = create_manual_trigger_log(
                {**calc_run_params, MANUAL_RUN_PARAMS.CONTEXT_ID: context_id_list}
            )
            logger.info(
                decorated_message(f"CalcManualRunParams for running DAG manually:\n{manual_run_params_log_message}")
            )

        return {CALC_TRIGGER_CONTEXT_ID: context_id}

    def get_calc_name(self) -> str:
        return self.calc_name

    def get_calculator(self) -> BaseCalculator:
        return self.calculator


class EdfTaskPublishHandler:
    """Callback handler for EDF task submission.

    It is automatically injected with:
        - context: Airflow context object
        - config: Airflow variable config
        - calc_run_params: Runtime parameters for the calculator
        - xcom_data: Previous task's XCom data
        - kwargs: Extra named arguments

    If the callback function returns `skip_task = True`, the task is skipped.

    Args:
        calculator (BaseCalculator): Calculator instance.

    Returns:
        dict | list[dict]: A dictionary (or list of dictionaries) containing CALC_TRIGGER_CONTEXT_ID.
    """

    def __init__(self, calculator: BaseCalculator) -> None:
        self.calculator = calculator
        self.calc_name = calculator.name

    def __call__(
        self,
        context: Any,
        config: dict,
        calc_run_params: dict,
        xcom_data: Any,
        **kwargs,
    ) -> Union[dict, list[dict]]:
        if hasattr(xcom_data, '__iter__') and not isinstance(xcom_data, (dict, str)): 
            # Simplified proxy check replacing the specific LazyXComSelectSequence behavior evaluation
            mapped_index = context["ti"].map_index
            xcom_data_list = list(
                filter(
                    lambda d: isinstance(d, dict) and CALC_TRIGGER_CONTEXT_ID in d,
                    list(xcom_data),
                )
            )
            xcom_data = xcom_data_list[mapped_index]

        context_id = xcom_data.get(CALC_TRIGGER_CONTEXT_ID)
        merged_args = {**calc_run_params, **kwargs}
        self.calc_name = self.calculator.get_derived_name(calc_run_params)

        task_id = get_task_id(
            config=self.calculator.config,
            name=self.calc_name,
            calc_type=self.calculator.calc_type,
        )

        payload = create_edf_event_payload(
            config,
            self.calc_name,
            merged_args,
            context_id,
            task_id,
        )
        event_id = publish_edf_event(config, payload)
        logger.info(
            f"EDF event: {event_id} published for calculator: {self.calc_name} in the publish_edf_event_handler"
        )
        return {CALC_TRIGGER_CONTEXT_ID: context_id}

    def get_calc_name(self) -> str:
        return self.calc_name

    def get_calculator(self) -> BaseCalculator:
        return self.calculator


class CalculatorTaskManager:
    """A class for calculator task management. Provides utility functions for
    various types of calculator task creation.

    `create_context_task`: Creates an EDF context generation task
    `publish_edf_event_task`: Create an EDF event publishing task
    `calculator_task`: Creates a Meg calculator task
    """

    def __init__(self, calculator: BaseCalculator) -> None:
        self.calculator = calculator

    def calculator_task(self, timeout: int, extra_args: dict = {}):
        context_task = self.create_context_task(extra_args=extra_args)
        edf_task = self.publish_edf_event_task(extra_args=extra_args)

        calc_run_task = HttpDeferrableSensor(
            calculator=self.calculator,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=ES_EVENT_ENDPOINT,
            task_id=f"RUN_{self.calculator.name}_CALCULATOR",
            poke_interval=DEFAULT_POKE_INTERVAL,
            extra_args=extra_args,
            execution_timeout=timedelta(seconds=timeout),
        )
        return [context_task, edf_task, calc_run_task]

    def create_context_task(
        self,
        task_id: str = "CREATE_CONTEXT",
        task_group=None,
        mapped_args=None,
        extra_args: dict = {},
    ):
        """A task for generating EDF context."""
        return generic_task.override(task_id=task_id, task_group=task_group)(
            CreateContextHandler(self.calculator),
            mapped_args=mapped_args,
            extra_args=extra_args
        )

    def publish_edf_event_task(
        self,
        task_id: str = "PUBLISH_EDF_EVENT",
        task_group=None,
        mapped_args=None,
        extra_args: dict = {}
    ):
        """A task for submitting an event to EDF.

        Args:
            task_id (str, optional): The task_id for this task. Defaults to "PUBLISH_EDF_EVENT".
            task_group (str, optional): The task_group to which this task will register to.
            mapped_args (dict, optional): dynamic args to create a mapped task at runtime.
            extra_args (dict, optional): Any optional extra arguments as dictionary.
        """
        return generic_task.override(task_id=task_id, task_group=task_group)(
            EdfTaskPublishHandler(self.calculator),
            mapped_args=mapped_args,
            extra_args=extra_args
        )