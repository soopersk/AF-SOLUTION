from abc import ABC, abstractmethod
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Protocol

# Assuming appropriate imports exist from Airflow and orchestration common layers
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

# Placeholders for global context mappings/functions used throughout
logger = logging.getLogger(__name__)
CALC_EVENT_TYPE = "CALC_EVENT"
CALC_EVENT_STATE_FINISH = "FINISH"
TRIGGER_CONDITION = namedtuple("TRIGGER_CONDITION", ["DATASET", "CALC", "SOURCE", "PIPELINEID"])(
    "DATASET", "CALC", "SOURCE", "PIPELINEID"
)
iso_region_mapping = {}


@dataclass
class CalcEventCriteria:
    calc_identifier: str
    calculator: Any  # BaseCalculator
    static_params: Dict[str, Any] = field(default_factory=dict)
    dynamic_params_keys: List[str] = field(default_factory=list)
    get_params_callback_function: Callable[[dict], dict] = lambda calc_run_params: dict()


DatasetEventCriteria = namedtuple(
    typename="DatasetEventCriteria",
    field_names=["dataset_name", "event_source", "unique_identifier", "static_params_map", "context_param_keys"],
    defaults=[None, None, None, None],  # Adjusted default matching rule length
)


TriggerCondition = namedtuple(
    typename="TriggerCondition",
    field_names=["trigger", "condition", "region", "is_iso_region"],
    defaults=[None, None, None, False],
)


CalcRegionCompanyCodeReadyCheck = namedtuple(
    typename="CalcRegionCompanyCodeReadyCheck",
    field_names=[
        "all_regions_ready",
        "ready_regions",
        "not_ready_regions",
        "all_companycodes_ready",
        "ready_companycodes",
        "not_ready_companycodes",
        "contextids",
    ],
    defaults=[False, None, None, False, None, None, None],
)


class CalcTriggerCriteria(Protocol):
    """A calculator run pre-requisite condition protocol"""
    @property
    def name(self) -> str:
        pass

    @property
    def uuid(self) -> str:
        pass

    def check(self, params: dict, **kwargs) -> bool:
        pass


def create_pre_condition_tasks(
    pre_conditions: list[CalcTriggerCriteria], 
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS, 
    task_group: Any = None
):
    pre_condition_tasks = []
    for condition in pre_conditions:
        if isinstance(condition, CalcCriteriaTask):
            pre_condition_tasks.append(condition.task(task_group))
        else:
            @task(task_id=f"{condition.name.upper()}_CONDITION", trigger_rule=trigger_rule, task_group=task_group)
            def pre_condition_task(criteria=condition, **context):
                run_params = context["dag_run"].conf
                if not run_params:
                    run_params = context["params"]
                cond_passed = False
                logger.info(f"run_params at create_pre_condition_tasks: {run_params}")
                
                if is_manual_run(run_params) and not isCheckParamPrecondition(run_params):
                    logger.info("bypass pre-condition checking with manual triggering")
                    return True

                try:
                    cond_passed = criteria.check(params=run_params)
                except Exception as exc:
                    logger.exception(f"Exception occurred during pre-condition check: {criteria.name}")
                    raise AirflowFailException(f"Failed during pre-condition check: {criteria.name}") from exc

                if not cond_passed:
                    raise AirflowSkipException(
                        f"Trigger pre-condition criteria: [{criteria.name}] FAILED! Skipping the task!!!"
                    )

                return cond_passed

            pre_condition_tasks.append(pre_condition_task())
            
    return pre_condition_tasks


def is_merival_dataset_completion_event(params: dict) -> bool:
    source = traverse_dict(params, keys=["event", "source"], default="")
    if source.upper() == "MERIVAL":
        dataset_event_type = traverse_dict(params, keys=["event", "additionalData", "TYPE"], default="")
        dataset_name = traverse_dict(params, keys=["event", "additionalData", "DATASET_NAME"], default="")
        if dataset_name and dataset_event_type.upper() == "INGESTION":
            return True
    return False


def is_meg_dataset_completion_event(params: dict) -> bool:
    """Determines if the event indicates a MEG dataset completion event."""
    additional_data = traverse_dict(params, keys=["event", "additionalData"], default={})
    event_type = additional_data.get("megdpEventType", "").upper()

    # Check whether ingestion-event is dataset-curation-event
    update_type = additional_data.get("updateType", "").upper()
    if event_type == "DATA-UPDATE" and update_type == "CURATION":
        return True

    # Check whether ingestion-event is dataset-scheduled-event
    task_event_type = additional_data.get("taskEventType", "").upper()
    if event_type == "TASK-EVENT" and task_event_type == "SCHEDULED":
        return True

    return False


def is_calc_completion_event(params: dict) -> bool:
    event_type = traverse_dict(params, keys=["event", "additionalData", "type"])
    if not event_type or event_type.upper() != CALC_EVENT_TYPE:
        return False

    event_state = traverse_dict(params, keys=["event", "additionalData", "STATE"])
    return event_state and event_state.upper() == CALC_EVENT_STATE_FINISH


def is_dataset_completion_event(event_source, params):
    """Check if the event represents a DATASET completion event"""
    if event_source == EventSource.MERIVAL:
        return is_merival_dataset_completion_event(params)
    elif event_source == EventSource.MEGDP:
        return is_meg_dataset_completion_event(params)
    return False


def is_source_completion_event(params):
    source = traverse_dict(params, keys=["source"])
    return bool(source)


def get_trigger_condition(trigger_condition: str) -> TriggerCondition:
    trigger_conditions = trigger_condition.split(sep=", ", maxsplit=2) if trigger_condition else []

    if len(trigger_conditions) < 2 or not trigger_conditions[0] or not trigger_conditions[1]:
        raise ValueError(f"Invalid trigger-condition: {trigger_condition=}")

    typed_trigger_condition = (
        TriggerCondition(
            trigger_conditions[0],
            trigger_conditions[1],
            trigger_conditions[2],
        )
        if len(trigger_conditions) == 3 and trigger_conditions[2] in iso_region_mapping.keys()
        else TriggerCondition(trigger_conditions[0], trigger_conditions[1])
    )

    supported_trigger_conditions = set([tc for tc in TRIGGER_CONDITION])
    if typed_trigger_condition.trigger not in supported_trigger_conditions:
        raise ValueError(
            f"Unsupported trigger-condition: expected={supported_trigger_conditions}, actual={typed_trigger_condition.trigger}"
        )

    supported_regions = set(list(iso_region_mapping.keys()) + list(iso_region_mapping.values()))
    if typed_trigger_condition.region and typed_trigger_condition.region not in supported_regions:
        raise ValueError(
            f"Unsupported trigger-region: expected={supported_regions}, actual={typed_trigger_condition.region}"
        )
    return typed_trigger_condition


def create_event_completion_criteria(trigger_condition: str):
    try:
        supported_trigger_condition = get_trigger_condition(trigger_condition)

        if supported_trigger_condition.trigger == TRIGGER_CONDITION.DATASET:
            return DatasetEventCompletionCriteria(
                name=supported_trigger_condition.condition,
                trigger_condition=supported_trigger_condition
            )
        elif supported_trigger_condition.trigger == TRIGGER_CONDITION.CALC:
            return CalcEventCompletionCriteria(
                name=supported_trigger_condition.condition,
                trigger_condition=supported_trigger_condition
            )
        elif supported_trigger_condition.trigger == TRIGGER_CONDITION.SOURCE:
            return SourceEventCompletionCriteria(
                name=supported_trigger_condition.condition,
                trigger_condition=supported_trigger_condition
            )
        elif supported_trigger_condition.trigger == TRIGGER_CONDITION.PIPELINEID:
            return PipelineIDEventCompletionCriteria(
                name=supported_trigger_condition.condition,
                trigger_condition=supported_trigger_condition
            )
    except (ValueError, KeyError):
        logger.exception(f"Received INVALID trigger-condition [trigger_condition={trigger_condition}]")
        raise AirflowSkipException(
            f"Trigger pre-condition criteria: [{trigger_condition}] FAILED due to invalid trigger-condition! Skipping execution."
        )


class CalcCriteria(ABC):
    def __init__(self, name: str):
        self._name = name
        self.config = get_config()

    @abstractmethod
    def check(self, params: dict, **kwargs) -> bool:
        """Abstract method delegated to sub-class."""
        raise NotImplementedError()

    @property
    def name(self) -> str:
        return self._name


class CalcCriteriaTask(CalcCriteria):
    @abstractmethod
    def task(self, task_group):
        pass


class EventCompletionCriteria(CalcCriteria):
    def __init__(self, name: str, trigger_condition: TriggerCondition):
        super().__init__(name)
        self._trigger_condition = trigger_condition

    @property
    def trigger_condition(self) -> TriggerCondition:
        return self._trigger_condition if self._trigger_condition else TriggerCondition()


class DatasetEventCompletionCriteria(EventCompletionCriteria):
    """Pre-condition class for checking completion and availability of `dataset`"""

    def __init__(self, name: str, trigger_condition: TriggerCondition = None, dataset_id=None):
        super().__init__(name, trigger_condition)
        self._dataset_id = dataset_id

    def check(self, params: dict, **kwargs) -> bool:
        if not params or not search_dict(params.get('event', {}), key='source'):
            logger.warning(decorated_message(f"Trigger pre-condition: [{self.name}] FAILED! Invalid params"))
            return False

        event_source = search_dict(params['event'], key='source')

        if not is_dataset_completion_event(event_source, params):
            logger.warning(
                decorated_message(
                    f"Trigger pre-condition: [{self.name}] FAILED! Trigger-event from source: {event_source}"
                )
            )
            return False

        try:
            if event_source == EventSource.MERIVAL:
                event_dataset_id = traverse_dict(params['event']['additionalData'], keys=['DATASET_UUID'])
                reporting_date = convert_to_standard_date(
                    traverse_dict(params['event']['additionalData'], keys=['region'])
                )
                region = traverse_dict(params['event']['additionalData'], keys=['region'])
                context_id = traverse_dict(params['event'], keys=['contextId'])
            else:
                event_dataset_id = traverse_dict(params['event']['additionalData'], keys=['datasetId'])
                reporting_date = traverse_dict(params['event']['context']['data'], keys=['reporting-date'])
                region = traverse_dict(params['event']['context']['data'], keys=['region'])
                context_id = traverse_dict(params['event']['additionalData'], keys=['context_id'])

            if event_dataset_id is None:
                event_dataset_id = any_match(params['context']['data'], keys=['datasetId', 'DATASET_UUID'])

            frequency = traverse_dict(params['context']['data'], keys=['frequency'])
            h3_region = traverse_dict(params['context']['data'], keys=['h3Region'])

            if self.uuid != event_dataset_id:
                logger.warning(
                    decorated_message(
                        f"Trigger pre-condition: [{self.name}] FAILED! "
                        f"Expected: [dataset_id={self.uuid}], Actual: [dataset_id={event_dataset_id}]"
                    )
                )
                return False

            if (
                self.trigger_condition.region
                and self.trigger_condition.is_iso_region
                and (not h3_region or self.trigger_condition.region.upper() != h3_region.upper())
            ):
                logger.warning(
                    decorated_message(
                        f"Trigger pre-condition: [{self.name}] FAILED! Expected: [dataset_id={self.uuid}, h3region={h3_region}] "
                        f"Actual: [dataset_id={event_dataset_id}, h3region={self.trigger_condition.region}]"
                    )
                )
                return False

            if (
                self.trigger_condition.region
                and not self.trigger_condition.is_iso_region
                and (not region or self.trigger_condition.region.upper() != region.upper())
            ):
                logger.warning(
                    decorated_message(
                        f"Trigger pre-condition: [{self.name}] FAILED! Expected: [dataset_id={self.uuid}, region={region}] "
                        f"Actual: [dataset_id={event_dataset_id}, region={self.trigger_condition.region}]"
                    )
                )
                return False

            logger.info(
                decorated_message(
                    f"Trigger pre-condition: [{self.name}] SUCCEEDED! Expected: [dataset_id={self.uuid}]; "
                    f"Actual: [dataset_id={event_dataset_id}] for "
                    f"[reporting_date={reporting_date}, frequency={frequency}, context_id={context_id}]"
                )
            )
            return True

        except KeyError:
            logger.exception(f"Received INVALID payload [params={params}] for [dataset={self.name}] event comp")
            raise ValueError(f"Received INVALID payload for [dataset={self.name}] event completion")

    @property
    def uuid(self) -> str:
        if self._dataset_id is None:
            self._dataset_id = get_dataset_uuid(self.config, self.name)
        return self._dataset_id