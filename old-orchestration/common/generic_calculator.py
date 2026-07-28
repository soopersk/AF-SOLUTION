import logging

from airflow.exceptions import AirflowException
from orchestration.common.af_utils import (
    clear_metadata_info,
    create_calc_run_details_log,
    decorated_message,
    push_xcom,
)
from orchestration.common.base_calculator import BaseCalculator
from orchestration.common.collection_utils import search_dict, traverse_dict
from orchestration.common.constants import (
    CALC_EVENT_TYPE,
    CALC_GENERATED_CONTEXT_ID,
    CALC_RUN_STATUS_CHECK_KEY,
    CALC_TRIGGER_CONTEXT_ID,
    ES_EVENT_ENDPOINT,
    MEG_CALC_TYPE_CALCULATOR,
    RUN_COMPLETION_CHECK_BY_MEG_EVENT,
)
from orchestration.common.edf_util import trigger_calculator_task
from orchestration.common.eventservice import fetch_data_from_event_service

logger = logging.getLogger(__name__)


class SimpleCalculator(BaseCalculator):
    """A Simple place holder calculator"""

    def __init__(
        self, name="Calculator", region_comp_group=None, calc_type=MEG_CALC_TYPE_CALCULATOR
    ):
        super().__init__(name, region_comp_group, calc_type)

    def process(self, context, run_params, **xargs):
        logger.info(f"{self.name} triggered with params: [{xargs}]")

        ctx_id = xargs.get(CALC_TRIGGER_CONTEXT_ID, "-")

        if ctx_id == "-":
            enriched_event = xargs.get("enriched_event", {})
            if enriched_event:
                ctx_id = (
                    traverse_dict(enriched_event, keys=["event", "additionalData", "triggerContextId"])
                    or traverse_dict(enriched_event, keys=["event", "additionalData", "contextId"])
                    or traverse_dict(enriched_event, keys=["event", "contextId"])
                    or "-"
                )

        # IF RUN_COMPLETION_CHECK_BY_MEG_EVENT is enabled, use MEG task-event parameters
        if self.config.get(RUN_COMPLETION_CHECK_BY_MEG_EVENT, False):
            return {"contextId": ctx_id, "taskEventType": "COMPLETED", "successful": "true|false"}

        # Default behavior - use existing CALC_EVENT parameters
        return {CALC_RUN_STATUS_CHECK_KEY: ctx_id, "type": CALC_EVENT_TYPE, "STATE": "FINISH|FAILED"}

    def post_process(self, context, response):
        logger.debug(f"{self.name} post process running with response {response}")

        try:
            # Check if response uses MEG task-event format (successful field)
            # or the existing CALC_EVENT format (STATE field)
            if self.config.get(RUN_COMPLETION_CHECK_BY_MEG_EVENT, False):
                event_data = traverse_dict(response[0], keys='event') or response[0]
                additional_data = traverse_dict(event_data, keys='additionalData')
                event_successful = additional_data.get('successful', None) if additional_data else None
                calc_context_id = traverse_dict(event_data, keys='contextId')

                # Map successful=true to FINISH, successful=false to FAILED
                event_successful_normalized = str(event_successful).lower() if event_successful is not None else None
                if event_successful_normalized == "true":
                    event_status = "FINISH"
                else:
                    if event_successful_normalized != "false":
                        logger.warning(f"Unexpected 'successful' value: {event_successful}, treating as FAILED")
                    event_status = "FAILED"
            else:
                event_status = traverse_dict(response[0]['event'], keys=['additionalData', 'STATE'])
                calc_context_id = traverse_dict(response[0]['event'], keys='contextId')

        except Exception as exc:
            logger.exception("Error fetching status of calculator finish event")
            raise AirflowException(f"{self.name} run failed!") from exc

        downstream_xcom = {CALC_GENERATED_CONTEXT_ID: calc_context_id}
        push_xcom(context, downstream_xcom)

        if event_status in ("FAILED", None):
            raise AirflowException(f"{self.name} run failed!")

        return downstream_xcom


class TaskTriggeringCalculator(SimpleCalculator):
    """Implements process method to trigger a Meg calculator job"""

    def __init__(self, name, region_comp_group=None, calc_type: str = MEG_CALC_TYPE_CALCULATOR) -> None:
        super().__init__(name, region_comp_group, calc_type)

    def process(self, context, run_params, **xargs):
        clear_metadata_info(xargs)
        logger.info(f"{self.name} Triggered with params: [{xargs}]")
        response = trigger_calculator_task(self.config, self, xargs)
        return response


class DisplayCalcRunDetails(BaseCalculator):
    XCOM_ENRICHED_EVENT_KEY = "enriched_event"

    def process(self, context, run_params, **xargs) -> dict[str, str]:
        logger.info(f"{self.name} triggered with xargs: [{xargs}]")
        trigger_context_id = xargs.get(CALC_TRIGGER_CONTEXT_ID, "-")
        return {"triggerContextId": trigger_context_id, "taskEventType": "STARTED"}

    def post_process(self, context, response):
        calc_name = self.name.upper()

        if not response or not isinstance(response, (list, dict)):
            logger.error(f"Display CalcRun details for {calc_name} FAILED!\n[response={response}]")
            return

        started_enriched_event = response[0] if isinstance(response, list) else response

        # DEV can emit STARTED without jobLink; backfill from SCHEDULED when needed.
        if not search_dict(started_enriched_event, key="jobLink"):
            started_enriched_event = self._backfill_job_link(started_enriched_event)

        calc_run_details_log = create_calc_run_details_log(started_enriched_event)

        if search_dict(started_enriched_event, key="successful") == "true":
            logger.info(decorated_message(f"{calc_name} Calculator Run Details:\n{calc_run_details_log}"))
        else:
            logger.error(f"Databricks Job scheduling for {calc_name} FAILED!\n{calc_run_details_log}")

        if CALC_TRIGGER_CONTEXT_ID not in self.xcom_data:
            recovered_ctx_id = self._extract_trigger_context_id(started_enriched_event)
            if recovered_ctx_id:
                self.xcom_data[CALC_TRIGGER_CONTEXT_ID] = recovered_ctx_id
                logger.info(
                    f"Recovered {CALC_TRIGGER_CONTEXT_ID}={recovered_ctx_id} from enriched event "
                    f"(xcom_data was reset after deferral)"
                )

        try:
            push_xcom(context, {**self.xcom_data, self.XCOM_ENRICHED_EVENT_KEY: started_enriched_event})
        except Exception:
            logger.warning("Failed to push enriched_event to XCom; non-critical.", exc_info=True)

    @staticmethod
    def _extract_trigger_context_id(event_data: dict) -> str | None:
        if not isinstance(event_data, dict):
            return None

        return (
            traverse_dict(event_data, keys=['event', 'additionalData', 'triggerContextId'])
            or traverse_dict(event_data, keys=['event', 'additionalData', 'contextId'])
            or traverse_dict(event_data, keys=['event', 'contextId'])
        )

    def _backfill_job_link(self, started_event: dict) -> dict:
        trigger_context_id = self._extract_trigger_context_id(started_event)
        if not trigger_context_id:
            logger.warning("Cannot fetch SCHEDULED event: no triggerContextId in STARTED event")
            return started_event

        try:
            scheduled_events = fetch_data_from_event_service(
                config=self.config,
                endpoint=ES_EVENT_ENDPOINT,
                query_params={"triggerContextId": trigger_context_id, "taskEventType": "SCHEDULED"},
            )
        except Exception:
            logger.warning("Failed to fetch SCHEDULED event for jobLink backfill; non-critical", exc_info=True)
            return started_event

        if not scheduled_events:
            logger.warning(f"No SCHEDULED event found for triggerContextId={trigger_context_id}")
            return started_event

        scheduled_job_link = search_dict(scheduled_events[0], key="jobLink")
        if not scheduled_job_link:
            logger.warning(f"SCHEDULED event missing jobLink for triggerContextId={trigger_context_id}")
            return started_event

        event_container = started_event.get("event")
        if not isinstance(event_container, dict):
            event_container = {}
            started_event["event"] = event_container

        additional_data = event_container.get("additionalData")
        if not isinstance(additional_data, dict):
            additional_data = {}
            event_container["additionalData"] = additional_data

        additional_data["jobLink"] = scheduled_job_link
        logger.info("Backfilled STARTED event jobLink from SCHEDULED event")

        return started_event