import datetime
import json
import logging
import time
from typing import List

import requests

# Assuming required global utilities/constants are imported elsewhere
# from orchestration.common.af_utils import get_config, get_app_user_secret, invoke_http_request
# from orchestration.common.constants import AIRFLOW_RUN_PARAMETERS, AIRFLOW_SERVICE_ENDPOINT, FRCA_MEGDP_SERVICE_CONN_KEY

logger = logging.getLogger(__name__)


def trigger_dag(dag_id: str, **kwargs):
    """Triggers a DAG given a dag_id using Airflow REST API.
    
    dag_run_id: If a dag_run_id is provided it will be used.
    params: runtime parameters from airflow context will be used as configuration.

    Args:
        dag_id (str): identifier of the DAG to be invoked
    """
    config = kwargs.get('config', get_config())
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conf = kwargs.get("data", kwargs.get(AIRFLOW_RUN_PARAMETERS))
    dag_run_id = kwargs.get("dag_run_id")

    payload = {
        "logical_date": timestamp,
        "execution_date": timestamp,
        "conf": conf,
    }

    if dag_run_id is not None:
        payload["dag_run_id"] = dag_run_id

    payload_json = json.dumps(payload)
    endpoint = f"{AIRFLOW_SERVICE_ENDPOINT}/dags/{dag_id}/dagRuns"
    
    # Log identifying/diagnostic context, not the full conf (can be large/sensitive)
    conf_keys = sorted(conf.keys()) if isinstance(conf, dict) else None
    logger.info(
        f"Invoking DAG [{dag_id}] run_id=[{dag_run_id}] endpoint=[{endpoint}] "
        f"conf_keys={conf_keys} payload_bytes={len(payload_json)}"
    )

    pwd = get_app_user_secret()
    auth = (config.get(AIRFLOW_APP_USER_ACCESS_KEY), pwd)

    start = time.monotonic()
    try:
        response = invoke_http_request(
            data=payload_json,
            auth=auth,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=endpoint,
            method='POST',
            timeout=resolve_request_timeout(),
        )
    except requests.exceptions.HTTPError as http_err:
        elapsed = time.monotonic() - start
        status_code = getattr(http_err.response, "status_code", None)
        logger.error(f"Failed to invoke DAG [{dag_id}] (status {status_code}, {elapsed:.3f}s)")
        raise
    except requests.exceptions.RequestException:
        logger.exception(f"Failed to invoke DAG [{dag_id}] after {time.monotonic() - start:.3f}s")
        raise

    elapsed = time.monotonic() - start
    if response.ok:
        logger.info(
            f"Remote DAG [{dag_id}] run_id=[{dag_run_id}] successfully invoked "
            f"with status {response.status_code} in {elapsed:.3f}s"
        )

        try:
            return response.json()
        except json.JSONDecodeError:
            logger.error(
                f"DAG [{dag_id}] invocation returned status {response.status_code} "
                f"but a non-JSON body (possible auth redirect): {response.text[:500]}"
            )
            raise RuntimeError(f"Failed to invoke DAG: {dag_id}")
    else:
        logger.error(
            f"Received an error response: {response.status_code} while invoking DAG [{dag_id}]: {response.text[:500]}"
        )
        raise RuntimeError(f"Failed to invoke DAG: {dag_id}")


def fetch_dag_info(dag_id: str, config: dict):
    pwd = get_app_user_secret()
    auth = (config.get(AIRFLOW_APP_USER_ACCESS_KEY), pwd)
    endpoint = f"{AIRFLOW_SERVICE_ENDPOINT}/dags/{dag_id}"
    
    try:
        response = invoke_http_request(
            auth=auth,
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=endpoint,
            method='GET',
        )
    except requests.exceptions.RequestException:
        logger.exception(f"Failed to fetch DAG info [{dag_id}]")
        raise

    if response.ok:
        logger.info(
            f"Successfully retrieved [{dag_id}] info with status: {response.status_code}"
        )
        return response.json()
    else:
        logger.error(
            f"Received an error response: {response.status_code} while fetching DAG info for [{dag_id}]"
        )
        raise RuntimeError(f"Failed to fetch DAG info: {dag_id}")


def get_trigger_conditions(dags_dir: str) -> dict:
    """Retrieves trigger conditions from `dag_trigger_criteria_map.json` file"""
    if is_dev_env():
        trigger_cond_file = f"{dags_dir}/dev/dag_trigger_criteria_map_dev.json"
    else:
        trigger_cond_file = f"{dags_dir}/dag_trigger_criteria_map.json"

    trigger_conditions = load_json_file(trigger_cond_file)
    return trigger_conditions


def create_control_tasks(dags_dir: str):
    """Creates a list of DAG trigger task groups that will then be executed in parallel"""
    trigger_conditions = get_trigger_conditions(dags_dir)
    dag_trigger_task_groups = []

    for dag_id, trigger_condition in trigger_conditions.items():
        trigger_task_group = DagTriggerWithConditionTaskGroup(
            dag_id=dag_id, trigger_condition=trigger_condition
        )
        dag_trigger_task_groups.append(trigger_task_group)

    return dag_trigger_task_groups


def evaluate_dag_start_criteria(dags_dir, params) -> List[tuple]:
    """Loads trigger condition from json file and runs evaluator
    to check each starter condition and return a list of dags to be triggered.
    """
    trigger_conditions = get_trigger_conditions(dags_dir)
    registered_trigger_conditions = []

    for dag_id, trigger_condition in trigger_conditions.items():
        if isinstance(trigger_condition, list):
            registered_trigger_conditions.extend(
                [
                    (dag_id, create_event_completion_criteria(tc)) for tc in trigger_condition
                ]
            )
        else:
            registered_trigger_conditions.append(
                (dag_id, create_event_completion_criteria(trigger_condition))
            )

    start_criteria_checker = TriggerCriteriaEvaluator(registered_trigger_conditions)
    filtered_dag_list = start_criteria_checker.evaluate(params)
    return filtered_dag_list