import logging
from typing import Any, Dict, List, Union
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


def get_events(config: dict[str, Any], query_params: dict[str, Any]) -> list[dict[str, Any]]:
    """Get 'enriched-events' by query-params

    Args:
        config (dict): Airflow custom configurations
        query_params (dict): query params to search for 'enriched-events'.
    Returns:
        list[dict]: List of 'enriched-events'
    Raises:
        ValueError: Should raise an exception for invalid 'input-parameters'
    """
    if not query_params:
        raise ValueError(f"Invalid input parameters: [query_params={query_params}]")

    events = fetch_data_from_event_service(
        config=config, endpoint=ES_EVENT_ENDPOINT, query_params=query_params
    )

    return events


def fetch_data_from_event_service(
    config: dict[str, Any], endpoint: str, query_params: dict[str, Any]
) -> Union[dict[str, Any], list[dict[str, Any]]]:
    """Fetch context or enriched-event from eventservice for a given query-params

    Args:
        config (dict): Airflow custom configurations
        endpoint (str): API endpoint
        query_params (dict): query params for GET api query

    Returns:
        Union[dict, list[dict]]: Dict or List of Context or EnrichedEvent
    Throws:
        RuntimeError: Should raise an exception for invalid api-response
    """

    if not endpoint or not query_params:
        raise ValueError(
            f"Invalid input parameters: [endpoint={endpoint}, query_params={query_params}]"
        )

    pwd = get_app_user_secret()
    airflow_app_user_access_key = config.get(AIRFLOW_APP_USER_ACCESS_KEY)

    if airflow_app_user_access_key is None:
        raise ValueError("Missing access key in config")

    auth = HTTPBasicAuth(airflow_app_user_access_key, pwd)

    try:
        response = invoke_http_request(
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,
            endpoint=endpoint,
            data=query_params,
            auth=auth,
        )
    except requests.exceptions.RequestException as exc:
        if (
            endpoint == ES_EVENT_ENDPOINT
            and isinstance(exc, requests.exceptions.HTTPError)
            and exc.response is not None
            and exc.response.status_code == 404
        ):
            logger.info(
                f"No data found from eventservice: [endpoint={endpoint}, query_params={query_params}]"
            )
            return []
        logger.exception("Error fetching data")
        raise

    if response.ok:
        response_data = response.json()
        if not response_data:
            logger.error(
                f"Fetched invalid response data: url={response.url}, status={response.status_code}, response={response_data}"
            )
            raise RuntimeError(
                f"Failed to fetch valid data: url={response.url}, status={response.status_code}, response={response_data}"
            )

        logger.info(
            f"Fetched data from eventservice successfully: url={response.url}, status={response.status_code}"
        )
        return response_data if isinstance(response_data, list) else [response_data]
    else:
        logger.exception(
            f"Error fetching data: url={response.url}, status={response.status_code}"
        )
        raise RuntimeError(
            f"Failed to fetch data: url={response.url}, status={response.status_code}"
        )