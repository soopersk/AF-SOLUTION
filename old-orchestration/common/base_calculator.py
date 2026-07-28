import logging
from abc import ABC, abstractmethod

from airflow.exceptions import AirflowSkipException
from airflow.models.xcom import LazyXComSelectSequence
from airflow.utils.context import Context

from orchestration.common.af_utils import (
    get_companies_from_run,
    get_config,
    get_env,
    get_full_task_id,
    get_xcom_key,
    push_xcom,
)
from orchestration.common.calculator_metadata import CalculatorMetadata
from orchestration.common.collection_utils import search_dict
from orchestration.common.constants import (
    AIRFLOW_PUSH_UPSTREAM_XCOM_DATA,
    AIRFLOW_UPSTREAM_TASK_ID,
    MEG_CALC_TYPE_CALCULATOR,
)

logger = logging.getLogger(__name__)


class BaseCalculator(ABC):

    def __init__(
        self,
        name: str,
        region_comp_group: dict | None = None,
        calc_type: str = MEG_CALC_TYPE_CALCULATOR,
        metadata: CalculatorMetadata | None = None,
    ) -> None:
        super().__init__()
        if not name:
            raise ValueError("Calculator must have a name!")
        
        self.name = name
        self.region_comp_group = region_comp_group
        self.calc_type = calc_type
        self.metadata = metadata or CalculatorMetadata(name=name)
        self.config = get_config()
        self.xcom_data: dict = {}

    def pre_process(self, context: Context, **kwargs) -> None:
        """Retrieves previous task's xcom data and initializes all variables as config object."""
        upstream_task_id = get_full_task_id(
            context['ti'].task_id, 
            str(kwargs.get(AIRFLOW_UPSTREAM_TASK_ID))
        )
        mapped_index = context['ti'].map_index
        xcom = context["ti"].xcom_pull(
            task_ids=upstream_task_id,
            map_indexes=mapped_index if mapped_index else None,
            key=get_xcom_key(context),
        )

        if isinstance(xcom, LazyXComSelectSequence):
            self.xcom_data.update(xcom[0])
        elif isinstance(xcom, dict):
            self.xcom_data.update(xcom)

        self.xcom_data = self.xcom_data if self.xcom_data is not None else {}

        if kwargs.get(AIRFLOW_PUSH_UPSTREAM_XCOM_DATA, False):
            push_xcom(context, self.xcom_data)

        logger.info(f"{self.name} calculator initialized with xcom_data={self.xcom_data}")

    @abstractmethod
    def process(self, context: Context, run_params: dict, **kwargs) -> dict:
        logger.error("Nothing here! This method is supposed to be implemented by a subclass")
        raise NotImplementedError()

    @abstractmethod
    def post_process(self, context: Context, response):
        logger.error("Nothing here! This method is supposed to be implemented by a subclass")
        raise NotImplementedError()

    def get_derived_name(self, calc_run_params: dict) -> str:
        if not self.region_comp_group:
            logger.info(f"No region_comp_group found for calc: {self.name}. Will use default name!")
            return self.name

        h3_region = search_dict(calc_run_params, key="h3Region")
        comp_codes = get_companies_from_run(calc_run_params)
        env_name = get_env(self.config)

        # Fetch environment-specific or default mapping
        calc_comp_mapping_by_env = self.region_comp_group.get(env_name) or self.region_comp_group.get("DEFAULT")

        if not calc_comp_mapping_by_env:
            raise AirflowSkipException(f"No mapping found for environment: {env_name} or DEFAULT")

        # Fetch region-specific or default mapping
        calc_name_by_comp_mapping = calc_comp_mapping_by_env.get(h3_region) or calc_comp_mapping_by_env.get("DEFAULT")

        if not calc_name_by_comp_mapping:
            raise AirflowSkipException(f"No mapping found for region: {h3_region} or DEFAULT")

        # Check each calculator name and its component code mappings
        for calc_name, value in calc_name_by_comp_mapping.items():
            flattened_comp_codes = [code for comps in value for code in comps]
            if any(code in flattened_comp_codes for code in comp_codes) or "*" in flattened_comp_codes:
                return calc_name

        # If no match is found, raise an exception
        logger.error(
            f"Calculator Name based on region:[{h3_region}] and companies:{comp_codes} could not be resolved"
        )
        raise AirflowSkipException(
            f"Unable to fetch calculator name dynamically for calc: {self.name}, region: {h3_region}, "
            f"env: {env_name}, comp_codes: {comp_codes}"
        )