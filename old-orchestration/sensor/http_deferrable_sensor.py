from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

# Assuming these Airflow/custom imports exist in your environment
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.context import Context


class HttpDeferrableSensor(HttpSensor):

    template_fields = (
        "mapped_args",
        "endpoint",
    )

    def __init__(
        self,
        *,
        calculator: Any,  # Replace with 'BaseCalculator' if imported
        http_conn_id: str,
        endpoint: str,
        poke_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        self.extra_args = kwargs.pop('extra_args', {})
        self.mapped_args = kwargs.pop('mapped_args', {})
        super().__init__(endpoint=endpoint, **kwargs)
        self.calculator = calculator
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.poke_interval = poke_interval
        
        # Fixed dict .get() syntax
        self.execution_timeout = kwargs.get(
            'execution_timeout', 
            timedelta(seconds=DEFAULT_CALC_SENSOR_TIMEOUT)  # Ensure DEFAULT_CALC_SENSOR_TIMEOUT is defined
        )
        self.update_header(**kwargs)

    def execute(self, context: Context) -> None: 
        self._validate_connection()
        sensor_name = self.__class__.__name__
        self.log.info(decorated_message(f"{sensor_name} STARTED for calculator: {self.calculator.name}"))

        merged_args = {**self.extra_args, **self.mapped_args}
        self.calculator.pre_process(context, **merged_args)

        aggregated_args = self.enrich_data(context)
        run_params = get_runtime_params(context)

        calc_out = self.calculator.process(context, run_params, **aggregated_args)

        self.request_params = {**self.request_params, **calc_out}
        self.request_params.pop(CALC_RUN_PARAMS, None)
        self.request_params.pop("edf_event_id", None)

        self.log.info(
            decorated_message(f"{sensor_name} querying rest endpoint with request_params")
        )

        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=MvlHttpTrigger(
                endpoint=self.endpoint,
                http_conn_id=self.http_conn_id,
                method=self.method,
                data=self.request_params,
                headers=self.headers,
                extra_options=self.extra_options,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event=None):
        if event is not None:
            sensor_name = self.__class__.__name__
            resp_status = event['status']
            resp_json = event['data']
            self.log.info(f"Response received with status: {resp_status}")

            if resp_status == "error" and not self.extra_args.get(AIRFLOW_IGNORE_API_CALL_ERRORS, False):
                raise AirflowException(f"{sensor_name} failed with error: {resp_json}")

            if resp_status == "error":
                self.log.error(f"{sensor_name} failed with error: {resp_json}")
            else:
                self.log.debug(f"{sensor_name} completed successfully with response: {resp_json}")

            self.log.debug(f"Response received from {sensor_name} : {resp_json}")

        if getattr(self.calculator, "_obs_enabled", False):
            self._rehydrate_calculator_state(context)
            self.calculator.post_process(context, resp_json)

        return

    def _rehydrate_calculator_state(self, context: Context) -> None:
        sensor_name = self.__class__.__name__
        # Drop the upstream re-push flag so pre_process only reloads state.

        rehydrate_args = {
            key: value
            for key, value in {**self.extra_args, **self.mapped_args}.items()
            if key != AIRFLOW_PUSH_UPSTREAM_XCOM_DATA
        }

        try:
            self.calculator.pre_process(context, **rehydrate_args)
            xcom_data = self.calculator.xcom_data or {}
            self.log.info(
                "%s rehydrated calculator state before post_process: "
                "calc=%s_obs_enabled=%s obs_run_id_present=%s xcom_keys=%s",
                sensor_name,
                self.calculator.name,
                getattr(self.calculator, "_obs_enabled", False), 
                bool(xcom_data.get(OBS_RUN_ID_XCOM_KEY)),
                sorted(xcom_data.keys()),
            )
        except Exception:
            self.log.warning("Failed rehydration")

    def enrich_data(self, context: Context) -> dict:
        dag_run = context["dag_run"]
        merged_args = {
            **self.mapped_args,
            **self.calculator.xcom_data,
        }

        if isinstance(dag_run, (DagRun, MagicMock)):
            include_dag_info = merged_args.get(INCLUDE_DAG_INFO, dag_run.conf.get(INCLUDE_DAG_INFO, "False"))
        else:
            raise ValueError("Unsupported type for dag_run")

        if include_dag_info == "True":
            merged_args["dag_id"] = dag_run.dag_id
            merged_args["run_id"] = dag_run.run_id

        return merged_args

    def update_header(self, **kwargs):
        self.headers = kwargs.get('headers', {})
        auth_user = kwargs.get(
            'auth_user',
            self.calculator.config.get(AIRFLOW_APP_USER_ACCESS_KEY)
        )
        auth_type = kwargs.get('auth_type', 'Basic')

        if 'Authorization' not in self.headers:
            pwd = get_app_user_secret()
            self.headers.update(get_http_basic_auth_header(auth_user, pwd))
            if auth_type == 'Bearer':
                self.headers['Authorization'] = f"Bearer {pwd}"

    def _validate_connection(self): 
        if self.http_conn_id is None or self.http_conn_id == "http_default": 
            self.log.exception("http_conn_id is not defined!")
            raise ValueError("http_conn_id is not defined")