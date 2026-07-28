# Assuming these Airflow/custom imports are defined in your environment
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule



class CompletionDeferrableRunCriteriaSensor(HttpDeferrableRunCriteriaSensor):

    def __init__(
        self,
        run_criteria: CalcTriggerCriteria,  # Make sure CalcTriggerCriteria is imported
        task_id: str,
        request_params: dict,
        trigger_rule: TriggerRule,
        **kwargs,
    ):
        super().__init__(
            run_criteria=run_criteria,       # Fixed '-' typo to '='
            task_id=task_id,
            request_params=request_params,
            trigger_rule=trigger_rule,       # Fixed '-' typo to '='
            http_conn_id=FRCA_MEGDP_SERVICE_CONN_KEY,  # Ensure this constant is imported
            endpoint=ES_EVENT_ENDPOINT,                # Ensure this constant is imported
            **kwargs,
        )

    def execute_complete(self, context: Context, event=None):
        super().execute_complete(context, event)  # Removed extra space before parenthesis
        
        if event is not None:
            xcom_key = get_xcom_key(context)  # Added missing '=' operator
            upstream_task_id = context["task"].upstream_task_ids.pop()
            upstream_xcom = context["ti"].xcom_pull(task_ids=upstream_task_id, key=xcom_key)  # Added missing '=' operator
            push_xcom(context, upstream_xcom)
            
        return
    

class DatasetIngestionCompletionSensor(CompletionDeferrableRunCriteriaSensor):  # Fixed unclosed parenthesis

    def __init__(
        self,
        run_criteria: CalcTriggerCriteria,
        task_id: str,
        static_params_map: dict,
        context_param_keys: list,
        event_source: EventSource,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        super().__init__(
            run_criteria=run_criteria,       # Fixed '-' typo to '='
            task_id=task_id,
            request_params=static_params_map,
            trigger_rule=trigger_rule,       # Fixed '-' typo to '='
            **kwargs,
        )
        self.context_param_keys = context_param_keys
        self.event_source = event_source

    def execute(self, context: Context) -> None: 
        calc_run_params = get_runtime_params(context)  # Added missing '='
        
        # Added missing '='
        query_params = {
            key: search_dict(calc_run_params, key) 
            for key in set(self.context_param_keys)
        }
        self.request_params.update(**query_params)

        if self.event_source == EventSource.MEGDP:
            self.request_params.setdefault("datasetId", self.run_criteria.uuid)
            self.request_params["updateType"] = "CURATION"
        elif self.event_source == EventSource.MERIVAL:  # Added missing '==' comparison
            # Fixed quote syntax from "keys: "datasetId" to "datasetId"
            dataset_id = any_match(self.request_params, "datasetId", "DATASET_UUID")  # Added missing '='
            self.request_params["DATASET_UUID"] = dataset_id or self.run_criteria.uuid
            self.request_params.pop("datasetId", None)

        super().execute(context)


class CalcRunCompletionSensor(CompletionDeferrableRunCriteriaSensor):

    def __init__(
        self,
        run_criteria: CalcTriggerCriteria,
        task_id: str,
        calc_event_criteria: CalcEventCriteria,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        super().__init__(
            run_criteria=run_criteria,
            task_id=task_id,
            request_params=DEFAULT_CALC_FINISH_EVENT_QUERY_PARAMS | calc_event_criteria.static_params,
            trigger_rule=trigger_rule,
            **kwargs,
        )
        self.calc_event_criteria = calc_event_criteria

    def execute(self, context: Context) -> None:
        calc_run_params = self._get_calc_run_params(context)
        Logger.info(f"CalcRunParams passed to CalcRunCompletionSensor[execute(...)]: {calc_run_params}")
        
        query_params = dict()
        if dynamic_params_keys := self.calc_event_criteria.dynamic_params_keys:
            query_params = {key: search_dict(calc_run_params, key) for key in set(dynamic_params_keys)}
            
        if params := self.calc_event_criteria.get_params_callback_function(calc_run_params):
            query_params.update(**params)
           
        self.request_params.update(**query_params)
        self.request_params.setdefault("taskId", self.run_criteria.uuid)
        
        super().execute(context)

    def execute_complete(self, context: Context, event=None):
        calc_run_params = self._get_calc_run_params(context)
        # Fixed unclosed f-string, smart quotes, and closed the log correctly
        logger.info(f"CalcRunParams passed to CalcRunCompletionSensor[execute_complete(...)]: {calc_run_params}")
        super().execute_complete(context, event)

    def _get_calc_run_params(self, context: Context) -> dict:
        calc_run_params = get_runtime_params(context)
        calc_name = self.calc_event_criteria.calculator.get_derived_name(calc_run_params)

        if calc_name.upper() != self.run_criteria.name.upper():
            self.run_criteria = CalcEventCompletionCriteria(calc_name)

        return calc_run_params