import logging
import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from orchestration.common.dag_utils import create_control_tasks

from dags.control.control_utils import get_dags_dir
from dags.control.dag_constants import CAPITAL_TAGS

logger = logging.getLogger("dags")

@dag(
    dag_id="orchestration_control_dag_capital",
    description='Orchestration Control Dag for Capital Calculators',
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.CONTROL],
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="Europe/Zurich"),
    catchup=False,
)
def control_dag():
    start = EmptyOperator(task_id="START")
    
    # create_control_tasks returns a list of task groups/tasks executed in parallel
    control_tasks = create_control_tasks(get_dags_dir())
    
    finish = EmptyOperator(
        task_id="FINISH", 
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Correct dependency mapping for a list of parallel tasks/task groups
    start >> control_tasks >> finish

control_dag()