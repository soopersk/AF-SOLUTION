import pendulum
from airflow.decorators import dag
from orchestration.common.base_task import calc_end, calc_start
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from dags.control.dag_constants import CAPITAL_TAGS
from dags.logic.capital.usrg_ihc_calculator import get_usrg_calc_group

@dag(
    dag_id="usrg_ihc_dag",
    description="USRG IHC Dag",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.CAPITAL],
    schedule=None,
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="Europe/Zurich"),
    catchup=False,
)
def usrg_ihc_dag():
    start = calc_start()
    capital_calc_group = get_usrg_calc_group()
    finish = calc_end()

    start >> capital_calc_group >> finish

usrg_ihc_dag()