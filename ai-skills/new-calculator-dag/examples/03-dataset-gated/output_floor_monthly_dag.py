"""Exemplar 03 — thin DAG file for a dataset-gated calculator.

The DAG file is identical in shape to exemplar 01's. All the gating lives in the
logic module, because gating is part of the calc group, not of the DAG.
"""
import pendulum
from airflow.decorators import dag
from orchestration.common.base_task import calc_end, calc_start
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from dags.control.dag_constants import CAPITAL_TAGS
from dags.logic.capital.floors_calculator import get_output_floor_calc_group


@dag(
    dag_id="output_floor_monthly_dag",
    description="Output Floor Monthly Dag",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.CAPITAL],
    schedule=None,
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="Europe/Zurich"),
    catchup=False,
)
def output_floor_monthly_dag():
    start = calc_start()
    floor_calc_group = get_output_floor_calc_group()
    finish = calc_end()

    start >> floor_calc_group >> finish


output_floor_monthly_dag()
