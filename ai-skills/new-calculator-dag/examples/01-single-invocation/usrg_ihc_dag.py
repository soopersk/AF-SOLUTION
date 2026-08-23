"""Exemplar 01 — thin DAG file for a single-invocation calculator.

Corrected from old-orchestration/dags/usrg_ihc_dag.py. The DAG file holds no
business logic: it wires calc_start >> group >> calc_end and nothing else.
"""
import pendulum
from airflow.decorators import dag
from orchestration.common.base_task import calc_end, calc_start
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from dags.control.dag_constants import CAPITAL_TAGS
from dags.logic.capital.usrg_ihc_calculator import get_usrg_calc_group


@dag(
    # dag_id == this file's stem == the registry key in dag_trigger_criteria_map.json.
    # Nothing in the framework checks that; break it and the DAG is unreachable by
    # events (see pitfalls.md #1 — the live amer_b3f_dag / amer_d_b3f_dag drift).
    dag_id="usrg_ihc_dag",
    description="USRG IHC Dag",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.CAPITAL],
    # Event-triggered, never scheduled: routing belongs to the registry. A schedule
    # here would be a second, uncoordinated trigger channel.
    schedule=None,
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="Europe/Zurich"),
    catchup=False,
)
def usrg_ihc_dag():
    start = calc_start()
    capital_calc_group = get_usrg_calc_group()
    finish = calc_end()

    start >> capital_calc_group >> finish


# Module-level invocation. Lose this line in a refactor and the file still parses
# cleanly while the DAG simply ceases to exist.
usrg_ihc_dag()
