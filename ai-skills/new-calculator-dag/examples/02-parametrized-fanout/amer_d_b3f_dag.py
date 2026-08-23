"""Exemplar 02 — thin DAG file for a parametrized, fanning-out calculator.

Corrected from old-orchestration/dags/amer_d_b3f_dag.py.

One logic module (`capital_calculator.py`) serves eleven regional B3F DAGs; each DAG
file differs only in its dag_id, its tag, and the (region, freq) pair it passes.
Adding a region means adding a DAG file and a registry entry — not new logic.
"""
import pendulum
from airflow.decorators import dag
from orchestration.common.base_task import calc_end, calc_start
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from dags.control.dag_constants import CAPITAL_TAGS
from dags.logic.capital.capital_calculator import get_capital_calc_group


@dag(
    # Registry key must match EXACTLY. The live registry says "amer_b3f_dag" while
    # this DAG says "amer_d_b3f_dag" — that DAG is unreachable by events as
    # registered (pitfalls.md #1). registry.json here has the corrected key.
    dag_id="amer_d_b3f_dag",
    description="AMER D B3F Dag",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.B3F],
    schedule=None,
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="Europe/Zurich"),
    catchup=False,
)
def amer_d_b3f_dag():
    start = calc_start()
    # The two parameters that specialise the shared logic module for this DAG.
    # H3RegionCriteria(region) inside the group additionally rejects events for
    # other regions, so a mis-registered condition cannot run the wrong region.
    capital_calc_group = get_capital_calc_group(region="AMER", freq="D")
    finish = calc_end()

    start >> capital_calc_group >> finish


# The production original names this function `b3f_context` in every regional file —
# legal, since Airflow reads dag_id from the decorator, but it means eleven DAGs share
# one function name. Naming it after the dag_id costs nothing and reads better.
amer_d_b3f_dag()
