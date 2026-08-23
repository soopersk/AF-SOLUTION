"""Exemplar 03 — unit tests for a dataset-gated calculator.

Two fixtures, because the registry entry has two conditions: this DAG is triggered by
EITHER the FLOORSCALC completion event OR the OUTPUTPOSTING dataset event, and the Init
must shape identical run params from both.
"""
import json
from pathlib import Path

import pytest

from dags.logic.capital import floors_calculator
from dags.logic.capital.floors_calculator import OutputFloorInit

HERE = Path(__file__).parent
CALC_FIXTURE = HERE / "fixture_floors_calc.json"
DATASET_FIXTURE = HERE / "fixture_outputposting_dataset.json"


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(params=[CALC_FIXTURE, DATASET_FIXTURE],
                ids=["calc-event", "dataset-event"])
def calc_run_params(request):
    """Run params as the framework hands them to CALC_INIT, for each trigger channel."""
    return dict(load(request.param)["context"]["data"])


@pytest.fixture
def pushed(monkeypatch):
    captured = {}

    def fake_push_xcom(context, value=None, **kwargs):
        captured.update(value or {})

    monkeypatch.setattr(
        "dags.logic.capital.floors_calculator.push_xcom", fake_push_xcom
    )
    return captured


@pytest.fixture(autouse=True)
def stub_domain(monkeypatch):
    monkeypatch.setattr(floors_calculator, "get_calc_run_params", dict)
    monkeypatch.setattr(floors_calculator, "enrich_region", lambda params: None)
    monkeypatch.setattr(floors_calculator, "OUTPUT_FLOOR_COMPONENTS", "FLOOR_COMPONENTS")


def run_init(calc_run_params):
    return OutputFloorInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )


def test_fanout_is_a_single_lane(calc_run_params, pushed):
    """A bare dict return => exactly one lane in the mapped group."""
    result = run_init(calc_run_params)

    assert isinstance(result, dict)


def test_run_params_are_shaped_the_same_from_both_channels(calc_run_params, pushed):
    """Both trigger channels must yield identical calculator parameters — otherwise
    which event happened to arrive first changes what gets computed."""
    result = run_init(calc_run_params)

    assert result["calcType"] == "FLOORS"
    assert result["enabledComponents"] == "FLOOR_COMPONENTS"
    assert result["frequency"] == "M"
    assert result["runType"] == "BATCH"


def test_push_xcom_carries_the_envelope(calc_run_params, pushed):
    run_init(calc_run_params)

    assert pushed["CALC_RUN_PARAMS"]["calcType"] == "FLOORS"


def test_get_calc_name():
    """floorscalc => group_id FLOORSCALC_CALC => FLOORSCALC_CALC_INIT,
    FLOORSCALC_MAPPED_GROUP, FLOORSCALC_RESULT."""
    assert OutputFloorInit().get_calc_name() == "floorscalc"


def test_fixtures_carry_the_fields_their_conditions_inspect():
    """Guards the registry/fixture coherence the validator warns about."""
    calc_event = load(CALC_FIXTURE)["event"]["additionalData"]
    assert calc_event["type"] == "CALC_EVENT"        # CALC.FLOORSCALC
    assert calc_event["STATE"] == "FINISH"

    dataset_event = load(DATASET_FIXTURE)["event"]["additionalData"]
    assert dataset_event["DATASET_NAME"] == "OUTPUTPOSTING"   # DATASET.OUTPUTPOSTING
    assert dataset_event["updateType"] == "CURATION"
