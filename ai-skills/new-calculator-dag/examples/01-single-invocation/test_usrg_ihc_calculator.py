"""Exemplar 01 — unit tests, instantiated from templates/test_calculator.py.tmpl.

The Init class is exercised directly: no Airflow, no DagBag. context and config are
plain dicts and push_xcom is monkeypatched, because what is worth testing here is the
fan-out shape and the run-param shaping, not the framework.
"""
import json
from pathlib import Path

import pytest

from dags.logic.capital.usrg_ihc_calculator import CapitalUsrgIhcInit

FIXTURE = Path(__file__).parent / "fixture_merival_usrg.json"


@pytest.fixture
def enriched_event():
    """The sample event this DAG is registered to trigger on (SOURCE.MERIVAL.USRG)."""
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture
def calc_run_params(enriched_event):
    """Run params as the framework hands them to CALC_INIT (context.data)."""
    return dict(enriched_event["context"]["data"])


@pytest.fixture
def pushed(monkeypatch):
    """Capture push_xcom instead of touching Airflow XCom."""
    captured = {}

    def fake_push_xcom(context, value=None, **kwargs):
        captured.update(value or {})

    monkeypatch.setattr(
        "dags.logic.capital.usrg_ihc_calculator.push_xcom", fake_push_xcom
    )
    return captured


def test_fanout_width(calc_run_params, pushed):
    """Single invocation: exactly one mapped lane."""
    result = CapitalUsrgIhcInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )

    lanes = result if isinstance(result, list) else [result]
    assert len(lanes) == 1


def test_run_params_are_shaped(calc_run_params, pushed):
    """The calculator-specific params this Init exists to set."""
    result = CapitalUsrgIhcInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )

    lanes = result if isinstance(result, list) else [result]
    for lane in lanes:
        assert lane["company-code-1"] == "B615"
        assert lane["cumulus"] == "false"
        assert lane["DerivedAttributesAdvancedApproachOff"] == "true"
        assert lane["enabledComponents"]


def test_push_xcom_carries_the_envelope(calc_run_params, pushed):
    """Without this push, downstream mapped tasks see the raw inbound params
    instead of the shaped ones (authoring-contract.md §4)."""
    CapitalUsrgIhcInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )

    assert "CALC_RUN_PARAMS" in pushed
    assert pushed["CALC_RUN_PARAMS"]["cumulus"] == "false"


def test_get_calc_name():
    """group_id is derived from this name; a change here renames every task in the
    group, including the hand-composed XCom pull targets."""
    assert CapitalUsrgIhcInit().get_calc_name() == "capitalusrgihccalc"
