"""Exemplar 02 — unit tests for a parametrized fan-out calculator.

Two things must be tested here that exemplar 01 does not have:
  1. the fan-out is WIDER than one, and its width tracks the company groups;
  2. the per-unit rule (the WM component swap) lands on the right lane and ONLY on
     that lane.

The company-group resolution and the component catalogues are domain collaborators —
they are stubbed, because what is under test is this module's fan-out and shaping
logic, not the catalogue's contents.
"""
import json
from pathlib import Path

import pytest

from dags.logic.capital import capital_calculator
from dags.logic.capital.capital_calculator import CapitalInit

FIXTURE = Path(__file__).parent / "fixture_merival_amer.json"

# Three company groups => three lanes. The middle one holds the region's flagship
# company code, so under a WM region it must get the *COMP* component list while the
# other two get the *G6L* one.
COMPANY_GROUPS = [
    {"company-code-1": "2074"},
    {"company-code-1": "3042"},
    {"company-code-1": "B615"},
]
FLAGSHIP = "3042"


@pytest.fixture
def enriched_event():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.fixture
def calc_run_params(enriched_event):
    return dict(enriched_event["context"]["data"])


@pytest.fixture
def pushed(monkeypatch):
    captured = {}

    def fake_push_xcom(context, value=None, **kwargs):
        captured.update(value or {})

    monkeypatch.setattr(
        "dags.logic.capital.capital_calculator.push_xcom", fake_push_xcom
    )
    return captured


@pytest.fixture
def stub_domain(monkeypatch):
    """Stub the catalogue collaborators; COMP_GROUPS is shaped so that the flagship
    resolution loop in __call__ picks FLAGSHIP."""
    monkeypatch.setattr(capital_calculator, "get_calc_run_params", dict)
    monkeypatch.setattr(capital_calculator, "enrich_region", lambda params: None)
    monkeypatch.setattr(capital_calculator, "generate_tracking_id", lambda: "TRACK-1")
    monkeypatch.setattr(
        capital_calculator, "insert_dynamic_parameters", lambda spec, params: None
    )
    monkeypatch.setattr(capital_calculator, "get_env", lambda: "PROD")
    monkeypatch.setattr(
        capital_calculator, "search_dict", lambda params, key: params.get(key)
    )
    monkeypatch.setattr(
        capital_calculator,
        "create_company_groups_by_region",
        lambda params, comp_groups: COMPANY_GROUPS,
    )
    monkeypatch.setattr(
        capital_calculator, "COMP_GROUPS", {"PROD": {"DEFAULT": {"L": [[FLAGSHIP]]}}}
    )
    monkeypatch.setattr(capital_calculator, "IB_B3F_COMPONENTS", "IB_COMPONENTS")
    monkeypatch.setattr(capital_calculator, "WM_B3F_COMP_COMPONENTS", "WM_COMP")
    monkeypatch.setattr(capital_calculator, "WM_G6L_B3F_COMPONENTS", "WM_G6L")


def run_init(calc_run_params):
    return CapitalInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )


def test_fanout_is_one_lane_per_company_group(calc_run_params, pushed, stub_domain):
    result = run_init(calc_run_params)

    assert isinstance(result, list)
    assert len(result) == len(COMPANY_GROUPS) > 1
    assert [lane["company-code-1"] for lane in result] == [
        g["company-code-1"] for g in COMPANY_GROUPS
    ]


def test_non_wm_region_keeps_the_ib_component_list(calc_run_params, pushed, stub_domain):
    result = run_init(calc_run_params)   # fixture region is AMER

    assert {lane["enabledComponents"] for lane in result} == {"IB_COMPONENTS"}


def test_wm_region_swaps_components_on_the_flagship_lane_only(
    calc_run_params, pushed, stub_domain
):
    calc_run_params["h3Region"] = "WMCH"

    result = run_init(calc_run_params)

    by_company = {lane["company-code-1"]: lane["enabledComponents"] for lane in result}
    assert by_company[FLAGSHIP] == "WM_COMP"
    assert by_company["2074"] == "WM_G6L"
    assert by_company["B615"] == "WM_G6L"


def test_lanes_do_not_share_mutable_state(calc_run_params, pushed, stub_domain):
    """A per-lane component swap must not leak into its neighbours — the reason each
    lane is built as a fresh {**run_params, **company_groups} dict."""
    calc_run_params["h3Region"] = "WMCH"

    result = run_init(calc_run_params)

    result[0]["enabledComponents"] = "MUTATED"
    assert result[1]["enabledComponents"] != "MUTATED"


def test_shared_params_are_set_and_pushed(calc_run_params, pushed, stub_domain):
    result = run_init(calc_run_params)

    for lane in result:
        assert lane["calcType"] == "B3F"
        assert lane["cumulus"] == "true"
        assert lane["lombardSwitch"] == "true"
        assert lane["trackingId"] == "TRACK-1"

    assert pushed["CALC_RUN_PARAMS"]["calcType"] == "B3F"


def test_manual_run_uses_companies_from_the_event(calc_run_params, pushed, stub_domain,
                                                  monkeypatch):
    """isManual=True must NOT consult the company-region table."""
    called = []
    monkeypatch.setattr(
        capital_calculator,
        "get_companycodes_by_runtype",
        lambda params: called.append(params) or [],
    )
    calc_run_params["isManual"] = True

    run_init(calc_run_params)

    assert called == []


def test_scheduled_run_consults_the_company_region_table(calc_run_params, pushed,
                                                         stub_domain, monkeypatch):
    called = []
    monkeypatch.setattr(
        capital_calculator,
        "get_companycodes_by_runtype",
        lambda params: called.append(params) or ["2074"],
    )

    run_init(calc_run_params)   # fixture has no isManual key

    assert len(called) == 1


def test_get_calc_name(monkeypatch):
    monkeypatch.setattr(
        capital_calculator, "get_regional_capital_calc_name", lambda: "capitalcalc"
    )
    assert CapitalInit().get_calc_name() == "capitalcalc"
