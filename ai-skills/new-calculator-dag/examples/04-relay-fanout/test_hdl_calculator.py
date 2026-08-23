"""Exemplar 04 — unit tests for a relay DAG.

Three groups of tests:
  1. the ordinary Init fan-out (as in exemplar 01);
  2. the payload resolver, across all three sources — these are PURE FUNCTIONS, which
     is exactly why the relay mechanism was factored out of the DAG file: a relay whose
     payload logic lives inline in the @dag body cannot be unit-tested at all;
  3. run-id determinism, the property the whole idempotency argument rests on.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dags.logic.liquidity import hdl_calculator
from dags.logic.liquidity.hdl_calculator import (
    PAYLOAD_SOURCE_CONF,
    PAYLOAD_SOURCE_FUNCTION,
    PAYLOAD_SOURCE_XCOM,
    TRIGGER_PAYLOAD_SOURCE_CONF_KEY,
    HdlProcessInit,
    build_payloads,
    build_trigger_conf,
    deterministic_relay_run_id,
    get_calc_result_task_id,
    get_trigger_payload_source,
    relay_payload_digest,
)

FIXTURE = Path(__file__).parent / "fixture_hdl_ingest.json"

RESULT_XCOM = {"contextId": "3c9a17e2", "companyCodes": ["3042"], "reportingDate": "2026-07-24"}
INCOMING_CONF = {"h3Region": "ZURI", "frequency": "D", "isManual": True}
DOWNSTREAM = ["nsfr_deposit_cals_dag", "nsfr_loans_cals_dag"]


def make_context(xcom_value=None, conf=None):
    """A fake Airflow context: a dag_run carrying conf, and a ti whose xcom_pull
    returns a canned value. No Airflow involved."""
    return {
        "dag_run": SimpleNamespace(conf=dict(conf) if conf is not None else {}),
        "ti": SimpleNamespace(xcom_pull=lambda **kwargs: xcom_value),
    }


@pytest.fixture(autouse=True)
def stub_xcom_key(monkeypatch):
    monkeypatch.setattr(hdl_calculator, "get_xcom_key", lambda context: "XCOM_KEY")


# ---------------------------------------------------------------------------
# 1. The calculator itself
# ---------------------------------------------------------------------------

@pytest.fixture
def calc_run_params():
    return dict(json.loads(FIXTURE.read_text(encoding="utf-8"))["context"]["data"])


@pytest.fixture
def pushed(monkeypatch):
    captured = {}

    def fake_push_xcom(context, value=None, **kwargs):
        captured.update(value or {})

    monkeypatch.setattr(hdl_calculator, "push_xcom", fake_push_xcom)
    monkeypatch.setattr(hdl_calculator, "get_calc_run_params", dict)
    monkeypatch.setattr(hdl_calculator, "HDL_PROCESS_COMPONENTS", "HDL_COMPONENTS")
    return captured


def test_init_fans_out_to_one_lane(calc_run_params, pushed):
    result = HdlProcessInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )

    assert len(result) == 1
    assert result[0]["enabledComponents"] == "HDL_COMPONENTS"
    assert result[0]["calcType"] == "HDL"


def test_init_pushes_the_envelope(calc_run_params, pushed):
    HdlProcessInit()(
        context={}, config={}, calc_run_params=calc_run_params, xcom_data={}
    )

    assert pushed["CALC_RUN_PARAMS"]["calcType"] == "HDL"


def test_result_task_id_is_derived_not_hardcoded():
    """Must match the framework's derivation: group_id minus _CALC, upper, + _RESULT —
    and follow get_calc_name(), so a calculator rename moves the pull target with it."""
    assert get_calc_result_task_id() == "LNFHDLPROCESSCALC_RESULT"
    assert get_calc_result_task_id() == (
        f"{HdlProcessInit().get_calc_name().upper()}_RESULT"
    )


# ---------------------------------------------------------------------------
# 2. The payload resolver — all three sources
# ---------------------------------------------------------------------------

def test_xcom_source_pulls_the_result_task():
    conf = build_trigger_conf(
        context=make_context(xcom_value=RESULT_XCOM),
        source_task_id="LNFHDLPROCESSCALC_RESULT",
        payload_source=PAYLOAD_SOURCE_XCOM,
    )

    assert conf == RESULT_XCOM


def test_xcom_source_unwraps_a_mapped_list():
    """A mapped result task pushes a list; the relay needs the single dict."""
    conf = build_trigger_conf(
        context=make_context(xcom_value=[RESULT_XCOM]),
        source_task_id="LNFHDLPROCESSCALC_RESULT",
        payload_source=PAYLOAD_SOURCE_XCOM,
    )

    assert conf == RESULT_XCOM


def test_xcom_source_never_returns_none():
    """A None conf would start the downstream DAG with no params at all — and look
    like success."""
    conf = build_trigger_conf(
        context=make_context(xcom_value=None),
        source_task_id="LNFHDLPROCESSCALC_RESULT",
        payload_source=PAYLOAD_SOURCE_XCOM,
    )

    assert conf == {}


def test_conf_source_forwards_the_incoming_dag_run_conf():
    conf = build_trigger_conf(
        context=make_context(conf=INCOMING_CONF),
        source_task_id="LNFHDLPROCESSCALC_RESULT",
        payload_source=PAYLOAD_SOURCE_CONF,
    )

    assert conf == INCOMING_CONF


def test_function_source_calls_the_author_supplied_builder():
    conf = build_trigger_conf(
        context=make_context(),
        source_task_id="LNFHDLPROCESSCALC_RESULT",
        payload_source=PAYLOAD_SOURCE_FUNCTION,
        custom_builder=lambda context: {"shaped": "payload"},
    )

    assert conf == {"shaped": "payload"}


def test_function_source_without_a_builder_fails_loudly():
    with pytest.raises(ValueError):
        build_trigger_conf(
            context=make_context(),
            source_task_id="LNFHDLPROCESSCALC_RESULT",
            payload_source=PAYLOAD_SOURCE_FUNCTION,
        )


def test_payload_source_defaults_to_xcom():
    assert get_trigger_payload_source(make_context()) == PAYLOAD_SOURCE_XCOM


def test_payload_source_honours_the_conf_override():
    context = make_context(conf={TRIGGER_PAYLOAD_SOURCE_CONF_KEY: PAYLOAD_SOURCE_CONF})

    assert get_trigger_payload_source(context) == PAYLOAD_SOURCE_CONF


def test_unknown_payload_source_falls_back_to_xcom():
    context = make_context(conf={TRIGGER_PAYLOAD_SOURCE_CONF_KEY: "nonsense"})

    assert get_trigger_payload_source(context) == PAYLOAD_SOURCE_XCOM


def test_build_payloads_gives_each_target_its_own_copy():
    payloads = build_payloads(
        context=make_context(xcom_value=RESULT_XCOM),
        source_task_id="LNFHDLPROCESSCALC_RESULT",
        downstream_dag_ids=DOWNSTREAM,
        payload_source=PAYLOAD_SOURCE_XCOM,
    )

    assert [p["trigger_dag_id"] for p in payloads] == DOWNSTREAM
    payloads[0]["conf"]["mutated"] = True
    assert "mutated" not in payloads[1]["conf"]


# ---------------------------------------------------------------------------
# 3. Run-id determinism
# ---------------------------------------------------------------------------

def test_same_target_and_conf_give_the_same_run_id():
    """The property retry-dedup depends on."""
    assert deterministic_relay_run_id("nsfr_deposit_cals_dag", RESULT_XCOM) == (
        deterministic_relay_run_id("nsfr_deposit_cals_dag", RESULT_XCOM)
    )


def test_key_order_does_not_change_the_run_id():
    reordered = dict(reversed(list(RESULT_XCOM.items())))

    assert deterministic_relay_run_id("nsfr_deposit_cals_dag", reordered) == (
        deterministic_relay_run_id("nsfr_deposit_cals_dag", RESULT_XCOM)
    )


def test_different_conf_gives_a_different_run_id():
    """Deterministic ids deduplicate retries, not legitimately different runs."""
    other = {**RESULT_XCOM, "reportingDate": "2026-07-25"}

    assert deterministic_relay_run_id("nsfr_deposit_cals_dag", other) != (
        deterministic_relay_run_id("nsfr_deposit_cals_dag", RESULT_XCOM)
    )


def test_different_target_gives_a_different_run_id():
    assert deterministic_relay_run_id("nsfr_loans_cals_dag", RESULT_XCOM) != (
        deterministic_relay_run_id("nsfr_deposit_cals_dag", RESULT_XCOM)
    )


def test_payload_digest_matches_the_templated_run_id_form():
    """The DAG file renders "relay_{{ task.trigger_dag_id }}_{{ digest }}"; this is
    the Python equivalent of that string."""
    digest = relay_payload_digest(RESULT_XCOM)

    assert len(digest) == 16
    assert f"relay_nsfr_deposit_cals_dag_{digest}".startswith("relay_")
    assert relay_payload_digest(dict(reversed(list(RESULT_XCOM.items())))) == digest


def relay_targets_from_dag_file():
    """Read DOWNSTREAM_DAG_IDS out of the DAG file by AST.

    Re-declaring the target list here would recreate the very drift class this skill
    exists to prevent, and the DAG file cannot be imported without Airflow — so parse
    it instead. This also proves the list is a static module-level constant, which is
    what the validator requires.
    """
    import ast

    tree = ast.parse(
        (Path(__file__).parent / "hdl_process_dag.py").read_text(encoding="utf-8")
    )
    for node in tree.body:
        if (isinstance(node, ast.Assign)
                and getattr(node.targets[0], "id", None) == "DOWNSTREAM_DAG_IDS"):
            return [e.value for e in node.value.elts]
    raise AssertionError("DOWNSTREAM_DAG_IDS not found as a module-level constant")


def test_every_relay_target_looks_like_a_dag_id():
    targets = relay_targets_from_dag_file()

    assert targets
    assert all(t.endswith("_dag") for t in targets)


def test_relay_targets_are_not_also_registry_routed():
    """The §6.5 dual-channel defect, asserted as a test: this exemplar's registry must
    not contain any relay target, or those DAGs would be triggered twice per event."""
    registry = json.loads(
        (Path(__file__).parent / "registry.json").read_text(encoding="utf-8")
    )

    assert not set(registry) & set(relay_targets_from_dag_file())
