import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_calculator_dag as v  # noqa: E402

FIX = Path(__file__).parent / "fixtures"


def make_args(tmp_path, test_src, fixture_obj, registry_obj=None, dag_id="x_dag"):
    t = tmp_path / "test_x.py"; t.write_text(test_src)
    fx = tmp_path / "fixture.json"; fx.write_text(json.dumps(fixture_obj))
    reg = tmp_path / "r.json"
    reg.write_text(json.dumps(registry_obj or {dag_id: "SOURCE.MERIVAL.USRG"}))
    return SimpleNamespace(dag_id=dag_id, dag_file=None,
                           logic_module=FIX / "logic_good.py",
                           registry=reg, test_file=t, fixture=fx, with_dagbag=False)


SOURCE_FIXTURE = {"event": {"source": "MERIVAL", "additionalData": {}}, "context": {"data": {}}}


def test_good_test_and_fixture_pass(tmp_path):
    a = make_args(tmp_path, "from logic_good import CapitalXInit\n", SOURCE_FIXTURE)
    assert not [f for f in v.check_test_and_fixture(a) if f.severity == "ERROR"]


def test_test_not_importing_init_is_error(tmp_path):
    a = make_args(tmp_path, "import json\n", SOURCE_FIXTURE)
    errs = [f for f in v.check_test_and_fixture(a) if f.severity == "ERROR"]
    assert any("Init" in f.message for f in errs)


def test_source_condition_requires_event_source_field(tmp_path):
    a = make_args(tmp_path, "from logic_good import CapitalXInit\n",
                  {"event": {"additionalData": {}}})
    warns = [f for f in v.check_test_and_fixture(a) if f.severity == "WARN"]
    assert any("source" in f.message for f in warns)


def test_calc_condition_requires_calc_event_fields(tmp_path):
    a = make_args(tmp_path, "from logic_good import CapitalXInit\n",
                  {"event": {"additionalData": {}}},
                  registry_obj={"x_dag": "CALC.CAPITALCALC"})
    warns = [f for f in v.check_test_and_fixture(a) if f.severity == "WARN"]
    assert any("CALC_EVENT" in f.message or "type" in f.message for f in warns)
