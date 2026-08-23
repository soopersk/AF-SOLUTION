import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_calculator_dag as v  # noqa: E402

FIX = Path(__file__).parent / "fixtures"


def args_for(registry, dag_id="usrg_ihc_dag"):
    return SimpleNamespace(dag_id=dag_id, registry=registry,
                           dag_file=None, logic_module=None,
                           test_file=None, fixture=None, with_dagbag=False)


def test_dag_id_present_passes():
    assert not [f for f in v.check_registry(args_for(FIX / "registry_good.json"))
                if f.severity == "ERROR"]


def test_missing_dag_id_is_error():
    errs = [f for f in v.check_registry(
        args_for(FIX / "registry_good.json", dag_id="amer_d_b3f_dag_TYPO"))
        if f.severity == "ERROR"]
    assert errs and "not registered" in errs[0].message


def test_real_amer_drift_caught(tmp_path):
    # Validating amer_d_b3f_dag against the drifted registry must ERROR
    # AND name the near-miss key so the author sees the drift, not just absence.
    findings = v.check_registry(args_for(FIX / "registry_drift.json",
                                         dag_id="amer_d_b3f_dag"))
    msgs = " ".join(f.message for f in findings if f.severity == "ERROR")
    assert "not registered" in msgs and "amer_b3f_dag" in msgs


def test_duplicate_keys_are_error(tmp_path):
    dup = tmp_path / "r.json"
    dup.write_text('{"a_dag": "CALC.X", "a_dag": "CALC.Y"}')
    errs = [f for f in v.check_registry(args_for(dup, dag_id="a_dag"))
            if f.severity == "ERROR"]
    assert any("duplicate" in f.message for f in errs)


def test_bad_condition_grammar_is_error(tmp_path):
    bad = tmp_path / "r.json"
    bad.write_text(json.dumps({"x_dag": "source.merival"}))
    errs = [f for f in v.check_registry(args_for(bad, dag_id="x_dag"))
            if f.severity == "ERROR"]
    assert errs


def test_condition_list_form_supported(tmp_path):
    r = tmp_path / "r.json"
    r.write_text(json.dumps({"x_dag": ["CALC.CAPITALCALC", "DATASET.OUTPUTPOSTING"]}))
    assert not [f for f in v.check_registry(args_for(r, dag_id="x_dag"))
                if f.severity == "ERROR"]
