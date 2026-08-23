import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_calculator_dag as v  # noqa: E402

FIX = Path(__file__).parent / "fixtures"


def args_for(path):
    return SimpleNamespace(dag_id="x_dag", dag_file=None, logic_module=path,
                           registry=None, test_file=None, fixture=None,
                           with_dagbag=False)


def errors(path):
    return [f for f in v.check_logic_module(args_for(path)) if f.severity == "ERROR"]


def test_good_module_passes():
    assert errors(FIX / "logic_good.py") == []


def test_seconds_truncation_is_error():
    errs = errors(FIX / "logic_bad_seconds.py")
    assert any("total_seconds" in f.message for f in errs)


def test_missing_protocol_method_is_error(tmp_path):
    src = (FIX / "logic_good.py").read_text().replace("def get_calc_name", "def renamed")
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("get_calc_name" in f.message for f in errors(bad))


def test_missing_push_xcom_is_error(tmp_path):
    src = (FIX / "logic_good.py").read_text().replace("push_xcom", "no_op")
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("push_xcom" in f.message for f in errors(bad))


def test_group_id_without_calc_suffix_is_error(tmp_path):
    src = (FIX / "logic_good.py").read_text().replace("_CALC\"", "_GROUP\"")
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("_CALC" in f.message for f in errors(bad))


def test_bad_frequency_value_is_error(tmp_path):
    # Design §5: allowed value sets for frequency (D/M only).
    src = (FIX / "logic_good.py").read_text().replace('frequency="M"', 'frequency="W"')
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("frequency" in f.message for f in errors(bad))


def test_bad_run_type_value_is_error(tmp_path):
    # Design §5: allowed value sets for run types (BATCH/INTRA only).
    src = (FIX / "logic_good.py").read_text().replace('"INTRA"', '"ADHOC"')
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("run type" in f.message for f in errors(bad))


def test_group_id_calc_not_at_end_is_error(tmp_path):
    # '..._CALC_GROUP' CONTAINS _CALC but does not END with it — normalize_group_id's
    # strip would be a no-op, so a substring match must not be accepted.
    src = (FIX / "logic_good.py").read_text().replace('_CALC"', '_CALC_GROUP"')
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("_CALC" in f.message for f in errors(bad))


def test_variable_mediated_seconds_is_error(tmp_path):
    # `td = timedelta(...)` then `td.seconds` is the same truncation bug one
    # assignment removed; the direct-attribute check alone misses it.
    src = (FIX / "logic_good.py").read_text().replace(
        "timeout=int(datetime.timedelta(hours=5).total_seconds()),",
        "timeout=_TD.seconds,")
    src += "\n_TD = datetime.timedelta(hours=5)\n"
    bad = tmp_path / "m.py"; bad.write_text(src)
    assert any("total_seconds" in f.message for f in errors(bad))


def test_unwrapped_total_seconds_warns(tmp_path):
    # The contract mandates int(...total_seconds()); a bare float should WARN.
    src = (FIX / "logic_good.py").read_text().replace(
        "timeout=int(datetime.timedelta(hours=5).total_seconds()),",
        "timeout=datetime.timedelta(hours=5).total_seconds(),")
    bad = tmp_path / "m.py"; bad.write_text(src)
    warns = [f for f in v.check_logic_module(args_for(bad)) if f.severity == "WARN"]
    assert any("int(" in f.message for f in warns)


def test_multiple_init_classes_warn(tmp_path):
    src = (FIX / "logic_good.py").read_text()
    src += "\n\nclass SecondInit:\n    pass\n"
    bad = tmp_path / "m.py"; bad.write_text(src)
    warns = [f for f in v.check_logic_module(args_for(bad)) if f.severity == "WARN"]
    assert any("only the first" in f.message for f in warns)
