"""--with-dagbag behaviour: the flag must never be a silent no-op.

The shell-out boundary (shutil.which / subprocess.run) is monkeypatched — these
tests pin down the contract: WARN when airflow is absent, ERROR when the DagBag
names our file, WARN when airflow fails without naming it, silence when clean.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_calculator_dag as v  # noqa: E402


def args_for(tmp_path, with_dagbag=True):
    f = tmp_path / "x_dag.py"
    f.write_text("x = 1\n")
    return SimpleNamespace(dag_id="x_dag", dag_file=f, logic_module=None,
                           registry=None, test_file=None, fixture=None,
                           with_dagbag=with_dagbag)


def test_flag_off_is_silent(tmp_path):
    assert v.check_dagbag(args_for(tmp_path, with_dagbag=False)) == []


def test_airflow_missing_warns(tmp_path, monkeypatch):
    monkeypatch.setattr(v.shutil, "which", lambda name: None)
    findings = v.check_dagbag(args_for(tmp_path))
    assert [f.severity for f in findings] == ["WARN"]
    assert "skipped" in findings[0].message


def test_import_error_for_dag_file_is_error(tmp_path, monkeypatch):
    monkeypatch.setattr(v.shutil, "which", lambda name: "/usr/bin/airflow")
    monkeypatch.setattr(v.subprocess, "run", lambda cmd, **kw: SimpleNamespace(
        returncode=1,
        stdout="dags/x_dag.py | ModuleNotFoundError: No module named 'foo'\n",
        stderr=""))
    findings = v.check_dagbag(args_for(tmp_path))
    assert any(f.severity == "ERROR" and "x_dag.py" in f.message for f in findings)


def test_clean_dagbag_passes(tmp_path, monkeypatch):
    monkeypatch.setattr(v.shutil, "which", lambda name: "/usr/bin/airflow")
    monkeypatch.setattr(v.subprocess, "run", lambda cmd, **kw: SimpleNamespace(
        returncode=0, stdout="No data found\n", stderr=""))
    assert v.check_dagbag(args_for(tmp_path)) == []


def test_airflow_command_failure_warns(tmp_path, monkeypatch):
    # airflow itself broke (no DB, bad config) without naming our file: the check
    # could not attest either way — surface that, do not fail the DAG for it.
    monkeypatch.setattr(v.shutil, "which", lambda name: "/usr/bin/airflow")
    monkeypatch.setattr(v.subprocess, "run", lambda cmd, **kw: SimpleNamespace(
        returncode=3, stdout="", stderr="sqlalchemy.exc.OperationalError: boom"))
    findings = v.check_dagbag(args_for(tmp_path))
    assert [f.severity for f in findings] == ["WARN"]
    assert "could not attest" in findings[0].message
