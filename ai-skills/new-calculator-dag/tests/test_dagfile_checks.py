import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_calculator_dag as v  # noqa: E402

GOOD_DAG = '''
import pendulum
from airflow.decorators import dag

@dag(dag_id="usrg_ihc_dag", schedule=None, catchup=False,
     start_date=pendulum.datetime(2021, 1, 1, tz="Europe/Zurich"))
def usrg_ihc_dag():
    pass

usrg_ihc_dag()
'''


def args_for(tmp_path, content, filename="usrg_ihc_dag.py", dag_id="usrg_ihc_dag"):
    f = tmp_path / filename
    f.write_text(content)
    return SimpleNamespace(dag_id=dag_id, dag_file=f, logic_module=None,
                           registry=None, test_file=None, fixture=None,
                           with_dagbag=False)


def test_good_dag_file_passes(tmp_path):
    assert not [f for f in v.check_dag_file(args_for(tmp_path, GOOD_DAG))
                if f.severity == "ERROR"]


def test_dag_id_mismatch_is_error(tmp_path):
    a = args_for(tmp_path, GOOD_DAG, dag_id="other_dag", filename="other_dag.py")
    errs = [f for f in v.check_dag_file(a) if f.severity == "ERROR"]
    assert any("dag_id" in f.message for f in errs)


def test_filename_stem_mismatch_is_error(tmp_path):
    a = args_for(tmp_path, GOOD_DAG, filename="wrong_name.py")
    errs = [f for f in v.check_dag_file(a) if f.severity == "ERROR"]
    assert any("filename" in f.message for f in errs)


def test_syntax_error_is_error(tmp_path):
    errs = [f for f in v.check_dag_file(args_for(tmp_path, "def broken(:"))
            if f.severity == "ERROR"]
    assert any("parse" in f.message for f in errs)


def test_schedule_not_none_warns(tmp_path):
    content = GOOD_DAG.replace("schedule=None", 'schedule="@daily"')
    warns = [f for f in v.check_dag_file(args_for(tmp_path, content))
             if f.severity == "WARN"]
    assert any("schedule" in f.message for f in warns)


def test_missing_module_level_invocation_is_error(tmp_path):
    # Without the module-level call, Airflow never registers the DAG — the file
    # parses cleanly while the DAG silently does not exist (contract §10).
    content = GOOD_DAG.replace("\nusrg_ihc_dag()\n", "\n")
    errs = [f for f in v.check_dag_file(args_for(tmp_path, content))
            if f.severity == "ERROR"]
    assert any("invoked" in f.message for f in errs)


def test_invocation_via_assignment_passes(tmp_path):
    content = GOOD_DAG.replace("\nusrg_ihc_dag()\n", "\nthe_dag = usrg_ihc_dag()\n")
    assert not [f for f in v.check_dag_file(args_for(tmp_path, content))
                if f.severity == "ERROR"]


def test_multiple_dag_functions_warn(tmp_path):
    second = GOOD_DAG.replace("usrg_ihc_dag", "second_dag")
    warns = [f for f in v.check_dag_file(args_for(tmp_path, GOOD_DAG + second))
             if f.severity == "WARN"]
    assert any("one DAG per file" in f.message for f in warns)
