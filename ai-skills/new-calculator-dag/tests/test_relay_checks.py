import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import validate_calculator_dag as v  # noqa: E402

GOOD_RELAY = '''
DOWNSTREAM_DAG_IDS = ["nsfr_deposit_cals_dag", "nsfr_loans_cals_dag"]

@task(task_id="BUILD_TRIGGER_CONF")
def build_trigger_conf_task(**context):
    return build_trigger_conf(context, "HDL_RESULT", "xcom")

trigger_conf = build_trigger_conf_task()

trigger_dags = TriggerDagRunOperator.partial(
    task_id="TRIGGER_DEPENDENT_DAGS",
    conf=trigger_conf,
    trigger_run_id=deterministic_relay_run_id,
    wait_for_completion=False,
).expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)
'''

# Same relay, but no payload-builder task in the file (design §5: BUILD_TRIGGER_CONF
# must exist when the conf is task/XCom-sourced).
RELAY_NO_BUILDER = '''
DOWNSTREAM_DAG_IDS = ["nsfr_deposit_cals_dag", "nsfr_loans_cals_dag"]

trigger_dags = TriggerDagRunOperator.partial(
    task_id="TRIGGER_DEPENDENT_DAGS",
    conf=trigger_conf,
    trigger_run_id=deterministic_relay_run_id,
    wait_for_completion=False,
).expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)
'''


def args_for(tmp_path, content, registry_obj=None):
    f = tmp_path / "hdl_process_dag.py"
    f.write_text(content)
    reg = tmp_path / "r.json"
    reg.write_text(json.dumps(registry_obj if registry_obj is not None
                              else {"hdl_process_dag": "CALC.LNFHDLINGESTCALC"}))
    return SimpleNamespace(dag_id="hdl_process_dag", dag_file=f, logic_module=None,
                           registry=reg, test_file=None, fixture=None,
                           with_dagbag=False)


def findings(tmp_path, content, **kw):
    return v.check_relay(args_for(tmp_path, content, **kw))


def test_good_relay_passes(tmp_path):
    assert findings(tmp_path, GOOD_RELAY) == []


def test_non_relay_dag_is_silent(tmp_path):
    assert findings(tmp_path, "x = 1\n") == []


def test_registry_overlap_warns_double_trigger(tmp_path):
    # The live §6.5 defect: relay target also event-registered.
    f = findings(tmp_path, GOOD_RELAY,
                 registry_obj={"hdl_process_dag": "CALC.LNFHDLINGESTCALC",
                               "nsfr_deposit_cals_dag": "CALC.LNFHDLPROCESSCALC"})
    warns = [x for x in f if x.severity == "WARN"]
    assert any("double-trigger" in x.message and "nsfr_deposit_cals_dag" in x.message
               for x in warns)


def test_missing_trigger_run_id_warns(tmp_path):
    content = GOOD_RELAY.replace("    trigger_run_id=deterministic_relay_run_id,\n", "")
    f = findings(tmp_path, content)
    assert any("trigger_run_id" in x.message and x.severity == "WARN" for x in f)


def test_missing_wait_for_completion_warns(tmp_path):
    content = GOOD_RELAY.replace("    wait_for_completion=False,\n", "")
    f = findings(tmp_path, content)
    assert any("wait_for_completion" in x.message and x.severity == "WARN" for x in f)


def test_dynamic_targets_are_error(tmp_path):
    content = GOOD_RELAY.replace(
        "expand(trigger_dag_id=DOWNSTREAM_DAG_IDS)",
        "expand(trigger_dag_id=compute_targets())")
    f = findings(tmp_path, content)
    assert any("static list" in x.message and x.severity == "ERROR" for x in f)


def test_non_dag_id_target_is_error(tmp_path):
    content = GOOD_RELAY.replace('"nsfr_loans_cals_dag"', '"nsfr_loans"')
    f = findings(tmp_path, content)
    assert any("does not look like a dag_id" in x.message and x.severity == "ERROR"
               for x in f)


def test_xcom_conf_without_payload_task_warns(tmp_path):
    f = findings(tmp_path, RELAY_NO_BUILDER)
    assert any("BUILD_TRIGGER_CONF" in x.message and x.severity == "WARN" for x in f)


def test_literal_conf_needs_no_payload_task(tmp_path):
    content = RELAY_NO_BUILDER.replace("conf=trigger_conf", 'conf={"static": "payload"}')
    f = findings(tmp_path, content)
    assert not any("BUILD_TRIGGER_CONF" in x.message for x in f)
