import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "update_registry.py"


def run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args],
                          capture_output=True, text=True)


def seed(tmp_path):
    reg = tmp_path / "reg.json"
    reg.write_text(json.dumps({"existing_dag": "CALC.CAPITALCALC"}, indent=2))
    return reg


def test_adds_entry_and_backup(tmp_path):
    reg = seed(tmp_path)
    r = run("--registry", str(reg), "--dag-id", "new_dag",
            "--condition", "SOURCE.MERIVAL.USRG")
    assert r.returncode == 0
    data = json.loads(reg.read_text())
    assert data["new_dag"] == "SOURCE.MERIVAL.USRG"
    assert data["existing_dag"] == "CALC.CAPITALCALC"        # preserved
    assert (tmp_path / "reg.json.bak").exists()


def test_multiple_conditions_become_list(tmp_path):
    reg = seed(tmp_path)
    run("--registry", str(reg), "--dag-id", "new_dag",
        "--condition", "CALC.FLOORSCALC", "--condition", "DATASET.OUTPUTPOSTING")
    assert json.loads(reg.read_text())["new_dag"] == [
        "CALC.FLOORSCALC", "DATASET.OUTPUTPOSTING"]


def test_refuses_duplicate(tmp_path):
    reg = seed(tmp_path)
    r = run("--registry", str(reg), "--dag-id", "existing_dag",
            "--condition", "CALC.OTHER")
    assert r.returncode == 1
    assert "already registered" in r.stdout + r.stderr
    assert json.loads(reg.read_text())["existing_dag"] == "CALC.CAPITALCALC"


def test_refuses_bad_grammar(tmp_path):
    reg = seed(tmp_path)
    r = run("--registry", str(reg), "--dag-id", "new_dag",
            "--condition", "source.merival")
    assert r.returncode == 1
    assert "new_dag" not in json.loads(reg.read_text())


def test_refuses_underscore_drift_against_existing_registry(tmp_path):
    # SOURCE.MERIVAL_AMER when the registry already uses SOURCE.MERIVAL.AMER
    # elsewhere: the guarded editor must refuse at write time, not leave it for
    # the validator to catch two steps later.
    reg = tmp_path / "reg.json"
    reg.write_text(json.dumps({"other_dag": "SOURCE.MERIVAL.AMER"}, indent=2))
    r = run("--registry", str(reg), "--dag-id", "new_dag",
            "--condition", "SOURCE.MERIVAL_AMER")
    assert r.returncode == 1
    assert "did you mean" in r.stdout + r.stderr
    assert "new_dag" not in json.loads(reg.read_text())


def test_refuses_registry_with_duplicate_keys(tmp_path):
    # JSON allows duplicate keys (last wins); rewriting such a file would silently
    # collapse them, so the editor must refuse and leave the file untouched.
    reg = tmp_path / "reg.json"
    reg.write_text('{"a_dag": "CALC.CAPITALCALC", "a_dag": "CALC.FLOORSCALC"}')
    original = reg.read_text()
    r = run("--registry", str(reg), "--dag-id", "new_dag",
            "--condition", "CALC.CAPITALCALC")
    assert r.returncode == 1
    assert "duplicate" in (r.stdout + r.stderr).lower()
    assert reg.read_text() == original


def test_near_miss_key_warns_but_succeeds(tmp_path):
    # Adding amer_d_b3f_dag next to the drifted amer_b3f_dag is legitimate (it may
    # BE the drift fix) — warn about the near-miss, do not refuse.
    reg = tmp_path / "reg.json"
    reg.write_text(json.dumps({"amer_b3f_dag": "SOURCE.MERIVAL.AMER"}, indent=2))
    r = run("--registry", str(reg), "--dag-id", "amer_d_b3f_dag",
            "--condition", "SOURCE.MERIVAL.AMER")
    assert r.returncode == 0
    assert "near-miss" in r.stdout + r.stderr
    assert json.loads(reg.read_text())["amer_d_b3f_dag"] == "SOURCE.MERIVAL.AMER"
