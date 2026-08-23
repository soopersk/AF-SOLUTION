"""The skill's regression net: every exemplar must stay validator-green, and the real
drift bug from old-orchestration must stay caught end-to-end."""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_calculator_dag.py"

EXEMPLARS = [
    ("01-single-invocation", "usrg_ihc_dag", "usrg_ihc_calculator.py",
     "test_usrg_ihc_calculator.py", "fixture_merival_usrg.json"),
    ("02-parametrized-fanout", "amer_d_b3f_dag", "capital_calculator.py",
     "test_capital_calculator.py", "fixture_merival_amer.json"),
    ("03-dataset-gated", "output_floor_monthly_dag", "floors_calculator.py",
     "test_floors_calculator.py", "fixture_floors_calc.json"),
    ("04-relay-fanout", "hdl_process_dag", "hdl_calculator.py",
     "test_hdl_calculator.py", "fixture_hdl_ingest.json"),
]


def run_validator(dirname, dag_id, logic, test, fixture):
    d = ROOT / "examples" / dirname
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--dag-id", dag_id,
         "--dag-file", str(d / f"{dag_id}.py"),
         "--logic-module", str(d / logic),
         "--registry", str(d / "registry.json"),
         "--test-file", str(d / test),
         "--fixture", str(d / fixture)],
        capture_output=True, text=True)


def test_all_exemplars_are_validator_green():
    for row in EXEMPLARS:
        r = run_validator(*row)
        assert r.returncode == 0, f"{row[0]} failed:\n{r.stdout}\n{r.stderr}"


def test_real_drift_registry_fails_for_amer():
    # The live bug from old-orchestration must be caught end-to-end.
    d = ROOT / "examples" / "02-parametrized-fanout"
    r = subprocess.run(
        [sys.executable, str(SCRIPT), "--dag-id", "amer_d_b3f_dag",
         "--dag-file", str(d / "amer_d_b3f_dag.py"),
         "--logic-module", str(d / "capital_calculator.py"),
         "--registry", str(ROOT / "tests" / "fixtures" / "registry_drift.json")],
        capture_output=True, text=True)
    assert r.returncode == 1
    assert "amer_b3f_dag" in r.stdout  # near-miss named
