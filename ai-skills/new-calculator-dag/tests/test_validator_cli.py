import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "validate_calculator_dag.py"


def run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args],
                          capture_output=True, text=True)


def test_missing_files_exit_2_with_message(tmp_path):
    r = run("--dag-id", "x_dag",
            "--dag-file", str(tmp_path / "nope.py"),
            "--logic-module", str(tmp_path / "nope2.py"),
            "--registry", str(tmp_path / "nope.json"))
    assert r.returncode == 2
    assert "not found" in r.stdout + r.stderr


def test_help_runs():
    r = run("--help")
    assert r.returncode == 0
    assert "--with-dagbag" in r.stdout
