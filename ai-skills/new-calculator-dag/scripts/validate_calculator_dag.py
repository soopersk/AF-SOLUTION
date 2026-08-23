"""Deterministic post-generation gate for calculator DAG artifacts.

Exit codes: 0 = all checks pass (warnings allowed), 1 = at least one ERROR,
2 = usage/IO problem (missing file, unreadable input).

stdlib-only. AST-based — no Airflow install required. --with-dagbag is a
CI-only escape hatch that shells out to a real `airflow dags list-import-errors`
if Airflow is on PATH (skipped otherwise with a WARN).
"""
from __future__ import annotations

import argparse
import ast
import difflib
import json
import shutil
import subprocess
import sys
from pathlib import Path

from condition_grammar import (
    Finding,
    check_condition,
    known_idents_from_registry,
    load_registry_detect_dupes,
)

CHECKS = []  # populated by register() below


def register(fn):
    CHECKS.append(fn)
    return fn


# --------------------------------------------------------------------------
# Registry checks
# --------------------------------------------------------------------------

# Shared with update_registry.py so both tools read the file identically.
_load_registry_detect_dupes = load_registry_detect_dupes


@register
def check_registry(args) -> list[Finding]:
    findings: list[Finding] = []
    try:
        registry, dupes = _load_registry_detect_dupes(args.registry)
    except json.JSONDecodeError as exc:
        return [Finding("ERROR", f"registry is not valid JSON: {exc}")]

    for d in dupes:
        findings.append(Finding("ERROR", f"registry has duplicate key '{d}'"))

    if args.dag_id not in registry:
        near = difflib.get_close_matches(args.dag_id, registry.keys(), n=1)
        hint = f" (near-miss existing key: '{near[0]}' — registry drift?)" if near else ""
        findings.append(Finding("ERROR",
            f"dag_id '{args.dag_id}' not registered in {args.registry.name}{hint}"))
    else:
        conds = registry[args.dag_id]
        conds = conds if isinstance(conds, list) else [conds]
        # Idents already used with a region elsewhere feed the underscore-drift guard.
        known_idents = known_idents_from_registry(registry)
        for c in conds:
            findings.extend(check_condition(c, known_idents=known_idents))
    return findings


# --------------------------------------------------------------------------
# DAG-file checks
# --------------------------------------------------------------------------

def _dag_decorator_kwargs(tree: ast.Module):
    """Yield (func_name, {kw: value_node}) for every @dag(...)-decorated function."""
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if (isinstance(dec, ast.Call)
                        and getattr(dec.func, "id", getattr(dec.func, "attr", "")) == "dag"):
                    yield node.name, {kw.arg: kw.value for kw in dec.keywords}


@register
def check_dag_file(args) -> list[Finding]:
    if args.dag_file is None:
        return []
    findings: list[Finding] = []
    try:
        tree = ast.parse(args.dag_file.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return [Finding("ERROR", f"DAG file failed to parse: {exc}")]

    decorated = list(_dag_decorator_kwargs(tree))
    if not decorated:
        return [Finding("ERROR", "no @dag(...)-decorated function found in DAG file")]
    if len(decorated) > 1:
        findings.append(Finding("WARN",
            f"file defines {len(decorated)} @dag-decorated functions — only the first "
            f"('{decorated[0][0]}') is checked; convention is one DAG per file"))

    func_name, kwargs = decorated[0]
    dag_id_node = kwargs.get("dag_id")
    declared = dag_id_node.value if isinstance(dag_id_node, ast.Constant) else None
    if declared != args.dag_id:
        findings.append(Finding("ERROR",
            f"@dag(dag_id={declared!r}) does not match expected dag_id '{args.dag_id}'"))
    if args.dag_file.stem != args.dag_id:
        findings.append(Finding("ERROR",
            f"filename '{args.dag_file.name}' stem must equal dag_id '{args.dag_id}' "
            f"(convention: one DAG per file, file named after it)"))
    sched = kwargs.get("schedule")
    if not (isinstance(sched, ast.Constant) and sched.value is None):
        findings.append(Finding("WARN",
            "schedule is not None — calculator DAGs are event-triggered via the "
            "registry; a real schedule belongs to prod/*_trigger_dag_prod.py"))

    # Module-level invocation: without `<func>()` at module level Airflow never
    # registers the DAG — the file parses cleanly while the DAG silently does not
    # exist (authoring-contract.md §10).
    invoked = False
    for node in tree.body:
        value = node.value if isinstance(node, (ast.Expr, ast.Assign)) else None
        if (isinstance(value, ast.Call) and isinstance(value.func, ast.Name)
                and value.func.id == func_name):
            invoked = True
            break
    if not invoked:
        findings.append(Finding("ERROR",
            f"@dag function '{func_name}' is never invoked at module level — Airflow "
            f"never registers the DAG; add '{func_name}()' at the bottom of the file"))
    return findings


# --------------------------------------------------------------------------
# Logic-module checks (the contract enforcement)
# --------------------------------------------------------------------------

# Allowed value sets per the trigger-conditions catalogue.
_ALLOWED_FREQUENCIES = {"D", "M"}
_ALLOWED_RUN_TYPES = {"BATCH", "INTRA"}


@register
def check_logic_module(args) -> list[Finding]:
    if args.logic_module is None:
        return []
    findings: list[Finding] = []
    src = args.logic_module.read_text(encoding="utf-8")
    try:
        tree = ast.parse(src)
    except SyntaxError as exc:
        return [Finding("ERROR", f"logic module failed to parse: {exc}")]

    # 1. Init class implements the TaskCallbackFunction protocol
    #    (base_task.py: __call__ + get_calc_name + get_calculator on the class).
    init_classes = [n for n in ast.walk(tree)
                    if isinstance(n, ast.ClassDef) and n.name.endswith("Init")]
    if not init_classes:
        findings.append(Finding("ERROR",
            "no '*Init' class found — the calc-init callback must be a class "
            "named <Calc>Init implementing the TaskCallbackFunction protocol"))
    else:
        if len(init_classes) > 1:
            findings.append(Finding("WARN",
                f"module defines {len(init_classes)} '*Init' classes — only the first "
                f"('{init_classes[0].name}') is checked against the protocol"))
        cls = init_classes[0]
        methods = {m.name for m in cls.body if isinstance(m, ast.FunctionDef)}
        for required in ("__call__", "get_calc_name", "get_calculator"):
            if required not in methods:
                findings.append(Finding("ERROR",
                    f"Init class '{cls.name}' missing method '{required}' "
                    f"(module-level definitions do not satisfy the protocol)"))
        call = next((m for m in cls.body
                     if isinstance(m, ast.FunctionDef) and m.name == "__call__"), None)
        if call is not None and "push_xcom" not in ast.dump(call):
            findings.append(Finding("ERROR",
                "__call__ never calls push_xcom — downstream mapped tasks pull "
                "CALC_RUN_PARAMS from XCom; without the push they see stale/empty params"))

    # 2. mapped_calc_task_group_component call: group_id ends with _CALC,
    #    required kwargs present, timeout arithmetic safe.
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call)
                and getattr(node.func, "id", getattr(node.func, "attr", ""))
                == "mapped_calc_task_group_component"):
            kwargs = {kw.arg: kw.value for kw in node.keywords}
            for required in ("group_id", "calculator", "calc_init_func", "timeout"):
                if required not in kwargs:
                    findings.append(Finding("ERROR",
                        f"mapped_calc_task_group_component missing kwarg '{required}'"))
            gid = kwargs.get("group_id")
            gid_src = ast.get_source_segment(src, gid) if gid is not None else ""
            # endswith, not substring: '..._CALC_GROUP' contains _CALC but
            # normalize_group_id's strip of a TRAILING _CALC would be a no-op.
            gid_norm = gid_src.rstrip("\"'") if gid_src else ""
            if gid_norm and not gid_norm.endswith("_CALC"):
                findings.append(Finding("ERROR",
                    f"group_id {gid_src} must end with '_CALC' — normalize_group_id "
                    f"strips that suffix to derive task ids; without it XCom wiring breaks"))

            timeout_node = kwargs.get("timeout")
            if (isinstance(timeout_node, ast.Call)
                    and isinstance(timeout_node.func, ast.Attribute)
                    and timeout_node.func.attr == "total_seconds"):
                findings.append(Finding("WARN",
                    "timeout=...total_seconds() without int(...) — the contract form "
                    "is int(timedelta(...).total_seconds())"))

    # 3. timedelta(...).seconds anywhere in the module is always a bug — whether
    #    directly on the call or through a variable assigned from timedelta(...).
    td_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            callee = getattr(node.value.func, "id",
                             getattr(node.value.func, "attr", ""))
            if callee == "timedelta":
                td_names.update(t.id for t in node.targets
                                if isinstance(t, ast.Name))
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == "seconds":
            base = node.value
            is_td = ((isinstance(base, ast.Call)
                      and getattr(base.func, "id",
                                  getattr(base.func, "attr", "")) == "timedelta")
                     or (isinstance(base, ast.Name) and base.id in td_names))
            if is_td:
                findings.append(Finding("ERROR",
                    "timedelta(...).seconds truncates the days component — "
                    "use int(timedelta(...).total_seconds())"))

    # 4. Pre-condition criteria value sets (design §5): FrequencyCriteria and
    #    RunTypeCriteria only accept catalogued values; anything else is a typo
    #    that would silently skip every run.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", getattr(node.func, "attr", ""))
        if name == "FrequencyCriteria":
            vals = [kw.value for kw in node.keywords if kw.arg == "frequency"]
            vals += list(node.args[:1])
            for val in vals:
                if isinstance(val, ast.Constant) and val.value not in _ALLOWED_FREQUENCIES:
                    findings.append(Finding("ERROR",
                        f"FrequencyCriteria frequency {val.value!r} not in allowed set "
                        f"{sorted(_ALLOWED_FREQUENCIES)}"))
        elif name == "RunTypeCriteria":
            vals = list(node.args) + [kw.value for kw in node.keywords]
            for val in vals:
                elts = val.elts if isinstance(val, (ast.List, ast.Tuple)) else [val]
                for e in elts:
                    if isinstance(e, ast.Constant) and e.value not in _ALLOWED_RUN_TYPES:
                        findings.append(Finding("ERROR",
                            f"RunTypeCriteria run type {e.value!r} not in allowed set "
                            f"{sorted(_ALLOWED_RUN_TYPES)}"))
    return findings


# --------------------------------------------------------------------------
# Test-file + fixture coherence checks
# --------------------------------------------------------------------------

# Fields the control-DAG condition evaluators actually read per condition type
# (trigger_conditions.py: is_source/is_calc/is_*_dataset completion checks).
_FIXTURE_EXPECTATIONS = {
    "SOURCE": (("event", "source"), "event.source"),
    "CALC": (("event", "additionalData", "type"),
             "event.additionalData.type (CALC_EVENT) + STATE"),
    "DATASET": (("event", "additionalData", "DATASET_NAME"),
                "event.additionalData.DATASET_NAME"),
    "PIPELINEID": (("event", "additionalData"), "event.additionalData pipeline id"),
}


def _dig(obj, keys):
    for k in keys:
        if not isinstance(obj, dict) or k not in obj:
            return None
        obj = obj[k]
    return obj


@register
def check_test_and_fixture(args) -> list[Finding]:
    if args.test_file is None or args.fixture is None:
        return []
    findings: list[Finding] = []

    # Test must import the Init class from the logic module.
    init_name = None
    logic_tree = ast.parse(args.logic_module.read_text(encoding="utf-8"))
    for n in ast.walk(logic_tree):
        if isinstance(n, ast.ClassDef) and n.name.endswith("Init"):
            init_name = n.name
            break
    if init_name and init_name not in args.test_file.read_text(encoding="utf-8"):
        findings.append(Finding("ERROR",
            f"test file never references '{init_name}' — the unit tests must "
            f"exercise the Init class fan-out directly"))

    # Fixture must carry the fields the registered condition type inspects.
    try:
        fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return findings + [Finding("ERROR", f"fixture is not valid JSON: {exc}")]

    registry, _ = _load_registry_detect_dupes(args.registry)
    conds = registry.get(args.dag_id, [])
    conds = conds if isinstance(conds, list) else [conds]
    for c in conds:
        ctype = str(c).split(".")[0]
        expectation = _FIXTURE_EXPECTATIONS.get(ctype)
        if expectation and _dig(fixture, expectation[0]) is None:
            findings.append(Finding("WARN",
                f"fixture lacks {expectation[1]}, which condition '{c}' inspects — "
                f"the generated test may not represent a routable event"))
    return findings


# --------------------------------------------------------------------------
# Relay checks (TriggerDagRunOperator fan-out)
# --------------------------------------------------------------------------

_TRIGGER_OP = "TriggerDagRunOperator"


def _module_list_constants(tree: ast.Module) -> dict[str, list]:
    consts = {}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, (ast.List, ast.Tuple))):
            consts[node.targets[0].id] = [
                e.value for e in node.value.elts if isinstance(e, ast.Constant)]
    return consts


def _has_payload_builder_task(tree: ast.Module) -> bool:
    """A BUILD_TRIGGER_CONF-style payload task: a function whose name mentions
    trigger_conf, or any call carrying a task_id containing TRIGGER_CONF."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and "trigger_conf" in node.name:
            return True
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if (kw.arg == "task_id" and isinstance(kw.value, ast.Constant)
                        and "TRIGGER_CONF" in str(kw.value.value)):
                    return True
    return False


@register
def check_relay(args) -> list[Finding]:
    """Relay fan-out checks (hdl_process_dag pattern). Guards the §6.5
    dual-channel double-trigger defect and the Risk-3 duplicate-run defect."""
    if args.dag_file is None:
        return []
    src = args.dag_file.read_text(encoding="utf-8")
    if _TRIGGER_OP not in src:
        return []
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []  # already reported by check_dag_file
    findings: list[Finding] = []
    consts = _module_list_constants(tree)

    relay_calls = []  # merged kwarg-node dicts, one per relay declaration
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "expand"):
            inner = node.func.value
            if (isinstance(inner, ast.Call) and isinstance(inner.func, ast.Attribute)
                    and inner.func.attr == "partial"
                    and getattr(inner.func.value, "id", "") == _TRIGGER_OP):
                merged = {kw.arg: kw.value for kw in inner.keywords}
                merged.update({kw.arg: kw.value for kw in node.keywords})
                relay_calls.append(merged)
        elif isinstance(node, ast.Call) and getattr(node.func, "id", "") == _TRIGGER_OP:
            relay_calls.append({kw.arg: kw.value for kw in node.keywords})

    if not relay_calls:
        return [Finding("WARN",
            f"{_TRIGGER_OP} referenced but no recognizable relay call found — "
            f"only .partial(...).expand(...) and direct construction are checked")]

    registry: dict = {}
    if args.registry is not None:
        try:
            registry, _ = _load_registry_detect_dupes(args.registry)
        except (json.JSONDecodeError, OSError):
            pass  # reported by check_registry

    for kwargs in relay_calls:
        target_node = kwargs.get("trigger_dag_id")
        targets: list = []
        if isinstance(target_node, ast.Name) and target_node.id in consts:
            targets = consts[target_node.id]
        elif isinstance(target_node, (ast.List, ast.Tuple)):
            targets = [e.value for e in target_node.elts if isinstance(e, ast.Constant)]
        elif isinstance(target_node, ast.Constant):
            targets = [target_node.value]
        if not targets:
            findings.append(Finding("ERROR",
                "relay targets must be a static list of dag-id strings "
                "(module-level constant or literal) — dynamic target expressions "
                "are not reviewable and defeat the chain-graph audit"))
        for t in targets:
            if not isinstance(t, str) or not t.endswith("_dag"):
                findings.append(Finding("ERROR",
                    f"relay target {t!r} does not look like a dag_id (expected '*_dag')"))
            elif t in registry:
                findings.append(Finding("WARN",
                    f"relay target '{t}' is also event-registered in the registry — "
                    f"dual-channel routing risks double-trigger (discovery §6.5); "
                    f"justify the relay or route via the registry only"))
        if "trigger_run_id" not in kwargs:
            findings.append(Finding("WARN",
                "relay has no trigger_run_id — retries create duplicate downstream "
                "runs (discovery Risk 3); default choice is "
                "sha1(target_dag_id + canonical conf)[:16]"))
        if "wait_for_completion" not in kwargs:
            findings.append(Finding("WARN",
                "wait_for_completion not explicit — declare fire-and-forget (False) "
                "or blocking (True) deliberately"))
        conf_node = kwargs.get("conf")
        if (conf_node is not None and not isinstance(conf_node, ast.Dict)
                and not _has_payload_builder_task(tree)):
            findings.append(Finding("WARN",
                "relay conf is task/XCom-sourced but no BUILD_TRIGGER_CONF-style "
                "payload task was found in this file — the conf XCom must be "
                "produced upstream of the trigger (see reference/relay-triggering.md)"))
    return findings


# --------------------------------------------------------------------------
# DagBag import check (--with-dagbag, CI only)
# --------------------------------------------------------------------------

@register
def check_dagbag(args) -> list[Finding]:
    """--with-dagbag: shell out to `airflow dags list-import-errors`.

    WARN (never silent) when airflow is not on PATH or the command fails without
    naming our file; ERROR when the DagBag reports an import error for it.
    """
    if not getattr(args, "with_dagbag", False):
        return []
    airflow = shutil.which("airflow")
    if airflow is None:
        return [Finding("WARN",
            "--with-dagbag: 'airflow' not on PATH — DagBag import check skipped; "
            "the AST checks still ran, but the real import test needs a CI box "
            "with Airflow installed")]
    proc = subprocess.run([airflow, "dags", "list-import-errors"],
                          capture_output=True, text=True)
    out = f"{proc.stdout or ''}{proc.stderr or ''}"
    dag_name = args.dag_file.name if args.dag_file is not None else args.dag_id
    if args.dag_file is not None and args.dag_file.name in out:
        lines = " | ".join(ln.strip() for ln in out.splitlines()
                           if args.dag_file.name in ln)
        return [Finding("ERROR",
            f"--with-dagbag: DagBag reports an import error for "
            f"{args.dag_file.name}: {lines[:500]}")]
    if proc.returncode != 0:
        return [Finding("WARN",
            f"--with-dagbag: `airflow dags list-import-errors` exited "
            f"{proc.returncode} without naming {dag_name} — could not attest; "
            f"output: {out.strip()[:300]}")]
    return []


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dag-id", required=True)
    p.add_argument("--dag-file", required=True, type=Path)
    p.add_argument("--logic-module", required=True, type=Path)
    p.add_argument("--registry", required=True, type=Path)
    p.add_argument("--test-file", type=Path)
    p.add_argument("--fixture", type=Path)
    p.add_argument("--with-dagbag", action="store_true",
                   help="CI only: also run a real Airflow DagBag import check")
    args = p.parse_args(argv)

    for path in [args.dag_file, args.logic_module, args.registry,
                 args.test_file, args.fixture]:
        if path is not None and not path.exists():
            print(f"ERROR: input not found: {path}")
            return 2

    findings: list[Finding] = []
    for check in CHECKS:
        findings.extend(check(args))

    errors = [f for f in findings if f.severity == "ERROR"]
    for f in findings:
        print(f"{f.severity}: {f.message}")
    print(f"\n{len(errors)} error(s), {len(findings) - len(errors)} warning(s) "
          f"for dag_id={args.dag_id}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
