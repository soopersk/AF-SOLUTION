"""Trigger-condition grammar: TYPE.IDENT[.REGION].

Canonical form is dot-separated as used by dags/dag_trigger_criteria_map.json
(e.g. SOURCE.MERIVAL.USRG). Idents may contain underscores
(e.g. SOURCE.AQUA_RISK_PLATFORM). Regions are the H3 region codes.

Also holds the shared registry-loading helpers so the validator and the guarded
editor read the file identically (duplicate-key detection included).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

CONDITION_TYPES = {"SOURCE", "CALC", "DATASET", "PIPELINEID"}
# Observed in the registry + discovery doc §6.3; unknown regions warn, not fail.
KNOWN_REGIONS = {"AMER", "ASIA", "AUNZ", "EURO", "LDNL", "WMAP", "WMCH",
                 "WMDE", "WMUS", "ZURI", "USRG"}
_SEGMENT = re.compile(r"^[A-Z0-9_]+$")


@dataclass(frozen=True)
class Finding:
    severity: str  # "ERROR" | "WARN"
    message: str


def check_condition(cond: str, known_idents: set[str] | None = None) -> list[Finding]:
    findings: list[Finding] = []
    if not isinstance(cond, str) or not cond:
        return [Finding("ERROR", f"condition must be a non-empty string, got {cond!r}")]

    parts = cond.split(".")
    if len(parts) < 2 or len(parts) > 3:
        return [Finding("ERROR",
                f"condition '{cond}' must be TYPE.IDENT or TYPE.IDENT.REGION (dot-separated)")]

    ctype, ident = parts[0], parts[1]
    if ctype not in CONDITION_TYPES:
        findings.append(Finding("ERROR",
            f"unknown condition type '{ctype}' (allowed: {sorted(CONDITION_TYPES)})"))
    for seg in parts:
        if not _SEGMENT.match(seg):
            findings.append(Finding("ERROR",
                f"segment '{seg}' in '{cond}' must be UPPERCASE A-Z0-9_"))

    # Drift guard: IDENT_REGION written with an underscore instead of a dot.
    if known_idents and len(parts) == 2 and "_" in ident:
        stem, _, tail = ident.rpartition("_")
        if stem in known_idents and tail in KNOWN_REGIONS:
            findings.append(Finding("ERROR",
                f"'{cond}' uses an underscore between ident and region — "
                f"did you mean '{ctype}.{stem}.{tail}'?"))

    if len(parts) == 3 and parts[2] not in KNOWN_REGIONS:
        findings.append(Finding("WARN",
            f"region '{parts[2]}' not in the known region set {sorted(KNOWN_REGIONS)} — "
            f"verify against dags/logic company-region mappings"))
    return findings


def load_registry_detect_dupes(path: Path) -> tuple[dict, list[str]]:
    """Load a registry JSON, reporting duplicate keys instead of silently keeping
    the last one (JSON allows duplicates; `json.loads` alone hides them)."""
    dupes: list[str] = []

    def hook(pairs):
        seen = {}
        for k, val in pairs:
            if k in seen:
                dupes.append(k)
            seen[k] = val
        return seen

    data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=hook)
    return data, dupes


def known_idents_from_registry(registry: dict) -> set[str]:
    """Idents already used with a region elsewhere in the registry — these feed
    the underscore-drift guard in check_condition."""
    idents: set[str] = set()
    for val in registry.values():
        for c in (val if isinstance(val, list) else [val]):
            parts = str(c).split(".")
            if len(parts) == 3:
                idents.add(parts[1])
    return idents
