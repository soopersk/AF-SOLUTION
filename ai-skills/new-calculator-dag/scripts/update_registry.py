"""Guarded editor for dag_trigger_criteria_map.json.

Refuses duplicates and bad condition grammar; writes a .bak first; preserves
existing entries and 2-space-indent formatting. Exit 0 on success, 1 on refusal.
"""
from __future__ import annotations

import argparse
import difflib
import json
import shutil
import sys
from pathlib import Path

from condition_grammar import (
    check_condition,
    known_idents_from_registry,
    load_registry_detect_dupes,
)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry", required=True, type=Path)
    p.add_argument("--dag-id", required=True)
    p.add_argument("--condition", action="append", required=True)
    args = p.parse_args(argv)

    if not args.registry.exists():
        print(f"ERROR: registry not found: {args.registry}")
        return 1
    try:
        registry, dupes = load_registry_detect_dupes(args.registry)
    except json.JSONDecodeError as exc:
        print(f"ERROR: registry is not valid JSON: {exc}")
        return 1

    # Rewriting a file with duplicate keys would silently collapse them (last
    # wins) — refuse and leave the file exactly as it is.
    if dupes:
        for d in dupes:
            print(f"ERROR: registry has duplicate key '{d}' — rewriting would "
                  f"silently collapse it; fix the registry first")
        return 1

    if args.dag_id in registry:
        print(f"ERROR: '{args.dag_id}' already registered with "
              f"{registry[args.dag_id]!r} — refusing to overwrite. "
              f"Remove the entry manually if replacement is intended.")
        return 1

    # A new key one edit away from an existing one is how the live
    # amer_b3f_dag / amer_d_b3f_dag drift happened. It can also be the drift
    # FIX, so warn rather than refuse.
    near = difflib.get_close_matches(args.dag_id, registry.keys(), n=1)
    if near:
        print(f"WARN: '{args.dag_id}' is a near-miss of existing key "
              f"'{near[0]}' — check for registry drift (pitfalls.md #1) "
              f"before merging")

    known_idents = known_idents_from_registry(registry)
    errors = [f for c in args.condition
              for f in check_condition(c, known_idents=known_idents)
              if f.severity == "ERROR"]
    for f in errors:
        print(f"ERROR: {f.message}")
    if errors:
        return 1

    shutil.copy2(args.registry, args.registry.with_suffix(".json.bak"))
    registry[args.dag_id] = (args.condition[0] if len(args.condition) == 1
                             else args.condition)
    args.registry.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    print(f"OK: registered '{args.dag_id}' -> {registry[args.dag_id]!r} "
          f"(backup: {args.registry.name}.bak)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
