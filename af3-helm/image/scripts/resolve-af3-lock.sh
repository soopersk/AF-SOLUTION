#!/usr/bin/env bash
# Resolve requirements.af3.in into requirements.af3.lock for the Airflow 3.3.2 / Python 3.14
# runtime image (linux/amd64, the AKS nodepool architecture).
#
# Versions come from vendor/constraints/constraints-3.3.2-python3.14.txt. Packages the
# constraints do not cover resolve to the highest compatible release; they are listed at the
# end so each one gets a deliberate review instead of riding in unnoticed.
#
# Usage:
#   image/scripts/resolve-af3-lock.sh           rewrite requirements.af3.lock
#   image/scripts/resolve-af3-lock.sh --check   fail if the committed lock is stale (CI)
#
# Behind a TLS-intercepting proxy, export UV_NATIVE_TLS=1 so uv trusts the OS certificate store.
set -euo pipefail

image_dir=$(cd "$(dirname "$0")/.." && pwd)
cd "$image_dir"

# Relative paths on purpose: uv writes them into the lock's "# via" annotations, and an
# absolute path would make the lock differ per machine.
constraints=../vendor/constraints/constraints-3.3.2-python3.14.txt
lock=requirements.af3.lock

command -v uv >/dev/null || { echo "uv is required: https://docs.astral.sh/uv/" >&2; exit 1; }

out=$lock
if [[ "${1:-}" == "--check" ]]; then
  out=$(mktemp)
  trap 'rm -f "$out"' EXIT
fi

uv pip compile requirements.af3.in \
  --constraint "$constraints" \
  --python-version 3.14 \
  --python-platform x86_64-manylinux_2_28 \
  --no-header \
  --annotation-style line \
  --quiet \
  --output-file "$out"

if [[ "${1:-}" == "--check" ]]; then
  diff -u "$lock" "$out" >&2 || { echo "requirements.af3.lock is stale: rerun without --check" >&2; exit 1; }
  echo "requirements.af3.lock is current"
  exit 0
fi

# apache-airflow, -core and -task-sdk are pinned by requirements.af3.in itself, not by the
# constraints file, so they are expected here and filtered out.
echo "Pins not supplied by the constraints file (review each):"
grep -v -- "-c $constraints" "$lock" \
  | grep -Ev '^apache-airflow(-core|-task-sdk)?==' \
  | sed 's/ *#.*//' \
  | sed 's/^/  /'
