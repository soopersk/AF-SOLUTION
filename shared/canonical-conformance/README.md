# Canonical-JSON / `dag_run_id` cross-engine conformance

This directory is the **single source of truth** for the one invariant two workstreams share
and must implement identically:

> `dag_run_id = orch_sha1(dag_id + canonical_json(conf))[:16]`

- `canonical_json` = **RFC 8785 (JCS)** — JSON Canonicalization Scheme.
- `orch_sha1(s)` = lowercase-hex **SHA-1** over the **UTF-8** bytes of `s`.
- `dag_id` is concatenated **directly** (no separator) to `canonical_json(conf)`.
- Take the **first 16 hex characters** of the digest.

## Why it matters (Amendment A6)

The legacy EMS never set an Airflow run id — Airflow auto-generated one
(`old-ems/EventSender.scala:86-103`, `old-orchestration/common/dag_utils.py:28-37`). So there is
**no legacy byte-parity target** for `dag_run_id`; A6 introduces a *new* deterministic scheme.

Two independent engines derive this id:

| Engine | Language | Canonicalizer | Consumer |
|--------|----------|---------------|----------|
| EMS | Java 17 | `io.github.erdtman:java-json-canonicalization` (JCS) | `dispatch/OutboxDispatcher` (Phase A) |
| control-plane / framework | Python | celpy side, RFC 8785 impl | `gates.py` / `dag_utils.orch_sha1` |

If they disagree by a single byte, racing evaluations produce **different** run ids, they do
**not** collide into one Airflow run, and the 409-dedup that makes cutover and rollback
race-free (ems-design §11, trigger-plan §2) silently breaks. Hence a shared fixture, not two
independently-trusted implementations.

## The contract test

Both engines load [`canonical_vectors.json`](canonical_vectors.json) and assert, for every
vector, that:

1. `canonical_json(conf)` equals `expected_jcs` (byte-for-byte), and
2. `dag_run_id` equals `expected_dag_run_id`.

- **EMS (Java):** `ems/src/test/java/com/orchestration/ems/canonical/CanonicalConformanceTest.java`
  resolves this file by walking up from the module directory (no copy — no drift).
- **framework (Python):** a mirrored test is added when workstream 3 unblocks (F0). It must load
  **this same file**. Do not fork the vectors.

## Adding a vector

1. Write `conf` (any key order — the test proves sorting).
2. Compute `expected_jcs` by the JCS rules (keys sorted by UTF-16 code unit, no whitespace,
   minimal escaping, ECMAScript number form).
3. Compute `expected_dag_run_id` = first 16 chars of `sha1hex(dag_id + expected_jcs)`, e.g.:

   ```sh
   printf '%s' 'amer_d_b3f_dag{"portfolio_id":"PF-123","reporting_date":"2026-07-17"}' \
     | sha1sum | cut -c1-16
   ```

Keep vectors ASCII-only unless you are deliberately pinning a number- or unicode-edge case on
**both** engines at once.
