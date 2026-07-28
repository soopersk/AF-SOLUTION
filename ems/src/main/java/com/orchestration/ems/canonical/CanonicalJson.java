package com.orchestration.ems.canonical;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.erdtman.jcs.JsonCanonicalizer;

/**
 * RFC 8785 (JSON Canonicalization Scheme, JCS) — the single canonical byte form of a JSON
 * value used to derive {@code dag_run_id} (see {@link DagRunId}).
 *
 * <p><b>Why this is a hard cross-engine invariant (Amendment A6).</b> The legacy EMS set no
 * Airflow run id at all — Airflow auto-generated one (old-ems/EventSender.scala:86-103,
 * old-orchestration/common/dag_utils.py:28-37), so there is <i>no</i> byte-parity target to
 * reproduce; A6 introduces a <i>new</i> deterministic scheme. EMS (this Java side) and the
 * framework/control-plane (celpy/Python side) must independently derive the <b>same</b>
 * {@code dag_run_id} from the same {@code (dag_id, conf)}, or racing triggers will not collide
 * on one run and the 409-dedup that makes cutover/rollback race-free (§11) breaks.
 *
 * <p>Both sides therefore agree on one canonicalization: RFC 8785. This class is a thin seam
 * over the reference implementation so the choice is isolated and swappable. The two
 * implementations are held equal by the shared conformance suite
 * ({@code src/test/resources/conformance/canonical_vectors.json}, mirrored on the Python side).
 *
 * <p>JCS guarantees: object keys sorted by UTF-16 code unit, no insignificant whitespace,
 * minimal string escaping, and ECMAScript number serialization. Orchestration {@code conf}
 * values are strings/integers/booleans/null/objects/arrays in practice; floating-point in
 * {@code conf} is discouraged precisely because it stresses the number-serialization rules.
 */
public final class CanonicalJson {

    private CanonicalJson() {
    }

    /**
     * @param json any valid JSON text
     * @return its RFC 8785 canonical form as a {@code String}
     * @throws IllegalArgumentException if {@code json} is not valid JSON
     */
    public static String canonicalize(String json) {
        try {
            return new JsonCanonicalizer(json).getEncodedString();
        } catch (IOException e) {
            throw new IllegalArgumentException("input is not valid JSON for JCS canonicalization", e);
        }
    }

    /**
     * @param json any valid JSON text
     * @return its RFC 8785 canonical form as UTF-8 bytes (the exact bytes hashed for
     *         {@code dag_run_id})
     * @throws IllegalArgumentException if {@code json} is not valid JSON
     */
    public static byte[] canonicalizeToUtf8(String json) {
        // getEncodedUTF8() is UTF-8 by contract; canonicalize(...).getBytes(UTF_8) is identical.
        return canonicalize(json).getBytes(StandardCharsets.UTF_8);
    }
}
