package com.orchestration.ems.canonical;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Deterministic, idempotent Airflow trigger identity (Amendment A6):
 *
 * <pre>
 *   dag_run_id = orch_sha1(dag_id + canonical_json(conf))[:16]
 * </pre>
 *
 * where {@code canonical_json} is RFC 8785 JCS ({@link CanonicalJson}) and
 * {@code orch_sha1(s)} is the lowercase hex SHA-1 of the UTF-8 bytes of {@code s}. The first
 * 16 hex characters are taken as the run id. HTTP 409 from Airflow means the run already
 * exists — i.e. success.
 *
 * <p>{@code dag_id} is concatenated directly (no separator) with the canonical conf. This is
 * unambiguous because {@code canonical_json(conf)} of an object always begins with {@code '{'}
 * and {@code dag_id} contains no brace, so no two distinct {@code (dag_id, conf)} pairs can
 * produce the same concatenation.
 *
 * <p>This scheme MUST match the framework/control-plane {@code orch_sha1}/{@code canonical_json}
 * byte-for-byte (see {@link CanonicalJson}); the shared conformance suite enforces it.
 */
public final class DagRunId {

    /** Number of leading hex characters of the SHA-1 digest that form the run id. */
    public static final int LENGTH = 16;

    private DagRunId() {
    }

    /**
     * {@code orch_sha1(s)} — lowercase hex SHA-1 over the UTF-8 bytes of {@code s} (full 40 hex
     * chars). Also used by the framework for {@code invocation_id} derivation.
     */
    public static String orchSha1(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest); // lowercase hex
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is guaranteed present on every JVM.
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
    }

    /**
     * Derive the 16-char {@code dag_run_id} from a DAG id and its conf JSON.
     *
     * @param dagId    target Airflow DAG id
     * @param confJson the conf as JSON text (canonicalized internally)
     * @return the 16-hex-character deterministic run id
     */
    public static String derive(String dagId, String confJson) {
        String canonical = CanonicalJson.canonicalize(confJson);
        return orchSha1(dagId + canonical).substring(0, LENGTH);
    }
}
