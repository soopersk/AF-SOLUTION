package com.orchestration.ems.canonical;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.DynamicTest;

/**
 * Cross-engine conformance for the A6 trigger identity. Loads the shared, authoritative
 * vectors ({@code shared/canonical-conformance/canonical_vectors.json}) — the SAME file the
 * Python/celpy side must pass — and asserts JCS output and {@code dag_run_id} byte-for-byte.
 *
 * <p>The file is resolved by walking up from the working directory rather than copied into
 * test resources, so the two engines can never drift onto different vectors.
 */
class CanonicalConformanceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VECTORS_REL = "shared/canonical-conformance/canonical_vectors.json";

    @TestFactory
    List<DynamicTest> conformanceVectors() throws IOException {
        JsonNode root = MAPPER.readTree(Files.readAllBytes(resolveVectors()));
        int expectedRunIdLength = root.path("spec").path("run_id_length").asInt(16);

        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode vector : root.withArray("vectors")) {
            String name = vector.get("name").asText();
            String dagId = vector.get("dag_id").asText();
            // Serialize conf compactly, preserving file order — JCS must do the sorting itself.
            String confJson = MAPPER.writeValueAsString(vector.get("conf"));
            String expectedJcs = vector.get("expected_jcs").asText();
            String expectedRunId = vector.get("expected_dag_run_id").asText();

            tests.add(DynamicTest.dynamicTest(name, () -> {
                assertThat(CanonicalJson.canonicalize(confJson))
                        .as("JCS canonical form for vector '%s'", name)
                        .isEqualTo(expectedJcs);

                String runId = DagRunId.derive(dagId, confJson);
                assertThat(runId)
                        .as("dag_run_id for vector '%s'", name)
                        .isEqualTo(expectedRunId)
                        .hasSize(expectedRunIdLength);
            }));
        }
        assertThat(tests).as("at least one conformance vector loaded").isNotEmpty();
        return tests;
    }

    @Test
    void orchSha1_isLowercaseHexOverUtf8() {
        // SHA-1("") — a fixed, well-known digest; guards the hashing itself independent of JCS.
        assertThat(DagRunId.orchSha1(""))
                .isEqualTo("da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    @Test
    void derive_isStableRegardlessOfInputKeyOrder() {
        String a = DagRunId.derive("d", "{\"x\":1,\"y\":2}");
        String b = DagRunId.derive("d", "{\"y\":2,\"x\":1}");
        assertThat(a).isEqualTo(b).hasSize(DagRunId.LENGTH);
    }

    @Test
    void canonicalize_rejectsMalformedJson() {
        assertThatThrownBy(() -> CanonicalJson.canonicalize("{not json"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Resolve the shared vectors file by walking up from the working directory (Maven sets it
     * to the module basedir; the walk tolerates IDE runners that start elsewhere).
     */
    private static Path resolveVectors() {
        Path dir = Paths.get("").toAbsolutePath();
        for (Path p = dir; p != null; p = p.getParent()) {
            Path candidate = p.resolve(VECTORS_REL);
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException(
                "Could not locate " + VECTORS_REL + " walking up from " + dir
                        + " — the shared conformance vectors must be present at the repo root.");
    }
}
