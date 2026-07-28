package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Java↔SQL parity: proves {@link Normalizer#normFreq(String)}/{@link Normalizer#normRegion(String)}
 * return exactly what the SQL {@code ems_norm_freq}/{@code ems_norm_region} functions return, over
 * the full finite value inventory. The Java authority and the Flyway-defined SQL columns MUST agree,
 * or CEL matching (Java-side normalization) and the stored generated columns (SQL-side) would
 * diverge.
 *
 * <p>Named {@code *IT} so Failsafe runs it. Extends {@link AbstractPostgresIT}, whose
 * {@code @Testcontainers(disabledWithoutDocker = true)} is {@code @Inherited}: this auto-skips
 * locally when Docker is absent and runs for real in CI. Null is deliberately NOT bound here
 * ({@code RETURNS NULL ON NULL INPUT}); null coverage lives in the unit test.
 */
class NormalizerSqlParityIT extends AbstractPostgresIT {

    private static final List<String> FREQ_INPUTS = List.of(
            "D", "DAILY", "M", "MONTHLY", "Q", "QUARTERLY", "d", "daily", "q", "weekly");

    private static final List<String> REGION_INPUTS = List.of(
            "AMERICAS", "Americas", "americas", "EMEA");

    @Test
    void normFreq_matchesSqlForEveryInput() {
        for (String input : FREQ_INPUTS) {
            String sql = jdbcClient()
                    .sql("SELECT ems_norm_freq(?)")
                    .param(input)
                    .query(String.class)
                    .single();
            assertThat(Normalizer.normFreq(input))
                    .as("ems_norm_freq parity for input '%s'", input)
                    .isEqualTo(sql);
        }
    }

    @Test
    void normRegion_matchesSqlForEveryInput() {
        for (String input : REGION_INPUTS) {
            String sql = jdbcClient()
                    .sql("SELECT ems_norm_region(?)")
                    .param(input)
                    .query(String.class)
                    .single();
            assertThat(Normalizer.normRegion(input))
                    .as("ems_norm_region parity for input '%s'", input)
                    .isEqualTo(sql);
        }
    }
}
