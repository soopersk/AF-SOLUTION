package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Runs {@code db/seed/V6__subscription_seed0.sql} through Flyway against a real PostgreSQL — the half of
 * the seed proof that {@code Seed0MigrationTest} cannot do without Docker.
 *
 * <p>What only a real database can answer: whether the SQL parses at all, whether the V3
 * {@code forward_requires_dag} and {@code uq_subscription} constraints accept these 16 rows, and whether
 * re-running the migration is genuinely a no-op rather than a duplicate-key failure. The
 * <b>re-runnability</b> check is the one that matters operationally: a seed that cannot be re-applied is
 * a seed nobody dares to correct.
 *
 * <p>The base class migrates {@code classpath:db/migration} only, so this class applies the seed location
 * itself — deliberately, because that is exactly the two-location wiring the azure/shadow/live profiles
 * use, and running it here proves the ordering (V6 after V1–V5) holds.
 */
class Seed0MigrationIT extends AbstractPostgresIT {

    private static final String SEED_LOCATION = "classpath:db/seed";
    private static final String MIGRATION_LOCATION = "classpath:db/migration";

    private final JdbcClient jdbc = jdbcClient();

    @Test
    void theSeedLands16RowsInTheExpectedSplit() {
        applySeed();

        assertThat(count("")).isEqualTo(16);
        assertThat(count("WHERE stage = 'PERSIST'")).isEqualTo(7);
        assertThat(count("WHERE stage = 'FORWARD' AND tenant_id = 'CAPITAL'")).isEqualTo(8);
        assertThat(count("WHERE stage = 'FORWARD' AND tenant_id = 'NSFR'")).isEqualTo(1);
        assertThat(count("WHERE registry_version = 'seed-0'")).isEqualTo(16);
        assertThat(count("WHERE updated_by = 'seed-0-migration'")).isEqualTo(16);
    }

    /**
     * Re-running must change the row set not at all. Flyway will not replay V6 within one schema history,
     * so the statements are executed directly — which is also what a hand re-run during an incident looks
     * like.
     */
    @Test
    void reRunningTheSeedIsACleanNoOp() {
        applySeed();
        List<String> before = ruleFingerprints();

        replaySeedStatements();

        assertThat(count("")).as("a re-run must not insert duplicates").isEqualTo(16);
        assertThat(ruleFingerprints())
                .as("a re-run must not change any rule text, dag id or enabled flag")
                .containsExactlyElementsOf(before);
    }

    /** 15 enabled: the NSFR row ships disabled, mirroring the legacy flag (ASSUMPTION-7). */
    @Test
    void loadEnabledReturnsFifteenRows() {
        applySeed();

        List<SubscriptionRow> enabled = new SubscriptionRepo(jdbc).loadEnabled();

        assertThat(enabled).hasSize(15);
        assertThat(enabled).noneMatch(row -> "nsfr_data-update".equals(row.ruleName()));
        assertThat(enabled).filteredOn(row -> row.stage() == Stage.PERSIST).hasSize(7);
    }

    private void applySeed() {
        jdbc.sql("DELETE FROM subscription").update();
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations(MIGRATION_LOCATION, SEED_LOCATION)
                .load()
                .migrate();
    }

    /**
     * Executes the seed statements a second time outside Flyway. Split on the statement terminator; the
     * migration contains no procedural blocks, so a naive split is exact here.
     */
    private void replaySeedStatements() {
        for (String statement : seedSql().split(";")) {
            if (statement.contains("INSERT INTO subscription")) {
                jdbc.sql(statement).update();
            }
        }
    }

    private static String seedSql() {
        try (InputStream in =
                Seed0MigrationIT.class.getResourceAsStream("/db/seed/V6__subscription_seed0.sql")) {
            if (in == null) {
                throw new IllegalStateException("the seed migration is not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private int count(String where) {
        return jdbc.sql("SELECT count(*) FROM subscription " + where).query(Integer.class).single();
    }

    private List<String> ruleFingerprints() {
        return jdbc.sql("""
                SELECT tenant_id || '|' || stage || '|' || rule_name || '|'
                       || COALESCE(control_dag_id, '-') || '|' || when_cel || '|'
                       || registry_version || '|' || enabled
                FROM subscription ORDER BY tenant_id, stage, rule_name
                """).query(String.class).list();
    }
}
