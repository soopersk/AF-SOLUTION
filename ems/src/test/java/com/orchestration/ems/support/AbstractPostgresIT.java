package com.orchestration.ems.support;

import static org.assertj.core.api.Assertions.assertThat;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.Driver;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Reusable Testcontainers base for EMS integration tests that need a real PostgreSQL 16 with the
 * Flyway V1–V5 schema already applied. Extracted from {@code FlywayMigrationIT} so that Batch A+
 * store/repository ITs share one container/Flyway/datasource wiring.
 *
 * <p>Container image, credentials and Flyway locations are copied verbatim from the original
 * {@code FlywayMigrationIT} ({@code postgres:16-alpine}, db/user/pass = {@code ems}, migrations at
 * {@code classpath:db/migration}).
 *
 * <p>{@code disabledWithoutDocker = true} makes the whole hierarchy self-skip (JUnit "disabled",
 * not error) when no Docker environment is reachable — {@code @Testcontainers} is {@code @Inherited},
 * so subclasses pick up both the extension and the skip. That keeps {@code mvn verify} green on a
 * dev box without Docker while CI (Docker present) runs these ITs for real.
 *
 * <p>Two ways to reach the migrated database:
 * <ul>
 *   <li>Raw JDBC / Flyway API — the container accessors + {@link #dataSource()} / {@link #jdbcClient()}
 *       (what {@code FlywayMigrationIT} uses; no Spring context required).</li>
 *   <li>A Spring test context — {@code @DynamicPropertySource} publishes the
 *       {@code spring.datasource.*} and {@code spring.flyway.*} properties so a {@code @SpringBootTest}
 *       subclass binds automatically. (Harmless no-op for non-Spring subclasses.)</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractPostgresIT {

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("ems")
                    .withUsername("ems")
                    .withPassword("ems");

    /** Result of the {@link #applyMigrations()} run — exposed so subclasses can assert on it. */
    protected static MigrateResult migrateResult;

    /** Publishes container coordinates for any {@code @SpringBootTest} subclass. */
    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.url", POSTGRES::getJdbcUrl);
        registry.add("spring.flyway.user", POSTGRES::getUsername);
        registry.add("spring.flyway.password", POSTGRES::getPassword);
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
    }

    /**
     * Applies Flyway V1–V5 against the container before any test runs, so subclasses that do not
     * stand up a Spring context still get a migrated schema. Asserts only that the migration
     * <em>succeeded</em>; the exact executed-count ({@code >= 5}, i.e. a clean migrate applied all of
     * V1–V5) is asserted in {@code FlywayMigrationIT}, where that count is the thing under test — the
     * base must stay valid even if container reuse (a warm schema, 0 executed) is ever enabled.
     */
    @BeforeAll
    static void applyMigrations() {
        migrateResult = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("classpath:db/migration")
                .load()
                .migrate();
        assertThat(migrateResult.success).isTrue();
    }

    /**
     * A {@link DataSource} pointed at the migrated container. Deliberately non-pooled
     * ({@link SimpleDriverDataSource}) so there is nothing to close or leak — callers open a plain
     * {@link java.sql.Connection} per use. (A {@code DataSourceBuilder} would hand back an unmanaged
     * HikariCP pool that nobody closes.)
     */
    protected static DataSource dataSource() {
        return new SimpleDriverDataSource(
                new Driver(), POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /** A {@link JdbcClient} over {@link #dataSource()} for subclasses/tests to query with. */
    protected static JdbcClient jdbcClient() {
        return JdbcClient.create(dataSource());
    }
}
