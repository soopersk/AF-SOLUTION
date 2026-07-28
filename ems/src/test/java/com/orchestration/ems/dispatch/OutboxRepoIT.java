package com.orchestration.ems.dispatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Integration proof for {@link OutboxRepo} against a real PostgreSQL: idempotent insert on the
 * deterministic {@code dag_run_id} (A6), pending drain filters delivered rows, {@code markDelivered}
 * flips exactly once, {@code recordAttempt} accumulates, and — critically — {@code FOR UPDATE SKIP
 * LOCKED} lets two concurrent dispatchers claim disjoint batches (§12). Auto-skips locally.
 */
class OutboxRepoIT extends AbstractPostgresIT {

    private final OutboxRepo repo = new OutboxRepo(jdbcClient());

    @BeforeEach
    void clean() {
        jdbcClient().sql("TRUNCATE dag_trigger_outbox").update();
    }

    @Test
    void insert_isIdempotentOnDagRunId_A6() {
        assertThat(repo.insert("run-1", "dag_x", "{\"a\":1}")).isEqualTo(1);
        assertThat(repo.insert("run-1", "dag_x", "{\"a\":1}")).isZero(); // ON CONFLICT DO NOTHING
    }

    @Test
    void drainPending_returnsOnlyUndelivered() {
        repo.insert("run-a", "dag_x", "{\"n\":1}");
        repo.insert("run-b", "dag_x", "{\"n\":2}");
        repo.markDelivered("run-a");

        List<PendingTrigger> pending = repo.drainPending(10);

        assertThat(pending).extracting(PendingTrigger::dagRunId).containsExactly("run-b");
        assertThat(pending).extracting(PendingTrigger::conf).containsExactly("{\"n\": 2}");
    }

    @Test
    void markDelivered_flipsOnceThenNoOp() {
        repo.insert("run-c", "dag_x", "{}");

        assertThat(repo.markDelivered("run-c")).isEqualTo(1);
        assertThat(repo.markDelivered("run-c")).isZero(); // already delivered
    }

    @Test
    void recordAttempt_incrementsAttemptsAndStoresLastError() {
        repo.insert("run-d", "dag_x", "{}");

        repo.recordAttempt("run-d", "503 from Airflow");
        repo.recordAttempt("run-d", "connect timeout");

        record Row(int attempts, String lastError) { }
        Row r = jdbcClient().sql(
                        "SELECT attempts, last_error FROM dag_trigger_outbox WHERE dag_run_id = ?")
                .param("run-d")
                .query((rs, n) -> new Row(rs.getInt(1), rs.getString(2)))
                .single();
        assertThat(r.attempts()).isEqualTo(2);
        assertThat(r.lastError()).isEqualTo("connect timeout");
    }

    @Test
    void concurrentDrain_skipLocked_claimsDisjointBatches() throws Exception {
        repo.insert("run-1", "dag_x", "{}");
        repo.insert("run-2", "dag_x", "{}");
        repo.insert("run-3", "dag_x", "{}");

        try (Connection c1 = conn(); Connection c2 = conn()) {
            c1.setAutoCommit(false);
            c2.setAutoCommit(false);

            OutboxRepo r1 = new OutboxRepo(JdbcClient.create(new SingleConnectionDataSource(c1, true)));
            OutboxRepo r2 = new OutboxRepo(JdbcClient.create(new SingleConnectionDataSource(c2, true)));

            // c1 claims 2 rows and holds their locks (tx stays open); c2 must skip those and get the 3rd.
            List<String> batch1 = r1.drainPending(2).stream().map(PendingTrigger::dagRunId).toList();
            List<String> batch2 = r2.drainPending(2).stream().map(PendingTrigger::dagRunId).toList();

            assertThat(batch1).hasSize(2);
            assertThat(batch2).hasSize(1);
            assertThat(batch1).doesNotContainAnyElementsOf(batch2);

            c1.rollback();
            c2.rollback();
        }
    }

    private static Connection conn() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
