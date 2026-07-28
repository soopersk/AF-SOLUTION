package com.orchestration.ems.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.support.AbstractPostgresIT;

/**
 * Chain-walk semantics for {@link ContextQueryRepository} over a real PostgreSQL (ems-design §4.3; legacy
 * old-ems/EventSender.scala:108-211). Seeds a small chain — {@code ctx-A → ctx-B} (parent), and the reverse
 * {@code ctx-A} is a child of {@code ctx-B} — and asserts the up-walk, the reverse-containment child lookup,
 * the all-params-match rule (top-level and {@code data.*} locations), and the 404 (empty) cases. Auto-skips
 * locally; runs in CI.
 */
class ContextQueryIT extends AbstractPostgresIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CTX_A =
            "{\"id\":\"ctx-A\",\"label\":\"L\",\"parentIds\":[\"ctx-B\"],\"data\":{\"foo\":\"x\"}}";
    private static final String CTX_B =
            "{\"id\":\"ctx-B\",\"parentIds\":[],\"data\":{\"foo\":\"y\",\"bar\":\"z\"}}";

    private JdbcClient jdbc;
    private ContextQueryRepository repo;

    @BeforeEach
    void setUp() {
        DataSource ds = dataSource();
        jdbc = JdbcClient.create(ds);
        jdbc.sql("TRUNCATE event, context").update();
        repo = new ContextQueryRepository(jdbc, MAPPER);
        insert("ctx-A", CTX_A);
        insert("ctx-B", CTX_B);
    }

    @Test
    void findById_presentAndAbsent() {
        assertThat(id(repo.findContextById("ctx-A"))).isEqualTo("ctx-A");
        assertThat(repo.findContextById("missing")).isEmpty();
    }

    @Test
    void parentWalk_emptyParams_returnsRootAncestor() {
        // empty params ⇒ walk up to the first context with no parentIds (ctx-B)
        assertThat(id(repo.findParentContext("ctx-A", Map.of()))).isEqualTo("ctx-B");
    }

    @Test
    void parentWalk_matchOnSelf_shortCircuits() {
        // ctx-A has data.foo=x → matches at level 0, returned without ascending
        assertThat(id(repo.findParentContext("ctx-A", Map.of("foo", "x")))).isEqualTo("ctx-A");
    }

    @Test
    void parentWalk_matchOnAncestor_dataLocation() {
        // ctx-A has no bar; ancestor ctx-B has data.bar=z → returned
        assertThat(id(repo.findParentContext("ctx-A", Map.of("bar", "z")))).isEqualTo("ctx-B");
    }

    @Test
    void parentWalk_matchOnTopLevelLocation() {
        // ctx-A has top-level label=L → matches via the top-level location
        assertThat(id(repo.findParentContext("ctx-A", Map.of("label", "L")))).isEqualTo("ctx-A");
    }

    @Test
    void parentWalk_noMatchAnywhere_isEmpty() {
        assertThat(repo.findParentContext("ctx-A", Map.of("foo", "zzz"))).isEmpty();
    }

    @Test
    void childLookup_reverseContainment_findsChildByParentId() {
        List<JsonNode> children = repo.findChildren("ctx-B", 10);
        assertThat(children).hasSize(1);
        assertThat(children.get(0).get("id").asText()).isEqualTo("ctx-A");
    }

    @Test
    void childContext_emptyParams_returnsSelf() {
        // matchesAllParams({}) is trivially true → the context itself is returned (legacy parity)
        assertThat(id(repo.findChildContext("ctx-B", Map.of(), 1))).isEqualTo("ctx-B");
    }

    @Test
    void childContext_matchesFirstChild() {
        // ctx-B does not match foo=x, but its child ctx-A does
        assertThat(id(repo.findChildContext("ctx-B", Map.of("foo", "x"), 1))).isEqualTo("ctx-A");
    }

    @Test
    void childContext_noMatchingChild_isEmpty() {
        assertThat(repo.findChildContext("ctx-B", Map.of("foo", "zzz"), 1)).isEmpty();
    }

    private void insert(String contextId, String json) {
        jdbc.sql("INSERT INTO context (context_id, json) VALUES (?, ?::jsonb)")
                .param(contextId).param(json).update();
    }

    private static String id(Optional<JsonNode> ctx) {
        return ctx.map(n -> n.get("id").asText()).orElse(null);
    }
}
