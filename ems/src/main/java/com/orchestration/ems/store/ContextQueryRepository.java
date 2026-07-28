package com.orchestration.ems.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Read path for {@code GET /context}, {@code /parentcontext} and {@code /childcontext} (ems-design §4.3).
 * Ports the legacy chain-walk (old-ems/EventSender.scala:108-211) with two deliberate redesign changes:
 *
 * <ul>
 *   <li><b>No EDF fallback on the read path.</b> The legacy {@code fetchContext} fell back to the EDF REST
 *       API on a local miss; in the redesign the EDF fetch belongs to the <em>ingest</em> pipeline
 *       (ems-design §4.2 step 4), so a context absent from the local store simply ends that branch of the
 *       walk (equivalent to the legacy {@code fetchContext → None}).</li>
 *   <li><b>Children are reverse-derived locally</b> via the A9 GIN index
 *       ({@code json->'parentIds' @> to_jsonb(id)}) instead of the legacy EDF child-hierarchy call. The EDF
 *       hierarchy contract is a §14 item-2 open item; the local reverse-{@code parentIds} lookup is the
 *       redesign's index-backed equivalent, and {@code limit} caps the candidate children scanned. Flagged
 *       as an assumption, not an amendment.</li>
 * </ul>
 *
 * <p>Matching semantics are verbatim from {@code checkRequestedParams} (EventSender.scala:132-141): a
 * context matches iff <b>every</b> requested param equals either {@code context.<key>} (top-level) or
 * {@code context.data.<key>}, compared as JSON strings; empty params ⇒ trivially matches.
 */
@Repository
public class ContextQueryRepository {

    private static final String FIND_BY_ID = "SELECT json FROM context WHERE context_id = ?";
    private static final String FIND_CHILDREN =
            "SELECT json FROM context WHERE json->'parentIds' @> to_jsonb(?::text) LIMIT ?";

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public ContextQueryRepository(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /** Local primary-key lookup (no EDF fallback — that is the ingest path's job). */
    public Optional<JsonNode> findContextById(String id) {
        if (id == null) {
            return Optional.empty();
        }
        return jdbc.sql(FIND_BY_ID)
                .param(id)
                .query((rs, rowNum) -> parse(rs.getString("json")))
                .optional();
    }

    /**
     * Walk up the {@code parentIds} chain from {@code id}, returning the first context that matches all
     * requested params (old-ems/EventSender.scala:121-130). With non-empty params, a matching context at
     * any level short-circuits; the root (no {@code parentIds}) is returned only if it matches (empty
     * params ⇒ the root always matches, so the walk returns the topmost ancestor).
     */
    public Optional<JsonNode> findParentContext(String id, Map<String, String> params) {
        Optional<JsonNode> ctxOpt = findContextById(id);
        if (ctxOpt.isEmpty()) {
            return Optional.empty();
        }
        JsonNode ctx = ctxOpt.get();
        List<String> parents = parentIds(ctx);

        if (!params.isEmpty() && matchesAllParams(ctx, params)) {
            return Optional.of(ctx);
        }
        if (parents.isEmpty()) {
            return matchesAllParams(ctx, params) ? Optional.of(ctx) : Optional.empty();
        }
        for (String parentId : parents) {
            Optional<JsonNode> found = findParentContext(parentId, params);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    /**
     * Return {@code id}'s own context if it matches the params, else the first matching child
     * (old-ems/EventSender.scala:166-204). Children are the contexts whose {@code parentIds} contains
     * {@code id} (reverse containment via the A9 GIN index), capped at {@code limit}.
     */
    public Optional<JsonNode> findChildContext(String id, Map<String, String> params, int limit) {
        Optional<JsonNode> ctxOpt = findContextById(id);
        if (ctxOpt.isEmpty()) {
            return Optional.empty();
        }
        JsonNode ctx = ctxOpt.get();
        if (matchesAllParams(ctx, params)) {
            return Optional.of(ctx);
        }
        for (JsonNode child : findChildren(id, limit)) {
            if (matchesAllParams(child, params)) {
                return Optional.of(child);
            }
        }
        return Optional.empty();
    }

    /** Contexts whose {@code parentIds} array contains {@code parentId} (A9 reverse lookup), capped at {@code limit}. */
    public List<JsonNode> findChildren(String parentId, int limit) {
        return jdbc.sql(FIND_CHILDREN)
                .param(parentId)
                .param(limit)
                .query((rs, rowNum) -> parse(rs.getString("json")))
                .list();
    }

    /** True iff every requested param equals {@code context.<key>} or {@code context.data.<key>} (JSON strings). */
    private static boolean matchesAllParams(JsonNode ctx, Map<String, String> params) {
        for (Map.Entry<String, String> e : params.entrySet()) {
            String value = e.getValue();
            if (!value.equals(text(ctx, e.getKey())) && !value.equals(text(ctx.path("data"), e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static String text(JsonNode parent, String key) {
        JsonNode node = parent.path(key);
        return node.isTextual() ? node.asText() : null;
    }

    private static List<String> parentIds(JsonNode ctx) {
        JsonNode arr = ctx.path("parentIds");
        if (!arr.isArray()) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (JsonNode n : arr) {
            if (n.isTextual()) {
                ids.add(n.asText());
            }
        }
        return ids;
    }

    private JsonNode parse(String json) {
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
