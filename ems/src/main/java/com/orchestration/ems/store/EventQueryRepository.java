package com.orchestration.ems.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.EnrichedEventView;

/**
 * Read path for {@code GET /event} (ems-design §4.3). A faithful port of the legacy enriched-event query
 * (old-ems/DatabaseEventRepository.scala:26-104) with the three redesign substitutions that are pure
 * index accelerators — <b>identical result sets</b>, never a semantic change (Amendment A10):
 *
 * <ul>
 *   <li><b>A3 indexed join</b>: {@code c.context_id = e.context_id} (both promoted, {@code idx_event_context_id})
 *       replaces the legacy JSONB-extraction join {@code c.context_id = (e.json->>'contextId')}.</li>
 *   <li><b>A9 array-containment</b>: {@code parentIds @> to_jsonb(?)} (GIN {@code idx_context_parentids})
 *       replaces the legacy {@code c.json->'parentIds' ?? ?}; same JSONB "array contains this string" test.</li>
 *   <li>Everything else is verbatim: the join-type selection, and the <b>four-location OR</b> per non-id
 *       param — {@code e.json}, {@code e.json->'additionalData'}, {@code c.json}, {@code c.json->'data'} —
 *       {@code |}-multivalue, <b>case-sensitive</b> against the raw stored JSON, with <b>no</b>
 *       param→column alias map and <b>no</b> value canonicalization (A10; the §4.3 canonicalization is
 *       deferred, evidence-gated on §14 item 1b — applying it against byte-verbatim storage would break
 *       byte-compat).</li>
 * </ul>
 *
 * <p>Response bodies are the raw {@code json} columns (parsed then re-serialized), so the wire format is
 * byte-compatible with today (ems-design §4.3). Constructor-injected {@link JdbcClient} so the same class
 * is a Spring bean and directly constructible over the {@code AbstractPostgresIT} datasource in ITs.
 */
@Repository
public class EventQueryRepository {

    private static final String BASE =
            "SELECT e.json AS event_json, c.json AS context_json "
            + "FROM context c %s JOIN event e ON c.context_id = e.context_id %s";

    private static final String EVENT_ID_TEMPLATE = "e.event_id = ?";
    private static final String CONTEXT_ID_TEMPLATE = "c.context_id = ?";
    private static final String PARENT_ID_TEMPLATE = "c.json->'parentIds' @> to_jsonb(?::text)";
    private static final String OTHER_TEMPLATE =
            "(e.json->>? IN (%s) OR e.json->'additionalData'->>? IN (%s) "
            + "OR c.json->>? IN (%s) OR c.json->'data'->>? IN (%s))";
    /** The four raw-JSON locations each non-id param is tested against (A10). */
    private static final int OTHER_LOCATIONS = 4;

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public EventQueryRepository(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * Find enriched events matching the supplied id filters (ANDed) and the per-param 4-location OR
     * groups (also ANDed). Reproduces the legacy join-type selection:
     * {@code eventId+contextId → INNER}, {@code eventId only → RIGHT OUTER}, else {@code LEFT OUTER}.
     *
     * @param eventId    optional {@code event_id} equality filter
     * @param contextId  optional {@code context_id} equality filter
     * @param parentId   optional {@code parentIds} array-containment filter
     * @param dataParams remaining params → their {@code |}-split value alternatives (insertion order preserved)
     * @return matching {@code (event, context)} pairs; empty when nothing matches (→ controller 404)
     */
    public List<EnrichedEventView> findEvents(Optional<String> eventId, Optional<String> contextId,
            Optional<String> parentId, Map<String, List<String>> dataParams) {

        List<String> templates = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        // id predicates, in the legacy order: event_id, context_id, parent_id
        eventId.ifPresent(v -> {
            templates.add(EVENT_ID_TEMPLATE);
            params.add(v);
        });
        contextId.ifPresent(v -> {
            templates.add(CONTEXT_ID_TEMPLATE);
            params.add(v);
        });
        parentId.ifPresent(v -> {
            templates.add(PARENT_ID_TEMPLATE);
            params.add(v);
        });

        // per-param 4-location OR groups (A10): one IN-list per param, repeated across the 4 locations;
        // params are laid out [key, values...] once per location (legacy List.fill(4)(k +: v).flatten)
        for (Map.Entry<String, List<String>> entry : dataParams.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            String inList = values.stream().map(v -> "?").collect(Collectors.joining(","));
            templates.add(OTHER_TEMPLATE.formatted(inList, inList, inList, inList));
            for (int location = 0; location < OTHER_LOCATIONS; location++) {
                params.add(key);
                params.addAll(values);
            }
        }

        String where = templates.isEmpty() ? "" : "WHERE " + String.join(" AND ", templates);
        String sql = BASE.formatted(joinType(eventId, contextId), where);

        return jdbc.sql(sql)
                .params(params)
                .query((rs, rowNum) -> new EnrichedEventView(
                        readTree(rs.getString("event_json")),
                        readTree(rs.getString("context_json"))))
                .list();
    }

    private static String joinType(Optional<String> eventId, Optional<String> contextId) {
        if (eventId.isPresent() && contextId.isPresent()) {
            return "INNER";
        }
        if (eventId.isPresent()) {
            return "RIGHT OUTER";
        }
        return "LEFT OUTER";
    }

    /** Parse a stored JSON column; a null column or unparseable payload maps to {@code null} (legacy parity). */
    private JsonNode readTree(String json) {
        if (json == null) {
            return null;
        }
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
