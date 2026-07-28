package com.orchestration.ems.store;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestration.ems.model.GateGroups;

/**
 * Read path for {@code GET /gate/groups} (ems-design §4.5) — the generic grouped-event query the heartbeat
 * consumes to find open gates in one round trip. EMS stays <b>rule-free</b>: every path and criterion arrives
 * from the caller (the heartbeat lowers them from the registry gate spec), so this class carries <b>no</b>
 * per-gate schema knowledge.
 *
 * <p>Implementation is exactly the design's two-stage plan:
 * <ol>
 *   <li><b>SQL, index-narrowed:</b> an {@code idx_event_created_at} lookback-window scan
 *       ({@code created_at >= now() - lookback}) plus the caller's {@code criteria} as promoted-column /
 *       raw-JSON residual filters — the same <b>4-location OR</b>, {@code |}-multivalue, case-sensitive
 *       matching vocabulary as {@code GET /event} (Amendment A10), so the two read surfaces speak one
 *       criteria language.</li>
 *   <li><b>In-service, over the small qualifying set:</b> extract the caller's {@code group_by} and
 *       {@code contributor} json-paths from each matched {@code (event, context)} pair and fold them into
 *       distinct groups → present-contributor sets. The window keeps this set small, so no per-gate columns
 *       or indexes are needed (ems-design §6 last note).</li>
 * </ol>
 *
 * <p><b>Json-path grammar</b> (CEL-style, as the gate spec authors them —
 * trigger_redesign_final_implementation_plan.md:169-176): a {@code event} or {@code context} root followed by
 * dotted and/or bracketed segments, e.g. {@code context.data["reporting-date"]}, {@code context.data.companyCode},
 * {@code event.additionalData.STATE}. Bracket segments ({@code ["k"]} / {@code ['k']} / {@code [k]}) normalize
 * to dotted before splitting, so hyphenated keys resolve. An unresolved path yields {@code null} (the row
 * contributes no group / no contributor), never an error — absence is a legitimate answer.
 */
@Repository
public class GateGroupsRepository {

    private static final String BASE =
            "SELECT e.json AS event_json, c.json AS context_json "
            + "FROM event e LEFT OUTER JOIN context c ON e.context_id = c.context_id "
            + "WHERE e.created_at >= now() - make_interval(secs => ?)%s";

    /** Per-criterion 4-location OR (A10), identical to {@code EventQueryRepository}: raw event JSON, its
     *  {@code additionalData}, raw context JSON, its {@code data}. */
    private static final String CRITERION_TEMPLATE =
            "(e.json->>? IN (%s) OR e.json->'additionalData'->>? IN (%s) "
            + "OR c.json->>? IN (%s) OR c.json->'data'->>? IN (%s))";
    private static final int CRITERION_LOCATIONS = 4;

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public GateGroupsRepository(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * Group the events in the lookback window that match {@code criteria} by their {@code groupByPath} value,
     * collecting each group's present {@code contributorPath} values.
     *
     * @param groupByPath     caller json-path whose value names the group (rows resolving to {@code null} are
     *                        dropped — they belong to no group)
     * @param contributorPath optional caller json-path identifying a contribution within the group
     * @param lookback        window width; only {@code created_at >= now() - lookback} events qualify
     * @param criteria        remaining caller params → their {@code |}-split value alternatives; matched via
     *                        the 4-location OR, ANDed (empty = no residual filter, the whole window)
     * @return the distinct groups, ordered by {@code group} value; contributors within each sorted & distinct
     */
    public GateGroups groups(String groupByPath, Optional<String> contributorPath,
            Duration lookback, Map<String, List<String>> criteria) {

        List<String> segmentsGroupBy = JsonPath.parse(groupByPath);
        Optional<List<String>> segmentsContributor = contributorPath.map(JsonPath::parse);

        List<String> templates = new ArrayList<>();
        List<Object> params = new ArrayList<>();
        // lookback lower bound first (SQL-clock relative — no app/db skew); double seconds for make_interval.
        params.add((double) lookback.toSeconds());

        for (Map.Entry<String, List<String>> entry : criteria.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            String inList = values.stream().map(v -> "?").collect(Collectors.joining(","));
            templates.add(CRITERION_TEMPLATE.formatted(inList, inList, inList, inList));
            for (int location = 0; location < CRITERION_LOCATIONS; location++) {
                params.add(key);
                params.addAll(values);
            }
        }

        String where = templates.isEmpty() ? "" : " AND " + String.join(" AND ", templates);
        String sql = BASE.formatted(where);

        // group value -> distinct contributor values (sorted); LinkedHashMap for stable pre-sort iteration.
        Map<String, TreeSet<String>> byGroup = new LinkedHashMap<>();
        jdbc.sql(sql)
                .params(params)
                .query((rs, rowNum) -> new Row(
                        readTree(rs.getString("event_json")),
                        readTree(rs.getString("context_json"))))
                .list()
                .forEach(row -> {
                    String group = JsonPath.extract(row.event(), row.context(), segmentsGroupBy);
                    if (group == null) {
                        return;
                    }
                    TreeSet<String> contributors = byGroup.computeIfAbsent(group, g -> new TreeSet<>());
                    segmentsContributor
                            .map(segs -> JsonPath.extract(row.event(), row.context(), segs))
                            .ifPresent(value -> {
                                if (value != null) {
                                    contributors.add(value);
                                }
                            });
                });

        List<GateGroups.Group> groups = byGroup.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> new GateGroups.Group(e.getKey(), List.copyOf(e.getValue())))
                .collect(Collectors.toList());
        return new GateGroups(groups);
    }

    /** Parse a stored JSON column into a tree; a null column or unparseable payload → {@code null}. */
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

    /** One matched pair; either side may be {@code null} (LEFT join / unparseable). */
    private record Row(JsonNode event, JsonNode context) {
    }

    /**
     * Minimal CEL-style json-path over the {@code event}/{@code context} roots. Not a general JSONPath engine —
     * only the {@code root.seg.seg} / {@code root.seg["key"]} shapes the gate specs use (rule-free: EMS never
     * interprets the segments, only walks them).
     */
    private static final class JsonPath {

        /** Split a path into [root, seg, seg, …], normalizing bracket segments to dotted first. */
        static List<String> parse(String path) {
            // ["k"] / ['k'] / [k] -> .k  (hyphenated keys survive; grounded paths have no dotted keys)
            String dotted = path.trim().replaceAll("\\[\\s*['\"]?([^\\]'\"]+)['\"]?\\s*\\]", ".$1");
            List<String> segments = new ArrayList<>();
            for (String seg : dotted.split("\\.")) {
                if (!seg.isEmpty()) {
                    segments.add(seg);
                }
            }
            return segments;
        }

        /** Resolve the path against the pair; {@code null} if the root/any segment is missing or non-scalar. */
        static String extract(JsonNode event, JsonNode context, List<String> segments) {
            if (segments.isEmpty()) {
                return null;
            }
            JsonNode node = switch (segments.get(0)) {
                case "event" -> event;
                case "context" -> context;
                default -> null;
            };
            for (int i = 1; i < segments.size() && node != null; i++) {
                node = node.get(segments.get(i));
            }
            return (node != null && node.isValueNode()) ? node.asText() : null;
        }

        private JsonPath() {
        }
    }
}
