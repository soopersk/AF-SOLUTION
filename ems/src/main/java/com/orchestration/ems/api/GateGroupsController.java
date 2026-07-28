package com.orchestration.ems.api;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.orchestration.ems.model.GateGroups;
import com.orchestration.ems.store.GateGroupsRepository;

/**
 * {@code GET /gate/groups} — the generic grouped-event query (ems-design §4.5) the heartbeat DAG consumes to
 * find open gates in one round trip (trigger_redesign_final_implementation_plan.md:355). EMS stays
 * <b>rule-free</b>: the caller supplies every path and criterion from its registry gate spec.
 *
 * <p>Reserved params:
 * <ul>
 *   <li>{@code group_by=<json-path>} — <b>required</b>; the value that names each group. Missing/blank → 400.</li>
 *   <li>{@code lookback=<dur>} — <b>required</b>; the window width ({@code 5d}, {@code 6h}, {@code 30m},
 *       {@code 45s}). Missing/unparseable → 400 (an unbounded scan is never implied).</li>
 *   <li>{@code contributor=<json-path>} — optional; identifies a contribution within a group.</li>
 * </ul>
 * Every <b>other</b> param is a criterion, {@code |}-split into alternatives (e.g. {@code state=FINISH|FAILED})
 * and matched via the repository's 4-location OR — the same vocabulary as {@code GET /event} (A10).
 *
 * <p><b>Always 200</b> with the (possibly empty) group list — an empty window is a legitimate "no open groups"
 * answer the heartbeat interprets, never a 404.
 */
@RestController
public class GateGroupsController {

    private static final String GROUP_BY = "group_by";
    private static final String CONTRIBUTOR = "contributor";
    private static final String LOOKBACK = "lookback";

    /** Compact duration: {@code <int><unit>}, unit ∈ {s,m,h,d} — the form gate specs author (trigger §169-178). */
    private static final Pattern LOOKBACK_PATTERN = Pattern.compile("(\\d+)([smhd])");

    private final GateGroupsRepository repository;

    public GateGroupsController(GateGroupsRepository repository) {
        this.repository = repository;
    }

    @GetMapping(value = "/gate/groups", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<GateGroups> gateGroups(@RequestParam Map<String, String> params) {
        Map<String, String> remaining = new LinkedHashMap<>(params);
        String groupBy = remaining.remove(GROUP_BY);
        Optional<String> contributor = Optional.ofNullable(remaining.remove(CONTRIBUTOR));
        Optional<Duration> lookback = parseLookback(remaining.remove(LOOKBACK));

        if (groupBy == null || groupBy.isBlank() || lookback.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }

        Map<String, List<String>> criteria = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : remaining.entrySet()) {
            // Scala-parity split (limit 0 drops trailing empties), same as GET /event.
            criteria.put(entry.getKey(), Arrays.asList(entry.getValue().split("\\|")));
        }

        return ResponseEntity.ok(repository.groups(groupBy, contributor, lookback.get(), criteria));
    }

    /** Parse {@code 5d}/{@code 6h}/{@code 30m}/{@code 45s} → {@link Duration}; empty for null/malformed input. */
    private static Optional<Duration> parseLookback(String raw) {
        if (raw == null) {
            return Optional.empty();
        }
        Matcher m = LOOKBACK_PATTERN.matcher(raw.trim());
        if (!m.matches()) {
            return Optional.empty();
        }
        long n = Long.parseLong(m.group(1));
        return Optional.of(switch (m.group(2)) {
            case "s" -> Duration.ofSeconds(n);
            case "m" -> Duration.ofMinutes(n);
            case "h" -> Duration.ofHours(n);
            case "d" -> Duration.ofDays(n);
            default -> throw new IllegalStateException("unreachable unit: " + m.group(2));
        });
    }
}
