package com.orchestration.ems.api;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.orchestration.ems.model.EnrichedEventView;
import com.orchestration.ems.store.EventQueryRepository;

/**
 * The byte-compatible enriched-event query endpoint (ems-design §4.3) — the sensor contract that keeps
 * unmigrated Airflow DAGs untouched across cutover. A 1:1 port of the legacy control flow
 * (old-ems/EventController.scala:37-54):
 *
 * <ul>
 *   <li>no params → <b>400</b> (a bare {@code GET /event} is a client error, not "match everything");</li>
 *   <li>{@code event_id}, {@code context_id}, {@code parent_id} are pulled out as the three id filters
 *       (case-sensitive names, exactly as legacy);</li>
 *   <li>every remaining param is {@code |}-split into value alternatives and handed to the repository's
 *       4-location OR (Amendment A10);</li>
 *   <li>≥ 1 match → <b>200</b> with the {@code [{event,context}, …]} list; none → <b>404</b>.</li>
 * </ul>
 *
 * <p>{@code @RequestParam Map<String,String>} collapses any repeated param to its first value, matching
 * the legacy {@code JavaMap[String,String]} binding; multi-value alternation travels inside a single
 * value via {@code |} (e.g. {@code state=FINISH|FAILED}).
 */
@RestController
public class EventController {

    private final EventQueryRepository repository;

    public EventController(EventQueryRepository repository) {
        this.repository = repository;
    }

    @GetMapping(value = "/event", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> event(@RequestParam Map<String, String> params) {
        if (params.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        Map<String, String> remaining = new LinkedHashMap<>(params);
        Optional<String> eventId = Optional.ofNullable(remaining.remove("event_id"));
        Optional<String> contextId = Optional.ofNullable(remaining.remove("context_id"));
        Optional<String> parentId = Optional.ofNullable(remaining.remove("parent_id"));

        Map<String, List<String>> dataParams = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : remaining.entrySet()) {
            // Scala String.split('|') (limit 0) — drops trailing empties; Java "\\|" matches it
            dataParams.put(entry.getKey(), Arrays.asList(entry.getValue().split("\\|")));
        }

        List<EnrichedEventView> events = repository.findEvents(eventId, contextId, parentId, dataParams);
        return events.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(events);
    }
}
