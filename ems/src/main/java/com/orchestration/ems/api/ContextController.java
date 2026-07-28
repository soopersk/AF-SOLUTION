package com.orchestration.ems.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.orchestration.ems.store.ContextQueryRepository;

/**
 * Byte-compatible context lookup + chain-traversal endpoints (ems-design §4.3; legacy
 * old-ems/EventController.scala:63-112). Each returns a single context object (200) or 404:
 *
 * <ul>
 *   <li>{@code GET /context?context_id=…} — primary-key lookup; empty id → 400.</li>
 *   <li>{@code GET /parentcontext?initial_context_id=…&<match params>} — walk up {@code parentIds} for the
 *       first ancestor matching all params; missing {@code initial_context_id} → 400.</li>
 *   <li>{@code GET /childcontext?initial_context_id=…&limit=N&<match params>} — the context itself if it
 *       matches, else the first matching child ({@code limit} default 1); missing
 *       {@code initial_context_id} → 400.</li>
 * </ul>
 */
@RestController
public class ContextController {

    private final ContextQueryRepository repository;

    public ContextController(ContextQueryRepository repository) {
        this.repository = repository;
    }

    @GetMapping(value = "/context", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> context(@RequestParam("context_id") String id) {
        if (id.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        return ok(repository.findContextById(id));
    }

    @GetMapping(value = "/parentcontext", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> parentContext(@RequestParam Map<String, String> params) {
        if (!params.containsKey("initial_context_id")) {
            return ResponseEntity.badRequest().build();
        }
        Map<String, String> rest = new LinkedHashMap<>(params);
        String initial = rest.remove("initial_context_id");
        return ok(repository.findParentContext(initial, rest));
    }

    @GetMapping(value = "/childcontext", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> childContext(@RequestParam Map<String, String> params) {
        if (!params.containsKey("initial_context_id")) {
            return ResponseEntity.badRequest().build();
        }
        Map<String, String> rest = new LinkedHashMap<>(params);
        String initial = rest.remove("initial_context_id");
        int limit = Integer.parseInt(rest.getOrDefault("limit", "1"));
        rest.remove("limit");
        return ok(repository.findChildContext(initial, rest, limit));
    }

    private static ResponseEntity<?> ok(Optional<JsonNode> found) {
        return found.<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
