package com.orchestration.ems.api;

import java.util.Optional;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.orchestration.ems.model.RunStatus;
import com.orchestration.ems.store.RunStatusRepository;

/**
 * {@code GET /run/status} — the framework-F0-unblocking lifecycle probe (ems-design §4.5). Returns a single
 * {@link RunStatus} summary over the event store, keyed by the {@code /event} correlation vocabulary:
 * {@code context_id} (the framework's {@code triggerContextId}) and/or {@code task_id}.
 *
 * <p>Semantics:
 * <ul>
 *   <li>Neither correlation key supplied → <b>400</b> (the probe must name a run).</li>
 *   <li>Otherwise <b>always 200</b> with the summary — a run with no events is a legitimate
 *       {@code NEVER_STARTED} candidate ({@code started=false}), <b>not</b> a 404. The framework's
 *       {@code SlaAwareHttpTrigger} classifies from the body, so an empty run must still return a body.</li>
 * </ul>
 *
 * <p>Only the two grounded correlation keys are accepted (YAGNI): the design names
 * "{@code triggerContextId}, {@code taskId}, …" and the two completion schemes key on exactly these. Extra
 * residual filters (state/type) are unnecessary — EMS applies the terminal vocabulary itself over the matched
 * set. Add more keys only on evidence a scheme needs them.
 */
@RestController
public class RunStatusController {

    private final RunStatusRepository repository;

    public RunStatusController(RunStatusRepository repository) {
        this.repository = repository;
    }

    @GetMapping(value = "/run/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RunStatus> runStatus(
            @RequestParam(value = "context_id", required = false) String contextId,
            @RequestParam(value = "task_id", required = false) String taskId) {

        Optional<String> context = nonBlank(contextId);
        Optional<String> task = nonBlank(taskId);
        if (context.isEmpty() && task.isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(repository.summarize(context, task));
    }

    private static Optional<String> nonBlank(String value) {
        return (value == null || value.isEmpty()) ? Optional.empty() : Optional.of(value);
    }
}
