package com.orchestration.ems.decisions;

import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.orchestration.ems.model.DecisionRecord;

/**
 * {@code POST /decisions} — batch ingest of the slim decision records dispatchers and heartbeat DAGs emit
 * after evaluating their rules (ems-design §4.5:219, trigger-plan §8). This is the L1/GATE half of the
 * audit trail; the L0 half is written by the ingest transaction itself
 * ({@link RoutingDecisionRepo#insertL0Batch}).
 *
 * <p>Request {@code {"decided_by": …, "decisions": [ … ]}}; response {@code {"received": n, "inserted": m}}.
 *
 * <p>Semantics:
 * <ul>
 *   <li><b>Validation rejects the whole batch</b> (400, nothing written) — a malformed record means the
 *       caller has a bug, and a partially-applied audit batch is worse than an outright rejection it can
 *       see and retry. Required: a non-blank {@code decided_by}, a {@code decisions} array, and per record
 *       an {@code event_id} plus a {@code tier}/{@code decision} from the V4 vocabularies
 *       ({@link DecisionRecord#isValid()}).</li>
 *   <li><b>Persistence is all-or-nothing</b> — a database failure surfaces as 5xx so the caller's
 *       retry-then-alert loop engages. "Audit never blocks dispatch" (trigger-plan §768) is a property of
 *       the <em>caller</em>: it proceeds regardless of what this endpoint answers. That is precisely why
 *       EMS must answer honestly instead of swallowing write failures behind a 200 — a silent drop is an
 *       audit record lost forever, because the caller would never retry it.</li>
 *   <li><b>An empty batch is 200</b> {@code {"received":0,"inserted":0}} — a control DAG whose rules all
 *       produced non-recorded verdicts has nothing to say, which is not an error.</li>
 *   <li>{@code inserted < received} is likewise normal: see {@link DecisionRecord.Ingested}.</li>
 * </ul>
 *
 * <p>{@code decided_by} is the <b>authenticated principal</b> (§4.5:201 — "requires the
 * dispatcher/heartbeat JWT identity"), never a body field: an audit trail a caller can name itself is not
 * an audit trail. A {@code decided_by} sent in the body is ignored, so the existing Python client needs no
 * change.
 */
@RestController
public class DecisionIngestController {

    private final RoutingDecisionRepo repository;

    public DecisionIngestController(RoutingDecisionRepo repository) {
        this.repository = repository;
    }

    @PostMapping(value = "/decisions",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DecisionRecord.Ingested> ingest(@RequestBody DecisionRecord.Batch batch,
            Authentication caller) {
        if (batch == null || batch.decisions() == null) {
            return ResponseEntity.badRequest().build();
        }
        List<DecisionRecord> records = batch.decisions();
        for (DecisionRecord record : records) {
            if (record == null || !record.isValid()) {
                return ResponseEntity.badRequest().build();
            }
        }

        int inserted = repository.insertBatch(records, caller.getName());
        return ResponseEntity.ok(new DecisionRecord.Ingested(records.size(), inserted));
    }
}
