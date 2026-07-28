package com.orchestration.ems.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.orchestration.ems.ingestion.DlqReplayService;
import com.orchestration.ems.model.DlqReplay;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionUpsert;
import com.orchestration.ems.subscription.CelPrograms;
import com.orchestration.ems.subscription.SubscriptionRepo;

/**
 * The two admin write surfaces (ems-design §4.5:220-221):
 * <ul>
 *   <li>{@code POST /admin/replay} — re-publish selected dead-lettered messages to their source topic,
 *       auditing each one ({@link DlqReplayService}).</li>
 *   <li>{@code PUT /admin/subscriptions} — upsert rendered Level-0 subscription rows, rejecting CEL that
 *       does not compile and (for {@code PERSIST}) any reference to {@code context.*} (Amendment A4).</li>
 * </ul>
 *
 * <p><b>Both writer identities are the authenticated principal</b> — {@code dlq_record.replayed_by} and
 * {@code subscription.updated_by} are stamped from it, never from the body. {@code SecurityConfig} gates
 * {@code /admin/**} on the elevated {@code EMS_ADMIN} group and {@code PUT /admin/subscriptions}
 * additionally on {@code EMS_CI} (§4.5:201), so the identity in those columns is one an operator can hold
 * someone to.
 *
 * <p><b>Rejections are 400 with an empty body</b>, matching the other Phase-3 controllers; the reason
 * (which rule, which compiler message) is logged at WARN rather than returned, because no document pins
 * an error shape and the registry CI contract has not named one. A machine-readable rejection body is
 * trivially additive once it does — the same discipline as Batch C's {@code scheduled} and Batch D's
 * per-group age.
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class);

    private final DlqReplayService replayService;
    private final SubscriptionRepo subscriptions;
    private final CelPrograms celPrograms;

    public AdminController(DlqReplayService replayService, SubscriptionRepo subscriptions,
            CelPrograms celPrograms) {
        this.replayService = replayService;
        this.subscriptions = subscriptions;
        this.celPrograms = celPrograms;
    }

    /**
     * {@code POST /admin/replay} — re-drive the named {@code dlq_record} rows.
     *
     * <p>Always 200 for a well-formed request: the body reports per-id what happened
     * ({@link DlqReplay.Result}), because a record that could not be replayed is not lost — its row stays
     * unstamped and re-replayable, so an opaque 5xx would tell the operator strictly less.
     */
    @PostMapping(value = "/replay",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DlqReplay.Result> replay(@RequestBody DlqReplay request,
            Authentication operator) {
        if (request == null || !request.isValid()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(replayService.replay(request.ids(), operator.getName()));
    }

    /**
     * {@code PUT /admin/subscriptions} — upsert a rendered registry slice.
     *
     * <p>A single unusable row rejects the <b>whole</b> slice (400, nothing written): a partially-applied
     * ruleset is a ruleset nobody rendered, and the L0 gate would then run on it for up to a
     * {@link com.orchestration.ems.subscription.SubscriptionService} refresh interval. The caller fixes
     * its render and re-PUTs, which is safe because the write is an upsert.
     */
    @PutMapping(value = "/subscriptions",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SubscriptionUpsert.Applied> upsertSubscriptions(
            @RequestBody SubscriptionUpsert.Batch batch, Authentication writer) {
        if (batch == null || batch.subscriptions() == null) {
            return ResponseEntity.badRequest().build();
        }

        List<SubscriptionRow> rows = new ArrayList<>();
        for (SubscriptionUpsert wire : batch.subscriptions()) {
            Optional<SubscriptionRow> row = wire == null ? Optional.empty() : wire.toRow();
            if (row.isEmpty()) {
                log.warn("rejecting subscription slice from {}: unusable row {}", writer.getName(), wire);
                return ResponseEntity.badRequest().build();
            }
            if (!compiles(row.get(), writer.getName())) {
                return ResponseEntity.badRequest().build();
            }
            rows.add(row.get());
        }

        int upserted = subscriptions.upsertAll(rows, writer.getName());
        return ResponseEntity.ok(new SubscriptionUpsert.Applied(rows.size(), upserted));
    }

    /**
     * The §4.5:221 CEL gate. {@link CelPrograms#compile} declares {@code context} only for
     * {@code FORWARD}, so a {@code PERSIST} rule that references it fails to compile as an undeclared
     * reference — A4 is enforced structurally by the same call that checks the CEL is valid at all, not by
     * a separate text search for "context." that a rule could evade.
     */
    private boolean compiles(SubscriptionRow row, String updatedBy) {
        try {
            celPrograms.compile(row);
            return true;
        } catch (IllegalArgumentException invalidCel) {
            log.warn("rejecting subscription slice from {}: rule '{}' ({}) does not compile: {}",
                    updatedBy, row.ruleName(), row.stage(), invalidCel.getMessage());
            return false;
        }
    }
}
