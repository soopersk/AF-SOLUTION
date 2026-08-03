package com.orchestration.ems.subscription;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import com.orchestration.ems.ingestion.Normalizer;
import com.orchestration.ems.model.EnrichedEvent;
import com.orchestration.ems.model.EventRow;
import com.orchestration.ems.model.SubscriptionMatch;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;

/**
 * Evaluates the two-stage Level-0 subscription pipeline (ems-design §4.2/§7, Amendment A4):
 *
 * <ul>
 *   <li><b>PERSIST</b> ({@link #persistMatches(EventRow)}) — the pre-enrichment drop gate. An OR over
 *       every enabled PERSIST rule against the <em>event only</em> ({@code {event}} activation). One
 *       match ⇒ the event is worth persisting; zero ⇒ drop.</li>
 *   <li><b>FORWARD</b> ({@link #forwardMatches(EnrichedEvent)}) — the post-enrichment routing fan-out.
 *       Every enabled FORWARD rule is evaluated against {@code {event, context}}; each hit becomes one
 *       {@link SubscriptionMatch} (→ one outbox row + one FORWARDED routing_decision).</li>
 * </ul>
 *
 * <p>Rules are evaluated against the <em>normalized</em> view of the event/context (this class is the
 * sole in-memory consumer of {@link Normalizer} on the routing path — the stored JSONB stays byte
 * verbatim, its typed columns normalized independently in-DB). A runtime evaluation error (missing key /
 * no such field) is treated as a non-match, mirroring the legacy {@code JsonFilterRuleset.filter}
 * {@code getOrElse(false)} semantics.
 *
 * <p>The enabled ruleset is loaded via the injected supplier ({@code SubscriptionRepo::loadEnabled} in
 * production; in-memory fixtures in tests), compiled once, and cached with Caffeine
 * {@code refreshAfterWrite(60s)} so subscription-table edits are picked up without a restart.
 */
public class SubscriptionService {

    private static final String RULESET_KEY = "enabled";
    private static final Map<String, Object> EMPTY_CONTEXT = Map.of();

    private final Supplier<List<SubscriptionRow>> loader;
    private final Normalizer normalizer;
    private final CelPrograms celPrograms;
    private final ObjectMapper mapper = new ObjectMapper();
    private final LoadingCache<String, Ruleset> ruleset;

    /**
     * @param loader      supplies the enabled subscription rows ({@code SubscriptionRepo::loadEnabled}
     *                    in production; a fixtures supplier in unit tests)
     * @param normalizer  the single canonicalization authority applied to event/context before CEL
     * @param celPrograms compiles + caches the CEL {@link Script}s (and enforces the A4 PERSIST guard)
     */
    public SubscriptionService(Supplier<List<SubscriptionRow>> loader, Normalizer normalizer,
            CelPrograms celPrograms) {
        this.loader = loader;
        this.normalizer = normalizer;
        this.celPrograms = celPrograms;
        this.ruleset = Caffeine.newBuilder()
                .refreshAfterWrite(Duration.ofSeconds(60))
                .build(key -> load());
    }

    /**
     * Stage 1 drop gate: does any enabled PERSIST rule match this event? Event fields only — no context.
     *
     * @param event the (raw) event row; its parsed tree is normalized before evaluation
     * @return {@code true} if at least one PERSIST rule matches (persist the event), else {@code false}
     */
    public boolean persistMatches(EventRow event) {
        Map<String, Object> activation = Map.of("event", eventMap(event.parsed()));
        for (CompiledRule rule : ruleset().persist()) {
            if (evaluate(rule.script(), activation)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Stage 2 routing fan-out: every enabled FORWARD rule that matches the enriched event yields a
     * {@link SubscriptionMatch}. Evaluated against {@code {event, context}} (a null context becomes an
     * empty map, so context-referencing rules simply do not match).
     *
     * @param enriched the event paired with its resolved context (context may be {@code null})
     * @return the matched forwards (possibly empty); order follows the loaded ruleset
     */
    public List<SubscriptionMatch> forwardMatches(EnrichedEvent enriched) {
        Map<String, Object> activation = Map.of(
                "event", eventMap(enriched.event()),
                "context", contextMap(enriched.context()));
        List<SubscriptionMatch> matches = new ArrayList<>();
        for (CompiledRule rule : ruleset().forward()) {
            if (evaluate(rule.script(), activation)) {
                SubscriptionRow row = rule.row();
                matches.add(new SubscriptionMatch(row.tenantId(), row.controlDagId(), row.ruleName(),
                        row.registryVersion()));
            }
        }
        return List.copyOf(matches);
    }

    /**
     * The §7 {@code PERSIST ⊇ FORWARD} invariant, checked for a single representative event: if the
     * event produces any FORWARD match it must also pass the PERSIST gate, so no forwarded event is
     * silently dropped upstream. Used by the CI conformance chain over the seed fixtures.
     *
     * @param event   the (raw) event row
     * @param context the resolved context tree, or {@code null}
     * @return {@code true} if the event either forwards nothing or (forwarding) also persists
     */
    public boolean forwardImpliesPersist(EventRow event, JsonNode context) {
        boolean forwards = !forwardMatches(new EnrichedEvent(event.parsed(), context)).isEmpty();
        return !forwards || persistMatches(event);
    }

    private Ruleset ruleset() {
        return ruleset.get(RULESET_KEY);
    }

    private Ruleset load() {
        List<CompiledRule> persist = new ArrayList<>();
        List<CompiledRule> forward = new ArrayList<>();
        for (SubscriptionRow row : loader.get()) {
            if (!row.enabled()) {
                continue;
            }
            CompiledRule compiled = new CompiledRule(row, celPrograms.compile(row));
            (row.stage() == Stage.PERSIST ? persist : forward).add(compiled);
        }
        return new Ruleset(List.copyOf(persist), List.copyOf(forward));
    }

    /**
     * Normalize, then case-fold (A15). Order matters: the {@link Normalizer} is the canonicalization
     * authority and its mutation counter must keep counting real rewrites, so the fold happens
     * <em>after</em> it and only on this in-memory activation — never on anything that is stored,
     * indexed, or sent onward.
     */
    private Map<String, Object> eventMap(JsonNode rawEvent) {
        return toMap(MatchView.fold(normalizer.normalizeEvent(rawEvent)));
    }

    private Map<String, Object> contextMap(JsonNode rawContext) {
        if (rawContext == null) {
            return EMPTY_CONTEXT;
        }
        return toMap(MatchView.fold(normalizer.normalizeContext(rawContext)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> toMap(JsonNode node) {
        if (node == null || !node.isObject()) {
            return Map.of();
        }
        return mapper.convertValue(node, Map.class);
    }

    private boolean evaluate(Script script, Map<String, Object> activation) {
        try {
            return Boolean.TRUE.equals(script.execute(Boolean.class, activation));
        } catch (ScriptException e) {
            // Legacy parity (JsonFilterRuleset.filter → getOrElse(false)): a runtime evaluation error
            // — typically a missing key / no such field — is a NON-match, not a failure.
            return false;
        }
    }

    /** One enabled rule paired with its compiled program. */
    private record CompiledRule(SubscriptionRow row, Script script) {
    }

    /** The compiled ruleset, partitioned by stage (the cached unit refreshed every 60s). */
    private record Ruleset(List<CompiledRule> persist, List<CompiledRule> forward) {
    }
}
