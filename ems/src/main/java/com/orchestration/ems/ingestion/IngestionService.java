package com.orchestration.ems.ingestion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.orchestration.ems.canonical.DagRunId;
import com.orchestration.ems.decisions.L0Decision;
import com.orchestration.ems.decisions.L0Decision.Verdict;
import com.orchestration.ems.decisions.RoutingDecisionRepo;
import com.orchestration.ems.dispatch.OutboxRepo;
import com.orchestration.ems.model.ContextRow;
import com.orchestration.ems.model.EnrichedEvent;
import com.orchestration.ems.model.EventRow;
import com.orchestration.ems.model.SubscriptionMatch;
import com.orchestration.ems.store.ContextRepository;
import com.orchestration.ems.store.EventRepository;
import com.orchestration.ems.subscription.CelPrograms;
import com.orchestration.ems.subscription.SubscriptionService;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * The normative §4.2 ingestion pipeline: the single unit of work that turns one raw upstream event
 * into a persisted, routed record. Called by {@link EventConsumer} once per Kafka message; returning
 * normally means the message may be acked, throwing means it must not (transient failure → park, A1).
 *
 * <p>Stages (ems-design §4.2):
 * <ol>
 *   <li><b>Parse</b> the raw payload into an {@link EventRow} (byte-verbatim {@code rawJson} retained).</li>
 *   <li><b>Persist gate (L0 stage 1)</b> — {@link SubscriptionService#persistMatches(EventRow)} over the
 *       event fields only (A4). Zero matches ⇒ increment {@code ems_events_dropped_total{source}} and
 *       return: no context fetch, no persist, no decision row. This is the intended firehose drop.</li>
 *   <li><b>Resolve context</b> — {@link ContextResolver#resolve(String)} (Caffeine → DB → EDF). Runs
 *       <em>outside</em> the transaction so an EDF outage ({@link EdfUnavailableException}) parks the
 *       partition before any DB write begins.</li>
 *   <li><b>Forward evaluation (L0 stage 2)</b> — {@link SubscriptionService#forwardMatches(EnrichedEvent)}
 *       over the {@code (event, context)} activation.</li>
 *   <li><b>Single transaction</b> ({@link #persist}) — event upsert, context upsert, L0
 *       {@code routing_decision} rows, and one outbox row per FORWARD match, committed atomically.</li>
 * </ol>
 *
 * <p><b>Decision model (grounded in {@link L0Decision} / {@code ux_rd_l0}).</b> A matched event writes one
 * {@code FORWARDED} row per {@link SubscriptionMatch} (idempotent on {@code (event_id, tenant_id)}). An
 * event that passes the persist gate but matches no FORWARD rule writes a single {@code NOT_SUBSCRIBED}
 * marker (null tenant) — the legacy persist-without-trigger outcome. Because the null-tenant marker is
 * <em>not</em> covered by the partial unique index, its redelivery idempotency comes from the
 * <b>event-insert guard</b>: the whole persist commits atomically, so a redelivered event whose insert is
 * a no-op ({@code ON CONFLICT DO NOTHING} returns 0) skips the decision/outbox writes entirely.
 *
 * <p><b>Conf.</b> The forwarded conf is {@link EnrichedEvent#toConf()} (merge shape, A5 — no
 * {@code contractVersion}); {@code dag_run_id = DagRunId.derive(controlDagId, conf)} (A6). Byte-parity of
 * the conf against the legacy Scala derivation is a Phase 4/5 shadow concern (§11), not asserted here.
 */
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);
    private static final String DROP_METRIC = "ems_events_dropped_total";
    private static final String UNKNOWN_SOURCE = "unknown";

    /** §10: the L0 verdict counter, the in-flight mirror of the {@code routing_decision} rows. */
    private static final String VERDICT_METRIC = "ems_subscription_verdicts_total";
    /** Tag value standing in for the null tenant of a {@code NOT_SUBSCRIBED} verdict. */
    private static final String NO_TENANT = "none";

    private final ObjectMapper mapper;
    private final SubscriptionService subscriptionService;
    private final ContextResolver contextResolver;
    private final EventRepository eventRepository;
    private final ContextRepository contextRepository;
    private final RoutingDecisionRepo routingDecisionRepo;
    private final OutboxRepo outboxRepo;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate txTemplate;

    public IngestionService(ObjectMapper mapper, SubscriptionService subscriptionService,
            ContextResolver contextResolver, EventRepository eventRepository,
            ContextRepository contextRepository, RoutingDecisionRepo routingDecisionRepo,
            OutboxRepo outboxRepo, MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager) {
        this.mapper = mapper;
        this.subscriptionService = subscriptionService;
        this.contextResolver = contextResolver;
        this.eventRepository = eventRepository;
        this.contextRepository = contextRepository;
        this.routingDecisionRepo = routingDecisionRepo;
        this.outboxRepo = outboxRepo;
        this.meterRegistry = meterRegistry;
        this.txTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Process one raw upstream event end-to-end (§4.2).
     *
     * @param rawJson the exact JSON text received from Kafka (stored byte-verbatim)
     * @return what the pipeline did with the record — the {@code outcome} dimension of
     *         {@code ems_events_consumed_total}, counted by the caller ({@link EventConsumer}) so that
     *         the topic tag stays where the topic is known
     * @throws IllegalArgumentException  if {@code rawJson} is not valid JSON (poison — routed to DLQ)
     * @throws EdfUnavailableException   if context resolution hits a transient EDF outage (park signal)
     */
    public IngestOutcome process(String rawJson) {
        EventRow event = EventRow.of(rawJson, mapper);

        // Stage 1: persist gate (event fields only). Zero matches ⇒ drop (count, ack, done).
        if (!subscriptionService.persistMatches(event)) {
            meterRegistry.counter(DROP_METRIC, "source", sourceOf(event)).increment();
            log.debug("Dropped event {} at persist gate (source={})", event.eventId(), sourceOf(event));
            return IngestOutcome.DROPPED;
        }

        // Resolve + enrich (outside the TX: an EDF outage parks before any write).
        Optional<ContextRow> context = contextResolver.resolve(event.contextId());
        JsonNode contextTree = context.map(ContextRow::parsed).orElse(null);
        EnrichedEvent enriched = new EnrichedEvent(event.parsed(), contextTree);

        // Stage 2: forward fan-out over (event, context).
        List<SubscriptionMatch> matches = subscriptionService.forwardMatches(enriched);

        if (!persist(event, context.orElse(null), enriched, matches)) {
            return IngestOutcome.DUPLICATE;
        }
        countVerdicts(matches);
        return IngestOutcome.PERSISTED;
    }

    /**
     * Mirror the L0 decision rows onto {@code ems_subscription_verdicts_total{tenant,decision}}: one
     * {@code FORWARDED} per match, or a single {@code NOT_SUBSCRIBED} when nothing matched — the same
     * shape {@link #decisions(String, List)} writes, so the counter and the {@code routing_decision}
     * table can be reconciled against each other.
     *
     * <p>Called only on the {@link IngestOutcome#PERSISTED} path, after the transaction commits. That
     * matters twice over: a redelivery writes no decision rows, so it must add no verdicts either
     * (otherwise the counter drifts above the table on every replay), and a rolled-back transaction
     * must not leave counts behind for decisions that do not exist.
     */
    private void countVerdicts(List<SubscriptionMatch> matches) {
        if (matches.isEmpty()) {
            meterRegistry.counter(VERDICT_METRIC,
                    "tenant", NO_TENANT, "decision", Verdict.NOT_SUBSCRIBED.name()).increment();
            return;
        }
        for (SubscriptionMatch match : matches) {
            meterRegistry.counter(VERDICT_METRIC,
                    "tenant", match.tenantId(), "decision", Verdict.FORWARDED.name()).increment();
        }
    }

    /**
     * The atomic §4.2 step 5 write: event + context + L0 decisions + outbox, one transaction.
     *
     * <p>The event upsert is the idempotency guard: when it inserts nothing (redelivery of an already
     * persisted event), the decisions and outbox — committed atomically on the first delivery — are
     * skipped, which is also what makes the null-tenant {@code NOT_SUBSCRIBED} row redelivery-safe.
     *
     * @return {@code true} if the event was new (decisions and outbox written), {@code false} on the
     *         redelivery no-op — the {@link IngestOutcome#PERSISTED}/{@link IngestOutcome#DUPLICATE} split
     */
    private boolean persist(EventRow event, ContextRow context, EnrichedEvent enriched,
            List<SubscriptionMatch> matches) {
        return Boolean.TRUE.equals(txTemplate.execute(status -> {
            int inserted = eventRepository.upsert(event);
            if (inserted == 0) {
                log.debug("Event {} already persisted; skipping decisions/outbox (redelivery no-op)",
                        event.eventId());
                return false;
            }
            // Context is typically already persisted (resolver save-on-fetch, or it came from the DB);
            // upserting here keeps the atomic write self-contained (§4.2 step 5) and is an idempotent no-op.
            if (context != null) {
                contextRepository.upsert(context);
            }
            routingDecisionRepo.insertL0Batch(decisions(event.eventId(), matches));
            writeOutbox(enriched, matches);
            return true;
        }));
    }

    /**
     * Build the L0 verdict rows: one {@code FORWARDED} per match, or a single {@code NOT_SUBSCRIBED}
     * marker when nothing matched.
     */
    private List<L0Decision> decisions(String eventId, List<SubscriptionMatch> matches) {
        if (matches.isEmpty()) {
            return List.of(L0Decision.notSubscribed(eventId, null, CelPrograms.ENGINE_VERSION));
        }
        List<L0Decision> rows = new ArrayList<>(matches.size());
        for (SubscriptionMatch m : matches) {
            rows.add(L0Decision.forwarded(eventId, m.tenantId(), m.controlDagId(), null,
                    m.registryVersion(), CelPrograms.ENGINE_VERSION));
        }
        return rows;
    }

    /** One outbox row per FORWARD match: same conf, {@code dag_run_id} derived per target DAG (A6). */
    private void writeOutbox(EnrichedEvent enriched, List<SubscriptionMatch> matches) {
        if (matches.isEmpty()) {
            return;
        }
        String conf = enriched.toConf().toString();
        for (SubscriptionMatch m : matches) {
            String dagRunId = DagRunId.derive(m.controlDagId(), conf);
            outboxRepo.insert(dagRunId, m.controlDagId(), conf);
        }
    }

    private static String sourceOf(EventRow event) {
        String source = event.parsed().path("source").asText("");
        return source.isBlank() ? UNKNOWN_SOURCE : source;
    }
}
