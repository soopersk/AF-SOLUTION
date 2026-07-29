package com.orchestration.ems.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit proof of the §4.2 pipeline in {@link IngestionService} with every collaborator mocked (no DB, no
 * Kafka — runs locally). Verifies stage ordering, the persist-gate drop (count + no resolve/persist), the
 * FORWARDED-per-match vs single-NOT_SUBSCRIBED decision mix, one outbox row per match with the A6
 * {@code dag_run_id}, the event-insert redelivery guard, and EDF-outage park propagation. Single-TX
 * atomicity itself is proven against a real PostgreSQL in Batch I.
 */
class IngestionServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DAG_CAPITAL = "orchestration_control_dag_capital";
    private static final String DAG_LIQUIDITY = "orchestration_control_dag_liquidity";
    private static final String VERDICT_METRIC = "ems_subscription_verdicts_total";

    private SubscriptionService subscription;
    private ContextResolver resolver;
    private EventRepository eventRepo;
    private ContextRepository contextRepo;
    private RoutingDecisionRepo decisionRepo;
    private OutboxRepo outboxRepo;
    private SimpleMeterRegistry meters;
    private IngestionService service;

    @BeforeEach
    void setUp() {
        subscription = mock(SubscriptionService.class);
        resolver = mock(ContextResolver.class);
        eventRepo = mock(EventRepository.class);
        contextRepo = mock(ContextRepository.class);
        decisionRepo = mock(RoutingDecisionRepo.class);
        outboxRepo = mock(OutboxRepo.class);
        meters = new SimpleMeterRegistry();

        PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
        when(txManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));

        service = new IngestionService(MAPPER, subscription, resolver, eventRepo, contextRepo,
                decisionRepo, outboxRepo, meters, txManager);
    }

    @Test
    void zeroMatch_dropsAtPersistGate_counted_noResolveNoPersist() {
        String raw = """
                {"id":"evt-noise","source":"FIREHOSE","additionalData":{"msgTypeEventType":"HEARTBEAT"}}""";
        when(subscription.persistMatches(any())).thenReturn(false);

        assertThat(service.process(raw)).isEqualTo(IngestOutcome.DROPPED);

        assertThat(dropCount("FIREHOSE")).isEqualTo(1.0);
        verify(subscription, never()).forwardMatches(any());
        verifyNoInteractions(resolver, eventRepo, contextRepo, decisionRepo, outboxRepo);
        // a dropped event produced no decision row, so it must produce no verdict either
        assertThat(meters.find(VERDICT_METRIC).counters()).isEmpty();
    }

    @Test
    void zeroMatch_missingSource_countsUnknown() {
        when(subscription.persistMatches(any())).thenReturn(false);

        assertThat(service.process("""
                {"id":"evt-nosrc","additionalData":{"x":"y"}}""")).isEqualTo(IngestOutcome.DROPPED);

        assertThat(dropCount("unknown")).isEqualTo(1.0);
    }

    @Test
    void matching_persistsEnrichesForwards_writesForwardedDecisionsAndOutbox_inOrder() {
        String raw = """
                {"id":"evt-mer","source":"MERIVAL","contextId":"ctx-2",
                 "additionalData":{"TYPE":"INGESTION","RUN_TYPE":"BATCH"}}""";
        ContextRow context = ContextRow.of("""
                {"id":"ctx-2","data":{"frequency":"DAILY"}}""", MAPPER);
        List<SubscriptionMatch> matches = List.of(
                new SubscriptionMatch("CAPITAL", DAG_CAPITAL, "cap.rule", "seed-0"),
                new SubscriptionMatch("LIQUIDITY", DAG_LIQUIDITY, "liq.rule", "seed-0"));

        when(subscription.persistMatches(any())).thenReturn(true);
        when(resolver.resolve("ctx-2")).thenReturn(Optional.of(context));
        when(subscription.forwardMatches(any())).thenReturn(matches);
        when(eventRepo.upsert(any())).thenReturn(1);

        assertThat(service.process(raw)).isEqualTo(IngestOutcome.PERSISTED);

        // §10: one FORWARDED verdict per match, tagged by tenant — the in-flight mirror of the rows below
        assertThat(verdictCount("CAPITAL", "FORWARDED")).isEqualTo(1.0);
        assertThat(verdictCount("LIQUIDITY", "FORWARDED")).isEqualTo(1.0);
        assertThat(verdictCount("none", "NOT_SUBSCRIBED")).isZero();

        // stage ordering: gate → resolve → forward → event → context → decisions → outbox
        InOrder ord = Mockito.inOrder(subscription, resolver, eventRepo, contextRepo, decisionRepo, outboxRepo);
        ord.verify(subscription).persistMatches(any());
        ord.verify(resolver).resolve("ctx-2");
        ord.verify(subscription).forwardMatches(any());
        ord.verify(eventRepo).upsert(any());
        ord.verify(contextRepo).upsert(context);
        ord.verify(decisionRepo).insertL0Batch(any());
        ord.verify(outboxRepo, times(2)).insert(anyString(), anyString(), anyString());

        // decision mix: two FORWARDED rows, correct tenant/target/registry/engine, no NOT_SUBSCRIBED
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<L0Decision>> decisions = ArgumentCaptor.forClass(Collection.class);
        verify(decisionRepo).insertL0Batch(decisions.capture());
        assertThat(decisions.getValue())
                .allSatisfy(d -> {
                    assertThat(d.decision()).isEqualTo(Verdict.FORWARDED);
                    assertThat(d.eventId()).isEqualTo("evt-mer");
                    assertThat(d.registryVersion()).isEqualTo("seed-0");
                    assertThat(d.engineVersion()).isEqualTo(CelPrograms.ENGINE_VERSION);
                })
                .extracting(L0Decision::tenantId).containsExactlyInAnyOrder("CAPITAL", "LIQUIDITY");

        // one outbox row per match; same conf; dag_run_id = derive(dagId, conf) (A6)
        String conf = new EnrichedEvent(EventRow.of(raw, MAPPER).parsed(), context.parsed()).toConf().toString();
        verify(outboxRepo).insert(DagRunId.derive(DAG_CAPITAL, conf), DAG_CAPITAL, conf);
        verify(outboxRepo).insert(DagRunId.derive(DAG_LIQUIDITY, conf), DAG_LIQUIDITY, conf);
    }

    @Test
    void persistButNoForward_writesSingleNotSubscribed_noOutbox() {
        String raw = """
                {"id":"evt-persist-only","source":"MERIVAL","contextId":"ctx-3"}""";
        when(subscription.persistMatches(any())).thenReturn(true);
        when(resolver.resolve("ctx-3")).thenReturn(Optional.empty());
        when(subscription.forwardMatches(any())).thenReturn(List.of());
        when(eventRepo.upsert(any())).thenReturn(1);

        assertThat(service.process(raw)).isEqualTo(IngestOutcome.PERSISTED);

        assertThat(verdictCount("none", "NOT_SUBSCRIBED")).isEqualTo(1.0);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<L0Decision>> decisions = ArgumentCaptor.forClass(Collection.class);
        verify(decisionRepo).insertL0Batch(decisions.capture());
        assertThat(decisions.getValue()).singleElement().satisfies(d -> {
            assertThat(d.decision()).isEqualTo(Verdict.NOT_SUBSCRIBED);
            assertThat(d.eventId()).isEqualTo("evt-persist-only");
            assertThat(d.tenantId()).isNull();
            assertThat(d.targetDagId()).isNull();
            assertThat(d.engineVersion()).isEqualTo(CelPrograms.ENGINE_VERSION);
        });
        verify(contextRepo, never()).upsert(any()); // context absent
        verifyNoInteractions(outboxRepo);
    }

    @Test
    void redelivery_eventInsertNoOp_skipsContextDecisionsAndOutbox() {
        String raw = """
                {"id":"evt-dup","source":"MERIVAL","contextId":"ctx-4"}""";
        ContextRow context = ContextRow.of("{\"id\":\"ctx-4\",\"data\":{}}", MAPPER);
        when(subscription.persistMatches(any())).thenReturn(true);
        when(resolver.resolve("ctx-4")).thenReturn(Optional.of(context));
        when(subscription.forwardMatches(any()))
                .thenReturn(List.of(new SubscriptionMatch("CAPITAL", DAG_CAPITAL, "cap.rule", "seed-0")));
        when(eventRepo.upsert(any())).thenReturn(0); // already persisted (ON CONFLICT DO NOTHING)

        assertThat(service.process(raw)).isEqualTo(IngestOutcome.DUPLICATE);

        verify(eventRepo).upsert(any());
        verify(contextRepo, never()).upsert(any());
        verify(decisionRepo, never()).insertL0Batch(any());
        verifyNoInteractions(outboxRepo);
        // the redelivery wrote no decision row, so it must add no verdict — otherwise the counter
        // drifts permanently above the routing_decision table on every replay
        assertThat(meters.find(VERDICT_METRIC).counters()).isEmpty();
    }

    @Test
    void edfOutage_propagatesParkSignal_noPersist() {
        String raw = """
                {"id":"evt-down","source":"MERIVAL","contextId":"ctx-down"}""";
        when(subscription.persistMatches(any())).thenReturn(true);
        when(resolver.resolve("ctx-down"))
                .thenThrow(new EdfUnavailableException("EDF returned status 503"));

        assertThatThrownBy(() -> service.process(raw)).isInstanceOf(EdfUnavailableException.class);

        verify(subscription, never()).forwardMatches(any());
        verifyNoInteractions(eventRepo, contextRepo, decisionRepo, outboxRepo);
    }

    private double dropCount(String source) {
        return meters.get("ems_events_dropped_total").tag("source", source).counter().count();
    }

    /** {@code ems_subscription_verdicts_total{tenant,decision}}, 0 when that series was never created. */
    private double verdictCount(String tenant, String decision) {
        var counter = meters.find(VERDICT_METRIC)
                .tag("tenant", tenant).tag("decision", decision).counter();
        return counter == null ? 0.0 : counter.count();
    }
}
