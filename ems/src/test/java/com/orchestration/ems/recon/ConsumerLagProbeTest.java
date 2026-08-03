package com.orchestration.ems.recon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit proof of {@link ConsumerLagProbe}'s offset arithmetic and — more importantly — of its behaviour
 * when the broker will not answer. The real-broker proof is {@code ConsumerLagProbeIT}.
 *
 * <p>The arithmetic is one subtraction each way, so the tests that matter are the failure ones: this
 * probe runs inside a scheduled sweep, and an unreachable broker must cost one WARN line and an empty
 * result — never a thrown exception (which would end the sweep tick) and never an unbounded wait (which
 * would wedge the scheduler thread and silently stop all five gauges).
 */
class ConsumerLagProbeTest {

    private static final String GROUP = "ems-ingest";
    private static final String TOPIC = "edf.events";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    private Admin admin;
    private ConsumerLagProbe probe;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        probe = new ConsumerLagProbe(admin, GROUP, Duration.ofSeconds(5));
    }

    @Test
    void lagIsEndMinusCommitted_andHeadroomIsCommittedMinusEarliest() {
        givenCommitted(Map.of(P0, 100L, P1, 250L));
        givenOffsets(OffsetSpec.latest(), Map.of(P0, 140L, P1, 250L));
        givenOffsets(OffsetSpec.earliest(), Map.of(P0, 90L, P1, 0L));

        assertThat(probe.probe()).containsExactlyInAnyOrder(
                //                                  lag = 140-100        headroom = 100-90
                new PartitionLag(TOPIC, 0, 40, 10),
                //                caught up: lag 0     headroom = 250-0
                new PartitionLag(TOPIC, 1, 0, 250));
    }

    /**
     * The earliest offset is the log start, so a partition whose committed offset equals it has consumed
     * nothing that retention has not already been free to delete next — zero headroom is the early page.
     */
    @Test
    void headroomIsZeroWhenTheCommittedOffsetHasReachedTheLogStart() {
        givenCommitted(Map.of(P0, 500L));
        givenOffsets(OffsetSpec.latest(), Map.of(P0, 9_000L));
        givenOffsets(OffsetSpec.earliest(), Map.of(P0, 500L));

        assertThat(probe.probe()).containsExactly(new PartitionLag(TOPIC, 0, 8_500, 0));
    }

    /** The three offsets are three separate calls; a partition that moves between them must not go negative. */
    @Test
    void skewBetweenTheThreeCallsIsClampedToZero() {
        givenCommitted(Map.of(P0, 100L));
        givenOffsets(OffsetSpec.latest(), Map.of(P0, 98L));    // head read before the commit landed
        givenOffsets(OffsetSpec.earliest(), Map.of(P0, 120L)); // log start read after a segment deletion

        assertThat(probe.probe()).containsExactly(new PartitionLag(TOPIC, 0, 0, 0));
    }

    @Test
    void groupThatHasNeverCommitted_publishesNothingAndAsksForNoOffsets() {
        givenCommitted(Map.of());

        assertThat(probe.probe()).isEmpty();
        verify(admin, never()).listOffsets(any());
    }

    /**
     * The partition was committed-to but is gone from the offsets response (topic deleted between calls).
     * Skipping it drops the series; inventing a number against a partition that no longer exists would be
     * worse, and an NPE here would take the whole sweep tick down.
     */
    @Test
    void partitionMissingFromTheOffsetsResponseIsSkipped_notNullDereferenced() {
        givenCommitted(Map.of(P0, 10L, P1, 20L));
        givenOffsets(OffsetSpec.latest(), Map.of(P0, 30L));
        givenOffsets(OffsetSpec.earliest(), Map.of(P0, 0L));

        assertThat(probe.probe()).containsExactly(new PartitionLag(TOPIC, 0, 20, 10));
    }

    @Test
    void unreachableBroker_yieldsAnEmptyResultInsteadOfThrowing() {
        when(admin.listConsumerGroupOffsets(anyString()))
                .thenThrow(new KafkaException("no resolvable bootstrap urls"));

        assertThat(probe.probe()).isEmpty();
    }

    /**
     * The bound that keeps the sweep alive: an {@link Admin} future that never completes must not park the
     * scheduler thread. {@code @Scheduled(fixedDelay)} runs on a single thread, so one unbounded wait here
     * would freeze every other gauge in the sweep too.
     */
    @Test
    void aFutureThatNeverCompletes_isBoundedByTheTimeout() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> hung = mock(KafkaFuture.class);
        when(hung.get(anyLong(), any())).thenThrow(new TimeoutException("broker did not answer"));
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(hung);
        when(admin.listConsumerGroupOffsets(GROUP)).thenReturn(result);

        assertThat(probe.probe()).isEmpty();
    }

    // --- fixtures ------------------------------------------------------------------------------------

    private void givenCommitted(Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        offsets.forEach((partition, offset) -> committed.put(partition, new OffsetAndMetadata(offset)));
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(committed));
        when(admin.listConsumerGroupOffsets(GROUP)).thenReturn(result);
    }

    /** Stubs {@code listOffsets} for exactly the request that asks for this {@link OffsetSpec}. */
    private void givenOffsets(OffsetSpec spec, Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, ListOffsetsResultInfo> infos = new HashMap<>();
        offsets.forEach((partition, offset) ->
                infos.put(partition, new ListOffsetsResultInfo(offset, -1L, Optional.empty())));
        ListOffsetsResult result = mock(ListOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(infos));
        when(admin.listOffsets(requestFor(spec))).thenReturn(result);
    }

    /**
     * Matches the {@code listOffsets} request whose every value is this kind of spec — the probe asks for
     * latest and earliest in two separate calls, and the two must not be stubbed interchangeably or the
     * test would pass with lag and headroom swapped. Matched by <em>class</em>
     * ({@code LatestSpec}/{@code EarliestSpec}): {@link OffsetSpec} hands back a fresh instance per call
     * and does not override {@code equals}, so an equality matcher here would silently never match.
     */
    private static Map<TopicPartition, OffsetSpec> requestFor(OffsetSpec spec) {
        return argThat(request -> request != null && !request.isEmpty()
                && request.values().stream().allMatch(value -> value.getClass() == spec.getClass()));
    }
}
