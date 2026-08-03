package com.orchestration.ems.recon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit proof of {@link ReconciliationSweep} over a mocked {@link ReconRepository} (the real-rows proof is
 * {@code ReconRepositoryIT}). Three properties matter here, and only one of them is "the number is right":
 * <ul>
 *   <li><b>Publication</b> — all five §10 gauges (three SQL-sourced, two from the mocked
 *       {@link ConsumerLagProbe}) exist with the table's names and tags.</li>
 *   <li><b>Recovery</b> — a topic that has been fully replayed loses its {@code ems_dlq_depth} series
 *       instead of freezing at its last depth, or the page never clears after the operator fixes it.</li>
 *   <li><b>Degradation</b> — a throwing repository does not escape the scheduled method (that can stop it
 *       being rescheduled, silently disabling the whole backstop) and does not take the healthy gauges
 *       down with it. The Kafka pair degrades the other way on purpose, and that asymmetry is asserted
 *       too: an empty probe clears its series rather than freezing them.</li>
 * </ul>
 * Plus the reason this bean exists at all: it is present when {@code ems.dispatch.enabled=false}.
 */
class ReconciliationSweepTest {

    private static final Duration WINDOW = Duration.ofHours(6);
    private static final Duration HORIZON = Duration.ofDays(7);

    private ReconRepository repo;
    private ConsumerLagProbe lagProbe;
    private SimpleMeterRegistry meters;
    private ReconciliationSweep sweep;

    @BeforeEach
    void setUp() {
        repo = mock(ReconRepository.class);
        lagProbe = mock(ConsumerLagProbe.class);
        meters = new SimpleMeterRegistry();
        when(repo.oldestPendingOutboxAgeSeconds()).thenReturn(0.0);
        when(repo.overdueInflightRuns(any(), any())).thenReturn(0L);
        when(repo.dlqDepthByTopic()).thenReturn(List.of());
        when(lagProbe.probe()).thenReturn(List.of());
        sweep = new ReconciliationSweep(repo, Optional.of(lagProbe), meters, WINDOW, HORIZON);
    }

    @Test
    void publishesTheThreeSqlSourcedGauges_withTheDesignTableNamesAndTags() {
        when(repo.oldestPendingOutboxAgeSeconds()).thenReturn(742.5);
        when(repo.overdueInflightRuns(WINDOW, HORIZON)).thenReturn(3L);
        when(repo.dlqDepthByTopic()).thenReturn(List.of(new DlqDepth("edf.events", 4)));

        sweep.sweep();

        assertThat(gauge(ReconciliationSweep.OUTBOX_AGE_METRIC)).isEqualTo(742.5);
        assertThat(gauge(ReconciliationSweep.OVERDUE_RUNS_METRIC)).isEqualTo(3.0);
        assertThat(meters.get(ReconciliationSweep.DLQ_DEPTH_METRIC)
                .tag(ReconciliationSweep.TOPIC_TAG, "edf.events")
                .gauge().value()).isEqualTo(4.0);
    }

    @Test
    void gaugesExistBeforeTheFirstSweep_soAScrapeAtStartupIsNotAGap() {
        assertThat(meters.find(ReconciliationSweep.OUTBOX_AGE_METRIC).gauge()).isNotNull();
        assertThat(meters.find(ReconciliationSweep.OVERDUE_RUNS_METRIC).gauge()).isNotNull();
    }

    @Test
    void dlqDepth_publishesOneSeriesPerTopic() {
        when(repo.dlqDepthByTopic())
                .thenReturn(List.of(new DlqDepth("edf.events", 2), new DlqDepth("meg.events", 7)));

        sweep.sweep();

        assertThat(meters.find(ReconciliationSweep.DLQ_DEPTH_METRIC).gauges())
                .extracting(g -> g.getId().getTag(ReconciliationSweep.TOPIC_TAG))
                .containsExactlyInAnyOrder("edf.events", "meg.events");
    }

    @Test
    void dlqDepth_replayedTopicLosesItsSeries_soThePageCanClear() {
        when(repo.dlqDepthByTopic()).thenReturn(List.of(new DlqDepth("edf.events", 3)));
        sweep.sweep();

        when(repo.dlqDepthByTopic()).thenReturn(List.of()); // operator replayed everything
        sweep.sweep();

        assertThat(meters.find(ReconciliationSweep.DLQ_DEPTH_METRIC).gauges()).isEmpty();
    }

    @Test
    void consumerLag_publishesLagAndHeadroomPerPartition() {
        when(lagProbe.probe()).thenReturn(List.of(
                new PartitionLag("edf.events", 0, 40, 10_000),
                new PartitionLag("edf.events", 1, 0, 250)));

        sweep.sweep();

        assertThat(lagSeries(ReconciliationSweep.CONSUMER_LAG_METRIC, "0")).isEqualTo(40.0);
        assertThat(lagSeries(ReconciliationSweep.CONSUMER_LAG_METRIC, "1")).isEqualTo(0.0);
        assertThat(lagSeries(ReconciliationSweep.RETENTION_HEADROOM_METRIC, "0")).isEqualTo(10_000.0);
        assertThat(lagSeries(ReconciliationSweep.RETENTION_HEADROOM_METRIC, "1")).isEqualTo(250.0);
    }

    /**
     * The deliberate asymmetry with {@link #aFailingQueryNeitherEscapesNorBlanksItsGauge()}: the probe
     * reports a failed offsets poll as an empty result, and that must <b>clear</b> the series. A frozen
     * lag stops rising exactly when lag starts to matter, so a {@code max(ems_consumer_lag) > N} rule
     * would evaluate for ever against a number that can no longer move.
     */
    @Test
    void consumerLag_emptyProbeClearsBothSeries_ratherThanFreezingThem() {
        when(lagProbe.probe()).thenReturn(List.of(new PartitionLag("edf.events", 0, 40, 10_000)));
        sweep.sweep();

        when(lagProbe.probe()).thenReturn(List.of()); // broker unreachable, or the group is gone
        sweep.sweep();

        assertThat(meters.find(ReconciliationSweep.CONSUMER_LAG_METRIC).gauges()).isEmpty();
        assertThat(meters.find(ReconciliationSweep.RETENTION_HEADROOM_METRIC).gauges()).isEmpty();
    }

    /** {@code ems.recon.kafka.enabled=false}: no probe bean, and the three SQL gauges carry on. */
    @Test
    void withNoLagProbeAtAll_theSqlGaugesStillPublish() {
        when(repo.oldestPendingOutboxAgeSeconds()).thenReturn(31.0);
        SimpleMeterRegistry brokerless = new SimpleMeterRegistry();

        new ReconciliationSweep(repo, Optional.empty(), brokerless, WINDOW, HORIZON).sweep();

        assertThat(brokerless.get(ReconciliationSweep.OUTBOX_AGE_METRIC).gauge().value()).isEqualTo(31.0);
        assertThat(brokerless.find(ReconciliationSweep.CONSUMER_LAG_METRIC).gauges()).isEmpty();
    }

    @Test
    void aFailingQueryNeitherEscapesNorBlanksItsGauge() {
        when(repo.oldestPendingOutboxAgeSeconds()).thenReturn(900.0);
        sweep.sweep();

        when(repo.oldestPendingOutboxAgeSeconds()).thenThrow(new IllegalStateException("connection reset"));

        assertThatCode(sweep::sweep).doesNotThrowAnyException();
        // stale-but-present beats absent: the alert rule still evaluates against the last-known backlog
        assertThat(gauge(ReconciliationSweep.OUTBOX_AGE_METRIC)).isEqualTo(900.0);
    }

    @Test
    void oneFailingSourceDoesNotTakeTheOtherGaugesDown() {
        when(repo.overdueInflightRuns(any(), any())).thenThrow(new IllegalStateException("statement timeout"));
        when(repo.oldestPendingOutboxAgeSeconds()).thenReturn(120.0);
        when(repo.dlqDepthByTopic()).thenReturn(List.of(new DlqDepth("edf.events", 1)));

        sweep.sweep();

        assertThat(gauge(ReconciliationSweep.OUTBOX_AGE_METRIC)).isEqualTo(120.0);
        assertThat(meters.get(ReconciliationSweep.DLQ_DEPTH_METRIC)
                .tag(ReconciliationSweep.TOPIC_TAG, "edf.events").gauge().value()).isEqualTo(1.0);
    }

    /**
     * The reason this bean is not gated on the dispatcher: in {@code shadow} the outbox fills by design
     * and nothing drains it, so {@code ems_outbox_pending_age_seconds} has to be published by something
     * that exists there. It is also present with {@code ems.consumer.enabled=false} — an API-only pod
     * still reports whether anything is stuck.
     */
    @Test
    void sweepBeanIsPresentWithDispatchAndConsumerDisabled_andOptOutIsExplicit() {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                // SpringApplication installs this at startup; a bare ApplicationContextRunner does not,
                // and without it the `6h`/`7d` @Value defaults cannot be converted to Duration.
                .withInitializer(context -> context.getBeanFactory()
                        .setConversionService(ApplicationConversionService.getSharedInstance()))
                .withUserConfiguration(SweepTestConfig.class)
                .withPropertyValues("ems.dispatch.enabled=false", "ems.consumer.enabled=false");

        runner.run(context -> assertThat(context).hasSingleBean(ReconciliationSweep.class));
        runner.withPropertyValues("ems.recon.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(ReconciliationSweep.class));
    }

    /**
     * The Kafka half has its own switch, for an environment with no reachable broker. Asserted here
     * because a toggle that does not actually gate the bean is only discovered when someone needs it.
     * The bootstrap address is deliberately unroutable — {@code Admin.create} must not connect, and a
     * probe that cannot reach a broker must still not stop the context from starting.
     */
    @Test
    void lagProbeBeanIsPresentByDefault_andRemovedByItsOwnToggle() {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withInitializer(context -> context.getBeanFactory()
                        .setConversionService(ApplicationConversionService.getSharedInstance()))
                .withUserConfiguration(LagProbeTestConfig.class)
                .withPropertyValues("spring.kafka.bootstrap-servers=localhost:1");

        runner.run(context -> assertThat(context).hasSingleBean(ConsumerLagProbe.class));
        runner.withPropertyValues("ems.recon.kafka.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(ConsumerLagProbe.class));
    }

    private double gauge(String name) {
        return meters.get(name).gauge().value();
    }

    /** One {@code {topic="edf.events", partition=...}} series of a Kafka gauge. */
    private double lagSeries(String name, String partition) {
        return meters.get(name)
                .tag(ReconciliationSweep.TOPIC_TAG, "edf.events")
                .tag(ReconciliationSweep.PARTITION_TAG, partition)
                .gauge().value();
    }

    @Configuration(proxyBeanMethods = false)
    @Import(ReconciliationSweep.class) // brings in @ConditionalOnProperty + the @Value defaults
    static class SweepTestConfig {

        @Bean
        ReconRepository reconRepository() {
            return mock(ReconRepository.class);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @Import(ConsumerLagProbe.class) // brings in @ConditionalOnProperty + the bootstrap/group @Value defaults
    static class LagProbeTestConfig { }
}
