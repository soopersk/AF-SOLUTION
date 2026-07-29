package com.orchestration.ems.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Unit proof of the {@code ems_registry_version{component,version}} info metric (ems-design §10) over a
 * mocked repository. The behaviour that matters operationally is the <em>series lifecycle</em>: the
 * divergence alert counts series, so a version that stops being enabled must stop being published — a
 * stale series frozen at 1 would keep the alert firing after the operator fixed the registry.
 */
class RegistryVersionMetricsTest {

    private SubscriptionRepo repo;
    private SimpleMeterRegistry meters;
    private RegistryVersionMetrics metrics;

    @BeforeEach
    void setUp() {
        repo = mock(SubscriptionRepo.class);
        meters = new SimpleMeterRegistry();
        metrics = new RegistryVersionMetrics(repo, meters);
    }

    @Test
    void oneEnabledVersion_publishesOneConstantOneSeries() {
        when(repo.distinctEnabledRegistryVersions()).thenReturn(List.of("seed-0"));

        metrics.publish();

        assertThat(meters.get(RegistryVersionMetrics.METRIC)
                .tag("component", RegistryVersionMetrics.COMPONENT)
                .tag("version", "seed-0")
                .gauge().value()).isEqualTo(1.0);
        assertThat(seriesCount()).isEqualTo(1);
    }

    @Test
    void twoEnabledVersions_publishTwoSeries_soTheDivergenceAlertCanCountThem() {
        when(repo.distinctEnabledRegistryVersions()).thenReturn(List.of("seed-0", "reg-2026-07-28"));

        metrics.publish();

        assertThat(meters.find(RegistryVersionMetrics.METRIC).gauges())
                .extracting(g -> g.getId().getTag("version"))
                .containsExactlyInAnyOrder("seed-0", "reg-2026-07-28");
    }

    @Test
    void retiredVersion_seriesIsRemoved_notFrozenAtOne() {
        when(repo.distinctEnabledRegistryVersions()).thenReturn(List.of("seed-0", "reg-2026-07-28"));
        metrics.publish();

        when(repo.distinctEnabledRegistryVersions()).thenReturn(List.of("reg-2026-07-28"));
        metrics.publish();

        assertThat(seriesCount()).isEqualTo(1);
        assertThat(meters.find(RegistryVersionMetrics.METRIC).tag("version", "seed-0").gauge()).isNull();
    }

    @Test
    void noEnabledRows_publishesNothingRatherThanAZeroSeries() {
        when(repo.distinctEnabledRegistryVersions()).thenReturn(List.of());

        metrics.publish();

        assertThat(seriesCount()).isZero();
    }

    private int seriesCount() {
        return (int) meters.find(RegistryVersionMetrics.METRIC).gauges().stream().map(Gauge::getId).count();
    }
}
