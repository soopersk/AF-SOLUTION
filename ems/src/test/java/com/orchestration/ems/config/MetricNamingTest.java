package com.orchestration.ems.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Pins the wire names of the ems-design §10 metric table against the registry that actually
 * publishes them (Amendment A11: the transport is Prometheus <em>pull</em>).
 *
 * <p><b>Why this test exists.</b> Micrometer meter names are not Prometheus exposition names. The
 * Prometheus naming convention appends a {@code _total} suffix to counters and a base-unit suffix to
 * timers/gauges, so a meter <em>already</em> named {@code ems_events_dropped_total} can surface as
 * {@code ems_events_dropped_total_total} — and every alert expression in
 * {@code deploy/helm/ems/templates/prometheusrule.yaml} is written against the §10 name verbatim. An
 * alert that never fires because of a suffix is worse than no alert, so the exposition name is
 * asserted here, on the real {@link PrometheusMeterRegistry}, rather than assumed.
 *
 * <p>The meters are registered with the same builders the production code uses, so this test tracks
 * production naming: if a Micrometer upgrade changes the convention, this fails before the chart
 * ships against a name that no longer exists.
 */
class MetricNamingTest {

    /**
     * Every §10 counter, by the name the alert rules and dashboards use. The four already in the
     * build plus the two Batch B adds.
     */
    private static final List<String> COUNTERS = List.of(
            "ems_events_dropped_total",
            "ems_context_fetch_total",
            "ems_normalization_mutations_total",
            "ems_events_consumed_total",
            "ems_subscription_verdicts_total");

    /**
     * Every §10 gauge: the outbox age (Batch C) plus the recon-owned backstop gauges (Batch C/D).
     *
     * <p>Note {@code ems_registry_version} — §10 names it {@code ems_registry_version_info}, but the
     * Prometheus client reserves {@code _info} as the Info-metric suffix and <b>strips</b> it from a
     * gauge, so that meter can never be scraped under the {@code _info} name.
     * {@link #infoSuffixIsStrippedSoTheMeterDoesNotCarryIt()} pins that behaviour.
     */
    private static final List<String> GAUGES = List.of(
            "ems_registry_version",
            "ems_consumer_lag",
            "ems_dlq_depth",
            "ems_overdue_inflight_runs");

    /** The one gauge carrying an explicit {@code baseUnit} — the base-unit doubling candidate. */
    private static final String SECONDS_GAUGE = "ems_outbox_pending_age_seconds";

    private PrometheusMeterRegistry registry;
    private String scrape;

    @BeforeEach
    void registerEveryDesignedMeter() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        for (String counter : COUNTERS) {
            registry.counter(counter, "probe", "naming").increment();
        }
        for (String gauge : GAUGES) {
            Gauge.builder(gauge, new AtomicInteger(1), AtomicInteger::doubleValue)
                    .tag("probe", "naming")
                    .register(registry);
        }
        Gauge.builder(SECONDS_GAUGE, new AtomicInteger(1), AtomicInteger::doubleValue)
                .baseUnit("seconds")
                .tag("probe", "naming")
                .register(registry);
        scrape = registry.scrape();
    }

    @Test
    void everyDesignedCounterIsScrapedUnderItsExactName() {
        for (String counter : COUNTERS) {
            assertThat(scrape)
                    .as("§10 counter '%s' must appear in the scrape verbatim — alert rules use this name",
                            counter)
                    .contains(counter + "{");
        }
    }

    @Test
    void everyDesignedGaugeIsScrapedUnderItsExactName() {
        for (String gauge : GAUGES) {
            assertThat(scrape)
                    .as("§10 gauge '%s' must appear in the scrape verbatim", gauge)
                    .contains(gauge + "{");
        }
        assertThat(scrape)
                .as("a baseUnit(\"seconds\") gauge already named '%s' must not gain a second suffix",
                        SECONDS_GAUGE)
                .contains(SECONDS_GAUGE + "{");
    }

    /**
     * The reason the registry-version meter is not named {@code ems_registry_version_info}: registering
     * it under that name yields a sample called {@code ems_registry_version}, so a
     * {@code PrometheusRule} written against the §10 spelling would match nothing. Named here so the
     * constraint is discoverable from the code rather than rediscovered from a silent alert.
     */
    @Test
    void infoSuffixIsStrippedSoTheMeterDoesNotCarryIt() {
        PrometheusMeterRegistry probe = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        Gauge.builder("ems_registry_version_info", new AtomicInteger(1), AtomicInteger::doubleValue)
                .tag("probe", "naming")
                .register(probe);

        assertThat(probe.scrape())
                .as("the Prometheus client strips the reserved _info suffix from a gauge")
                .contains("ems_registry_version{")
                .doesNotContain("ems_registry_version_info");
    }

    @Test
    void noSuffixIsAppliedTwice() {
        assertThat(scrape).doesNotContain("_total_total");
        assertThat(scrape).doesNotContain("_seconds_seconds");
    }
}
