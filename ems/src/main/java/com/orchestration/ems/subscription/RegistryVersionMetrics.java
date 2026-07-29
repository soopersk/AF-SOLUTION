package com.orchestration.ems.subscription;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;

/**
 * Publishes {@code ems_registry_version{component,version}} (ems-design §10) — the info metric that says
 * which subscription registry version each pod is actually evaluating.
 *
 * <p><b>What the alert reads.</b> One constant-{@code 1} series per distinct {@code registry_version}
 * across the <em>enabled</em> rows. Steady state is exactly one series; two or more means a registry
 * render landed only partially, or two versions are enabled at once — which is a real routing hazard,
 * because the {@code routing_decision.registry_version} audit column then stops identifying a single
 * ruleset. {@code count(count by (version) (ems_registry_version)) > 1} is the divergence alert.
 *
 * <p><b>Naming.</b> §10 originally called this {@code ems_registry_version_info}; the Prometheus client
 * reserves {@code _info} as the Info-metric suffix and strips it from a gauge, so the meter carries the
 * unsuffixed name and {@code MetricNamingTest} pins that.
 *
 * <p><b>Why a scheduled read rather than a hook on the load path.</b> The value must be right on a pod
 * that is not consuming (an API-only or shadow pod, where {@link SubscriptionService}'s Caffeine cache
 * may never be touched), and it must converge after a hand edit made through another pod. Polling on the
 * shared reconciliation interval gets both for one cheap {@code SELECT DISTINCT}. {@link MultiGauge}
 * owns the series lifecycle, so a retired version's series is removed rather than frozen at 1 forever.
 */
@Component
public class RegistryVersionMetrics {

    private static final Logger log = LoggerFactory.getLogger(RegistryVersionMetrics.class);

    /** §10 metric name — deliberately without the reserved {@code _info} suffix (see class javadoc). */
    static final String METRIC = "ems_registry_version";

    /** The component whose registry version this is; Phase B adds more without changing this class. */
    static final String COMPONENT = "ems-subscriptions";

    private final SubscriptionRepo repo;
    private final MultiGauge versions;

    public RegistryVersionMetrics(SubscriptionRepo repo, MeterRegistry meterRegistry) {
        this.repo = repo;
        this.versions = MultiGauge.builder(METRIC)
                .description("Subscription registry version(s) currently enabled; value is always 1")
                .register(meterRegistry);
    }

    /**
     * Re-publish the series set from the table. Shares {@code ems.recon.interval-ms} with the
     * reconciliation sweep: both are out-of-band pollers whose freshness requirement is the same order
     * of magnitude, and one knob is easier to reason about in an incident than two.
     */
    @Scheduled(fixedDelayString = "${ems.recon.interval-ms:60000}")
    public void publish() {
        List<String> enabled = repo.distinctEnabledRegistryVersions();
        versions.register(enabled.stream()
                .map(version -> MultiGauge.Row.of(Tags.of("component", COMPONENT, "version", version), 1))
                .toList(), true);
        if (enabled.size() > 1) {
            log.warn("Subscription registry divergence: {} versions enabled at once {}",
                    enabled.size(), enabled);
        }
    }
}
