package com.orchestration.ems.config;

import java.time.Duration;
import java.util.Set;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

/**
 * Turns on latency histograms for the three §10 endpoints — and only those (ems-design §10, "per-endpoint
 * latency histograms (`/event`, `/run/status`, `/gate/groups`) — warn on p95 regression").
 *
 * <p><b>Why a filter and not {@code management.metrics.distribution.*} properties.</b> A histogram is not
 * free: each URI gains one series per bucket, and Boot's property form is either global or a per-name
 * prefix — neither can say "these three URIs of {@code http.server.requests}". The three endpoints here
 * are the ones with a stated §12 latency budget (p95 &lt; 50 ms for the §4.3 query and {@code /run/status});
 * every other URI keeps the default count/sum timer, so the actuator, {@code /token}, the admin routes and
 * the decision-ingest path add no cardinality.
 *
 * <p><b>Why buckets and not client-side percentiles.</b> {@code percentiles(0.95)} would publish a
 * pod-local p95 that cannot be aggregated across pods — averaging pre-computed percentiles is arithmetic
 * nonsense. Publishing buckets lets Prometheus compute a fleet-wide
 * {@code histogram_quantile(0.95, ...)}, which is what the alert expression actually evaluates.
 */
@Configuration
public class MetricsConfig {

    /** The Boot timer every HTTP request records into. */
    static final String HTTP_TIMER = "http.server.requests";

    /** The §10 endpoints. Matched against the {@code uri} tag, which is the route template, not the path. */
    static final Set<String> TIMED_URIS = Set.of("/event", "/run/status", "/gate/groups");

    /**
     * Explicit SLO boundaries straddling the §12 budget (50 ms), so {@code histogram_quantile} has real
     * resolution where the decision is made instead of interpolating across a wide default bucket.
     */
    private static final Duration[] SLO_BUCKETS = {
            Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(25),
            Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
            Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(5) };

    /**
     * @return a filter that adds histogram buckets to {@value #HTTP_TIMER} for {@link #TIMED_URIS},
     *         leaving every other meter's distribution config exactly as it was
     */
    @Bean
    public MeterFilter emsEndpointLatencyHistograms() {
        return new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                if (!HTTP_TIMER.equals(id.getName())) {
                    return config;
                }
                String uri = id.getTag("uri");
                if (uri == null || !TIMED_URIS.contains(uri)) {
                    return config;
                }
                return DistributionStatisticConfig.builder()
                        .percentilesHistogram(true)
                        .serviceLevelObjectives(sloNanos())
                        .build()
                        .merge(config);
            }
        };
    }

    /** SLO boundaries in the Timer's base unit (nanoseconds). */
    private static double[] sloNanos() {
        double[] nanos = new double[SLO_BUCKETS.length];
        for (int i = 0; i < SLO_BUCKETS.length; i++) {
            nanos[i] = SLO_BUCKETS[i].toNanos();
        }
        return nanos;
    }
}
