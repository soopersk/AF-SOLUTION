package com.orchestration.ems.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Proves {@link MetricsConfig}'s {@code MeterFilter} does exactly two things: give the three §10
 * endpoints real Prometheus buckets (so {@code histogram_quantile(0.95, ...)} is computable fleet-wide),
 * and leave every other URI alone (so the histogram's cardinality cost is bounded and deliberate).
 *
 * <p>Asserted against a real {@link PrometheusMeterRegistry} scrape rather than against
 * {@code DistributionStatisticConfig}, because the thing that must be true is on the wire — an alert
 * expression reads {@code _bucket} series, not a Micrometer config object.
 */
class MetricsConfigTest {

    private PrometheusMeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().meterFilter(new MetricsConfig().emsEndpointLatencyHistograms());
    }

    @Test
    void theThreeDesignatedEndpointsPublishBuckets() {
        for (String uri : MetricsConfig.TIMED_URIS) {
            record(uri, Duration.ofMillis(30));
        }

        String scrape = registry.scrape();
        for (String uri : MetricsConfig.TIMED_URIS) {
            assertThat(bucketLines(scrape, uri))
                    .as("%s must publish histogram buckets — §10 alerts on its p95", uri)
                    .isNotEmpty();
        }
    }

    @Test
    void everyOtherEndpointKeepsThePlainTimer_noBucketCardinality() {
        record("/context", Duration.ofMillis(30));
        record("/actuator/health", Duration.ofMillis(30));

        String scrape = registry.scrape();
        assertThat(bucketLines(scrape, "/context")).isEmpty();
        assertThat(bucketLines(scrape, "/actuator/health")).isEmpty();
        // ...but they are still timed: count and sum remain (tags are exposed in alphabetical order)
        assertThat(scrape).contains("http_server_requests_seconds_count{status=\"200\",uri=\"/context\"}");
    }

    @Test
    void bucketsStraddleTheFiftyMillisecondBudget() {
        record("/run/status", Duration.ofMillis(30));

        assertThat(bucketLines(registry.scrape(), "/run/status"))
                .as("§12 budget is p95 < 50 ms, so a boundary must sit exactly there")
                .anyMatch(line -> line.contains("le=\"0.05\""));
    }

    private void record(String uri, Duration took) {
        Timer.builder(MetricsConfig.HTTP_TIMER)
                .tag("uri", uri)
                .tag("status", "200")
                .register(registry)
                .record(took);
    }

    private static java.util.List<String> bucketLines(String scrape, String uri) {
        return scrape.lines()
                .filter(line -> line.startsWith("http_server_requests_seconds_bucket"))
                .filter(line -> line.contains("uri=\"" + uri + "\""))
                .toList();
    }
}
