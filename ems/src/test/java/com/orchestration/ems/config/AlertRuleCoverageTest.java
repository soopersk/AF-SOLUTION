package com.orchestration.ems.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Holds the shipped alert rules to the ems-design §10 alert column — and, more usefully, holds them to
 * the metric names this codebase actually registers.
 *
 * <p><b>Why a test and not a review.</b> A {@code PrometheusRule} fails silently in the one direction
 * that matters. A rule whose {@code expr} names {@code ems_dlq_dept} is accepted by Prometheus, evaluates
 * cleanly forever, and never fires — it is indistinguishable from a healthy system right up until the
 * incident it was written for. Nothing downstream catches that: {@code helm lint} does not know the metric
 * names, {@code promtool check rules} validates syntax rather than existence, and a human reading the diff
 * sees a plausible string. So the names are cross-checked here, against the {@code ems_*} literals in
 * {@code src/main/java}, on every build.
 *
 * <p>The template is parsed as <b>text</b>, deliberately. It is a Helm template, not YAML: the
 * {@code {{- if .Values.dispatch.enabled }}} guard around the outbox rule makes the document invalid YAML
 * until Helm has rendered it, and Helm is not available on every box this build runs on.
 */
class AlertRuleCoverageTest {

    /** Surefire runs with the module directory as its working directory. */
    private static final Path RULES = Path.of("deploy/helm/ems/templates/prometheusrule.yaml");
    private static final Path MAIN_SOURCES = Path.of("src/main/java");
    private static final Path USER_GUIDE = Path.of("../docs/ems-user-guide.md");

    /**
     * Every rule that must exist, and the group it must sit in. The group is the severity contract:
     * {@code ems.page} wakes someone up, {@code ems.warn} does not.
     */
    private static final Map<String, String> EXPECTED_RULES = new LinkedHashMap<>();
    static {
        EXPECTED_RULES.put("EmsDlqDepthNonZero", "ems.page");
        EXPECTED_RULES.put("EmsOutboxBacklogStale", "ems.page");
        EXPECTED_RULES.put("EmsConsumerLagSustained", "ems.page");
        EXPECTED_RULES.put("EmsRetentionHeadroomLow", "ems.page");
        EXPECTED_RULES.put("EmsLagProbeSilent", "ems.warn");
        EXPECTED_RULES.put("EmsDropRateAnomaly", "ems.warn");
        EXPECTED_RULES.put("EmsSubscriptionVerdictsDrop", "ems.warn");
        EXPECTED_RULES.put("EmsNormalizationMutations", "ems.warn");
        EXPECTED_RULES.put("EmsRegistryDivergence", "ems.warn");
        EXPECTED_RULES.put("EmsOverdueInflightRuns", "ems.warn");
        EXPECTED_RULES.put("EmsEndpointP95Regression", "ems.warn");
    }

    /**
     * Every §10 row whose "Alert" cell is non-empty, by the metric that row is about. This is the reverse
     * check: not "does every rule reference a real metric" but "does every metric the design says to alert
     * on actually have a rule". Dropping a rule is otherwise a silent, invisible reduction in coverage.
     */
    private static final Set<String> SECTION_10_ALERTED_METRICS = Set.of(
            "ems_events_dropped_total",
            "ems_subscription_verdicts_total",
            "ems_dlq_depth",
            "ems_outbox_pending_age_seconds",
            "ems_consumer_lag",
            "ems_consumer_retention_headroom_records",
            "ems_normalization_mutations_total",
            "ems_registry_version",
            "ems_overdue_inflight_runs");

    /**
     * The one metric an alert reads that this codebase does not name: Spring Boot's HTTP timer, published
     * as buckets for the three §10 endpoints by {@link MetricsConfig}. Derived from the timer name rather
     * than written out, so a Boot change to the meter name breaks this test rather than the alert.
     */
    private static final String HTTP_BUCKETS =
            MetricsConfig.HTTP_TIMER.replace('.', '_') + "_seconds_bucket";

    /** PromQL vocabulary — everything left over after these are removed is a metric name. */
    private static final Set<String> PROMQL_KEYWORDS = Set.of(
            "sum", "max", "min", "avg", "count", "topk", "bottomk", "rate", "irate", "increase",
            "absent", "absent_over_time", "histogram_quantile", "by", "without", "offset", "on",
            "ignoring", "group_left", "group_right", "and", "or", "unless");

    private static String template;
    private static List<Rule> rules;
    private static Set<String> registeredMetrics;

    @BeforeAll
    static void loadEverything() throws IOException {
        assertThat(RULES)
                .as("the PrometheusRule template must live at %s — if it moved, this test moves with it "
                        + "rather than passing vacuously", RULES)
                .exists();
        template = Files.readString(RULES);
        rules = parseRules(template);
        registeredMetrics = scanRegisteredMetricNames();
        assertThat(registeredMetrics)
                .as("no ems_* metric literals found under %s — the source scan is broken, and every "
                        + "name check below would be meaningless", MAIN_SOURCES)
                .isNotEmpty();
    }

    @Test
    void everyExpectedRuleExistsInItsExpectedGroup() {
        Map<String, String> actual = new LinkedHashMap<>();
        rules.forEach(rule -> actual.put(rule.name(), rule.group()));

        assertThat(actual)
                .as("the shipped rule set must match the §10 alert column exactly — an extra rule is "
                        + "unreviewed, a missing one is silent lost coverage")
                .containsExactlyInAnyOrderEntriesOf(EXPECTED_RULES);
    }

    @Test
    void everyRuleCarriesExprForSeverityAndARunbook() {
        for (Rule rule : rules) {
            assertThat(rule.expr()).as("%s has no expr", rule.name()).isNotBlank();
            assertThat(rule.duration())
                    .as("%s has no `for:` — an instantaneous rule pages on a single scrape blip",
                            rule.name())
                    .isNotBlank();
            assertThat(rule.summary())
                    .as("%s has no summary annotation — the responder sees only a rule name", rule.name())
                    .isNotBlank();
            assertThat(rule.runbook())
                    .as("%s has no runbook annotation", rule.name())
                    .isNotBlank();
            assertThat(rule.severity())
                    .as("%s sits in group %s, so its severity label must agree with it",
                            rule.name(), rule.group())
                    .isEqualTo(rule.group().substring("ems.".length()));
        }
    }

    /**
     * The typo check. Every {@code ems_*} name an alert evaluates must be a name the service registers,
     * because a rule against a name that does not exist is a rule that never fires.
     */
    @Test
    void everyMetricAnAlertReadsIsOneTheCodebaseRegisters() {
        for (Rule rule : rules) {
            for (String metric : metricNamesIn(rule.expr())) {
                if (metric.equals(HTTP_BUCKETS)) {
                    continue; // Boot's, not ours — pinned by httpBucketMetricMatchesTheConfiguredTimer()
                }
                assertThat(registeredMetrics)
                        .as("%s reads '%s', which no class under %s registers — this alert would evaluate "
                                + "cleanly and never fire", rule.name(), metric, MAIN_SOURCES)
                        .contains(metric);
            }
        }
    }

    @Test
    void everySection10MetricWithAnAlertColumnHasARule() {
        Set<String> covered = new LinkedHashSet<>();
        rules.forEach(rule -> covered.addAll(metricNamesIn(rule.expr())));

        assertThat(covered)
                .as("every §10 row with a non-empty Alert cell must be covered by a shipped rule")
                .containsAll(new TreeSet<>(SECTION_10_ALERTED_METRICS));
        assertThat(covered)
                .as("the §10 endpoint-latency row is covered by the HTTP bucket metric")
                .contains(HTTP_BUCKETS);
    }

    /**
     * A runbook link that goes nowhere is worse than none: it costs the responder a detour at the point
     * they are least able to afford one.
     */
    @Test
    void everyRunbookAnnotationNamesASectionThatExists() throws IOException {
        assertThat(USER_GUIDE).as("the operations runbooks must be at %s", USER_GUIDE).exists();
        String guide = Files.readString(USER_GUIDE);

        for (Rule rule : rules) {
            Matcher section = Pattern.compile("§(\\d+\\.\\d+)").matcher(rule.runbook());
            assertThat(section.find())
                    .as("%s's runbook annotation ('%s') must name a §N.N section", rule.name(),
                            rule.runbook())
                    .isTrue();
            assertThat(guide)
                    .as("%s points at §%s, which has no heading in %s", rule.name(), section.group(1),
                            USER_GUIDE)
                    .contains("### " + section.group(1) + " ");
        }
    }

    /**
     * The p95 rule can only quantile URIs that publish buckets, and {@link MetricsConfig} publishes them
     * for exactly three. Quantiling a fourth returns an empty vector — another silently-never-fires shape.
     */
    @Test
    void theP95RuleOnlyQuantilesUrisThatPublishBuckets() {
        Rule p95 = ruleNamed("EmsEndpointP95Regression");
        Matcher selector = Pattern.compile("uri=~\"([^\"]+)\"").matcher(p95.expr());

        assertThat(selector.find()).as("the p95 rule must scope itself to the timed URIs").isTrue();
        assertThat(selector.group(1).split("\\|"))
                .as("every URI in the p95 alert must be one MetricsConfig gave histogram buckets")
                .containsExactlyInAnyOrderElementsOf(MetricsConfig.TIMED_URIS);
    }

    /**
     * Pins the two guards that make the chart safe to install before cutover. Both are one deleted line
     * away from a permanently-firing alert, which is how alerting gets switched off wholesale.
     */
    @Test
    void theRulesAreGuardedByTheirValuesToggles() {
        assertThat(template)
                .as("the whole PrometheusRule must be optional")
                .contains("{{- if .Values.metrics.prometheusRule.enabled }}");
        assertThat(template)
                .as("EmsOutboxBacklogStale must be gated on the §11 dispatch toggle: while dispatch is "
                        + "off, outbox rows are deliberately never drained and the age climbs forever")
                .contains("{{- if .Values.dispatch.enabled }}");
        assertThat(template)
                .as("EmsLagProbeSilent must be gated for environments with no reachable broker")
                .contains("{{- if .Values.alerts.expectConsumerLag }}");
    }

    // --- parsing -----------------------------------------------------------------------------------

    private record Rule(String group, String name, String expr, String duration, String severity,
            String summary, String runbook) { }

    private static List<Rule> parseRules(String text) {
        List<Rule> parsed = new ArrayList<>();
        String group = null;
        // The group a rule belongs to is the one in force when its `- alert:` line appeared, not the one
        // in force when it is flushed — the last rule of a group is flushed after the next group header.
        String ruleGroup = null;
        String name = null;
        String expr = null;
        String duration = null;
        String severity = null;
        String summary = null;
        String runbook = null;

        for (String raw : text.lines().toList()) {
            String line = raw.trim();
            if (line.startsWith("{{") || line.startsWith("#")) {
                continue; // Helm control flow and comments are not part of the rule document
            }
            if (line.startsWith("- name: ems.")) {
                group = line.substring("- name: ".length()).trim();
            } else if (line.startsWith("- alert:")) {
                if (name != null) {
                    parsed.add(new Rule(ruleGroup, name, expr, duration, severity, summary, runbook));
                }
                name = line.substring("- alert:".length()).trim();
                ruleGroup = group;
                expr = duration = severity = summary = runbook = null;
            } else if (line.startsWith("expr:")) {
                expr = unquote(line.substring("expr:".length()));
            } else if (line.startsWith("for:")) {
                duration = line.substring("for:".length()).trim();
            } else if (line.startsWith("severity:")) {
                severity = unquote(line.substring("severity:".length()));
            } else if (line.startsWith("summary:")) {
                summary = unquote(line.substring("summary:".length()));
            } else if (line.startsWith("runbook:")) {
                runbook = unquote(line.substring("runbook:".length()));
            }
        }
        if (name != null) {
            parsed.add(new Rule(ruleGroup, name, expr, duration, severity, summary, runbook));
        }
        return parsed;
    }

    private static String unquote(String value) {
        String trimmed = value.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("'") && trimmed.endsWith("'")) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    /**
     * Reduces a PromQL expression to the metric names it reads: Helm placeholders, range selectors, label
     * selectors, {@code by (...)} groupings and {@code offset} durations are stripped, and what survives
     * that is not PromQL vocabulary is a metric.
     */
    private static Set<String> metricNamesIn(String expr) {
        String stripped = expr
                .replaceAll("\\{\\{.*?\\}\\}", " ")      // Helm placeholders (thresholds)
                .replaceAll("\\[[^\\]]*\\]", " ")        // range selectors
                .replaceAll("\\{[^}]*\\}", " ")          // label selectors
                .replaceAll("\\b(by|without)\\s*\\([^)]*\\)", " ")
                .replaceAll("\\boffset\\s+\\d+[a-z]+", " ");

        Set<String> names = new LinkedHashSet<>();
        Matcher token = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*").matcher(stripped);
        while (token.find()) {
            String candidate = token.group();
            if (!PROMQL_KEYWORDS.contains(candidate)) {
                names.add(candidate);
            }
        }
        return names;
    }

    /** Every {@code "ems_*"} string literal in main sources — the names the service can actually publish. */
    private static Set<String> scanRegisteredMetricNames() throws IOException {
        Pattern literal = Pattern.compile("\"(ems_[a-z0-9_]+)\"");
        Set<String> found = new TreeSet<>();
        try (Stream<Path> sources = Files.walk(MAIN_SOURCES)) {
            sources.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                try {
                    Matcher match = literal.matcher(Files.readString(path));
                    while (match.find()) {
                        found.add(match.group(1));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        return found;
    }

    private static Rule ruleNamed(String name) {
        return rules.stream().filter(rule -> rule.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no rule named " + name));
    }
}
