#!/usr/bin/env bash
#
# Chart gate for the EMS Helm chart. Run by CI (.github/workflows/ems-ci.yml, `helm` job); runnable by
# hand wherever `helm` is on PATH.
#
# It exists because `helm lint` proves almost nothing that matters here. The failures this chart can
# actually ship are: a cutover toggle flipped by accident, an observability object silently dropped from
# a values override, a secret arriving as an environment variable, and a "required" guard that stopped
# guarding. Each of those is asserted below with its own message and its own non-zero exit.
#
# NOTE: this script has never been executed on the authoring workstation — helm is not installed there.
# Its contract is that CI runs it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="${SCRIPT_DIR}/ems"
OUT="$(mktemp -d)"
trap 'rm -rf "${OUT}"' EXIT

RELEASE=ems

# The minimum an environment must supply. Every one of these is `required` in the chart precisely
# because its in-code fallback points at localhost, and a pod that boots healthy against localhost is
# the hardest failure in this system to notice.
REQUIRED_VALUES=(
  --set kafka.bootstrapServers=kafka.example:9093
  --set consumer.topics=edf.events
  --set edf.baseUrl=https://edf.example
  --set airflow.baseUrl=https://airflow.example/api/v1
  --set auth.issuerUri=https://login.microsoftonline.com/tenant/v2.0
  --set auth.groups.dispatcher=00000000-0000-0000-0000-000000000001
  --set auth.groups.admin=00000000-0000-0000-0000-000000000002
  --set auth.groups.ci=00000000-0000-0000-0000-000000000003
)

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

pass() {
  echo "  ok: $*"
}

echo "==> helm lint"
helm lint "${CHART}" "${REQUIRED_VALUES[@]}" || fail "helm lint rejected the chart"

echo "==> helm template (defaults)"
helm template "${RELEASE}" "${CHART}" "${REQUIRED_VALUES[@]}" > "${OUT}/default.yaml" \
  || fail "the chart does not render with only the required values supplied"

# --- 1. the §11 cutover defaults ----------------------------------------------------------------
# These two are the switch. They must be OFF in a default render, and nothing but an explicit values
# override may turn them on.
echo "==> cutover defaults"
grep -q 'value: "shadow"' "${OUT}/default.yaml" \
  || fail "the rendered Deployment does not set SPRING_PROFILES_ACTIVE=shadow — the §11 default flipped"
pass "SPRING_PROFILES_ACTIVE=shadow"

grep -A1 'name: EMS_DISPATCH_ENABLED' "${OUT}/default.yaml" | grep -q 'value: "false"' \
  || fail "the rendered Deployment does not set EMS_DISPATCH_ENABLED=false — dispatch would be live"
pass "EMS_DISPATCH_ENABLED=false"

# --- 2. the ops objects are all there -------------------------------------------------------------
# A values override that quietly disables one of these leaves a service with no alerts and no scrape,
# which looks exactly like a healthy service.
echo "==> operational objects"
for kind in PodDisruptionBudget PrometheusRule ServiceMonitor; do
  grep -q "^kind: ${kind}$" "${OUT}/default.yaml" || fail "${kind} is missing from the default render"
  pass "${kind} present"
done
grep -q 'grafana_dashboard: "1"' "${OUT}/default.yaml" \
  || fail "the Grafana dashboard ConfigMap is missing (or lost its sidecar label, which is the same thing)"
pass "dashboard ConfigMap present and labelled"

grep -q 'vault.hashicorp.com/agent-inject: "true"' "${OUT}/default.yaml" \
  || fail "Vault injection is not enabled — 'no secrets in the manifests' would then be trivially true"
pass "Vault agent injection enabled"

# --- 3. no secret arrives as configuration --------------------------------------------------------
# Secrets are files written by the Vault agent. Anything secret-shaped appearing as a ConfigMap key or
# as a container env var is a leak into `kubectl describe pod` and into every crash dump. Scoped to
# those two surfaces deliberately: the Vault ANNOTATIONS legitimately name secret paths, and grepping
# the whole document would force us to whitelist exactly the thing under test.
echo "==> secret hygiene"
SECRET_PATTERN='(PASSWORD|SECRET|TOKEN|API_?KEY|CREDENTIAL|PRIVATE_?KEY)'

# --output-dir rather than --show-only: the latter matches on the chart-prefixed manifest path in older
# helm releases, so the same invocation succeeds on one CI image and errors on another.
helm template "${RELEASE}" "${CHART}" "${REQUIRED_VALUES[@]}" --output-dir "${OUT}/rendered" > /dev/null \
  || fail "could not render the chart to ${OUT}/rendered"
CONFIGMAP="${OUT}/rendered/ems/templates/configmap.yaml"
DEPLOYMENT="${OUT}/rendered/ems/templates/deployment.yaml"
[ -f "${CONFIGMAP}" ] || fail "no ConfigMap was rendered — the non-secret wiring is missing entirely"
[ -f "${DEPLOYMENT}" ] || fail "no Deployment was rendered"

if grep -Ei "^ *[A-Za-z0-9_]*${SECRET_PATTERN}[A-Za-z0-9_]*:" "${CONFIGMAP}"; then
  fail "a secret-shaped key is present in the ConfigMap (see the match above)"
fi
pass "ConfigMap carries no secret-shaped keys"

if grep -Ei "^ *- name: [A-Za-z0-9_]*${SECRET_PATTERN}[A-Za-z0-9_]*$" "${DEPLOYMENT}"; then
  fail "a secret-shaped environment variable is present on the Deployment (see the match above)"
fi
pass "Deployment carries no secret-shaped env vars"

# --- 4. the required-value guards still guard -----------------------------------------------------
# A `required` that has been softened to a default is invisible until production. Prove each one still
# refuses to render.
echo "==> required-value guards"
guard_holds() {
  local omitted="$1"; shift
  if helm template "${RELEASE}" "${CHART}" "$@" > /dev/null 2>&1; then
    fail "the chart rendered with ${omitted} unset — that guard no longer guards"
  fi
  pass "${omitted} is still required"
}
guard_holds "kafka.bootstrapServers" "${REQUIRED_VALUES[@]}" --set kafka.bootstrapServers=""
guard_holds "consumer.topics"        "${REQUIRED_VALUES[@]}" --set consumer.topics=""
guard_holds "auth.issuerUri"         "${REQUIRED_VALUES[@]}" --set auth.issuerUri=""

# --- 5. the cutover flip is a values change, not a rebuild (§10) -----------------------------------
echo "==> cutover render"
helm template "${RELEASE}" "${CHART}" "${REQUIRED_VALUES[@]}" \
  --set dispatch.enabled=true --set springProfile=live > "${OUT}/live.yaml" \
  || fail "the chart does not render in the live configuration — the cutover would need a chart change"
helm template "${RELEASE}" "${CHART}" "${REQUIRED_VALUES[@]}" \
  --set dispatch.enabled=true --set springProfile=live --output-dir "${OUT}/rendered-live" > /dev/null \
  || fail "could not render the live configuration to ${OUT}/rendered-live"

grep -q 'value: "live"' "${OUT}/live.yaml" || fail "springProfile=live did not reach the Deployment"
grep -A1 'name: EMS_DISPATCH_ENABLED' "${OUT}/live.yaml" | grep -q 'value: "true"' \
  || fail "dispatch.enabled=true did not reach the Deployment"
pass "live render carries profile=live and dispatch=true"

# The outbox page rule is gated on that same toggle: off in shadow (rows are deliberately never
# drained, so the age gauge climbs forever), armed in live.
# Scoped to the rendered PrometheusRule, not the whole document: the dashboard JSON mentions the rule
# by name in a panel description, and a whole-render grep would match that instead of the rule.
RULES_SHADOW="${OUT}/rendered/ems/templates/prometheusrule.yaml"
RULES_LIVE="${OUT}/rendered-live/ems/templates/prometheusrule.yaml"
if grep -q 'alert: EmsOutboxBacklogStale' "${RULES_SHADOW}"; then
  fail "EmsOutboxBacklogStale is present in the shadow render — it would page continuously"
fi
pass "EmsOutboxBacklogStale absent in shadow"
grep -q 'alert: EmsOutboxBacklogStale' "${RULES_LIVE}" \
  || fail "EmsOutboxBacklogStale is missing from the live render — the outbox would go unwatched after cutover"
pass "EmsOutboxBacklogStale armed in live"

echo
echo "chart OK"
