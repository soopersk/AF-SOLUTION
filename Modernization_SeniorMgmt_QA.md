# Orchestration Modernization — Senior Management Follow-Up

**Answer pack** · companion to `Orchestrator_Modernization_Strategy.docx` and
`Modernization_Roadmap_Executive_v4.pptx`
Classification: Internal — Senior Management · Prepared 2026-08-07

---

## Question 1

> *"I would like to see clearer success measures, phased deliverables, effort estimates, key
> dependencies and the business outcomes expected from each phase. Currently we are a team of 8
> people."*

---

## How to read this answer

Three kinds of number appear below, and they are not equally firm. They are labelled throughout.

| Label | Meaning | Where it comes from |
|---|---|---|
| **Measured** | Read from production code or a running test suite | `system_discovery.md`, `old-ems/`, `old-orchestration/`, the 207-test EMS suite |
| **Designed** | A target with a named verification method and an acceptance gate | `ems-design.md` §12, trigger plan §10, framework plan §12 |
| **Modelled** | An estimate built bottom-up from a work-unit inventory, with stated assumptions | This document, §1 and §3 |

Every effort figure is **Modelled**. Every success measure is **Measured** or **Designed**. That
distinction is the honest answer to "how confident are you" — and §7 lists the six assumptions that,
if wrong, move the numbers.

---

## 1. What "a team of 8" actually buys

The single most important input to this plan is not the headcount — it is the **net capacity after
business-as-usual**. Slide 3 of the deck commits to BAU continuing uninterrupted: month-end and
quarter-end close, new calculator onboarding, production support, and regulatory change already
committed for the period. That commitment has to be paid for out of the same 8 people.

### 1.1 Capacity model

| Line | % of gross | Person-weeks per quarter |
|---|---:|---:|
| Gross capacity — 8 people × 13 weeks | 100% | 104 |
| *less* Leave and public holidays | 12% | −12.5 |
| *less* Ceremonies, line management, admin, recruitment | 8% | −8.3 |
| *less* Production support, incident response, month-end / quarter-end close | 20% | −20.8 |
| *less* Committed regulatory change + new calculator onboarding (BAU delivery) | 20% | −20.8 |
| **Net capacity available to modernization** | **~40%** | **~41** |

**Planning assumption: ~40 person-weeks per quarter, equal to ~3.1 FTE of an 8-person team.**
Sensitivity band: 38–45 pw depending on the actual BAU draw.

This is the number to challenge first. If the real BAU draw is 30% rather than 40%, the plan gains
roughly 10 person-weeks a quarter and the Q2 2027 pinch in §3 disappears. If it is 50%, the program
extends by approximately one quarter. **Every schedule statement in this pack is a function of this
one line**, which is why it is stated first rather than buried.

### 1.2 Two constraints that headcount cannot relieve

1. **Elapsed-time gates.** The shadow-mode windows are calendar-bound, not effort-bound. Phase 1
   requires days-to-weeks of production shadow-consume plus a 2-week observation window
   (`ems-design.md` §11 stages 1 and 3). Phase 2 requires a zero-mismatch shadow window **spanning a
   month-end** (trigger plan §10, Phase B). Adding people does not shorten these. They are also the
   controls that make the deck's "zero disruption" claim true, so they are not negotiable.
2. **Peak concurrency is two workstreams, never three** (slide 3 speaker notes). The event backbone
   closes before the framework migration waves begin. This is a design property of the sequencing,
   and it is what allows a team of 8 to run the program at all.

---

## 2. Phase-by-phase

### Phase 1 — FOUNDATION: Event backbone (Q3 2026)

**Delivers a fast, resilient event service. Everything downstream depends on this landing first.**

#### 2.1.1 Deliverables and evidence gates

| # | Deliverable | Evidence that it worked | Status |
|---|---|---|---|
| 1.1 | Indexed, generated-column event/context store with managed retention | Before/after `EXPLAIN (ANALYZE, BUFFERS)`: seq-scan + hash-join → index nested-loop | **Built** (Flyway V1–V5) |
| 1.2 | Kafka ingestion with poison-only DLQ, retry classification, audited replay | Poison drill: DLQ < 1 min, partition not stalled, `POST /admin/replay` heals end-to-end | **Built** |
| 1.3 | Transactional outbox + dispatcher (Airflow-outage-proof triggering) | Kill-Airflow drill: zero lost triggers, backlog fully drains on recovery | **Built** |
| 1.4 | Query API, byte-compatible with today (existing sensors untouched) | Contract suite vs recorded current responses; 200/404 semantics on every param combination | **Built** |
| 1.5 | `GET /run/status` — the hard blocker for the Phase 3 framework | Contract suite green against the framework's expectations | **Built** |
| 1.6 | Reconciliation sweep, 11 Prometheus alert rules, Grafana dashboard, Helm chart | `check.sh` 16 assertions green; alert ↔ metric ↔ runbook coverage test | **Built** |
| 1.7 | **Performance gate at 10M-row scale** | Canonical query p95 **< 50 ms**; `/run/status` p95 < 50 ms | **Harness built, never executed** |
| 1.8 | **`seed-0` routing translation sign-off** | Human sign-off on the FORWARD interpretation; per-environment row deltas collected | **Open** |
| 1.9 | **Full CI run on build infrastructure** | 99 integration tests execute for the first time (they auto-skip locally — no Docker) | **Open** |
| 1.10 | **Production shadow-consume + parity + 13-month backfill** | Conf byte-parity zero diffs; drop parity explained-or-zero | **Open** |
| 1.11 | **Cutover flip + rehearsed rollback + acceptance sign-off** | Rollback drill in staging: route back, gap reprocess, no loss, no double runs | **Open** |

Deliverables 1.1–1.6 are code-complete and green: `mvn verify` reports **207 unit tests passing, 99
integration tests skipping** for want of Docker on the development machine. That is a real
de-risking of the phase, and it is why the decision in front of management is *approval to run the
production shadow*, not funding for a blank sheet.

#### 2.1.2 Success measures

| Measure | Baseline (today) | Target | How verified | When |
|---|---|---|---|---|
| Completion-check query latency (p95) | 5+ min — **Measured**, `ems-design.md` §1 | < 50 ms | Perf gate @ 10M events + 1M contexts; `EXPLAIN` plan assertion | Gate 1.7 |
| Sensor round-trip p95 @ 100 concurrent pollers | Not instrumented | < 1 s | Load harness in CI | Gate 1.7 |
| Conf byte-parity vs the current service | n/a | **Zero diffs** on mirrored live traffic | Shadow parity job | Gate 1.10 |
| Triggers lost during an Airflow outage | Unbounded (no outbox) | **Zero**; backlog drains | Kill-Airflow drill | Gate 1.3 |
| Time for a poison message to leave the pipeline | Never — it retries 9× and stalls | < 1 min to DLQ | Poison drill | Gate 1.2 |
| Connection-pool exhaustion events (HikariCP cap 40) | Recurring at month-end | 0 | Prometheus, 1 month-end post-cutover | Cutover + 1 month |
| False-negative completions (sensor timeout, run actually finished) | Recurring | 0 | Incident record, categorized | Cutover + 1 quarter |

#### 2.1.3 Effort — residual only

| Work package | pw |
|---|---:|
| Execute the performance gate (harness exists; needs a Docker-capable runner) | 1.0 |
| `seed-0` translation sign-off — the FORWARD half is an interpretation, not a mechanical port | 1.5 |
| First full CI run of the 99 integration tests, and fixing what it surfaces | 2.0 |
| Environment provisioning: Azure PG 16, consumer group, Vault, Istio, AKS, Prometheus scrape | 3.0 |
| Close remaining external open items (EDF contract, context immutability, volumes, retention) | 2.0 |
| Shadow-consume rehearsal, parity jobs, 13-month historical backfill | 4.0 |
| Cutover flip, rollback drill, acceptance sign-off pack | 3.0 |
| Retention / archival DAG | 2.0 |
| Fail-closed auth default, secrets rotation, cross-pod dispatcher backoff | 1.5 |
| Security review, architecture review, change advisory board | 2.0 |
| **Subtotal** | **22.0** |
| Contingency @ 25% | 5.5 |
| **Phase 1 residual — Modelled** | **~28 pw (band 26–32)** |

*For context: the work already complete represents an estimated 55–70 pw equivalent — roughly one
and a half quarters of the team's net modernization capacity, already banked and green in test.
**Caution:** that figure describes value delivered, not a velocity to extrapolate from. The
estimates in this pack assume normal team velocity.*

#### 2.1.4 Key dependencies

| Dependency | Owner | Needed by | Risk if late |
|---|---|---|---|
| Azure PostgreSQL 16 provisioned, HA/DR posture confirmed | Infrastructure | Shadow start | Blocks the phase entirely — it is the one hard dependency |
| Docker-capable CI runner | Build engineering | Gates 1.7, 1.9 | 99 integration tests and the perf gate stay unexecuted; **the phase cannot be signed off** |
| EDF Context REST API contract, auth, rate limits, immutability guarantee | EDF team (external) | Shadow start | Context resolution and the 24h cache validity are unverified |
| New Kafka consumer group + topic retention headroom | Kafka platform | Shadow start | Retention headroom is what makes rollback safe |
| Sign-off on the `seed-0` routing rows | Owner of `post_filter_control_dag_map` | Cutover | An events-admitted regression is a silent non-trigger |
| `routing_decision` retention horizon (7 years typical for regulatory capital) | Compliance / Records | Partitioning design | Retrofitting partitioning after multi-year accumulation is expensive |

#### 2.1.5 Business outcomes

- **Removes the mechanism behind month-end instability.** Completion checks are polled every five
  minutes but take longer than five minutes to answer, so under load each poll starts another query
  before the last finishes. They stack until the 40-connection pool is exhausted and the failure
  cascades rather than degrading gently. Sub-50 ms queries break that cascade at its root.
- **Lost events become recoverable rather than terminal.** Today a genuinely lost completion event
  polls to a ~3-hour timeout with no DLQ and no reconciliation; for portfolios the fan-in
  accumulator can block permanently with no alert. After Phase 1: quarantine, replay, and a
  reconciliation sweep that alerts on overdue runs.
- **Regulatory reporting survives an Airflow outage.** The outbox holds triggers and drains on
  recovery instead of dropping them.
- **It makes the audit trail possible.** Routing-decision recording lands here; Phase 2 consumes it.

---

### Phase 2 — CAPABILITY: One routing rulebook (Q4 2026)

**One versioned, auditable source of truth for what runs when.**

#### 2.2.1 Deliverables and evidence gates

| # | Deliverable | Evidence that it worked | Rollback |
|---|---|---|---|
| 2.1 | CEL condition engine in the framework, with a pinned-engine seam and cross-engine conformance suite | Conformance suite green on both engines; `dag_run_id` byte-parity against the locked vectors | Additive — nothing to roll back |
| 2.2 | Registry v2: mechanical translation of the trigger criteria map into CEL, with per-rule fixtures | CI compiles, type-checks and fixture-tests every clause; peer-reviewed by the original DAG author | n/a |
| 2.3 | v2 control DAG — one `EVALUATE_AND_DISPATCH` task replacing ~85 task groups | Shadow mode: full per-rule verdicts recorded, **triggers nothing** | Delete the shadow DAG |
| 2.4 | Shadow parity across a month-end | `parity_mismatch_total == 0` over the agreed window; every mismatch explained | Remove the dual-route row |
| 2.5 | Per-(tenant, event-class) cutover — 8 CAPITAL event classes, flipped one at a time | Zero missed or extra triggers vs decision records | Flip the row back |
| 2.6 | Portfolio gates + heartbeat *(scope decision — see below)* | Portfolio runs per reporting date 10 → 1; lost-event drill pages naming the missing contributor | Per-portfolio: restore routes + precondition |
| 2.7 | Retirement of the second routing source | Both registries deleted; CI enforces registration through the new registry only | Tag before delete |

**Scope decision to make at Q4 entry.** The portfolio gate work (2.6) is separable. It is the
highest-complexity item in the phase and the only part with a hard dependency on Phase 1's indexed
store. Deferring it to Q1 2027 removes ~9 pw from Q4 and removes the phase's only cross-workstream
blocking dependency — at the cost of leaving the portfolio fan-in on mutable Airflow Variables for
one more quarter. **Recommendation: keep it in Q4 if Phase 1 cuts over on time; defer it at the
first sign of slip.** It is the designed release valve for this phase.

#### 2.2.2 Success measures

| Measure | Baseline (today) | Target | How verified |
|---|---|---|---|
| Scheduler operations per incoming event | ~150–300 task instances for ≤1 useful trigger — **Measured** | 1 | Airflow metadata DB task-instance count, sampled daily |
| Airflow metadata-DB state writes per event | ~150–300 | 3–5 | Metadata DB write-rate metric |
| Routing shadow mismatches over a month-end | n/a | **0** | Parity DAG, blocking gate before any cutover |
| Sources of routing truth | 2 (a database table and a flat file, in two codebases) | 1 versioned registry | Deletion of both, CI-enforced |
| Routing decisions with a structured record | 0% — failures present as "the calculator did not run" | 100% | `routing_decision` coverage query |
| Time to answer *"why did DAG X not run for event Y?"* | No structured record — reconstruct by hand | A single query | Timed on a real support ticket |
| Lead time for a routing-rule change | Two-system coordinated release | One reviewed merge request | Median lead time, before vs after |
| Portfolio runs per reporting date *(if 2.6 in scope)* | ~10, of which 9 are wasted no-ops — **Measured** | 1 | DAG-run count per reporting date |

#### 2.2.3 Effort

| Work package | pw |
|---|---:|
| Condition engine, conformance suite, activation fold, evaluator, registry loader, verdict rendering, deterministic run-id | 5.0 |
| Registry v2 translation + fixtures + CI pipeline | 5.0 |
| v2 control DAG, shadow route, parity DAG, metrics | 4.0 |
| Per-event-class cutover across 8 classes (each requires a rolling restart — see §7, assumption 4) | 3.0 |
| Retirement of the legacy registries and control DAG | 2.0 |
| **Subtotal — routing** | **19.0** |
| Contingency @ 25% | 4.8 |
| **Routing subtotal** | **~24 pw** |
| Portfolio gates + heartbeat + per-portfolio cutover across 7 portfolio DAGs *(optional in Q4)* | ~9 pw |
| **Phase 2 — Modelled** | **~33 pw with gates (band 30–38); ~24 pw without** |

#### 2.2.4 Key dependencies

| Dependency | Owner | Note |
|---|---|---|
| **Phase 1 is *not* a prerequisite for the routing work** | — | Verified against source: the dispatcher receives byte-identical input either way, and shadow dual-routing is a data-only change requiring no code change to the current service. **This is what lets Phase 1 and Phase 2 overlap, and it is the single most important scheduling fact in this pack.** |
| Phase 1 indexed store + `/gate/groups` | Own team | Required for the portfolio gates (2.6) only |
| Peer review of every translated rule by its original DAG author | Calculator owners (outside the 8) | The named mitigation for the highest-severity risk in the register. Budget their time explicitly |
| A calculator/dataset catalogue importable at DAG parse time | Own team | Dataset-family rules are **not** a mechanical translation — catalogue lookups must resolve to constants at registry load |
| A month-end inside the shadow window | Calendar | Non-negotiable gate; sets the earliest possible cutover date |
| Confirmed count of registry entries and registered DAGs | Own team, week 1 | See §7, assumption 1 |

#### 2.2.5 Business outcomes

- **Run-cost per event falls by roughly 95%,** evidenced by metadata-DB write volume. Scheduler
  headroom returns exactly when month-end needs it.
- **Routing becomes explainable.** "Why was this calculator not triggered?" moves from a
  reconstruction exercise to a query — directly relevant when the question comes from a regulator or
  an auditor rather than from support.
- **Change velocity and change risk both improve.** A routing change stops being a coordinated
  release across two systems where a mis-sync fails silently, and becomes one reviewed merge request
  with CI-validated fixtures.
- **Onboarding a calculator stops adding proportional waste.** Today each additional calculator
  multiplies the per-event churn. After Phase 2 the dispatch cost is flat in the number of
  calculators — this is the change that makes the shared-platform vision arithmetically possible.

---

### Phase 3 — PLATFORM: Framework + Airflow 3.x (Q1 2027, waves into Q2 2027)

**Modern runtime and a simple authoring pattern, co-delivered so each calculator is touched once.**

#### 2.3.1 Deliverables and evidence gates

| # | Deliverable | Evidence that it worked | Rollback |
|---|---|---|---|
| 3.1 | **F0** — framework: factory, domain models, SLA-aware completion stack, DAG chaining, testing kit | Unit + contract + schedule suites green; the testing kit runs inside the DAG repo's own CI | None needed — additive release line |
| 3.2 | Airflow 3.x runtime built **alongside** the current one; provider and plugin inventory; REST API migration | New runtime green on the pilot; current runtime untouched | Do not migrate; the old runtime is still live |
| 3.3 | **F1** — pilot: one calculator fully rewritten, with its condition-classification table in the same PR | ≥1 month-end of green runs; support demonstrates diagnosing a *seeded* failure from grid, XCom and logs alone | Revert a single file pair |
| 3.4 | **F2** — family waves: regional, then the rest. One merge request per DAG | Per wave: zero self-skipped calculator runs for migrated DAGs | Per-DAG PR revert |
| 3.5 | **F3** — portfolio DAGs onto the gated input | Portfolio runs 10 → 1 per reporting date, with evidence visible in the run log | Per-DAG revert |
| 3.6 | **F4** — deletion of the legacy surface; framework 6.0.0 | Grep-zero legacy imports; DAG bag green; metrics steady for 2 weeks | Tag before delete |

**Why the Airflow upgrade is co-delivered, in one sentence for the room:** it is a breaking upgrade
that touches every calculator, so pairing it with the framework rewrite means each DAG is migrated
once — new pattern and new runtime in a single change, one review, one rollback unit.

#### 2.3.2 Success measures

| Measure | Baseline (today) | Target | How verified |
|---|---|---|---|
| Framework adoption | 0 | 100% of calculators | The factory stamps a version tag into the DAG's tags — countable in the Airflow UI. **The plan builds its own progress metric** |
| Wasted calculator self-skip runs | 9 of every 10 portfolio runs — **Measured** | ~0 for migrated DAGs | Skipped-run counter per DAG |
| Airflow runtime line | 2.10.5 — a maintenance line — **Measured** | 3.x — current supported line | Version endpoint |
| Tenant isolation at runtime | None — shared execution model | Task-level isolation | Runtime configuration audit |
| Authoring surface for a new calculator | ~80 lines across two files, three layers — **Measured** | One plan function + one declaration | Line/file count on the next onboarding |
| Lead time to onboard a new calculator | *To be baselined from the last 3 onboardings* | Target set after the pilot | Elapsed time, first commit to first green production run |
| Completion failures with a machine-readable cause | 0% — a timeout is a timeout | 100% categorized | Categorized-failure metric |

#### 2.3.3 Effort — the phase that decides the program's shape

| Work package | pw |
|---|---:|
| F0 — framework build (factory, models, completion stack, chaining, testing kit) | 14.0 |
| Airflow 3.x runtime build, provider/plugin inventory, REST API migration, deployment automation | 12.0 |
| F1 — pilot DAG, including one month-end of observation | 3.0 |
| F2 — family waves: **~85 DAGs at ~0.4 pw each** | 34.0 |
| F3 — 7 portfolio DAGs onto gated input | 5.0 |
| F4 — deletion campaign and major version bump | 3.0 |
| **Subtotal** | **71.0** |
| Contingency @ 30% *(higher — this phase carries the most unpriced work)* | 21.3 |
| **Phase 3 — Modelled** | **~92 pw (band 80–105)** |

**The Airflow 3.x line is the least firm number in this pack.** It is not costed in any existing
plan document. The 12 pw assumes the upgrade is a contained runtime migration with a provider
inventory pass; it does not cover a discovery that a provider or plugin has no 3.x equivalent.
Expect this question from engineering leadership, and treat a Q4 2026 spike as the answer.

**Sensitivity of the largest line (F2) to the DAG count:**

| Registered DAGs | F2 effort | Phase 3 total (with contingency) |
|---:|---:|---:|
| 60 | 24 pw | ~79 pw |
| **85** *(planning assumption)* | **34 pw** | **~92 pw** |
| 100 | 40 pw | ~100 pw |

#### 2.3.4 Key dependencies

| Dependency | Owner | Note |
|---|---|---|
| Phase 1 `GET /run/status` in production | Own team | **Hard blocker** on F0 — the framework's completion stack calls it |
| Phase 2 registry rule live for a given DAG | Own team | A DAG cannot migrate before its routing rule is live |
| Phase 2 gates live | Own team | Blocks F3 (portfolio DAGs) only |
| Second Airflow environment provisioned and dual-running | Infrastructure | Dual-running the estate for 2+ quarters has an infrastructure cost that should be priced separately |
| Calculator SLA metadata present and correct across all calculators | Calculator owners | Named as an F0 gate. Missing SLA data produces noisy or blind completion probes |
| Calculator owner review of each migration PR | Calculator owners (outside the 8) | ~85 reviews. This is a real draw on people who are not in the team of 8 |

#### 2.3.5 Business outcomes

- **This is the phase that delivers the scalability leg of the crew vision.** Onboarding a new
  client calculator becomes a declaration plus a plan function, reviewed against a contract test kit
  — capacity added without risk added.
- **Back on a supported runtime,** with the security and patch posture that implies, and with
  stability behaviour we currently engineer around delivered out of the box.
- **Real enforcement for the tenant model** built in Phase 2 — task-level isolation makes the
  shared-platform proposition credible to a second crew.
- **Support cost falls.** One authoring pattern and machine-readable failure causes mean an incident
  no longer requires specialist knowledge of an individual calculator's hand-rolled structure.

---

## 3. Consolidated effort and capacity plan

### 3.1 Demand vs supply, by quarter

Supply is the §1 model: ~40 pw per quarter. Note that Q3 2026 is already ~55% elapsed, and the
capacity it contained has been spent — on the Phase 1 build that is now green in test.

| Quarter | Work in the quarter | Demand (pw) | Supply (pw) | Balance |
|---|---|---:|---:|---|
| **Q3 2026** *(remaining)* | Phase 1 residual: gates, provisioning, shadow start | 28 | ~25 | **−3 — tight** |
| **Q4 2026** | Phase 1 cutover tail + observation; Phase 2 build, shadow, cutover; Airflow 3.x spike | 41 | 40 | **−1 — no float** |
| **Q1 2027** | Phase 3 F0 framework + Airflow 3.x runtime + F1 pilot; Phase 2 gates if deferred | 38 | 40 | **+2** |
| **Q2 2027** | Phase 3 F2 waves + F3 portfolios + F4 deletion | 55 | 40 | **−15 — the pinch** |
| **Total** | | **162** | **145** | **−17 pw** |

### 3.2 What this table actually says

1. **The plan is achievable with 8 people through the end of Q1 2027** — but with no schedule float
   beyond the contingency already embedded in each estimate. One serious production incident month,
   or one unplanned regulatory change, consumes the margin.
2. **Phase 1's completion date is set by elapsed-time gates, not by effort.** Realistically the
   production cutover completes late Q3 to mid-Q4 2026. The shadow window and the 2-week observation
   window are the driver. This is a feature of the safety design, not a delay.
3. **Q4 2026 works only because Phase 1 and Phase 2 can overlap.** That overlap is not an
   assumption — it was verified against source code: the routing work needs no change to the current
   event service, its database, or the external contracts.
4. **Q2 2027 is the pinch, and it is entirely the migration waves.** This is the honest finding of
   the exercise. It is also the *most* parallelizable work in the whole program: one merge request
   per DAG, mechanical after the pilot, with a per-DAG rollback unit.

### 3.3 Three levers for the Q2 2027 gap — and a recommendation

| Lever | Effect | Cost | Assessment |
|---|---|---|---|
| **A. Add 2 FTE for Q1–Q2 2027** | Closes the 15 pw gap with margin | 2 FTE for 2 quarters | **Recommended.** The wave work is template-driven after the pilot, so it absorbs additional hands better than any other part of the program. It need not be permanent headcount |
| **B. Accept a tail into Q3 2027** | ~5 weeks of migration slip | Both Airflow runtimes co-resident ~1 quarter longer, with the infrastructure and operational cost that carries | Viable. The framework's additive release line explicitly supports legacy and new DAGs coexisting |
| **C. Pause new calculator onboarding for Q2 2027** | Releases ~10 pw | Breaks the "BAU continues uninterrupted" commitment on slide 3 | Fallback only. It trades a commitment already made to the business |

**Recommendation: Lever A, with Lever B as the pre-agreed fallback if the headcount is not
available.** Decide at the F1 pilot gate, not now — see §3.4.

### 3.4 A designed re-baseline point

The per-DAG migration figure (0.4 pw) is the largest single assumption in the program and the
hardest to know in advance. The plan already contains the right place to test it: the **F1 pilot**
migrates one calculator end-to-end, with the explicit purpose of producing a repeatable recipe.

**Proposal: re-baseline the Q2 2027 estimate against the pilot's measured cost before committing to
the resourcing lever.** That converts the biggest estimate in the pack from a judgement into a
measurement, and it does so at a point where the decision is still cheap.

---

## 4. Dependency register — consolidated

Ordered by what senior management can actually unblock.

| # | Dependency | Type | Owner | Needed by | Consequence if late |
|---|---|---|---|---|---|
| 1 | Docker-capable CI runner | Internal, blocking | Build engineering | **Now** | 99 integration tests and the perf gate stay unexecuted; Phase 1 cannot be signed off |
| 2 | Azure PostgreSQL 16 + HA/DR posture | Infrastructure | Infrastructure | Shadow start | Blocks Phase 1 entirely |
| 3 | EDF Context REST API contract, auth, rate limits, immutability | **External** | EDF team | Shadow start | Context resolution unverified; cache validity unproven |
| 4 | Kafka consumer group + retention headroom | Platform | Kafka platform | Shadow start | Retention headroom is what makes rollback safe |
| 5 | Calculator-owner time: rule review (Phase 2) and PR review (Phase 3) | **People outside the 8** | Calculator owners | Q4 2026 onward | The named mitigation for the highest-severity risk in the program |
| 6 | Second Airflow environment, dual-running | Infrastructure | Infrastructure | Q1 2027 | Blocks the whole of Phase 3 |
| 7 | Calculator SLA metadata, complete and correct | Data quality | Calculator owners | F0 gate | Noisy or blind completion probes across the estate |
| 8 | `routing_decision` retention horizon | Governance | Compliance / Records | Phase 1 partitioning | Retrofitting partitioning later is expensive |
| 9 | Architecture and security review, change advisory approval | Governance | Chief Architect, Security | Each cutover | Sits on the critical path of every phase boundary |
| 10 | A month-end inside each shadow window | Calendar | — | Phases 1 and 2 | Sets the earliest possible cutover date; cannot be compressed |
| 11 | MEG and EDF payload contracts | **External — no change required** | — | — | **Confirmed no dependency.** Worth saying out loud: this program requires nothing from MEG |

---

## 5. Program-level success measures

Beyond the per-phase measures, four things should be reported to this forum each quarter.

| Measure | Baseline | Target | Caveat to state when reporting |
|---|---|---|---|
| Orchestration-caused production incidents | **A 12-month categorized baseline is the first deliverable, not the number** | ~40% reduction | This is a deliberate **floor**, not a forecast, and it is scoped to incidents *caused by orchestration*. It excludes upstream data quality, calculator business logic, infrastructure outages, deployment error, and first-year teething on the new runtime. The mechanical reductions elsewhere in this pack are ~95%+ because they are arithmetic; incident counts involve people and process, so this target is set far below them on purpose |
| Run-cost per event | ~150–300 metadata-DB state writes | 3–5 | Evidenced by write volume, not by a currency figure. If asked for a money number, offer to derive it from the infrastructure bill after the Phase 2 cutover, not before |
| Calculator onboarding lead time | To be baselined from the last 3 onboardings | Set after the F1 pilot | The scalability claim; measure it before Phase 3 starts or it cannot be claimed afterwards |
| Delivery to the committed BAU plan | 100% | 100%, unchanged | The program's licence to operate. If this slips, the phasing has failed regardless of technical progress |

**Baselines are the first deliverable of the program, not an afterthought.** Three of the four
measures above cannot be claimed at the end unless the baseline is captured before anything changes
in production. That capture is inside the Phase 1 residual estimate.

---

## 6. What each phase is worth on its own

A fair question from this forum is: *what if we stop after phase N?* Each phase stands alone.

| Stop after | What has been permanently gained | What remains unfixed |
|---|---|---|
| **Phase 1** | Sub-50 ms queries; no more connection-pool cascade at month-end; lost events recoverable; Airflow-outage-proof triggering; audit trail possible | The churn — every event still costs ~150–300 scheduler operations; routing still lives in two systems |
| **Phase 2** | ~95% of the waste gone; one auditable rulebook; routing explainable; onboarding cost flat in the number of calculators | Calculators still use the old authoring pattern; still on a trailing Airflow line; no tenant isolation |
| **Phase 3** | The full vision: efficiency, speed, stability, scalability | — |

There is no phase after which the platform is *worse off*, and no phase whose value depends on the
next one being funded. That is a direct consequence of the strangler-fig sequencing, and it is the
strongest risk answer available to this program.

---

## 7. Assumptions to confirm before this is presented

These six move the numbers. Each has a named owner and can be closed in week one.

| # | Assumption | Currently | Why it matters | Close by |
|---|---|---|---|---|
| 1 | **Calculator / DAG count.** The deck says ~100; the discovery document says ~85 registered DAGs; the routing registry snapshot in the workspace holds **24 DAG ids across 33 condition strings** | Unreconciled | Drives the largest single line in the program (F2 waves, 34 pw) **and** the churn baseline quoted on slides 1, 2 and 4. The three figures may all be right and measuring different things — registered DAGs vs DAGs carrying routing rules — but that must be established, not assumed | Query the production registry and DAG bag; make the deck and this pack agree |
| 2 | **BAU draw is ~40% of gross capacity** | Modelled | Every schedule statement in §3 is a function of it. At 30% the Q2 2027 pinch disappears; at 50% the program extends a quarter | Two quarters of actual time records |
| 3 | **Airflow 3.x upgrade is a contained runtime migration** (12 pw) | Not costed anywhere | The least firm number in the pack. Provider and plugin inventory across the estate has never been done | A Q4 2026 spike, before Phase 3 is committed |
| 4 | **Routing-rule cutover requires a rolling restart, not a live configuration flip** | Verified in code — contradicts the strategy document's "no redeploy" claim | Changes the cutover runbook and the per-class cutover cost. Already priced in at 3 pw | Confirm against the live service; correct the strategy document |
| 5 | **The `seed-0` routing translation is an interpretation, not a mechanical port** | Verified — the parser for the legacy condition grammar is absent from the source snapshot | It is a human sign-off item on the critical path to cutover, and a silent-non-trigger risk if wrong | Sign-off from the owner of the routing rows |
| 6 | **Which Airflow 3.x release carries the multi-team capability** | Open | Task-level isolation is the 3.0 foundation; multi-team deployment support lands later in the line. The tenant-isolation and throttling claims must name a real release | Airflow release notes; update slides 2 and 4 |

Two smaller consistency items for the deck itself: slide 1 says *95% of the work is wasted* while
the derivation (~150–300 operations for one useful trigger) supports ~99%; and the completion-check
baseline is quoted as 5+ minutes on slides 1 and 4 while an earlier draft used 10+ minutes. Pick one
figure for each and change every occurrence together — an inconsistency found in the room costs more
than the difference between the numbers.

---

## One-paragraph answer, if the room wants it short

> Eight people is roughly 3 FTE of net modernization capacity once month-end close, production
> support, and committed regulatory delivery are funded first. Against that, the program is about
> 162 person-weeks of work: ~28 remaining on the event backbone, ~33 on the routing rulebook, ~92 on
> the framework and Airflow upgrade. It fits 8 people through Q1 2027 with no float, and the
> migration waves in Q2 2027 need either 2 additional people for two quarters or a five-week tail
> into Q3. Every phase has a named success measure with a baseline captured before we change
> anything, a rollback that is a single configuration change, and standalone value — so the program
> can be stopped at any phase boundary without stranding the investment. The decision we would like
> today is approval to run the production shadow for the event backbone, which is already built and
> green in test.
