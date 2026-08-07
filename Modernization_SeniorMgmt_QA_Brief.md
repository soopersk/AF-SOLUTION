# Orchestration Modernization — Senior Management Brief

**Business summary.** Answers the question: *success measures, deliverables, effort, dependencies
and business outcomes per phase, for a team of 8.*

Prepared 2026-08-07 · The detailed technical version is `Modernization_SeniorMgmt_QA.md`

---

## Bottom line

| | |
|---|---|
| **Total effort** | ~162 person-weeks — about **12 months** of delivery at the team's current capacity |
| **Already delivered** | Phase 1 is **built and passing its tests**. An estimated 55–70 person-weeks of work is already banked |
| **Fits 8 people?** | Yes — through March 2027. The final quarter (Apr–Jun 2027) is short by about **15 person-weeks** |
| **Decision needed** | Approve the Phase 1 production trial now. Decide the Q2 2027 resourcing at the pilot checkpoint in Q1 2027, not today |
| **Risk of stopping** | None stranded. Every phase delivers standalone value and can be stopped at its boundary |

---

## What "a team of 8" actually buys

Eight people is not eight people's worth of modernization. Business-as-usual is funded first, and
we have committed to protecting it.

| Where the 8 people go | Share |
|---|---|
| Month-end / quarter-end close, production support, incident response | 20% |
| Regulatory change and new calculator onboarding already committed | 20% |
| Leave, public holidays | 12% |
| Management, admin, recruitment | 8% |
| **Available for modernization** | **40% — about 3 people's worth** |

**Planning figure: ~40 person-weeks per quarter.** This is the single most important number in the
plan. If business-as-usual turns out lighter, the timeline improves; if heavier, it extends. It is
the first number worth challenging.

Two things extra headcount cannot buy: the **trial periods** (each phase runs alongside the live
system for weeks, including a full month-end, before anything is switched on) and the rule that we
**never run more than two workstreams at once**. Both are what make the "no disruption to
regulatory delivery" promise true.

---

## Phase 1 — Event Backbone · Q3 2026

**What it is:** a fast, resilient service underneath everything else. Nothing downstream works
without it.

### Success measures

| Measure | Today | Target |
|---|---|---|
| Time to answer "has this calculation finished?" | **5+ minutes** | **Under a tenth of a second** |
| Work lost if the workflow engine goes down | Unbounded — triggers are dropped | **Zero** — held and replayed on recovery |
| Time for a bad message to be quarantined | Never — it jams the queue | **Under 1 minute**, with replay |
| Month-end stalls caused by system overload | Recurring | **Zero** |
| Completed work wrongly reported as unfinished | Recurring | **Zero** |

### Business value and outcome

- **Removes the mechanism behind month-end instability.** Today's completion checks take longer to
  answer than the interval at which we ask them, so under load the queries pile up until the system
  runs out of connections and fails hard rather than slowing gracefully. Fast queries break that
  chain at its source.
- **Lost work becomes recoverable.** Today a lost message means a three-hour wait, then a blocked
  portfolio with no alert. After Phase 1: quarantined, replayed, and alerted.
- **Regulatory reporting survives an outage** instead of silently dropping work.
- **Creates the audit trail** that Phase 2 turns into answers.

### Effort

| | Person-weeks | Status |
|---|---:|---|
| Already delivered (build and test) | *55–70 (estimated equivalent)* | **Complete, tests passing** |
| Remaining: performance test, sign-offs, environment setup, live trial, switchover | **~28** | Open |

**Remaining ≈ 9 weeks of elapsed delivery.** The completion date is driven by the trial and
observation periods, not by effort — realistically late Q3 to mid-Q4 2026.

---

## Phase 2 — One Routing Rulebook · Q4 2026

**What it is:** one source of truth for what runs when, replacing two systems that must be kept in
step by hand.

### Success measures

| Measure | Today | Target |
|---|---|---|
| Internal steps triggered by one incoming update | **~150–300**, to produce one useful action | **1** |
| Portfolio runs per reporting date | **~10**, of which 9 do nothing | **1** |
| Places the routing rules live | **2** systems, kept in step manually | **1** |
| Routing decisions we can explain afterwards | **None recorded** | **Every one** |
| Making a routing change | A coordinated release across two systems | **One reviewed change** |
| Disagreements with the live system during the trial | — | **Zero**, across a full month-end |

### Business value and outcome

- **Run-cost per update falls by roughly 95%.** The system stops doing 200 things to accomplish one.
  Capacity comes back exactly when month-end needs it.
- **"Why did this calculator not run?" becomes a question we can answer** — from a record, in
  seconds. Relevant when the question comes from a regulator or auditor rather than from support.
- **Adding a calculator stops adding waste.** Today every new calculator multiplies the per-update
  cost. After Phase 2 that cost is flat however many we onboard — this is what makes the shared
  platform ambition arithmetically possible.
- **Rule changes get faster and safer** — no more silent failures caused by two systems drifting
  apart.

### Effort

| | Person-weeks |
|---|---:|
| Routing rulebook, trial, phased switchover | ~24 |
| Portfolio readiness rework *(deferrable to Q1 2027 if Phase 1 slips)* | ~9 |
| **Total** | **~33** |

**≈ 11 weeks of elapsed delivery.** Phase 2 can start while Phase 1 is still in its trial — verified,
not assumed. That overlap is what makes Q4 2026 work.

---

## Phase 3 — Calculator Framework + Workflow Engine Upgrade · Q1–Q2 2027

**What it is:** a simpler way to build calculators, delivered together with the upgrade of the
workflow engine that runs them.

**Why together, in one sentence:** the engine upgrade is a breaking change that touches every one of
our ~85 calculators, so pairing it with the rewrite means each calculator is changed **once** instead
of twice.

### Success measures

| Measure | Today | Target |
|---|---|---|
| Calculators on the modern pattern | 0 | **All (~85)** — countable on a dashboard |
| Workflow engine version | A trailing maintenance line | **Current supported line** |
| Work to add a new calculator | ~80 lines across two files and three layers | **One definition + one declaration** |
| Wasted calculator runs | 9 in every 10 portfolio runs | **~0** |
| Failures with a clear, automatic cause | None — a timeout is just a timeout | **All categorised** |
| Isolation between teams sharing the platform | None | **Enforced by the runtime** |

### Business value and outcome

- **This is the phase that delivers the growth story.** Onboarding a new client calculator becomes a
  small, reviewed, templated change — capacity added without risk added.
- **Back on a supported product**, with the security patching and vendor support that implies.
- **Real separation between teams**, which is what makes offering this as a shared platform to
  another crew credible rather than aspirational.
- **Lower support cost** — one pattern to learn instead of 85 variations, and failures that explain
  themselves.

### Effort

| | Person-weeks |
|---|---:|
| Build the new framework | 14 |
| Build and prepare the upgraded workflow engine | 12 |
| Pilot: one calculator, end to end, through a month-end | 3 |
| Migrate ~85 calculators (one small change each) | 34 |
| Portfolio calculators | 5 |
| Remove the old system | 3 |
| Contingency (30% — this phase carries the most unknowns) | 21 |
| **Total** | **~92** |

**≈ 30 weeks — about 7 months of elapsed delivery.** This is the largest phase by a wide margin, and
the migration of the ~85 calculators is over a third of it.

---

## Effort summary

| Phase | Effort | Elapsed at current capacity | When |
|---|---:|---|---|
| 1 — Event backbone *(remaining)* | 28 pw | ~9 weeks | Q3 → early Q4 2026 |
| 2 — Routing rulebook | 33 pw | ~11 weeks | Q4 2026 |
| 3 — Framework + engine upgrade | 92 pw | ~30 weeks | Q1 – Q2 2027 |
| **Total** | **162 pw** | **~12 months** | **Q3 2026 – Q2 2027** |

Available capacity across those four quarters is ~145 person-weeks. **We are about 15 person-weeks
short, entirely in the final quarter, and entirely in the calculator migration.**

### The one gap, and three ways to close it

| Option | Effect | Trade-off |
|---|---|---|
| **A. Add 2 people for Q1–Q2 2027** *(recommended)* | Closes the gap with margin | Temporary, not permanent headcount. This work splits well across people — it is ~85 small, near-identical changes |
| **B. Accept ~5 weeks running into Q3 2027** | Same work, slightly later | We run both workflow engines side by side for a quarter longer, at an infrastructure cost |
| **C. Pause new calculator onboarding for one quarter** | Frees ~10 person-weeks | Breaks a commitment already made to the business. Fallback only |

**Recommendation: decide this at the pilot checkpoint in Q1 2027, not today.** The pilot migrates
one calculator end to end for exactly this purpose — it converts our largest estimate from a
judgement into a measurement while the decision is still cheap.

---

## What we need from the business

| # | We need | From | By |
|---|---|---|---|
| 1 | A build environment capable of running the full test suite | Build engineering | **Now** — Phase 1 cannot be signed off without it |
| 2 | Database and hosting environments provisioned | Infrastructure | Before the Phase 1 trial |
| 3 | Confirmation of the external event platform's interface | The EDF team (outside our organisation) | Before the Phase 1 trial |
| 4 | **Calculator owners' time** to review rules and changes | Calculator owners (not among the 8) | Q4 2026 onward — this is our top-rated risk control |
| 5 | A second workflow-engine environment, running alongside the current one | Infrastructure | Q1 2027 |
| 6 | How long routing records must be retained (7 years is typical for regulatory capital) | Compliance | Before the Phase 1 design is fixed |
| 7 | Decision on Q2 2027 resourcing | This forum | At the Q1 2027 pilot checkpoint |

Worth noting what we do **not** need: no change is required from MEG or from any calculator's
business logic. This programme is contained within our own platform.

---

## If we stop early

| Stopping after | What we keep permanently | What stays broken |
|---|---|---|
| **Phase 1** | Fast queries, no month-end overload cascade, lost work recoverable, outage-proof triggering | Every update still costs ~200 internal steps; rules still live in two systems |
| **Phase 2** | ~95% of the waste gone, one auditable rulebook, routing decisions explainable, growth cost flat | Calculators still on the old pattern; engine still on a trailing version |
| **Phase 3** | The full outcome: efficiency, speed, stability, scalability | — |

There is no point at which the platform is left worse than today, and no phase whose value depends
on the next one being funded.

---

## How firm are these numbers

| | |
|---|---|
| **Measured — solid** | Everything in the "Today" columns. Read from the running production code |
| **Targeted — has a test** | Everything in the "Target" columns. Each has a named verification method and a gate that must pass before we switch anything on |
| **Estimated — a judgement** | All effort figures. Built bottom-up from a task inventory, with 25–30% contingency already included |

Four things would move the numbers if they turn out different, and all four can be settled quickly:

1. **The calculator count.** Our documents variously say ~100, ~85, and 24. These may be counting
   different things, but it drives our largest estimate and must be reconciled — a week-one action.
2. **The business-as-usual load** (assumed 40%). Two quarters of actual time records would confirm it.
3. **The workflow engine upgrade** (12 person-weeks). This is the least firm figure — it is not
   costed in any existing plan. We propose a short investigation in Q4 2026 before committing.
4. **The cost per calculator migration.** Measured by the Q1 2027 pilot, before the resourcing
   decision is taken.

One caveat on the incident-reduction target quoted elsewhere (~40% fewer): it is a deliberate
**floor, not a forecast**, and it covers only incidents caused by orchestration — not data quality,
calculator logic, infrastructure outages, or deployment error. We would commit to it against a
12-month baseline captured before any change goes live. **That baseline is a deliverable of Phase 1,
not an assumption.**
