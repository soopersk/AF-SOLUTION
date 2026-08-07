# Orchestration Modernization Program

Implementation workspace for the three-workstream modernization of the orchestration platform.
Authoritative kickoff: [phase2_dag_framework_planning_prompt.md](phase2_dag_framework_planning_prompt.md).

All design decisions are **closed** (see kickoff § Binding Decisions). This repository is for
*implementation*: verification spikes → build → gated rollout with recorded evidence at every gate.

## Workstreams (each in its own parent folder)

| Folder | Workstream | Authoritative plan | Delivers |
|---|---|---|---|
| [`ems/`](ems/) | **EMS rewrite** (Java 17 / Spring Boot) | [ems-design.md](ems-design.md) | Trigger-plan **Phase A**: re-platform + performance schema + subscription routing, outbox, `routing_decision`, poison-only DLQ + audited replay, `GET /run/status`, `GET /gate/groups` |
| [`control-plane/`](control-plane/) | **Control plane** (trigger semantics) | [trigger_redesign_final_implementation_plan.md](trigger_redesign_final_implementation_plan.md) | Phases **B–E**: registry v2 + CEL dispatcher (shadow → cutover), stateless gates + heartbeat, precondition retirement + legacy deletions |
| [`framework/`](framework/) | **Calculator DAG framework** | [framework_redesign_final_implementation_plan.md](framework_redesign_final_implementation_plan.md) | Steps **F0–F4**: `calculator_dag(spec, plan)` factory, `SlaAwareHttpTrigger` wait stack, DAG chaining, per-DAG migration of ~85 DAGs, framework 6.0.0 deletions |

Supporting reference: [system_discovery.md](system_discovery.md) (current state), [properties.sql](properties.sql) (legacy
filter + control-DAG-map seed evidence), [trigger_event_context.json](trigger_event_context.json) (sample Merival `EnrichedEvent`).

## Critical path

```
EMS verification spike (ems §14)
  → EMS build + shadow + flip  (Phase A)
    → Phase B shadow  ∥  F0 (framework lands)
      → Phase C cutover  ∥  F1/F2 (pilot + family waves)
        → Phase D (gates + heartbeat)
          → F3 (gated/portfolio)
            → Phase E (precondition retirement) ∥ F4 (deletions, 6.0.0)
```

Gate discipline: **a step is done when its gate evidence is recorded, not when its code merges.**

## Working agreement (from the kickoff)

1. **Per-step task plans at step start** — author the detailed plan, review with the requester, then execute. One step, one plan, one rollback unit.
2. **Gate discipline** — record evidence (`EXPLAIN` captures, drill logs, parity reports, month-end windows) before advancing. Never skip a rollback rehearsal.
3. **Amendment protocol** — if implementation contradicts a plan, record a numbered amendment in the owning doc (the A1–A5 pattern); never silently diverge.
4. **Rollback paths stay warm** until each gate closes.
5. **Iterate with the requester** revision-by-revision.

## Program status

See each workstream README for its phase board and current blockers. Program-level blockers are
tracked in the session hand-off notes; the headline items are the EMS Phase-0 verification spike
(external inputs) and the absence of the legacy source repositories referenced by the kickoff's
"Code to Verify Before Building" section.

## STARTING POINT

Recommended review set — Tiers 1+2 (~258 KB, ≈65k tokens)
Tier 1 — the claims and the evidence under them (~79 KB)

Doc	Why it's in
phase2_dag_framework_planning_prompt.md	Read first. Defines document precedence and the binding decisions. Without it a reviewer can't tell which doc wins when two disagree — and several do
README.md	One-page map of the three workstreams
system_discovery.md	The current-state evidence base. Every number in the deck traces here
Modernization_Roadmap_Executive_v4.pptx	The claims as presented. Include the speaker notes — they carry the sourcing and the caveats
Modernization_SeniorMgmt_QA.md	Measures, effort, capacity model, dependencies, and the §7 assumption register
Tier 2 — the designs actually being reviewed (~180 KB)

Doc	Covers
ems-design.md (87 KB)	Workstream 1, plus the A1–A18 amendment register in §0
trigger_redesign_final_implementation_plan.md (52 KB)	Workstream 2 — routing, gates, phases B–E
framework_redesign_final_implementation_plan.md (40 KB)	Workstream 3 — framework, F0–F4
Add only if the review goes there
SESSION_HANDOFF.md (18 KB) for build state and open gates · the two Aug-6 routing plans (49 KB) if routing is the focus · docs/ems-seed0-assumptions.md (14 KB) if cutover risk is · Orchestrator_Modernization_Strategy.docx for historical framing only.

Leave out by default
docs/ems-redesign-gitlab-epic.md (117 KB — a delivery backlog, not a design), docs/ems-technical-specification.md (117 KB) and docs/ems-user-guide.md (60 KB) — both written against the code as read, so they audit the implementation rather than review the plan. Also the v2/v3 decks (superseded) and ai-integration-proposal.md (a separate proposal). Those alone are ~294 KB — they'd more than double the context for almost no review value.

The thing most likely to waste the session
A large part of this plan has already been critically reviewed, and the findings are recorded. A fresh reviewer who doesn't know that will spend the session rediscovering them. Tell it upfront:

ems-design.md §0 — amendments A1–A18, contradictions found against legacy code with file:line evidence
T1–T5 in the Aug-6 routing plan — five more, including that the routing-table cutover needs a rolling restart, contradicting the strategy docx
docs/ems-seed0-assumptions.md — 12 assumptions: 1 signed off, 1 void, 10 open
§7 of the QA pack — 6 open assumptions, including the unreconciled DAG count (deck ~100 / discovery ~85 / registry 24)
Known-stale: the strategy docx's Q1/Q2/Q3 roadmap, and SESSION_HANDOFF.md's NEXT section
The standing rule in this workspace is verify against code, not docs — old-ems/ (33 files) and old-orchestration/ are in-workspace read-only for exactly that. A reviewer should grep them rather than be handed them.

---

#### Paste-ready opener
Fresh critical review of the Orchestration modernization plan — no implementation.

Read in this order: phase2_dag_framework_planning_prompt.md (precedence + binding decisions — these govern which document wins), README.md, system_discovery.md (current-state evidence), the v4 deck with speaker notes, and Modernization_SeniorMgmt_QA.md. Then the three workstream designs: ems-design.md, trigger_redesign_final_implementation_plan.md, framework_redesign_final_implementation_plan.md.

Before raising any finding, read the registers of findings already made — ems-design.md §0 (A1–A18), T1–T5 in docs/plans/2026-08-06-routing-first-cel-dispatcher.md, docs/ems-seed0-assumptions.md (10 of 12 open), and §7 of the QA pack. Do not re-report what is already recorded there; tell me what those registers miss.

Verify against code, not docs — old-ems/ and old-orchestration/ are the read-only legacy source. Design decisions are closed; if you think one is wrong, say so explicitly as a challenge rather than quietly re-opening it.

Deliver: a ranked findings list, each with evidence, severity, and what it changes. Start by telling me what you'd need that isn't in the set.

That last line matters — it lets the reviewer pull Tier 3 on demand instead of you guessing up front.