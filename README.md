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
