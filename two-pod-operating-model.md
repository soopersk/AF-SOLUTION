# Two-Pod Operating Model: Runtime & Reliability / Runtime & Integration

Nov 15, 2026

## How the model works

Two pods own separate products and backlogs, while one pool of engineers, one Product Owner (PO) and one Tech Lead (TL) serve both. Five rules hold it together:

1. **Ownership is strict; people are shared.** Runtime & Reliability owns the Airflow-based Orchestration Platform. Runtime & Integration owns the Observability Service. Any engineer can work on either.
2. **One issue lives in exactly one project**, the repository where the change lands. Nothing is cloned between pods.
3. **Work that spans both products is one group-level epic with one child issue per project**, each owned by the pod whose product changes.
4. **One sprint calendar.** Both pods run the same sprint dates, one capacity ledger and one set of ceremonies with a segment per pod.
5. **Priority conflicts between pods go to the PO; technical ownership disputes go to the TL.** Both are recorded in the group wiki's decision log.

**Assumptions to confirm:** two-week sprints; both repositories sit in one GitLab group (needed for group boards and shared milestones); the team is roughly 6 to 10 engineers; GitLab features marked † are available on your plan. Items in [brackets] need your input.

## 1. Pod charters and boundaries

Each product has exactly one owning pod, and that pod owns its backlog, roadmap, quality bar and incident response. Engineers from either pod contribute under the owning pod's standards.

### Charters at a glance

Mission, consumers and success measures below are drafted from the pod names; replace them with your real ones.

| | Runtime & Reliability (Pod 1) | Runtime & Integration (Pod 2) |
| --- | --- | --- |
| Product owned | Airflow-based Orchestration Platform | Observability Service |
| Mission | Keep the orchestration platform reliable, scalable and easy to build on for [internal consumers] | Make the health of our systems visible and trustworthy, and make it easy for [source systems] to connect |
| Owns | Airflow deployment and upgrades; scheduler, executor and worker capacity; platform tooling (base images, shared operators, DAG deploy pipeline); platform SLOs; security patching; tenant onboarding standards | Ingestion and collection pipelines; storage and query; dashboards and the alerting engine; instrumentation libraries, exporters and connectors; SLO reporting |
| Does not own | Individual teams' DAG logic; Observability Service internals | Metrics and logs emitted by other products; other products' SLOs |
| Success measures [pick 3 to 4] | Scheduler lag, DAG run success rate, Airflow version currency, time to restore | Ingest latency and loss, query latency, alert accuracy, integrations onboarded |
| Repository and backlog | `[group]/orchestration-platform`, label `pod::reliability` | `[group]/observability-service`, label `pod::integration` |

### Boundary rules

The producer owns emitting; the Observability Service owns everything after ingest; whoever owns the thing measured owns its SLO. These cases settle most disputes:

| Situation | Owner | Rule |
| --- | --- | --- |
| Airflow metrics or logs missing in Observability | Emitting: Pod 1. Ingest, storage, display: Pod 2 | Schema changes between the two need TL sign-off |
| SLO and alert thresholds for Airflow | Thresholds and definitions: Pod 1. Alerting engine: Pod 2 | Pod 2 owns the machinery that evaluates the SLO, not the SLO itself |
| A DAG that Pod 2 runs on Airflow | DAG: Pod 2. Platform: Pod 1 | Pod 2 is a tenant like any other team and follows the DAG standards |
| Orchestration platform outage | Pod 1 leads | Pod 2 supports if telemetry is affected |
| Observability Service outage | Pod 2 leads | Pod 1 supports if its platform is the source |
| Cause unclear during an incident | TL or on-call primary names the lead pod at the start | Hand-over is never delayed by who owns it; sort out ownership in the follow-up |
| Shared runtime infrastructure (clusters, CI templates, base images) | Open decision; default Pod 1 | See open decisions at the end |

### Intake

Requests reach the right backlog through one weekly rhythm, so requesters never have to guess perfectly.

1. **File it.** The requester opens an issue in the GitLab project of the product concerned, using the Request, Bug or Question template. Unsure which? File it in either project; triage will move it.
2. **First pass, within 1 business day.** A rotating triage captain (an engineer, not the PO or TL) checks for duplicates, asks for missing detail, sets `type::` and `pod::`, and moves the issue to the right project with `/move`.
3. **Prioritize, weekly (Tuesday, 30 minutes).** The PO ranks new issues into the owning pod's backlog and sets `priority::`. The TL sizes feasibility and flags dependencies on the other product.
4. **Answer the requester within one week of filing:** accepted with a target sprint, deferred, or declined with a reason.
5. **Urgent work skips triage.** Production incidents and launch blockers go through on-call [link]; the incident issue lives in the owning project with `priority::1`. Any mid-sprint interruption needs PO and TL agreement and gets the `unplanned` label.

### Escalation

Targets are proposed defaults; adjust to your support commitments.

| Level | Who | Handles | Target response |
| --- | --- | --- | --- |
| 0 | Pod engineers | Questions and blockers within one product | Same day, in standup or chat |
| 1 | Tech Lead | Technical direction, boundary and ownership disputes | Next business day |
| 2 | Product Owner | Priority conflicts between pods or stakeholders, scope trade-offs | Next triage, or within 2 business days |
| 3 | [Engineering Manager or Program Lead] | Capacity conflicts, PO and TL disagreement, stakeholder escalations | Within 2 business days |
| Incident | On-call primary, then TL as incident commander, then [Engineering Manager] | Live production issues | Per your on-call policy |

Because one PO serves both pods, a published order settles competing demands on the same people: production incidents first, then committed external dates, then sprint goals, then everything else.

### Decision rights

R does the work, A answers for the outcome, C is consulted, I is informed.

| Decision | PO | Tech Lead | Engineers | Eng. Manager |
| --- | --- | --- | --- | --- |
| Backlog order for each product | A/R | C | C | I |
| Sprint goal for each pod | A/R | C | C | I |
| Priority between the two pods | A/R | C | I | C |
| Technical design, standards, boundary disputes | C | A/R | R | I |
| Capacity allocation across pods | C | R | C | A |
| First-pass triage | I | A | R | I |
| Release approval for each product | C | A | R | I |
| Incident command | I | A/R | R | I |

### Working agreements for engineers who cross pods

- Follow the owning pod's repository conventions, merge request template and definition of done.
- At least one approval on every merge request comes from the owning pod's CODEOWNERS, so knowledge of each product stays anchored.
- The issue stays in the owning project and on the owning pod's board; nothing is mirrored into the contributor's other pod.

## 2. GitLab issue and backlog tracking

Each pod tracks its work in its own GitLab project, and a shared parent group gives one place to see people, sprints and cross-pod work, so nothing is duplicated. Features marked † are Premium-tier as far as I know; confirm against your plan before launch.

### Structure

Put both repositories under one parent group (for example `runtime`). Issues, boards and CODEOWNERS stay per project; labels, sprint milestones, epics† and cross-pod boards live at group level.

| Object | Level | Setup |
| --- | --- | --- |
| Labels | Group | One shared taxonomy (below), inherited by both projects |
| Sprint | Group | One group milestone per sprint with the same dates for both pods, or a group iteration cadence†. The description holds both sprint goals |
| Release milestones | Project | Only if a product ships versioned releases (for example an Airflow upgrade); kept separate from sprints |
| Epics† | Group | One per initiative that spans both products; owned by the PO |
| Issue templates | Project | Request, Bug, Question; each pre-applies `pod::` and `workflow::needs-triage` with quick actions |
| Wiki | Group | Roster, capacity ledger, decision log, boundary rules, this model |

### Labels

| Label | Values | Set by | Purpose |
| --- | --- | --- | --- |
| `pod::`† | `reliability`, `integration` | Template default, triage | Owning pod; enables group-wide views. Always matches the project |
| `workflow::`† | `needs-triage`, `backlog`, `ready`, `in-progress`, `in-review`, `blocked`, `verify` | Engineer, triage | Board columns; closed means done. One value at a time |
| `type::`† | `feature`, `bug`, `tech-debt`, `spike`, `support`, `incident-followup` | Requester, triage | Reporting and interrupt tracking |
| `priority::`† | `1`, `2`, `3` | PO | 1 is drop-everything, 2 is this or next sprint, 3 is normal ranking |
| `contrib::cross-pod`† | (single value) | Assignee, at planning | Issue is worked by an engineer whose primary pod is the other pod |
| `unplanned` | (no scope) | Whoever adds mid-sprint work | Measures interrupts to calibrate the capacity buffer |

On a plan without scoped labels, use prefixed plain labels such as `workflow-in-progress` and let a triage bot flag issues that carry two values.

### Rules for work items

1. **One issue, one project.** It lives where the code change lands and belongs to that project's pod.
2. **A cross-pod contributor is simply the assignee.** Assign the engineer and add `contrib::cross-pod`. Never clone the issue into their other pod's project.
3. **Work that spans both products** is a group epic† named `[Initiative] ...` with one child issue per project. Without epics, use a parent issue in the leading pod's project, linked with `relates to`.
4. **A dependency on the other pod's code** is an issue in the owner's project, linked `blocks` or `is blocked by`, with `workflow::blocked` on the waiting issue.
5. **Wrong project?** Use the `/move` quick action so history and links follow. Never copy and paste.
6. **Merge requests** live in the repository being changed and close the issue with `Closes #123` or `Closes group/project#123`.
7. **The PO ranks each backlog.** A contributor who disagrees with a priority raises it at triage rather than reordering another pod's issues.

### Boards

| Board | Level | Lists and filter | Used for |
| --- | --- | --- | --- |
| Pod 1 sprint board | Project `orchestration-platform` | `workflow::` lists, current sprint | Pod 1 standup and planning |
| Pod 2 sprint board | Project `observability-service` | `workflow::` lists, current sprint | Pod 2 standup and planning |
| Team capacity board† | Group | One list per assignee across both projects, current sprint | TL and planning: each person's load across both products |
| Triage board | Group | `workflow::needs-triage`, split by `pod::` | Tuesday intake triage and the triage captain |
| Cross-pod board | Group | Lists for `contrib::cross-pod` and `workflow::blocked` | Spotting borrowed capacity and stuck dependencies |
| Initiative roadmap† | Group epics | Roadmap view of open epics | PO and stakeholder view of cross-product work |

Without assignee lists, filter the group issue list by sprint and review it engineer by engineer at planning.

### Sprint mechanics

- One sprint is one group milestone (or iteration†) shared by both projects. Each pod's goal is written in its description.
- Only `workflow::ready` issues enter a sprint. Ready means acceptance criteria are written, an estimate is set, dependencies are linked, and `pod::` and `priority::` are labeled.
- Unfinished issues roll into the next sprint at the review, with a one-line note on why.

### Visibility without extra reporting

Four numbers per pod per sprint come straight from labels and milestones; review them at the retro.

- **Committed versus completed** issues, from each project's milestone.
- **Cross-pod share:** issues with `contrib::cross-pod` divided by all sprint issues.
- **Unplanned share:** issues with `unplanned` divided by all sprint issues.
- **Carry-over:** issues rolled into the next sprint.

Do not compare velocity between pods; the products differ.

### Light automation

- Issue and merge request templates that apply default labels through quick actions.
- A scheduled triage bot, such as the open-source gitlab-triage, that flags issues with no `pod::` label, issues in progress with no assignee, and issues assigned across pods without `contrib::cross-pod`.
- CODEOWNERS in each repository set to that pod's primary engineers plus the TL.

## 3. Ceremonies and capacity management

Run one combined set of ceremonies on one sprint calendar, with a segment per pod, which keeps the PO's ceremony time near 10 hours and the TL's near 13.5 hours in a 10-day sprint. Fully separate ceremonies would roughly double both.

### Cadence

| Ceremony | Format | Time-box | PO hrs per sprint | TL hrs per sprint | Engineers |
| --- | --- | --- | --- | --- | --- |
| Daily standup | One combined standup; walk the Pod 1 board, then Pod 2 (alternate which goes first) | 15 min daily | 1.0 (Mon and Thu) | 2.5 | All |
| Sprint planning | One session: capacity and cross-pod allocation, then a segment per pod | 3 h (30 + 75 + 75 min) | 3.0 | 3.0 | All; may skip the other pod's segment if allocated under 20% to it |
| Refinement | Alternating: Pod 1 in week 1, Pod 2 in week 2 | 75 min each | 2.5 | 2.5 | Rotating 3 to 4, plus anyone expected to pick up cross-pod items |
| Intake triage | Both pods' new issues, ranked by the PO | 30 min weekly | 1.0 | 1.0 | Triage captain |
| Sprint review | One session, a segment per pod, so shared stakeholders attend once | 60 min (25 + 25 + 10 outlook) | 1.0 | 1.0 | All |
| Retrospective | One team retro; action items tagged by pod | 60 min | 1.0 | 1.0 | All |
| Cross-pod roadmap and dependency sync | Epics and shared dependencies | 45 min monthly | 0.5 | 0.5 | TL and 1 to 2 engineers |
| TL office hours | Open slot for design questions on either product | 60 min weekly | 0 | 2.0 | Drop-in |
| **Total** | | | **10.0** | **13.5** | |

That is about 12.5% of the PO's 80-hour sprint and 17% of the TL's.

### A sprint at a glance

Standup runs every day. Wednesday afternoons stay meeting-free.

| Day | Week 1 | Week 2 |
| --- | --- | --- |
| Mon | Sprint planning (3 h) | |
| Tue | Intake triage | Intake triage |
| Wed | Pod 1 refinement | Pod 2 refinement |
| Thu | TL office hours | TL office hours; Ready cutoff for next sprint |
| Fri | | Review, then retro; capacity ledger updated for next sprint |

Why this shape: engineers are the same people in both pods, so standup, review and retro are combined; the PO and TL make different decisions per product, so planning and refinement keep a separate segment for each.

Split the standup into two only if the combined one regularly runs past 15 minutes. For the first three sprints, add a standing retro item on how the two-pod model is working.

### Capacity management

Capacity is allocated per person across pods first, then forecast per pod, so that no engineer is counted twice.

```latex
C_{pod} = (1 - b_{pod}) \sum_{e} D_e \cdot F \cdot A_{e,pod}
```

- **D** is each engineer's available days in the sprint, after PTO, holidays and on-call shifts.
- **F** is the focus factor: the share of an available day that goes to sprint work after meetings, reviews and support. Start at 0.7.
- **A** is the engineer's allocation to the pod; their allocations across both pods sum to 100%.
- **b** is the buffer for unplanned work. Start at 15% for both pods, then recalibrate after three sprints from the `unplanned` label.

Example: an engineer with 9 available days has 9 × 0.7 = 6.3 focus days. At a 60/40 split that is 3.8 days for Pod 1 and 2.5 for Pod 2, or 3.2 and 2.1 after the 15% buffer.

### Allocation rules

1. **Every engineer has a primary pod** (at least 60% of focus time), set at planning from the capacity ledger.
2. **Allocate in 20% steps** (100/0, 80/20, 60/40). Finer slivers are lost to context switching.
3. **One capacity ledger** in the group wiki is the single source of truth. The TL drafts it and the Eng. Manager confirms it by Friday of week 2.
4. **One cross-pod issue in progress per engineer** at a time.
5. **Cross-pod work is planned, not drive-by.** Picking up the other pod's issue mid-sprint needs TL approval, swaps out planned work and is labeled `unplanned`.
6. **Credit follows the issue.** Throughput counts for the pod that owns the issue; capacity cost is charged to the engineer's allocation. Forecast each pod from its own history.
7. **Rebalance when the data says so.** If cross-pod share exceeds 40% of a pod's sprint issues, or carry-over exceeds 30% for two sprints running, revisit primary-pod assignments at the next planning. These thresholds are starting points.
8. **Keep at least two engineers able to ship each product**, so one absence never stalls a pod.

## 4. Launch communication

Send the announcement below once the GitLab group, boards and templates exist, so the links in it work. The reasons are drafted from common drivers for a split like this; replace them with your actual ones.

### Announcement

```markdown
**Subject: Runtime is now two pods. What changes and what doesn't**

Hi all,

From [start date], the Runtime team is organized into two pods, each with a clear product focus:

- **Runtime & Reliability** owns the Airflow-based Orchestration Platform.
- **Runtime & Integration** owns the Observability Service.

**Why we are doing this.** One backlog for two products made it hard to see what we are committing to for each, and hard for you to know who to ask. Two pods give each product its own backlog, roadmap and sprint goal, so priorities and ownership are clear.

**What stays the same.** It is the same people, the same Product Owner ([name]) and the same Tech Lead ([name]). Engineers will keep working across both products when it makes sense. We are organizing the work, not building a wall between people.

**How day-to-day work runs**
- **Requests and bugs:** file an issue in the GitLab project of the product concerned ([link], [link]). Not sure which? File it in either one; we route it at our weekly triage and you will get an answer within a week.
- **Tracking:** each product has its own GitLab project, backlog and board. Every item lives in exactly one project, so there are no duplicate tickets.
- **Cadence:** both pods run the same two-week sprints, with shared planning, standup, review and retro and a segment for each pod. Review segments are open to stakeholders.
- **Urgent production issues:** unchanged. Page on-call ([link]).

**What to do now:** read the one-page guide ([wiki link]), and bring questions to [channel] or our Q&A on [date].

Thanks,
[Name]
```

### One-line version for a channel topic

```markdown
Runtime is now two pods: Runtime & Reliability (Airflow Orchestration Platform) and Runtime & Integration (Observability Service). Same team, same PO and TL. File requests in the product's GitLab project: [link]
```

### Rollout checklist

Before the announcement:

- [ ] Confirm both projects sit in one GitLab group and your plan includes the features marked †
- [ ] Create group labels, the two sprint boards, and the group triage, capacity and cross-pod boards
- [ ] Add issue and merge request templates with default labels; set CODEOWNERS in both repositories
- [ ] Draft the sprint 1 capacity ledger and get the Eng. Manager's sign-off
- [ ] Decide the open items in the table below, at least shared infrastructure and on-call

Before sprint 1:

- [ ] Send the announcement and hold a 30-minute Q&A
- [ ] Send calendar invites for standup, planning, refinement, triage, review, retro and TL office hours
- [ ] Give every open issue exactly one `pod::` label and `/move` any that sit in the wrong project
- [ ] Publish the group wiki page: this model, the boundary table, the capacity ledger

After launch:

- [ ] Keep a two-pod retro item for sprints 1 to 3
- [ ] After sprint 3, recalibrate the focus factor and buffer and check the rebalance thresholds

### Risks and open decisions

| Risk or open decision | Proposed default or mitigation |
| --- | --- |
| Shared runtime infrastructure (clusters, CI templates, base images) has no named owner | Pod 1 owns it until decided; record the decision in the boundary table |
| On-call model across two products | One shared rotation with a runbook per product; incident issues live in the owning project |
| One PO and one TL become the bottleneck for both pods | Triage captain does the first pass, office hours replace ad hoc interruptions, and each names a deputy [names] |
| Engineers fragment across both pods | 20% allocation steps; one cross-pod issue in progress at a time |
| Pods drift into silos | Monthly cross-pod sync, one shared retro, at least two engineers able to ship each product |
| Labels drift and boards lose trust | Triage bot flags missing or conflicting labels; the triage captain cleans up weekly |
| Scoped labels, iterations or epics are not on your GitLab plan | Use the fallbacks noted in the GitLab section |
