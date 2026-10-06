# Airflow 2.10.5 to 3.3.2 Migration - GitLab epics and issues

Plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588

How to use: for each epic create an epic with the given title and paste its description block. For each issue create an issue with the given title, set the milestone and labels shown, and paste the description block (it contains the acceptance criteria as a checklist).

## Milestones

| Key | Title | Due date | Notes |
| --- | --- | --- | --- |
| M0 | M0 - Readiness complete | 2026-10-30 | Phase 0 done. DAG fixes released on v2, Python 3.13 image built, decisions recorded. Committed date 2026-11-06. |
| M1 | M1 - Dev migration complete | 2026-11-06 | Dev cutover and rollback drills pass, runbook v1 corrected. Committed date 2026-11-13. |
| M2 | M2 - UAT complete | 2026-12-04 | All 25 tests pass, business sign-off on tier-0 DAGs. Extended validation continues through the Dec 15 - Jan 1 prod freeze. Committed date 2026-12-11. |
| M3 | M3 - Prod stack built and rehearsed | 2026-12-11 | Idle prod v3 stack healthy and rehearsal on a fresh clone done before the Dec 15 prod freeze. Committed date 2026-12-14. |
| M4 | M4 - Production cutover | 2027-01-10 | Cutover weekend Jan 9-10, 2027. Committed outer date 2027-01-24. |
| M5 | M5 - Stabilization and v2 standby complete | 2027-01-24 | 14 days of standby with no Sev1 for 7 days, no open Sev2 at day 14, then retire v2. Committed date 2027-02-07. |
| M6 | M6 - Phase 2 (NFS and Blob logging) | TBD | Starts only after stabilization sign-off. Dates to be set after M5. |

## Contents

- E1 Program governance and planning (6 issues)
- E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions) (15 issues)
- E3 v3 platform build blocks (reusable per environment) (12 issues)
- E4 Database migration (clone, trim, migrate, validate) (8 issues)
- E5 Dev environment migration and drills (6 issues)
- E6 UAT environment migration, full test matrix and drills (12 issues)
- E7 Production readiness and rehearsal (4 issues)
- E8 Production cutover (Jan 9-10, 2027) (9 issues)
- E9 Rollback readiness (5 issues)
- E10 Stabilization and v2 decommissioning (5 issues)
- E11 Phase 2 - NFS 4.1 premium share for DAGs (6 issues)
- E12 Phase 2 - Azure Blob remote logging (6 issues)

---

# EPIC E1 Program governance and planning

- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness

Epic description (copy):

```markdown
Roles, tiering, schedule, capacity and communications for the migration. This epic makes sure the date management hears is defensible and that every cutover role has a named person.
Plan sections 3, 4 and 15.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## G-01 - Confirm roles, RACI and decision authority

- Epic: E1
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::change-owner
- Depends on: none

Issue description (copy):

```markdown
Turn the role placeholders in the plan into named people. The change owner calls go/no-go and rollback, so a named deputy is required.

## Acceptance criteria
- [ ] Every role in the plan has a named person, including a deputy for the change owner
- [ ] Rollback authority is documented, including that the on-call engineer may pause DAGs and scale v3 to zero in a Sev1 without approval
- [ ] Availability for Nov 2026 to Jan 2027 is recorded per person (leave and holidays)
- [ ] RACI is published on the team wiki and linked from this issue

## Context
- Epic: E1 Program governance and planning
- Migration plan reference: Plan section 4 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## G-02 - Classify all DAGs into tiers 0, 1 and 2 with named owners

- Epic: E1
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::dag-owners
- Depends on: none

Issue description (copy):

```markdown
Tiers drive test depth, unpause order and rollback reconciliation. Export all DAGs and get an owner and tier for each.

## Acceptance criteria
- [ ] Export lists every DAG with owner, schedule, catchup, start_date, pool and 90-day success rate
- [ ] Every DAG has a tier (0, 1 or 2) and a named owner
- [ ] Tier-0 owners confirm they will join the cutover and rollback bridge
- [ ] DAGs with no owner or no recent runs are listed for a retire-or-keep decision

## Context
- Epic: E1 Program governance and planning
- Migration plan reference: Plan section 4 (DAG tiers) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## G-03 - Approve schedule, prod freeze calendar and cutover window

- Epic: E1
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::decision, role::change-owner
- Depends on: G-01

Issue description (copy):

```markdown
Agree the milestone dates with management: cutover target Jan 9-10, 2027, committed Jan 23-24, 2027. Prod freeze is Dec 15 to Dec 31; non-prod work continues during the freeze.

## Acceptance criteria
- [ ] Milestones M0 to M5 and their target and committed dates are approved by management
- [ ] Prod build and rehearsal are scheduled to finish by Dec 14
- [ ] Cutover window avoids month-end and any tier-0 SLA peaks
- [ ] CAB or change-advisory submission dates are booked
- [ ] The window length formula is recorded (1.5x rehearsal total + drain + 2h validation + rollback reserve) to be filled after rehearsal

## Depends on
- G-01 - Confirm roles, RACI and decision authority

## Context
- Epic: E1 Program governance and planning
- Migration plan reference: Plan sections 4 and 10 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## G-04 - Confirm capacity - quota, nodepool decision, database SKU headroom

- Epic: E1
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::decision, role::cloud, role::dba
- Depends on: none

Issue description (copy):

```markdown
Check that both stacks can coexist during rehearsals. Decide per environment whether v3 uses a new nodepool or a new namespace on the existing pool. A minimum idle v3 stack needs about 4 CPU and 15Gi of requests free if the pool is shared.

## Acceptance criteria
- [ ] Subscription vCPU quota is confirmed for the parallel period in each environment
- [ ] Nodepool decision (new pool or shared pool with ResourceQuota) is recorded per environment with its reason
- [ ] PostgreSQL facts are recorded: SKU vCores and memory, PgBouncer pool mode, max_client_conn, idle timeout, minimum pool size, and v2 peak connections
- [ ] Free storage on the DB server is at least 2.5x the database size if the same-server clone is used

## Context
- Epic: E1 Program governance and planning
- Migration plan reference: Plan sections 6.1, 7.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## G-05 - Assign risk owners and schedule re-baseline checkpoints

- Epic: E1
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::change-owner
- Depends on: none

Issue description (copy):

```markdown
Make the risk register actionable and set two checkpoints where dates are re-confirmed with management.

## Acceptance criteria
- [ ] All 22 risks have an owner and a next review date
- [ ] Checkpoint 1 (end of week 2, after DAG scan) and checkpoint 2 (after dev completes) are in calendars
- [ ] Management is briefed with target versus committed dates and the levers if dates must move
- [ ] Escalation path for a slipping milestone is written down

## Context
- Epic: E1 Program governance and planning
- Migration plan reference: Plan section 15 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## G-06 - Communications plan and hypercare rota

- Epic: E1
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::change-owner, role::ops
- Depends on: none

Issue description (copy):

```markdown
Prepare stakeholder communications and the on-call rota for the cutover and hypercare, including the weekend of Jan 9-10.

## Acceptance criteria
- [ ] Stakeholder and DAG-owner distribution lists exist
- [ ] Templates are ready for T-14d, T-2d, T-0 start, completion and rollback announcements
- [ ] Status page or announcement process is agreed
- [ ] Hypercare rota covers Jan 9 to Jan 24 including weekends with named backups
- [ ] Bridge details and escalation contacts are published

## Context
- Epic: E1 Program governance and planning
- Migration plan reference: Plan sections 10, 11 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)

- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness

Epic description (copy):

```markdown
Make the DAG estate, configuration and image ready for Airflow 3.3.2 while everything still runs on v2. DAG fixes ship to v2 first, so platform and code changes never land together. The runtime also moves from Python 3.11 (v2) to Python 3.13 (v3).
Plan section 5.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: G-02

Issue description (copy):

```markdown
Build the full inventory that every later decision depends on.

## Acceptance criteria
- [ ] Installed providers and versions are listed from the v2 image (pip freeze)
- [ ] Custom plugins, operators, hooks, airflow_local_settings.py, cluster policies and DAG callbacks are listed
- [ ] External REST API callers are listed with how each authenticates
- [ ] Connections, variables and secrets backends are listed and the Fernet key location is confirmed
- [ ] Auth setup (FAB roles, LDAP or OAuth config, user and role list) is captured

## Depends on
- G-02 - Classify all DAGs into tiers 0, 1 and 2 with named owners

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-02 - Capture v2 performance and reliability baselines

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng, role::dba
- Depends on: none

Issue description (copy):

```markdown
Without baselines the go/no-go thresholds in section 11 cannot be set. Capture 30 days of data.

## Acceptance criteria
- [ ] Baselines recorded for DAG parse time, scheduler loop time, queue latency, task success rate, worker CPU and memory
- [ ] DB connection counts and PgBouncer waiting clients and wait time are recorded
- [ ] Peak and p95 values are stored in a baseline document linked from this issue
- [ ] Numeric go/no-go thresholds for section 11 are proposed from these baselines

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-03 - Run Ruff AIR rules on the DAG repository and triage findings

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: R-01

Issue description (copy):

```markdown
Run ruff check --preview --select AIR30 on the DAG code. AIR301 and AIR302 are breaking changes; AIR311 and AIR312 are recommended and deferred until v2 is retired.

## Acceptance criteria
- [ ] The scan runs on the full DAG repository and the output is saved
- [ ] Findings are grouped by DAG owner and tier
- [ ] A sub-issue exists per owner team for AIR301 and AIR302 findings
- [ ] AIR311 and AIR312 findings are logged as deferred work

## Depends on
- R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-04 - Fix AIR301 and AIR302 findings with v2-compatible changes

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::dag-owners
- Depends on: R-03

Issue description (copy):

```markdown
Replace removed constructs with versions that work on both 2.10.5 and 3.3.2. Do not use imports that exist only in Airflow 3.

## Acceptance criteria
- [ ] Ruff reports zero AIR301 and AIR302 findings
- [ ] Each change is verified to run on 2.10.5 in v2 CI or dev
- [ ] No imports that exist only in Airflow 3 are used
- [ ] Changes are reviewed by the DAG owner and merged

## Depends on
- R-03 - Run Ruff AIR rules on the DAG repository and triage findings

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-05 - Manual review of changes Ruff cannot detect

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng, role::dag-owners
- Depends on: R-03

Issue description (copy):

```markdown
Search for direct metadata DB access from tasks (settings sessions, provide_session, raw ORM queries), SLAs, SubDAGs, execution_date, schedule_interval, provide_context, days_ago, airflow.contrib imports, custom XCom backends or pickled XComs, and callbacks or templates that use removed context keys.

## Acceptance criteria
- [ ] A search report lists every hit by DAG and category
- [ ] Each hit has a fix or a signed exception
- [ ] Direct DB access in tasks is refactored to supported interfaces
- [ ] Fixes are tested on v2 in dev

## Depends on
- R-03 - Run Ruff AIR rules on the DAG repository and triage findings

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-06 - Make schedule semantics explicit on every DAG

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::dag-owners
- Depends on: R-03

Issue description (copy):

```markdown
Airflow 3 changes catchup and cron data interval defaults. Set catchup, start_date and the schedule or timetable explicitly so behavior is identical on both versions.

## Acceptance criteria
- [ ] Every DAG sets catchup explicitly
- [ ] Schedule and start_date are explicit and reviewed with owners for tier-0 and tier-1 DAGs
- [ ] Run times for a sample of cron-scheduled DAGs are compared between v2 and v3 and differences are documented

## Depends on
- R-03 - Run Ruff AIR rules on the DAG repository and triage findings

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan sections 2, 5.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-07 - Python 3.13 dependency compatibility analysis

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: R-01

Issue description (copy):

```markdown
Resolve every dependency of DAGs, custom operators and plugins against the 3.3.2 constraints file for Python 3.13 (constraints-3.3.2/constraints-3.13.txt).

## Acceptance criteria
- [ ] pip freeze from the v2 (3.11) image is resolved against the Python 3.13 constraints with conflicts and resolutions listed
- [ ] Packages with native code have 3.13 wheels, or an alternative is chosen
- [ ] A code search for modules removed in Python 3.12 and 3.13 (distutils, imp, asyncore, asynchat, smtpd, cgi, cgitb, crypt, pipes, telnetlib, nntplib, imghdr, uu, xdrlib, lib2to3) is clean or fixed
- [ ] Internal libraries are published to Nexus for both Python 3.11 and 3.13
- [ ] Library version changes forced by the constraints are listed for regression testing

## Depends on
- R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.5 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-08 - CI matrix for Python 3.11 and 3.13 on the DAG repository

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::devops
- Depends on: R-07

Issue description (copy):

```markdown
A single DAG release must run on v2 (3.11) and v3 (3.13). Enforce that in CI until v2 is retired.

## Acceptance criteria
- [ ] CI runs on Python 3.11 and Python 3.13
- [ ] Each job compiles every DAG file, runs a DAG import test with zero import errors, runs unit tests and runs ruff
- [ ] Both jobs are required for merge
- [ ] The 3.11 job is kept until v2 is retired

## Depends on
- R-07 - Python 3.13 dependency compatibility analysis

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.5 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-09 - Provider, plugin and custom code compatibility

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: R-01

Issue description (copy):

```markdown
Resolve provider versions that support Airflow 3.3.2, choose the FAB provider version if needed, and test plugins and policies.

## Acceptance criteria
- [ ] Provider versions are resolved using the 3.3.2 constraints file and unused providers are removed
- [ ] Custom plugins and UI extensions are tested or have a rewrite or retire plan
- [ ] airflow_local_settings.py and cluster policies are reviewed for Airflow 3
- [ ] Custom operators and hooks pass import and unit tests on Python 3.13

## Depends on
- R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-10 - Decide auth manager and design role mapping

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::decision, role::security, role::airflow-eng
- Depends on: R-01

Issue description (copy):

```markdown
Airflow 3 moves FAB to a separate provider with a pluggable auth manager. Decide FAB provider versus an alternative, map roles, and decide how the service admin user is created.

## Acceptance criteria
- [ ] Decision record names the auth manager and provider version
- [ ] Existing roles, users, LDAP or OAuth settings map to the chosen manager with no loss of access control
- [ ] The default service user approach used in v2 values is replaced with a v3-supported method
- [ ] Existing users in the cloned DB are confirmed to keep working
- [ ] Security signs off the design

## Depends on
- R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan sections 2, 5.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-11 - Lint and update Airflow configuration

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: R-01

Issue description (copy):

```markdown
Run airflow config lint on the v2 configuration, then apply airflow config update to a copy to find removed and renamed settings.

## Acceptance criteria
- [ ] Lint output is reviewed and each finding has a decision
- [ ] The updated configuration is applied to a copy and the resulting changes are documented
- [ ] No removed or deprecated keys remain in the v3 values

## Depends on
- R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-12 - Build, scan and publish the Python 3.13 v3 image

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng, role::devops
- Depends on: R-07, R-09, R-11

Issue description (copy):

```markdown
One immutable image used by scheduler, api-server, dag-processor, triggerer, workers and the migration Job.

## Acceptance criteria
- [ ] The image is built on the official Python 3.13 variant of 3.3.2 (verify the exact tag exists) with extras installed through the 3.3.2 constraints file
- [ ] No DAGs are baked into the image
- [ ] The vulnerability scan passes agreed thresholds
- [ ] The image is referenced by digest everywhere
- [ ] A SBOM or package list is stored

## Depends on
- R-07 - Python 3.13 dependency compatibility analysis
- R-09 - Provider, plugin and custom code compatibility
- R-11 - Lint and update Airflow configuration

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.4 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-13 - Release DAG fixes to dev, UAT and prod (v2) and soak for a business cycle

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::dag-owners, role::devops
- Depends on: R-04, R-05, R-06, R-08

Issue description (copy):

```markdown
Release the v2-compatible fixes through Nexus in the normal order and let them run on v2 for at least one full business cycle.

## Acceptance criteria
- [ ] Fixes are live on v2 in dev, UAT and prod
- [ ] Task success rate is within 2 percentage points of the baseline for one business cycle
- [ ] No platform or DAG regression is open
- [ ] Release notes list every changed DAG

## Depends on
- R-04 - Fix AIR301 and AIR302 findings with v2-compatible changes
- R-05 - Manual review of changes Ruff cannot detect
- R-06 - Make schedule semantics explicit on every DAG
- R-08 - CI matrix for Python 3.11 and 3.13 on the DAG repository

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan section 5.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-14 - Inventory and plan migration of external API callers

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: R-01

Issue description (copy):

```markdown
Airflow 3 uses /api/v2 and a token-based flow. Every caller needs a migration plan and a handover time at cutover.

## Acceptance criteria
- [ ] Every caller is listed with owner and current authentication
- [ ] Each caller has a change plan for /api/v2 and the new token flow
- [ ] Each caller is tested against UAT before the prod cutover
- [ ] Handover time for each caller is agreed with its owner

## Depends on
- R-01 - Inventory DAGs, providers, plugins, connections, callers and auth setup

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan sections 2, 10 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## R-15 - Review significant changes from Airflow 3.0 through 3.3.2

- Epic: E2
- Milestone: M0 - Readiness complete
- Labels: airflow-v3, phase::readiness, type::task, role::airflow-eng
- Depends on: none

Issue description (copy):

```markdown
Moving from 2.10.5 crosses four minor versions. Read the significant-changes sections of each release note, including the 3.3.2 backfill endpoint change and the 3.3.0 changes to remote logging resolution, bundle versioning and example DAG bundles.

## Acceptance criteria
- [ ] A document lists each significant change from 3.0 to 3.3.2 with impact (none, low, action needed)
- [ ] Items needing action are logged as issues or added to existing issues
- [ ] Plan open items in appendix D are updated

## Context
- Epic: E2 Phase 0 readiness (DAGs, Python 3.13, image, decisions)
- Migration plan reference: Plan appendix D item 4 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E3 v3 platform build blocks (reusable per environment)

- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev

Epic description (copy):

```markdown
The reusable building blocks for the idle v3 stack: Helm values, sizing, namespace and nodepool, secrets, Redis, database roles and PgBouncer endpoints, storage, Nexus dual-deploy, ingress, observability and a smoke suite. Built and proven in dev, then repeated in UAT and prod.
Plan sections 3 and 6.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## P-01 - Helm chart and values baseline for Airflow 3.3.2

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::airflow-eng
- Depends on: R-11

Issue description (copy):

```markdown
Choose the chart version that supports Airflow 3.3.2 and build the values skeleton, per-environment overlays and differences from the v2 values (apiServer replaces webserver, dagProcessor is new, JWT and API secret references, execution API URL).

## Acceptance criteria
- [ ] The chart version that supports 3.3.2 is verified in its release notes and recorded
- [ ] helm template and a dry run succeed for dev, UAT and prod overlays
- [ ] A table lists every difference from the v2 values and why
- [ ] External Redis, external DB secrets, existing claims and tolerations are expressed in values
- [ ] Scheduler, triggerer and workers can be set to zero replicas for the idle build

## Depends on
- R-11 - Lint and update Airflow configuration

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan section 6.7 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-02 - Size v3 components from the current v2 resource configuration

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::decision, role::airflow-eng, role::cloud
- Depends on: R-02, G-04

Issue description (copy):

```markdown
Starting point from the v2 values: workers up to 4 x 1 CPU / 5Gi (limit 10Gi), api-server 2 to 3 x 500m / 3Gi (limit 6Gi), scheduler fixed at 2 x 500m / 1Gi (limit 2Gi), dag-processor 1 to 2 x 500m / 1Gi (limit 3Gi), triggerer 2 x 500m / 1Gi (limit 2Gi). Expect about 8.5 CPU and 35Gi of requests at peak. Replace with measured p95 data and load-test results.

## Acceptance criteria
- [ ] Sizes are approved by platform and cloud engineers using measured p95 usage
- [ ] How workers and other components autoscale in the chart is confirmed (KEDA, HPA or the v2 autoscaler setting) and worker scale-in respects the longest task (termination grace period)
- [ ] Scheduler replicas are pinned to 2 in v3 with no autoscaling
- [ ] The database connection budget fits the PgBouncer pools
- [ ] Sizing is revisited after the UAT load test

## Depends on
- R-02 - Capture v2 performance and reliability baselines
- G-04 - Confirm capacity - quota, nodepool decision, database SKU headroom

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan section 6.1 and sizing discussion (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-03 - Namespace, nodepool, quotas, PDBs and network policies

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::cloud
- Depends on: G-04

Issue description (copy):

```markdown
Create the v3 namespace and the placement decided in G-04. If the pool is shared, add a ResourceQuota and make sure the autoscaler maximum covers both stacks during overlap.

## Acceptance criteria
- [ ] Namespace airflow-v3 exists with quota and LimitRange
- [ ] Nodeselector and tolerations match the placement decision
- [ ] PDBs exist for api-server, scheduler, dag-processor and workers
- [ ] Network policies allow workers to api-server (execution API), all components to DB and Redis, and ingress to api-server only
- [ ] AKS and node image upgrades are blocked during rehearsal and cutover windows

## Depends on
- G-04 - Confirm capacity - quota, nodepool decision, database SKU headroom

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan sections 6.1, 6.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-04 - Secrets for v3 (Fernet reuse, new JWT and API keys, Key Vault)

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::airflow-eng, role::security
- Depends on: none

Issue description (copy):

```markdown
v3 reuses the v2 Fernet key and nothing else. JWT secret and API secret key are new. Connection strings are separate for pooled and direct endpoints.

## Acceptance criteria
- [ ] Fernet key is the same as v2
- [ ] JWT secret and API secret key are new, random and shared by the right components only
- [ ] DB, broker and result backend secrets point to v3 resources only
- [ ] Secrets come from Key Vault or Kubernetes secrets and never appear in Git or Helm values
- [ ] A decrypt test of a sample connection and variable passes
- [ ] A rotation procedure is written

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan section 6.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-05 - New Redis instance pattern for v3

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::cloud
- Depends on: none

Issue description (copy):

```markdown
A new Redis per environment so v2 queue messages can never reach v3 workers.

## Acceptance criteria
- [ ] New Azure Cache for Redis instance with TLS and private endpoint or VNet rules
- [ ] Eviction settings do not evict Celery queue keys
- [ ] Credentials and queue names are separate from v2
- [ ] The v3 nodepool can reach Redis and v2 cannot use it

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan section 6.4 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-06 - v3 database roles and PgBouncer database and user pair

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::dba
- Depends on: G-04

Issue description (copy):

```markdown
Create the v3 owner role (with CREATEDB if cloning on the same server) and an application role, plus a separate PgBouncer database and user pair so v2 and v3 each get their own pool. Runtime uses port 6432 and migration and admin jobs use port 5432.

## Acceptance criteria
- [ ] Owner and application roles exist with least privilege
- [ ] PgBouncer has a v3 pair with initial pool size 60
- [ ] Both ports are reachable from the v3 namespace
- [ ] Runtime connection strings use 6432 and the migration Job uses 5432
- [ ] How the pool size is applied (per database and user pair) is confirmed with the DBA

## Depends on
- G-04 - Confirm capacity - quota, nodepool decision, database SKU headroom

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan sections 6.5, 7.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-07 - Azure Files for v3 (DAG decision, logs share, PV/PVC, ownership)

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::cloud
- Depends on: none

Issue description (copy):

```markdown
Logs always get a separate v3 share and path so v2 and v3 cannot overwrite each other after a rollback. For DAGs decide per environment between a separate share or sharing the existing one (read-only for v3 during rehearsals, with a snapshot before cutover).

## Acceptance criteria
- [ ] A decision record covers the DAG share for each environment with risks accepted
- [ ] A new logs share exists with PV and PVC, ReadWriteMany and Retain
- [ ] The ownership Job sets UID 50000 and GID 0
- [ ] Mounts follow the plan (DAGs on scheduler, dag-processor, triggerer and workers; logs on all components including api-server)
- [ ] If the DAG share is shared, a snapshot is taken before cutover and the v3 mount is read-only until cutover

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan sections 3, 6.6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-08 - Nexus release pipeline deploys one DAG version to all target shares

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::devops
- Depends on: none

Issue description (copy):

```markdown
Extend the existing process so one released DAG package version is deployed to the v2 and v3 targets from one pipeline, with versioned folders and a pointer to the current release.

## Acceptance criteria
- [ ] A single release deploys to every configured target
- [ ] Folders are versioned and a current pointer exists per target
- [ ] A checksum job compares targets and alerts on drift
- [ ] Redeploying the previous version works as the DAG rollback and is tested
- [ ] Releases are checked against Python 3.11 and 3.13 in CI before deploy

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan section 6.8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-09 - Ingress, DNS, TLS and SSO for v3

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::cloud, role::security
- Depends on: none

Issue description (copy):

```markdown
A test hostname for v3, SSO redirect URIs for both hostnames, and a plan to lower the production DNS TTL to 60 seconds at T-2 days.

## Acceptance criteria
- [ ] A test hostname with TLS reaches the v3 api-server
- [ ] SSO redirect URIs for the v3 and v2 hostnames are registered
- [ ] The previous production DNS or ingress values are recorded for rollback
- [ ] The v2 ingress object is kept and not removed
- [ ] The TTL change procedure is documented

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan sections 6.2, 10.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-10 - Observability for v3 (metrics, logs, dashboards, alerts, probes)

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::ops, role::airflow-eng
- Depends on: none

Issue description (copy):

```markdown
Dashboards and alerts that key off the webserver must move to the api-server and dag-processor. Add probes and PgBouncer wait metrics.

## Acceptance criteria
- [ ] Probes are in place (api-server health, scheduler, dag-processor and triggerer job checks)
- [ ] Dashboards show every signal in the go/no-go table in section 11
- [ ] Alerts are created for each Sev1 trigger and tested by forcing a failure
- [ ] PgBouncer waiting clients and wait time are visible

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan sections 8 (test 19), 11 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-11 - Smoke DAG and platform smoke test suite

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::airflow-eng
- Depends on: none

Issue description (copy):

```markdown
A DAG with schedule none that works on v2 and v3 and proves the platform end to end.

## Acceptance criteria
- [ ] The DAG uses one connection, one variable and one pool, pushes and pulls an XCom, writes a log and fires a callback
- [ ] It runs on v2 and v3 from the same release
- [ ] A script also checks the health endpoint, a token API call and the log view
- [ ] The smoke suite is documented in the runbook

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan section 10.2 (Phase D) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## P-12 - Isolation checklist automation and sign-off per environment

- Epic: E3
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::airflow-eng
- Depends on: P-03, P-04, P-05, P-06, P-07

Issue description (copy):

```markdown
Automate the appendix A checks so isolation is proved, not assumed, before every rehearsal and cutover.

## Acceptance criteria
- [ ] A job checks separate DB, DB roles, Redis, share paths, secrets, image and hostnames, and that the other stack has zero schedulers running
- [ ] Result is attached to each change record
- [ ] The check must pass before a rehearsal or cutover starts

## Depends on
- P-03 - Namespace, nodepool, quotas, PDBs and network policies
- P-04 - Secrets for v3 (Fernet reuse, new JWT and API keys, Key Vault)
- P-05 - New Redis instance pattern for v3
- P-06 - v3 database roles and PgBouncer database and user pair
- P-07 - Azure Files for v3 (DAG decision, logs share, PV/PVC, ownership)

## Context
- Epic: E3 v3 platform build blocks (reusable per environment)
- Migration plan reference: Plan appendix A (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E4 Database migration (clone, trim, migrate, validate)

- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev

Epic description (copy):

```markdown
The v3 database is always a migrated copy of the v2 database, so a failed or slow migration costs only the clone. Direct 2.10.5 to 3.3.2 is the chosen path; a hop through 2.11.x on the clone is a fallback only.
Plan section 7.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## D-01 - Choose and prove the clone method

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::decision, role::dba
- Depends on: G-04, P-06

Issue description (copy):

```markdown
Compare same-server CREATE DATABASE ... TEMPLATE, point-in-time restore to a new server, and pg_dump or pg_restore. Measure each in dev and UAT and record the decision for prod.

## Acceptance criteria
- [ ] Permission needed for CREATE DATABASE ... TEMPLATE on Azure Database for PostgreSQL Flexible Server is confirmed (CREATEDB and membership in the source owner role)
- [ ] Clone times for each viable option are measured in dev and UAT
- [ ] Decision for production is recorded with storage and IOPS headroom evidence
- [ ] Clone naming convention for rehearsals is agreed (for example airflow_v3_rehearsal_YYYYMMDD_n)

## Depends on
- G-04 - Confirm capacity - quota, nodepool decision, database SKU headroom
- P-06 - v3 database roles and PgBouncer database and user pair

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan section 7.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-02 - Backup and restore procedures verified

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::dba
- Depends on: none

Issue description (copy):

```markdown
Belt and braces for the v2 database, even though rollback never needs a restore.

## Acceptance criteria
- [ ] An on-demand backup of the v2 server is taken with the Azure CLI and recorded
- [ ] A point-in-time restore is tested in dev and its duration recorded
- [ ] A logical dump (pg_dump -Fc) is taken to storage outside the cluster and readable with pg_restore --list
- [ ] Backup retention is 14 days or more

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan section 7.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-03 - History trim policy and airflow db clean rehearsal

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::dba, role::airflow-eng
- Depends on: D-01

Issue description (copy):

```markdown
Trim old history on the clone before migrating so it migrates faster. Run with the v2 image on the direct port with --skip-archive. The full history stays in v2.

## Acceptance criteria
- [ ] Retention period (for example 90 days) is agreed with business and compliance
- [ ] The cleanup runs on a clone and its duration is recorded
- [ ] The most recent scheduled run per DAG survives the cleanup so scheduling state is preserved (verified)
- [ ] Migration time with and without trimming is compared

## Depends on
- D-01 - Choose and prove the clone method

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan section 7.5 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-04 - Migration Job automation

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::airflow-eng, role::dba
- Depends on: R-12, P-06

Issue description (copy):

```markdown
A Kubernetes Job template that runs airflow db migrate with the v3 image against the clone on the direct port, from inside the cluster only.

## Acceptance criteria
- [ ] The Job runs airflow db migrate then airflow db check-migrations
- [ ] Start time, end time and full log are captured to durable storage
- [ ] A failed run is handled by dropping the clone and redoing, never by repairing in place
- [ ] The Job is never run from a laptop

## Depends on
- R-12 - Build, scan and publish the Python 3.13 v3 image
- P-06 - v3 database roles and PgBouncer database and user pair

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan section 7.6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-05 - Prove direct 2.10.5 to 3.3.2 migration in dev (decision gate)

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::decision, role::airflow-eng, role::dba
- Depends on: D-03, D-04

Issue description (copy):

```markdown
Direct is the preferred path. Migrate a clone directly, run the full validation and a DAG regression. Fall back to a hop through the latest 2.11.x on the clone only if migration fails, the 3.3.2 documentation names a minimum starting version that 2.10.5 does not meet, or validation shows data problems.

## Acceptance criteria
- [ ] The 3.3.2 documentation is checked for the supported starting version and the result is recorded
- [ ] A direct migration of a dev clone passes the validation list (D-06) and a DAG regression
- [ ] Decision is recorded as direct, or as a hop with its added time measured
- [ ] Any hop runs on the clone only and never on the v2 database

## Depends on
- D-03 - History trim policy and airflow db clean rehearsal
- D-04 - Migration Job automation

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan sections 3 (D10), 7.6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-06 - Validation automation for the migrated database

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::dba, role::airflow-eng
- Depends on: D-04

Issue description (copy):

```markdown
Automate the checks that prove the migrated clone is usable and export the DAG paused states needed for the controlled start and rollback.

## Acceptance criteria
- [ ] Schema version equals the head revision from a fresh airflow db migrate on an empty database
- [ ] Row counts match post-clean expectations for dag, dag_run, task_instance, connection, variable, slot_pool and FAB user and role tables
- [ ] No task instances are in running, queued, scheduled, up_for_retry or deferred states
- [ ] Three sample connections and variables decrypt and stable connections pass airflow connections test
- [ ] dag_id and is_paused are exported to CSV before migration

## Depends on
- D-04 - Migration Job automation

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan section 7.7 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-07 - Cutover DB handling for PgBouncer leftovers

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::dba
- Depends on: P-06

Issue description (copy):

```markdown
PgBouncer keeps idle server connections after clients leave, which blocks CREATE DATABASE ... TEMPLATE. Script the connection check, the termination of leftovers and the re-check, on the direct port, only after v2 is stopped.

## Acceptance criteria
- [ ] A script counts sessions on the source database and terminates leftovers with pg_terminate_backend after v2 is stopped
- [ ] The script re-checks until the count is zero and then clones
- [ ] If PgBouncer has a minimum pool size, the DBA documents how to disable the v2 database in PgBouncer during the clone
- [ ] The script is tested in dev and UAT

## Depends on
- P-06 - v3 database roles and PgBouncer database and user pair

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan sections 7.4, 10.2 (Phase B) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## D-08 - Timing record and window sizing method

- Epic: E4
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::task, role::dba, role::change-owner
- Depends on: D-04

Issue description (copy):

```markdown
Fill the timing table in every rehearsal and use it to size the production window.

## Acceptance criteria
- [ ] Timing table (DB size, clone, trim, migrate, validate, total) is filled for dev, UAT, prod rehearsal and prod cutover
- [ ] Production window is computed as 1.5x the prod rehearsal total plus drain plus 2h validation plus rollback reserve
- [ ] The window is approved by the change owner

## Depends on
- D-04 - Migration Job automation

## Context
- Epic: E4 Database migration (clone, trim, migrate, validate)
- Migration plan reference: Plan section 7.8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E5 Dev environment migration and drills

- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev

Epic description (copy):

```markdown
Dev proves the mechanics: build, clone, migrate, cutover and rollback. Its output is a corrected runbook.
Plan sections 8 and 9.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## DEV-01 - Build the idle v3 stack in dev

- Epic: E5
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev, type::task, role::airflow-eng, role::cloud
- Depends on: R-12, P-01, P-02, P-03, P-04, P-05, P-06, P-07, P-08, P-09, P-12

Issue description (copy):

```markdown
Build the whole v3 stack in dev with scheduler, triggerer and workers at zero or all DAGs paused.

## Acceptance criteria
- [ ] helm template and a dry run succeed and all pods schedule on the designated placement
- [ ] Isolation checklist (P-12) passes and is signed
- [ ] The DAG share holds the current release with matching checksums
- [ ] Secrets verified, probes healthy, no restarts in 24 hours

## Depends on
- R-12 - Build, scan and publish the Python 3.13 v3 image
- P-01 - Helm chart and values baseline for Airflow 3.3.2
- P-02 - Size v3 components from the current v2 resource configuration
- P-03 - Namespace, nodepool, quotas, PDBs and network policies
- P-04 - Secrets for v3 (Fernet reuse, new JWT and API keys, Key Vault)
- P-05 - New Redis instance pattern for v3
- P-06 - v3 database roles and PgBouncer database and user pair
- P-07 - Azure Files for v3 (DAG decision, logs share, PV/PVC, ownership)
- P-08 - Nexus release pipeline deploys one DAG version to all target shares
- P-09 - Ingress, DNS, TLS and SSO for v3
- P-12 - Isolation checklist automation and sign-off per environment

## Context
- Epic: E5 Dev environment migration and drills
- Migration plan reference: Plan section 6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## DEV-02 - Clone, trim, migrate and validate the dev database

- Epic: E5
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev, type::task, role::dba, role::airflow-eng
- Depends on: DEV-01, D-01, D-02, D-03, D-04, D-06

Issue description (copy):

```markdown
Run the full database procedure end to end in dev, including a deliberate failed-migration drill.

## Acceptance criteria
- [ ] The validation list passes on the migrated dev clone
- [ ] Timings are recorded in the timing table
- [ ] A failed-migration drill (drop the clone and redo) succeeds while the v2 database stays unchanged

## Depends on
- DEV-01 - Build the idle v3 stack in dev
- D-01 - Choose and prove the clone method
- D-02 - Backup and restore procedures verified
- D-03 - History trim policy and airflow db clean rehearsal
- D-04 - Migration Job automation
- D-06 - Validation automation for the migrated database

## Context
- Epic: E5 Dev environment migration and drills
- Migration plan reference: Plan section 7 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## DEV-03 - Platform and DAG import tests in dev (tests 1-6 and 21)

- Epic: E5
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev, type::test, role::qa
- Depends on: DEV-02, P-10, P-11

Issue description (copy):

```markdown
Pod health and probes, restarts, clone and migrate validation, DAG import with zero errors, and storage and deployment checks.

## Acceptance criteria
- [ ] Tests 1 to 6 and 21 pass with evidence attached
- [ ] DAG import errors are zero and parse time is recorded against the v2 baseline
- [ ] Releases deploy to all targets with matching checksums

## Depends on
- DEV-02 - Clone, trim, migrate and validate the dev database
- P-10 - Observability for v3 (metrics, logs, dashboards, alerts, probes)
- P-11 - Smoke DAG and platform smoke test suite

## Context
- Epic: E5 Dev environment migration and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## DEV-04 - Dev cutover drill using the production runbook

- Epic: E5
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev, type::drill, role::change-owner, role::airflow-eng
- Depends on: DEV-03, D-07, G-06

Issue description (copy):

```markdown
Execute section 10 phases A to G in dev, including drain, clone, migrate, controlled unpause and traffic switch. Someone other than the runbook author drives.

## Acceptance criteria
- [ ] All phases and checkpoints are executed and timed
- [ ] The drain, PgBouncer cleanup and controlled start steps work as written
- [ ] Every defect or unclear step is logged and fixed in the runbook
- [ ] External API caller handover is exercised with a test caller

## Depends on
- DEV-03 - Platform and DAG import tests in dev (tests 1-6 and 21)
- D-07 - Cutover DB handling for PgBouncer leftovers
- G-06 - Communications plan and hypercare rota

## Context
- Epic: E5 Dev environment migration and drills
- Migration plan reference: Plan section 10 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## DEV-05 - Dev rollback drill and abort paths A, B and C

- Epic: E5
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev, type::drill, role::change-owner, role::airflow-eng
- Depends on: DEV-04, RB-01

Issue description (copy):

```markdown
Roll back after the dev cutover and test the pre-switch abort paths.

## Acceptance criteria
- [ ] v2 serves traffic again within 30 minutes of the rollback decision
- [ ] Abort paths A, B and C each return to v2 cleanly
- [ ] Proof the v2 database is unmodified (schema version and row counts equal the T0 record)
- [ ] A basic reconciliation of v3-window runs is performed

## Depends on
- DEV-04 - Dev cutover drill using the production runbook
- RB-01 - Finalize rollback runbook (stages 1 to 5)

## Context
- Epic: E5 Dev environment migration and drills
- Migration plan reference: Plan sections 10.3, 12 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## DEV-06 - Dev sign-off and runbook v1.1

- Epic: E5
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, env::dev, type::gate, role::change-owner
- Depends on: DEV-05, D-05

Issue description (copy):

```markdown
Close dev with the exit criteria and update the plan with what was learned.

## Acceptance criteria
- [ ] Dev exit criteria in section 9 are met
- [ ] Direct versus hop migration decision is recorded
- [ ] Runbook v1.1 is published with all fixes
- [ ] Open gaps carried to UAT are listed and owned
- [ ] Checkpoint 2 date re-baseline is presented to management

## Depends on
- DEV-05 - Dev rollback drill and abort paths A, B and C
- D-05 - Prove direct 2.10.5 to 3.3.2 migration in dev (decision gate)

## Context
- Epic: E5 Dev environment migration and drills
- Migration plan reference: Plan section 9 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E6 UAT environment migration, full test matrix and drills

- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat

Epic description (copy):

```markdown
UAT proves the DAG estate, the timings and the rollback with production-like data. Testing continues through the Dec 15 to Jan 1 prod freeze to use that time.
Plan sections 8 and 9.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## UAT-01 - Build the idle v3 stack in UAT

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::task, role::airflow-eng, role::cloud
- Depends on: DEV-06

Issue description (copy):

```markdown
Same build as dev, with sizing from dev findings.

## Acceptance criteria
- [ ] Isolation checklist passes and is signed
- [ ] Pods are healthy with no restarts in 24 hours
- [ ] DAG share, secrets, Redis, PgBouncer pair and ingress are in place
- [ ] Sizing differences from dev are recorded

## Depends on
- DEV-06 - Dev sign-off and runbook v1.1

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-02 - Clone, trim, migrate and validate the UAT database (timed)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::task, role::dba
- Depends on: UAT-01

Issue description (copy):

```markdown
Use a clone of the UAT database that is as close to production in size and shape as possible and record timings.

## Acceptance criteria
- [ ] Validation list passes
- [ ] Timings are recorded and compared with dev
- [ ] Production clone method is confirmed from UAT evidence

## Depends on
- UAT-01 - Build the idle v3 stack in UAT

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 7 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-03 - Tier-0 DAG regression and scheduling semantics (tests 7, 9, 10)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::qa, role::dag-owners
- Depends on: UAT-02

Issue description (copy):

```markdown
Run every tier-0 DAG end to end against sandbox targets, and check schedules, catchup, start_date, pools, priorities and concurrency limits.

## Acceptance criteria
- [ ] Every tier-0 DAG matches v2 outputs and status
- [ ] Schedules produce the run times the owner expects
- [ ] Pool, priority weight, concurrency and max_active_runs limits behave as on v2
- [ ] Evidence is attached and each DAG owner signs

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-04 - Tier-1 and tier-2 regression, connections, secrets and plugins (tests 8, 11, 12, 16)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::qa, role::dag-owners
- Depends on: UAT-02

Issue description (copy):

```markdown
Cover each operator, sensor, deferrable and dynamic mapping pattern in use, every connection system class, secrets backend lookups and custom plugins.

## Acceptance criteria
- [ ] Representative tier-1 and tier-2 DAGs match v2 outcomes
- [ ] Each connection class passes airflow connections test
- [ ] Secrets backend lookups work from workers and the dag-processor
- [ ] Custom plugins and UI extensions load and work

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-05 - Authentication, RBAC and external API callers (tests 13, 14)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::security, role::qa
- Depends on: UAT-02, R-10, R-14

Issue description (copy):

```markdown
Verify login, role mapping and permissions, and that every external caller works against /api/v2 with the new token flow.

## Acceptance criteria
- [ ] Login, roles and permissions are mapped from v2 with a least-privilege review
- [ ] Each external caller passes in UAT
- [ ] Security signs the role mapping

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)
- R-10 - Decide auth manager and design role mapping
- R-14 - Inventory and plan migration of external API callers

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-06 - Logs, observability and alert tests (tests 15, 19)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::ops, role::qa
- Depends on: UAT-02, P-10

Issue description (copy):

```markdown
Task logs for success, failure, retry and running tasks, plus dashboards and alerts for the new components.

## Acceptance criteria
- [ ] Logs are readable within seconds for every task state
- [ ] Each Sev1 alert fires in a forced-failure test
- [ ] Dashboards show all go/no-go signals from section 11

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)
- P-10 - Observability for v3 (metrics, logs, dashboards, alerts, probes)

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-07 - Performance and load test including PgBouncer waits (tests 17, 18)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::airflow-eng, role::dba
- Depends on: UAT-02, R-02, P-02

Issue description (copy):

```markdown
Run a peak-hour DAG count and concurrency. Compare parse time, scheduler loop time, task start latency and utilization with the v2 baseline, and watch PgBouncer waiting clients.

## Acceptance criteria
- [ ] Metrics are within 10 percent of baseline or the difference is explained
- [ ] No queue starvation and DB CPU stays inside limits
- [ ] PgBouncer waiting clients and wait time stay low, or the v3 pool size is raised to 80 to 100 with SKU headroom confirmed
- [ ] Final sizing for production is recorded

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)
- R-02 - Capture v2 performance and reliability baselines
- P-02 - Size v3 components from the current v2 resource configuration

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-08 - Security review (test 20)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::security
- Depends on: UAT-02

Issue description (copy):

```markdown
Network policies, TLS, JWT secret rotation and image scan.

## Acceptance criteria
- [ ] Network policies are enforced as designed (tested from a pod)
- [ ] TLS is verified on every external endpoint
- [ ] JWT secret rotation is tested without extended outage
- [ ] Image scan is clean at the agreed threshold
- [ ] Security sign-off is recorded

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-09 - Python 3.13 regression across the DAG estate (test 25)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::qa, role::dag-owners
- Depends on: UAT-02, R-08

Issue description (copy):

```markdown
Prove that the same DAG release runs on Python 3.11 (v2) and Python 3.13 (v3), including library versions changed by the constraints file.

## Acceptance criteria
- [ ] DAG import, unit tests and dependency installs pass on both versions from the same release
- [ ] No removed-module imports remain
- [ ] Behavior changes from library version drift are tested for tier-0 and tier-1 DAGs

## Depends on
- UAT-02 - Clone, trim, migrate and validate the UAT database (timed)
- R-08 - CI matrix for Python 3.11 and 3.13 on the DAG repository

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 5.5 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-10 - Timed cutover drill and rollback drill with reconciliation (tests 22-24)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::drill, role::change-owner, role::airflow-eng
- Depends on: UAT-03, UAT-04, UAT-07, RB-01, RB-02

Issue description (copy):

```markdown
Run cutover, let v3 run sample DAGs for about 2 hours, then roll back with reconciliation. Repeat with a different engineer reading the runbook.

## Acceptance criteria
- [ ] Cutover and rollback are timed and within the plan
- [ ] Service is restored within 30 minutes of the rollback decision
- [ ] No duplicates or gaps on sample tier-0 DAGs after reconciliation
- [ ] The technique for marking missed intervals complete in v2 is validated
- [ ] A second engineer completes the drill with no tribal knowledge

## Depends on
- UAT-03 - Tier-0 DAG regression and scheduling semantics (tests 7, 9, 10)
- UAT-04 - Tier-1 and tier-2 regression, connections, secrets and plugins (tests 8, 11, 12, 16)
- UAT-07 - Performance and load test including PgBouncer waits (tests 17, 18)
- RB-01 - Finalize rollback runbook (stages 1 to 5)
- RB-02 - Reconciliation procedure and export scripts for the v3 window

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan sections 10, 12.6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-11 - Extended UAT validation during the prod freeze (Dec 15 to Dec 31)

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::test, role::qa, role::airflow-eng
- Depends on: UAT-10

Issue description (copy):

```markdown
Use the prod freeze period for longer soak: month-end and weekly cycles, chaos tests and a repeat rollback drill. Account for reduced staffing.

## Acceptance criteria
- [ ] A full set of weekly and month-end schedules runs on v3 in UAT
- [ ] Restart, eviction and node drain tests (tests 2 and 3) pass
- [ ] A repeat rollback drill passes with a different engineer
- [ ] Findings are logged and fixed before the Jan 4 mini-rehearsal

## Depends on
- UAT-10 - Timed cutover drill and rollback drill with reconciliation (tests 22-24)

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan section 8 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## UAT-12 - UAT exit report, business sign-off and production window sizing

- Epic: E6
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::uat, env::uat, type::gate, role::change-owner, role::dag-owners
- Depends on: UAT-03, UAT-04, UAT-05, UAT-06, UAT-07, UAT-08, UAT-09, UAT-10, D-08

Issue description (copy):

```markdown
Confirm that all 25 tests pass or have a documented exception signed by the change owner, and size the production window from measured timings.

## Acceptance criteria
- [ ] Test results are published with evidence
- [ ] Business owners sign off tier-0 DAGs
- [ ] Production window is sized from timing data (D-08)
- [ ] Go for the production idle build is recorded

## Depends on
- UAT-03 - Tier-0 DAG regression and scheduling semantics (tests 7, 9, 10)
- UAT-04 - Tier-1 and tier-2 regression, connections, secrets and plugins (tests 8, 11, 12, 16)
- UAT-05 - Authentication, RBAC and external API callers (tests 13, 14)
- UAT-06 - Logs, observability and alert tests (tests 15, 19)
- UAT-07 - Performance and load test including PgBouncer waits (tests 17, 18)
- UAT-08 - Security review (test 20)
- UAT-09 - Python 3.13 regression across the DAG estate (test 25)
- UAT-10 - Timed cutover drill and rollback drill with reconciliation (tests 22-24)
- D-08 - Timing record and window sizing method

## Context
- Epic: E6 UAT environment migration, full test matrix and drills
- Migration plan reference: Plan sections 8, 9 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E7 Production readiness and rehearsal

- Milestone: M3 - Prod stack built and rehearsed
- Labels: airflow-v3, phase::prod, env::prod

Epic description (copy):

```markdown
Build the idle v3 stack in production, rehearse on a fresh clone of the production database, and pass go/no-go 1 before the Dec 15 freeze. A mini-rehearsal after the freeze confirms nothing drifted.
Plan sections 9 and 10.1.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## PR-01 - Build the idle v3 stack in production

- Epic: E7
- Milestone: M3 - Prod stack built and rehearsed
- Labels: airflow-v3, phase::prod, env::prod, type::task, role::airflow-eng, role::cloud
- Depends on: UAT-12

Issue description (copy):

```markdown
Build v3 in production with no traffic and no running scheduler. Watch v2 resource use during the build.

## Acceptance criteria
- [ ] Isolation checklist passes and is signed
- [ ] Quota, placement and autoscaler limits are confirmed with v2 headroom unaffected
- [ ] v3 has zero scheduler replicas or all DAGs paused and receives no user traffic
- [ ] Done and healthy before Dec 14

## Depends on
- UAT-12 - UAT exit report, business sign-off and production window sizing

## Context
- Epic: E7 Production readiness and rehearsal
- Migration plan reference: Plan section 6 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## PR-02 - Production rehearsal on a fresh clone of the production database

- Epic: E7
- Milestone: M3 - Prod stack built and rehearsed
- Labels: airflow-v3, phase::prod, env::prod, type::drill, role::dba, role::airflow-eng
- Depends on: PR-01, D-01, D-07

Issue description (copy):

```markdown
Clone production data, trim, migrate and validate with all DAGs paused and no side-effect connections reachable. Run tests 1, 4, 6, 17, 21 and 22.

## Acceptance criteria
- [ ] Rehearsal uses a fresh clone and all DAGs are paused on the clone
- [ ] Timings are recorded and fit the window
- [ ] Tests 1, 4, 6, 17, 21 and 22 pass
- [ ] The rehearsal clone is dropped after the review
- [ ] Done before Dec 14

## Depends on
- PR-01 - Build the idle v3 stack in production
- D-01 - Choose and prove the clone method
- D-07 - Cutover DB handling for PgBouncer leftovers

## Context
- Epic: E7 Production readiness and rehearsal
- Migration plan reference: Plan section 9 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## PR-03 - Go/no-go 1 and change approvals

- Epic: E7
- Milestone: M3 - Prod stack built and rehearsed
- Labels: airflow-v3, phase::prod, env::prod, type::gate, role::change-owner
- Depends on: PR-02, G-03

Issue description (copy):

```markdown
Decide whether to proceed to the cutover date, and obtain formal change approval.

## Acceptance criteria
- [ ] Timings fit the window, tests pass and the UAT rollback drill is current
- [ ] v3 idle stack is healthy
- [ ] Change approval (CAB) is recorded for the cutover window
- [ ] Decision and attendees are recorded

## Depends on
- PR-02 - Production rehearsal on a fresh clone of the production database
- G-03 - Approve schedule, prod freeze calendar and cutover window

## Context
- Epic: E7 Production readiness and rehearsal
- Migration plan reference: Plan section 9 (gates) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## PR-04 - Mini-rehearsal after the freeze (Jan 4 to Jan 6)

- Epic: E7
- Milestone: M3 - Prod stack built and rehearsed
- Labels: airflow-v3, phase::prod, env::prod, type::drill, role::dba, role::airflow-eng
- Depends on: PR-03

Issue description (copy):

```markdown
Re-clone, migrate for timing, and smoke test to prove nothing drifted over the freeze.

## Acceptance criteria
- [ ] Fresh clone migrates within the rehearsal timing
- [ ] No config, quota, image or version drift since Dec 14 (verified against recorded values)
- [ ] Smoke DAG and isolation checklist pass
- [ ] Result feeds go/no-go 2

## Depends on
- PR-03 - Go/no-go 1 and change approvals

## Context
- Epic: E7 Production readiness and rehearsal
- Migration plan reference: Plan section 4 (schedule) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E8 Production cutover (Jan 9-10, 2027)

- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod

Epic description (copy):

```markdown
Execute the production cutover runbook: drain and stop v2, clone and migrate the database, start v3 with all DAGs paused, switch traffic, then unpause in tiers. One stack runs at a time.
Plan section 10.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## C-01 - T-14 to T-3 days - freezes, approvals, comms and rota confirmed

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::task, role::change-owner
- Depends on: PR-03, G-06

Issue description (copy):

```markdown
Freeze infrastructure changes on both stacks, confirm approvals, window, comms list, on-call rota and bridge.

## Acceptance criteria
- [ ] Infrastructure freeze is in effect on v2 and v3
- [ ] Approvals, window, rota and bridge details are confirmed in writing
- [ ] Announcement for T-14d is sent

## Depends on
- PR-03 - Go/no-go 1 and change approvals
- G-06 - Communications plan and hypercare rota

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-02 - T-2 days - DNS TTL, SSO redirect URIs, API caller readiness, DAG release freeze

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::task, role::cloud, role::security, role::devops
- Depends on: C-01, P-09, R-14

Issue description (copy):

```markdown
Lower the production DNS TTL to 60 seconds, register the v3 SSO redirect URI, confirm API caller owners are ready, and start the DAG release freeze so only releases that go to all targets and stay v2-compatible are allowed.

## Acceptance criteria
- [ ] TTL is 60 seconds and verified
- [ ] SSO redirect URI for v3 is registered and tested
- [ ] Every API caller owner confirms readiness and handover time
- [ ] DAG release freeze is announced and enforced in the pipeline

## Depends on
- C-01 - T-14 to T-3 days - freezes, approvals, comms and rota confirmed
- P-09 - Ingress, DNS, TLS and SSO for v3
- R-14 - Inventory and plan migration of external API callers

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-03 - T-1 day - go/no-go 2

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::gate, role::change-owner
- Depends on: C-02, RB-03, PR-04

Issue description (copy):

```markdown
Last go/no-go before the window.

## Acceptance criteria
- [ ] v2 backups are verified and the DAG shares have matching checksums
- [ ] Quota is confirmed and on-call is staffed
- [ ] Rollback prerequisites (RB-03) are verified
- [ ] Comms are sent and the tier-0 owners are confirmed on the bridge
- [ ] Decision is recorded

## Depends on
- C-02 - T-2 days - DNS TTL, SSO redirect URIs, API caller readiness, DAG release freeze
- RB-03 - Verify rollback prerequisites before cutover
- PR-04 - Mini-rehearsal after the freeze (Jan 4 to Jan 6)

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 9 (gates) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-04 - Cutover phases A and B - snapshot v2 state, drain and stop v2

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::runbook, role::airflow-eng, role::dba
- Depends on: C-03

Issue description (copy):

```markdown
Record the v2 state, export paused states, scale the scheduler to zero, drain running work within the agreed limit, scale remaining v2 components to zero, and confirm zero DB sessions and empty queues. Checkpoint 1 follows.

## Acceptance criteria
- [ ] v2 state, values, image digests, paused-state CSV, schema version and row counts are recorded
- [ ] No tasks in flight, or each stopped task is recorded with a tier-0 owner decision
- [ ] v2 pods are gone, the v2 DB has zero sessions after leftover PgBouncer backends are terminated, and v2 Redis queues are empty
- [ ] Checkpoint 1 is called (continue or abort path A)

## Depends on
- C-03 - T-1 day - go/no-go 2

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.2 (A, B) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-05 - Cutover phase C - backup, clone, trim, migrate, validate, pause all DAGs

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::runbook, role::dba, role::airflow-eng
- Depends on: C-04

Issue description (copy):

```markdown
Take the on-demand backup, clone the database, trim history with the v2 image, fix states of forced-stopped tasks on the clone only, run the migration Job, validate, and set every DAG paused on the v3 database. Checkpoint 2 follows.

## Acceptance criteria
- [ ] Backup name is recorded and the clone is created
- [ ] Migration completes and the validation list passes, within 1.5x the rehearsal time
- [ ] All DAGs are paused on the v3 database and the paused-state CSV is stored
- [ ] Checkpoint 2 is called (continue or abort path B)

## Depends on
- C-04 - Cutover phases A and B - snapshot v2 state, drain and stop v2

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.2 (C) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-06 - Cutover phase D - start v3 and smoke test

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::runbook, role::airflow-eng
- Depends on: C-05

Issue description (copy):

```markdown
Point the v3 release at the migrated database, start api-server and dag-processor, check DAG list and import errors, start workers, triggerer and finally the scheduler, then run the smoke DAG. Checkpoint 3 follows.

## Acceptance criteria
- [ ] API server is healthy, SSO login works, DAG count matches expectation and import errors are zero
- [ ] Job checks for scheduler, dag-processor and triggerer pass and workers register
- [ ] Smoke DAG passes including logs, XCom, callback and an API token call
- [ ] Checkpoint 3 is called (continue or abort path C)

## Depends on
- C-05 - Cutover phase C - backup, clone, trim, migrate, validate, pause all DAGs

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.2 (D) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-07 - Cutover phases E and F - traffic switch and controlled unpause

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::runbook, role::cloud, role::airflow-eng, role::dag-owners
- Depends on: C-06

Issue description (copy):

```markdown
Switch the production hostname to v3, hand over API callers, then unpause DAGs in tiers from the T0 CSV, watching for catchup bursts.

## Acceptance criteria
- [ ] Production hostname serves v3 with TLS and SSO and every external caller is verified
- [ ] Only DAGs unpaused at T0 are unpaused, tier-0 first, in small batches with owner confirmation
- [ ] Tier-1 and tier-2 are unpaused only after 30 minutes of healthy tier-0 runs
- [ ] Any unexpected catchup burst is paused immediately and logged

## Depends on
- C-06 - Cutover phase D - start v3 and smoke test

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.2 (E, F) (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-08 - Cutover phase G - hypercare gates at T+1h, T+4h, T+24h and T+7d

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::gate, role::change-owner, role::ops
- Depends on: C-07

Issue description (copy):

```markdown
Review the go/no-go signals against the numeric thresholds at each gate. Any Sev1 starts the rollback within 15 minutes unless a fix is already running and expected within 30 minutes.

## Acceptance criteria
- [ ] Each gate review is recorded against the section 11 thresholds
- [ ] Tier-0 owners confirm outputs at T+4h and T+24h
- [ ] DAG release freeze stays in force through hypercare
- [ ] Decision (continue, fix forward or rollback) is recorded at every gate

## Depends on
- C-07 - Cutover phases E and F - traffic switch and controlled unpause

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan sections 10.2 (G), 11 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## C-09 - Cutover record, timings and post-cutover report

- Epic: E8
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::prod, env::prod, type::task, role::change-owner
- Depends on: C-08

Issue description (copy):

```markdown
Close the change with the facts that future work depends on.

## Acceptance criteria
- [ ] Actual timing per phase is recorded and section 7.8 is updated
- [ ] List of tasks stopped at drain and DAGs that created catchup runs is attached
- [ ] Issues and surprises are logged with owners
- [ ] Report is shared with stakeholders

## Depends on
- C-08 - Cutover phase G - hypercare gates at T+1h, T+4h, T+24h and T+7d

## Context
- Epic: E8 Production cutover (Jan 9-10, 2027)
- Migration plan reference: Plan section 10.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E9 Rollback readiness

- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::rollback

Epic description (copy):

```markdown
Rollback is stop v3, start v2 against its untouched database with all DAGs paused, reconcile the v3 window, then unpause in tiers. This epic makes sure it is written, scripted, drilled and approved before any production cutover.
Plan section 12.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## RB-01 - Finalize rollback runbook (stages 1 to 5)

- Epic: E9
- Milestone: M1 - Dev migration complete
- Labels: airflow-v3, phase::dev, type::rollback, type::runbook, role::airflow-eng, role::change-owner
- Depends on: none

Issue description (copy):

```markdown
Turn the five rollback stages into executable steps with commands, owners and times. Target restore of service within 30 minutes of the decision.

## Acceptance criteria
- [ ] Each stage lists commands, owner, expected time and verification
- [ ] The runbook states the single-writer rule and that v3 is fully stopped before v2 starts
- [ ] v2 restart order and pausing of all v2 DAGs before the scheduler starts are explicit
- [ ] Runbook is reviewed by someone other than its author

## Context
- Epic: E9 Rollback readiness
- Migration plan reference: Plan section 12.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## RB-02 - Reconciliation procedure and export scripts for the v3 window

- Epic: E9
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::dev, type::rollback, type::task, role::airflow-eng, role::dag-owners
- Depends on: RB-01

Issue description (copy):

```markdown
v2 does not know what v3 ran. Script the exports and agree per-DAG-type handling so a rollback does not duplicate or skip work.

## Acceptance criteria
- [ ] Scripts export DAG runs and task outcomes since cutover and connection, variable and pool changes
- [ ] The decision table by DAG type (idempotent, catchup false, catchup true, side-effecting, event-triggered, sensors) is agreed with owners
- [ ] The technique for marking intervals complete in v2 before unpausing is tested
- [ ] Business owners approve resume plans for tier-0 DAGs

## Depends on
- RB-01 - Finalize rollback runbook (stages 1 to 5)

## Context
- Epic: E9 Rollback readiness
- Migration plan reference: Plan section 12.4 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## RB-03 - Verify rollback prerequisites before cutover

- Epic: E9
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::dev, type::rollback, type::task, role::airflow-eng, role::cloud, env::prod
- Depends on: RB-01

Issue description (copy):

```markdown
Check each requirement in the section 12.2 table before T-1d.

## Acceptance criteria
- [ ] v2 database is unmodified, release and image digests are saved, and v2 values from T0 are stored
- [ ] The v2 DAG share has a v2-compatible release and the same release is on all targets
- [ ] v2 Redis, secrets, nodepool and quota are untouched and previous DNS or ingress values are saved
- [ ] Result is attached to the change record

## Depends on
- RB-01 - Finalize rollback runbook (stages 1 to 5)

## Context
- Epic: E9 Rollback readiness
- Migration plan reference: Plan section 12.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## RB-04 - Approve rollback triggers, thresholds and authority

- Epic: E9
- Milestone: M2 - UAT complete
- Labels: airflow-v3, phase::dev, type::rollback, type::decision, role::change-owner, role::ops
- Depends on: R-02

Issue description (copy):

```markdown
Set numeric thresholds from the v2 baselines and agree who calls rollback.

## Acceptance criteria
- [ ] Sev1 and Sev2 thresholds in section 11 are filled with numbers from baselines
- [ ] Rollback authority and deputy are named
- [ ] On-call may pause DAGs and scale v3 to zero without approval in a Sev1
- [ ] Management and DAG owners acknowledge the thresholds

## Depends on
- R-02 - Capture v2 performance and reliability baselines

## Context
- Epic: E9 Rollback readiness
- Migration plan reference: Plan sections 11, 12.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## RB-05 - v2 retention and fallback plan for 14 days

- Epic: E9
- Milestone: M4 - Production cutover
- Labels: airflow-v3, phase::dev, type::rollback, type::task, role::cloud, role::dba, env::prod
- Depends on: RB-01

Issue description (copy):

```markdown
Keep v2 and its database restorable for the rollback window, including when v2 itself fails to start.

## Acceptance criteria
- [ ] v2 Helm release, nodepool and quota are kept and not shrunk in a way that blocks restart
- [ ] AKS version and node image are kept stable through the window
- [ ] Procedures for v2 pods that will not start, a damaged v2 DB (restore) and a damaged DAG share (redeploy from Nexus) are written
- [ ] Restore time for the v2 DB is measured in dev

## Depends on
- RB-01 - Finalize rollback runbook (stages 1 to 5)

## Context
- Epic: E9 Rollback readiness
- Migration plan reference: Plan sections 12.8, 13 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E10 Stabilization and v2 decommissioning

- Milestone: M5 - Stabilization and v2 standby complete
- Labels: airflow-v3, phase::stabilization, env::prod

Epic description (copy):

```markdown
v2 stays installed and scaled to zero for 14 days after cutover, then is retired in stages so nothing irreversible happens early.
Plan section 13.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## S-01 - Hypercare days 0 to 2 and daily review

- Epic: E10
- Milestone: M5 - Stabilization and v2 standby complete
- Labels: airflow-v3, phase::stabilization, env::prod, type::task, role::ops, role::airflow-eng
- Depends on: C-08

Issue description (copy):

```markdown
Named rota, daily review of the section 11 signals, DAG release freeze in force.

## Acceptance criteria
- [ ] Daily review notes are recorded for each day
- [ ] No Sev1 is open, or rollback was followed
- [ ] Sev2 incidents have owners and dates

## Depends on
- C-08 - Cutover phase G - hypercare gates at T+1h, T+4h, T+24h and T+7d

## Context
- Epic: E10 Stabilization and v2 decommissioning
- Migration plan reference: Plan section 13 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## S-02 - Weekly-cycle observation, capacity and cost review, alert tuning (days 3 to 7)

- Epic: E10
- Milestone: M5 - Stabilization and v2 standby complete
- Labels: airflow-v3, phase::stabilization, env::prod, type::task, role::ops, role::cloud
- Depends on: S-01

Issue description (copy):

```markdown
Watch weekly and month-boundary schedules, review capacity and cost of the nodepool, and tune alerts.

## Acceptance criteria
- [ ] Weekly-cycle DAGs have run correctly on v3
- [ ] Capacity and cost review is documented
- [ ] Noisy or missing alerts are fixed
- [ ] DAG release freeze is relaxed at day 7 only with v2-compatible releases, or an explicit risk acceptance

## Depends on
- S-01 - Hypercare days 0 to 2 and daily review

## Context
- Epic: E10 Stabilization and v2 decommissioning
- Migration plan reference: Plan section 13 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## S-03 - Day-14 decision to retire v2

- Epic: E10
- Milestone: M5 - Stabilization and v2 standby complete
- Labels: airflow-v3, phase::stabilization, env::prod, type::gate, role::change-owner
- Depends on: S-02

Issue description (copy):

```markdown
Decide whether to start retiring v2.

## Acceptance criteria
- [ ] No Sev1 for 7 days and no open Sev2 at day 14
- [ ] Tier-0 owners sign off
- [ ] Decision and attendees are recorded

## Depends on
- S-02 - Weekly-cycle observation, capacity and cost review, alert tuning (days 3 to 7)

## Context
- Epic: E10 Stabilization and v2 decommissioning
- Migration plan reference: Plan section 13 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## S-04 - Retire v2 in two stages

- Epic: E10
- Milestone: M5 - Stabilization and v2 standby complete
- Labels: airflow-v3, phase::stabilization, env::prod, type::task, role::cloud, role::dba
- Depends on: S-03

Issue description (copy):

```markdown
Stage 1 uninstalls the v2 release and deletes v2 workloads and the v2 Redis, keeping the v2 database, share and final dump. Stage 2 deletes the v2 database, share and secrets after 90 days with compliance sign-off.

## Acceptance criteria
- [ ] Stage 1 is approved and done, and a final v2 database backup and DAG share snapshot exist
- [ ] Stage 2 is scheduled for 90 days later with compliance sign-off
- [ ] Cost of retained resources is tracked

## Depends on
- S-03 - Day-14 decision to retire v2

## Context
- Epic: E10 Stabilization and v2 decommissioning
- Migration plan reference: Plan section 13 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## S-05 - Cleanup, DAG modernization and lessons learned

- Epic: E10
- Milestone: M5 - Stabilization and v2 standby complete
- Labels: airflow-v3, phase::stabilization, env::prod, type::task, role::airflow-eng, role::dag-owners
- Depends on: S-04

Issue description (copy):

```markdown
Remove leftovers and apply the deferred recommended changes once v2 compatibility is no longer needed.

## Acceptance criteria
- [ ] v2 hostnames, ingress, SSO redirect URIs and DNS entries are removed and DNS TTL restored
- [ ] Nexus pipeline targets only v3 and rehearsal clone databases are dropped
- [ ] Dashboards, alerts and runbooks use v3 component names
- [ ] Deferred AIR311 and AIR312 changes are scheduled
- [ ] Lessons learned are published

## Depends on
- S-04 - Retire v2 in two stages

## Context
- Epic: E10 Stabilization and v2 decommissioning
- Migration plan reference: Plan section 13 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E11 Phase 2 - NFS 4.1 premium share for DAGs

- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2

Epic description (copy):

```markdown
Optional after stabilization. Only if SMB parse times miss targets. One change at a time, dev first, never in the same window as Blob logging. The old SMB share keeps receiving every release during a 7-day soak.
Plan section 14.2.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## N-01 - Decide whether NFS is justified by measured v3 parse times

- Epic: E11
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::decision, role::airflow-eng
- Depends on: S-03

Issue description (copy):

```markdown
Compare v3 parse time and scheduler loop time on SMB with the targets from section 11 before spending effort on NFS.

## Acceptance criteria
- [ ] Measured v3 SMB data is attached
- [ ] Decision is go or no-go with the target improvement stated
- [ ] If go, a change and window are planned

## Depends on
- S-03 - Day-14 decision to retire v2

## Context
- Epic: E11 Phase 2 - NFS 4.1 premium share for DAGs
- Migration plan reference: Plan section 14.1 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## N-02 - Provision premium FileStorage, NFS share, network and PV/PVC

- Epic: E11
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::task, role::cloud, role::security
- Depends on: N-01

Issue description (copy):

```markdown
Premium FileStorage account, NFS 4.1 share of at least 100 GiB, private endpoint or service endpoint, secure transfer off, and a static PV and PVC with Retain.

## Acceptance criteria
- [ ] Account, share and network access exist and only the AKS subnet can reach them
- [ ] Security accepts that NFS 4.1 traffic is not encrypted in transit on the VNet
- [ ] PV and PVC bind and mount in a test pod
- [ ] Private endpoint DNS resolves from the nodepool
- [ ] Snapshot and backup support for NFS shares in the region is checked and recorded

## Depends on
- N-01 - Decide whether NFS is justified by measured v3 parse times

## Context
- Epic: E11 Phase 2 - NFS 4.1 premium share for DAGs
- Migration plan reference: Plan section 14.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## N-03 - Ownership Job and Nexus deployer for NFS

- Epic: E11
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::task, role::devops, role::airflow-eng
- Depends on: N-02, P-08

Issue description (copy):

```markdown
NFS shares cannot be written through azcopy or the REST API, so the deployer must mount the claim. Verify checksums, unpack into releases per version, and test how the dag-processor reacts when the current link changes.

## Acceptance criteria
- [ ] Ownership Job sets UID 50000 and GID 0 and root-squash behavior is verified
- [ ] Deployer pulls from Nexus, verifies the checksum, unpacks and switches the current link atomically
- [ ] The deployer also writes to the SMB share during the soak
- [ ] Behavior of the dag-processor on a link switch is tested in UAT, and unpack-in-place is chosen if the link causes unwanted re-parsing

## Depends on
- N-02 - Provision premium FileStorage, NFS share, network and PV/PVC
- P-08 - Nexus release pipeline deploys one DAG version to all target shares

## Context
- Epic: E11 Phase 2 - NFS 4.1 premium share for DAGs
- Migration plan reference: Plan section 14.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## N-04 - UAT rehearsal - parse time comparison SMB versus NFS

- Epic: E11
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::test, role::airflow-eng, env::uat
- Depends on: N-03

Issue description (copy):

```markdown
Run the same DAG set against SMB and NFS and compare parse time, scheduler loop time and the delay before a new DAG file is visible (actimeo tradeoff).

## Acceptance criteria
- [ ] Comparison data is attached
- [ ] Improvement meets the target agreed from the v3 SMB baseline
- [ ] New DAG file visibility delay is acceptable
- [ ] Adopt or stop decision is recorded

## Depends on
- N-03 - Ownership Job and Nexus deployer for NFS

## Context
- Epic: E11 Phase 2 - NFS 4.1 premium share for DAGs
- Migration plan reference: Plan section 14.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## N-05 - Production NFS cutover with rollback to SMB

- Epic: E11
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::runbook, role::airflow-eng, role::change-owner, env::prod
- Depends on: N-04

Issue description (copy):

```markdown
Drain with the same limit as section 10, change the DAG claim in the chart values, roll dag-processor, triggerer, workers and then the scheduler. Roll back by restoring the SMB claim.

## Acceptance criteria
- [ ] Window is announced and the current release is on NFS with matching checksums
- [ ] NFS mount is verified on every pod, import errors are zero, DAG count is unchanged and the smoke DAG passes
- [ ] Rollback to SMB is tested in UAT and takes about 15 minutes
- [ ] Both shares receive every release during the soak

## Depends on
- N-04 - UAT rehearsal - parse time comparison SMB versus NFS

## Context
- Epic: E11 Phase 2 - NFS 4.1 premium share for DAGs
- Migration plan reference: Plan section 14.2 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## N-06 - 7-day soak, acceptance and SMB DAG share retirement

- Epic: E11
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::gate, role::cloud, role::change-owner
- Depends on: N-05

Issue description (copy):

```markdown
Close Phase 2 for DAGs.

## Acceptance criteria
- [ ] 7 days with no storage-related incident
- [ ] Parse time and scheduler loop time meet the target
- [ ] SMB DAG share is read-only for 30 days and then deleted
- [ ] PVs, PVCs, chart values and runbooks are updated

## Depends on
- N-05 - Production NFS cutover with rollback to SMB

## Context
- Epic: E11 Phase 2 - NFS 4.1 premium share for DAGs
- Migration plan reference: Plan sections 14.4, 14.5 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---

# EPIC E12 Phase 2 - Azure Blob remote logging

- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2

Epic description (copy):

```markdown
Optional after stabilization. Durable, retention-managed task logs in Blob using workload identity and no storage keys. Local logs are kept during a soak so rollback is a configuration revert.
Plan section 14.3.

Migration plan: https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588
```

## B-01 - Provision storage account, container, network, soft delete and lifecycle policy

- Epic: E12
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::task, role::cloud
- Depends on: S-03

Issue description (copy):

```markdown
General-purpose v2 storage account and container for logs, private endpoint or service endpoint, blob soft delete, and a lifecycle policy such as cool after 30 days and delete after the retention period.

## Acceptance criteria
- [ ] Account and container exist with no public access
- [ ] Private endpoint DNS resolves from every pod that writes or reads logs
- [ ] Lifecycle policy is tested on a test container with short retention
- [ ] Cost is estimated from current daily log volume

## Depends on
- S-03 - Day-14 decision to retire v2

## Context
- Epic: E12 Phase 2 - Azure Blob remote logging
- Migration plan reference: Plan section 14.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## B-02 - Workload identity, federated credentials and role assignments

- Epic: E12
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::task, role::cloud, role::security
- Depends on: B-01

Issue description (copy):

```markdown
A writer identity for workers, triggerer and (if used) dag-processor with Storage Blob Data Contributor, and a reader identity for the api-server with Storage Blob Data Reader, scoped to the container.

## Acceptance criteria
- [ ] AKS OIDC issuer and workload identity are enabled
- [ ] Federated credentials exist for each component service account and pods carry the workload identity label and client ID annotation
- [ ] Roles are least-privilege and scoped to the container
- [ ] No storage account keys are used in Airflow

## Depends on
- B-01 - Provision storage account, container, network, soft delete and lifecycle policy

## Context
- Epic: E12 Phase 2 - Azure Blob remote logging
- Migration plan reference: Plan section 14.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## B-03 - Configure remote logging for the pinned provider version and run the test matrix

- Epic: E12
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::test, role::airflow-eng, role::qa
- Depends on: B-02

Issue description (copy):

```markdown
Confirm the exact setting names in the Azure provider documentation for the version pinned by the 3.3.2 constraints. Run the matrix in dev and then UAT: success, failure and retry attempts, skipped, long-running, deferrable and trigger logs, killed worker, large log, api-server restart, removed role, private endpoint DNS and lifecycle.

## Acceptance criteria
- [ ] Settings are confirmed against the pinned provider documentation and recorded
- [ ] Every row in the test matrix passes or has an accepted note
- [ ] Role removal leaves tasks succeeding with a clear error and a firing alert
- [ ] Time from task end to log visible in the UI is recorded

## Depends on
- B-02 - Workload identity, federated credentials and role assignments

## Context
- Epic: E12 Phase 2 - Azure Blob remote logging
- Migration plan reference: Plan section 14.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## B-04 - Production rollout with local logs kept 14 to 30 days

- Epic: E12
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::runbook, role::airflow-eng, role::change-owner, env::prod
- Depends on: B-03

Issue description (copy):

```markdown
Short window to add the settings and roll workers, triggerer and api-server. Keep delete_local_logs false during the soak.

## Acceptance criteria
- [ ] Smoke DAG and a tier-0 DAG show logs in the UI and in Blob
- [ ] Local logs are retained during the soak
- [ ] Rollback (remote logging off and restart) is tested in UAT
- [ ] 14 to 30 day soak ends with no unexplained upload errors

## Depends on
- B-03 - Configure remote logging for the pinned provider version and run the test matrix

## Context
- Epic: E12 Phase 2 - Azure Blob remote logging
- Migration plan reference: Plan section 14.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## B-05 - Strategy for historical logs

- Epic: E12
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::decision, role::cloud, role::airflow-eng
- Depends on: B-03

Issue description (copy):

```markdown
Choose between leaving the old share readable until retention ends, copying logs to Blob with the same relative path layout, or archiving them without UI visibility.

## Acceptance criteria
- [ ] Decision is recorded with reasons
- [ ] If copying, the path layout Airflow expects is confirmed before the bulk copy
- [ ] Retention for old logs is agreed and scheduled

## Depends on
- B-03 - Configure remote logging for the pinned provider version and run the test matrix

## Context
- Epic: E12 Phase 2 - Azure Blob remote logging
- Migration plan reference: Plan section 14.3 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

## B-06 - Operations, acceptance and log share retirement

- Epic: E12
- Milestone: M6 - Phase 2 (NFS and Blob logging)
- Labels: airflow-v3, phase::phase2, type::gate, role::ops, role::cloud
- Depends on: B-04

Issue description (copy):

```markdown
Alerts, cost and access reviews, then retire the log share after retention.

## Acceptance criteria
- [ ] Alerts exist for log upload errors and Blob 403 and 5xx metrics
- [ ] Cost review monthly and role assignment review quarterly are scheduled
- [ ] Logs are readable in the UI for every task in the soak
- [ ] Log share is read-only until retention ends and then deleted, with SMB resources removed from chart values

## Depends on
- B-04 - Production rollout with local logs kept 14 to 30 days

## Context
- Epic: E12 Phase 2 - Azure Blob remote logging
- Migration plan reference: Plan sections 14.3, 14.4, 14.5 (https://claude.ai/code/artifact/70867a81-f6cc-4c6a-8bbc-d2fba334c588)
```

---
