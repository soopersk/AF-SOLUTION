# Airflow 2.10.5 → 3.3.2 and Python 3.11 → 3.14

GitLab delivery backlog • 6 epics • 27 issues

## Delivery approach

Upgrade Airflow and Python together in a tested, immutable runtime. Prove the migration in Dev, validate workflows in UAT, rehearse with production-sized data, then cut over production with a tested return to the preserved v2 stack.

This backlog uses the sample's AKS, Helm, Celery/Redis, PostgreSQL/PgBouncer, Azure Files, Key Vault and Nexus setup as working assumptions. Confirm them in MIG-01; omit implementation details that do not apply. Keep the existing executor, DAG distribution and logging approach unless compatibility requires a change.

Airflow 3.3.2 lists Python 3.14 as supported [S1]. This does not establish compatibility of your providers, native packages or internal libraries; MIG-06 must prove those. Use standard CPython 3.14 and pin the patch version. A Python 3.13 fallback would require an explicit scope decision, rather than silently changing this target.

Airflow's upgrade guide permits starting from 2.7 or later and recommends the latest 2.x [S2]. Rehearse a direct upgrade from 2.10.5 on a clone. Add an intermediate 2.x step only if required by evidence, and include it in the tested runbook.

### GitLab setup

- Create each epic with its description and completion criteria. Create the issues beneath it; the text under each issue title is its description.
- IDs such as MIG-01 are planning references. Replace dependency references with actual GitLab issue links and blocking relationships after creation.
- Apply `airflow-migration` to all items. Use one phase label: `phase::readiness`, `phase::build`, `phase::validation`, `phase::cutover` or `phase::stabilization`. Add environment labels only where useful.
- Each issue needs one accountable assignee. Suggested owner roles below are placeholders to resolve during planning.
- Use the milestone gates below. Set dates after the inventory and rehearsal; the sample's dates and freeze windows are proposals requiring confirmation.
- Split an issue by DAG owner team or external caller only when that allows independent delivery. Keep cutover commands and checkpoints in one runbook rather than separate tickets for every operational step.

### Milestones

| Milestone | Exit condition | Epics |
| --- | --- | --- |
| M0 — Readiness agreed | Inventory, scope, owners and measurable release gates agreed | E1 |
| M1 — Dev migration proven | Target image, compatible DAG release, Dev migration and rollback pass | E2–E4 |
| M2 — UAT accepted | Workflow, integration, security, load and recovery evidence accepted | E5 |
| M3 — Production ready | Production rehearsal passes and change is approved | E6: MIG-23–24 |
| M4 — Production migrated | Controlled cutover and initial validation pass | E6: MIG-25 |
| M5 — Migration closed | Stabilization accepted and v2 retirement completed | E6: MIG-26–27 |

### Shared completion rule

Close an issue when its acceptance criteria pass, relevant configuration/code is reviewed and merged, and evidence is linked. Record an explicit “not applicable” reason for unused features. A known blocker to the agreed target is not an accepted exception; unresolved nonblocking risks need an owner and approval from the change owner.

Keep secret values and sensitive production records out of issue attachments. Link to access-controlled evidence instead.

---

## EPIC E1 — Establish migration scope and release gates

**Description:** Agree what must migrate, who owns it, and how the team will judge readiness. Establish a small set of measurable gates for Dev, UAT and production.

**Completion criteria:** The retained DAGs and integrations have owners, the target stack is confirmed, and cutover/rollback thresholds and authority are agreed.

**Milestone:** M0 • **Label:** `phase::readiness`

### MIG-01 — Inventory the current Airflow estate and confirm scope

**Owner:** Airflow engineer • **Depends on:** None

Capture the configuration and workload needed to migrate the existing service without losing required behavior.

**Acceptance criteria**

- [ ] Inventory includes DAGs, schedules, owners, criticality, providers, custom code, API callers, authentication, secrets, executor, database, storage and deployment versions.
- [ ] Every retained DAG and external caller has an owner; unused or unowned items have a keep/retire decision.
- [ ] Critical DAGs, external side effects and recovery constraints are identified for testing and rollback reconciliation.
- [ ] Scope confirms Airflow 3.3.2/Python 3.14, the existing stack assumptions, and any required platform upgrades against the target's prerequisites.

### MIG-02 — Agree responsibilities, milestones and the change window

**Owner:** Change owner • **Depends on:** MIG-01

Define delivery ownership and a practical release sequence, including business availability and production freeze constraints.

**Acceptance criteria**

- [ ] Every issue has an accountable owner; go/no-go and rollback decision makers have named deputies.
- [ ] Dev, UAT, rehearsal, cutover and stabilization dates are agreed with owners and respect confirmed freeze periods.
- [ ] Cutover support, escalation contacts, stakeholder communications and change approval requirements are recorded.
- [ ] A cutover/rollback runbook has an owner and a review location; detailed timings will be completed after rehearsal.

### MIG-03 — Capture the baseline and agree release/rollback thresholds

**Owner:** Airflow engineer • **Depends on:** MIG-01

Use current operational data to define objective acceptance limits. Avoid arbitrary resource sizes or an unexplained percentage tolerance.

**Acceptance criteria**

- [ ] Baseline records task success, critical DAG completion times, scheduling/queue delay, DAG parse duration, component resource use and database/pool pressure across a representative business cycle.
- [ ] A gate table specifies numeric performance/reliability limits, observation periods and the required critical workflow checks.
- [ ] Rollback triggers, the decision deadline and maximum service recovery time are agreed; start with the sample's 30-minute recovery target and validate feasibility in drills.
- [ ] DAG owners and Operations accept the thresholds, including any expected maintenance delay.

---

## EPIC E2 — Make DAGs and the Python 3.14 runtime compatible

**Description:** Produce a reproducible Airflow 3.3.2/Python 3.14 image and a tested DAG release. Release compatible fixes on v2 first where practical, while preserving a usable v2 fallback release.

**Completion criteria:** The target dependency set installs cleanly, affected code and schedules are tested, CI checks the required runtime combinations, and both target and fallback releases are recorded.

**Milestone:** M1 • **Label:** `phase::build`

### MIG-04 — Fix Airflow 3 incompatibilities in DAGs and custom code

**Owner:** DAG engineering lead • **Depends on:** MIG-01

Scan and review DAGs, operators, hooks, plugins, callbacks, templates and policies for removed or changed Airflow behavior. Review applicable release changes from 3.0 through 3.3.2 [S2, S3].

**Acceptance criteria**

- [ ] Ruff AIR301/AIR302 findings are resolved; manual review covers direct metadata DB access in tasks, removed features/context keys, custom XCom handling and plugins.
- [ ] Task code uses supported SDK/provider/API interfaces; required standard-provider imports and custom plugin replacements are implemented.
- [ ] All retained DAGs import without errors on the target runtime, and tests for affected custom code and callbacks pass.
- [ ] Fixes intended for v2 also pass on 2.10.5/Python 3.11; v3-only changes are isolated in a versioned target release with a preserved v2 release.

### MIG-05 — Preserve scheduling and data interval behavior

**Owner:** DAG engineering lead • **Depends on:** MIG-01

Make intended scheduling behavior explicit so the upgrade does not create missed intervals, duplicate processing or an unexpected catchup burst.

**Acceptance criteria**

- [ ] Retained DAGs have reviewed schedules/timetables, timezone, catchup, start dates and active-run/task limits; behavior-sensitive defaults are explicit.
- [ ] Tests compare expected next runs and data intervals for each scheduling pattern in use, including timezone/DST boundaries where applicable.
- [ ] Critical DAG owners accept expected behavior for scheduled, manually triggered, backfilled and asset/dataset-triggered runs where used.
- [ ] Any intentional behavior change is documented, and the cutover runbook defines the expected first run and missed-interval handling.

### MIG-06 — Resolve providers and dependencies for Python 3.14

**Owner:** Airflow engineer • **Depends on:** MIG-01

Resolve Airflow, providers, internal libraries and DAG dependencies using the Airflow 3.3.2 constraints for Python 3.14 as the starting point [S4]. Review Python changes across 3.12–3.14.

**Acceptance criteria**

- [ ] A clean Python 3.14 installation resolves the full target package set with Airflow pinned to 3.3.2; `pip check` passes and exact versions are recorded.
- [ ] Native dependencies have compatible wheels for the deployment architecture or a tested, reproducible build; required internal libraries install and pass their relevant tests.
- [ ] Removed standard-library usage is fixed; affected code is tested for Python 3.14 annotation evaluation and multiprocessing/process-pool changes [S5].
- [ ] Dependency changes that could affect outputs have regression coverage; no unresolved package blocker remains for the agreed target.

### MIG-07 — Build and publish the Airflow 3.3.2/Python 3.14 image

**Owner:** DevOps engineer • **Depends on:** MIG-06

Build one immutable target image using a verified compatible base. Promote the same image through environments rather than rebuilding for production.

**Acceptance criteria**

- [ ] The image reports Airflow 3.3.2 and the pinned CPython 3.14 patch version; its base image, build inputs and package manifest are recorded.
- [ ] Build, package checks and vulnerability scan pass the team's release policy; exceptions are approved and owned.
- [ ] Required component entrypoints and provider/plugin imports pass smoke checks using the deployment user.
- [ ] The published digest is used by target components and migration jobs; the v2 image remains available for rollback.

### MIG-08 — Add runtime compatibility checks and release the migration DAG package

**Owner:** DevOps engineer • **Depends on:** MIG-04, MIG-05, MIG-07

Enforce the required compatibility in CI and publish traceable DAG releases using the existing delivery pipeline.

**Acceptance criteria**

- [ ] CI checks Airflow 2.10.5/Python 3.11 for the fallback release and Airflow 3.3.2/Python 3.14 for the target release, including import tests, affected unit tests and Ruff breaking-change rules.
- [ ] Required checks block merging/deployment on failure; a shared DAG release is tested in both combinations if that approach is used.
- [ ] Compatible fixes are deployed through the normal v2 release process and show no blocking regression during an agreed representative cycle.
- [ ] Target/fallback DAG artifact versions and checksums are recorded; deploying the previous release is demonstrated in Dev.

---

## EPIC E3 — Prepare the Airflow 3 platform and integrations

**Description:** Adapt the existing platform for Airflow 3 components, execution API communication, access control and operational visibility. Keep target resources isolated from the v2 fallback.

**Completion criteria:** The target configuration deploys, runtime resources and credentials are correctly isolated, users/callers can integrate, and operators can observe and diagnose the service.

**Milestone:** M1 • **Label:** `phase::build`

### MIG-09 — Update Helm values and Airflow configuration for v3

**Owner:** Platform engineer • **Depends on:** MIG-01, MIG-07

Select and pin a compatible Helm chart. Update the configuration for the API server, standalone DAG processor, scheduler, workers and triggerer where used [S2, S6].

**Acceptance criteria**

- [ ] The pinned chart supports the target deployment; Dev, UAT and production values render and pass deployment validation.
- [ ] Removed/renamed configuration is resolved using target-version tooling; required executor/provider and DAG source settings are included.
- [ ] The task execution API URL and required component commands are configured and verified in Dev.
- [ ] Initial requests, limits and database connection budgets fit measured capacity; automatic migration/startup behavior cannot change the v2 database or start business DAGs during an idle build.

### MIG-10 — Provision isolated database, broker and storage resources

**Owner:** Platform engineer • **Depends on:** MIG-01, MIG-03

Prepare target resources with sufficient capacity for rehearsals and fallback. Use the current DAG and logging storage approach.

**Acceptance criteria**

- [ ] The target metadata database and credentials are separate from v2; PgBouncer routing and direct migration access reach the intended target.
- [ ] Celery broker queues and result backend are isolated from v2 where applicable; no v2 queue messages can be consumed by v3 workers.
- [ ] DAG artifacts and logs use the intended storage paths, mount permissions and retention; release checksums match the selected target artifact.
- [ ] Capacity supports the required overlap without breaching the v2 baseline limits; non-production rehearsal tasks cannot write to production business systems.

### MIG-11 — Configure secrets, execution API access and network controls

**Owner:** Platform engineer • **Depends on:** MIG-09, MIG-10

Supply the target with the keys and connectivity needed to run tasks securely, including internal execution API communication.

**Acceptance criteria**

- [ ] Migrated encrypted connections/variables decrypt with the retained Fernet key; required auth/JWT/API secrets are supplied consistently to the appropriate components.
- [ ] Secret references resolve from the existing secret store, and no secret values are committed to Git or exposed in logs.
- [ ] Tests prove required API, database, broker and business-system routes work and unnecessary routes are blocked; database access accounts for the selected executor's actual requirements.
- [ ] TLS, DNS and clock synchronization support authentication, and key rotation/recovery steps are documented.

### MIG-12 — Migrate login, roles and UI access

**Owner:** Airflow engineer • **Depends on:** MIG-01, MIG-09, MIG-11

Configure a production-appropriate auth manager and preserve required user access, using the FAB provider if selected.

**Acceptance criteria**

- [ ] The chosen auth manager/provider and supported user provisioning method are pinned and configured.
- [ ] Existing SSO/login works on the target hostname, including the required callback/redirect settings.
- [ ] Role tests demonstrate permitted actions and deny unauthorized DAG/admin actions; migrated users/roles are verified or explicitly remapped.
- [ ] Admin recovery access and the production hostname switch are documented and accepted by the access-control owner.

### MIG-13 — Update external API callers for Airflow 3

**Owner:** Integration lead • **Depends on:** MIG-01, MIG-11, MIG-12

Update each retained caller for the Airflow 3 REST API (`/api/v2`), its request/response contracts and the chosen authentication flow.

**Acceptance criteria**

- [ ] Every retained caller has a tested target endpoint, credentials/permissions and updated request/response handling.
- [ ] Caller tests cover the operations actually used, token expiry/reacquisition and relevant 3.3.2 authentication/backfill response changes [S3].
- [ ] Each caller has a cutover and fallback configuration with a named owner and a verified safe trigger/retry behavior.
- [ ] Caller owners accept the UAT test evidence and coordinated production switch procedure.

### MIG-14 — Update monitoring, logs and a reusable smoke test

**Owner:** Operations engineer • **Depends on:** MIG-03, MIG-09, MIG-11

Provide an end-to-end check and the operational signals needed for release gates and incident response.

**Acceptance criteria**

- [ ] Health checks and dashboards cover deployed v3 components, execution API failures, task failures/queue delays, resource use and database/pool pressure.
- [ ] Alerts implement the agreed gates and route to the responsible team; representative failures demonstrate delivery.
- [ ] A harmless smoke DAG proves task execution, connection/variable lookup, pool use, XCom, callback and readable logs.
- [ ] Runbook checks also verify login and an authenticated REST API operation; instructions identify where to find component and task logs.

---

## EPIC E4 — Prove database migration and recovery in Dev

**Description:** Establish a repeatable backup, clone, migrate and validate procedure. Demonstrate controlled Dev cutover and rollback, including reconciliation of work performed while v3 was active.

**Completion criteria:** Dev passes the target smoke tests and migration checks, the original v2 database is preserved, and another engineer can execute the corrected recovery procedure.

**Milestone:** M1 • **Label:** `phase::build`

### MIG-15 — Verify database backup, restore and clone procedures

**Owner:** DBA • **Depends on:** MIG-10

Use the existing database service's supported backup/restore mechanism and select one workable clone method. Avoid comparing every possible option unless the preferred method fails.

**Acceptance criteria**

- [ ] A recorded backup restores successfully in an isolated environment, with timing and retention sufficient for the agreed fallback window.
- [ ] The selected clone method produces a consistent, usable copy; required permissions, session handling and storage headroom are documented.
- [ ] Clone configuration and credentials cannot modify the source v2 database; rehearsal access is restricted appropriately.
- [ ] The runbook includes source quiescence requirements and handling of remaining PgBouncer/database sessions where required by the chosen method.

### MIG-16 — Rehearse and validate the metadata database upgrade

**Owner:** DBA • **Depends on:** MIG-07, MIG-09, MIG-15

Run `airflow db migrate` against a clone of the 2.10.5 database. Capture the actual upgrade path, checks and timings [S2].

**Acceptance criteria**

- [ ] The selected direct or intermediate upgrade path completes on the clone, with migration logs, duration and schema readiness checks recorded.
- [ ] Retained DAG/run/task history, XCom samples, pools, connection/variable decryption and applicable auth records pass migration-aware checks; counts are reconciled to intentional schema/retention changes.
- [ ] Source paused states and active/deferred/retry work are captured; target startup prevents accidental scheduling and each pending item has a drain/recovery decision.
- [ ] Any history cleanup has an approved retention rule and is tested on the clone; failed migration recovery recreates the clone from a known source, preserving v2.

### MIG-17 — Deploy Dev and execute the cutover procedure

**Owner:** Airflow engineer • **Depends on:** MIG-08, MIG-12, MIG-14, MIG-16

Deploy the isolated target stack in Dev and execute the draft production runbook with safe test targets.

**Acceptance criteria**

- [ ] The pinned target image/configuration and selected DAG release deploy with healthy components and zero retained-DAG import errors.
- [ ] The runbook covers pause/drain, stop v2, capture state, clone/migrate, start v3 safely, smoke checks and controlled resume; timings and checkpoints are recorded.
- [ ] Smoke tests and representative DAGs pass, including the first scheduled run, without duplicate external processing.
- [ ] Defects and unclear steps are corrected in the runbook; Dev exit evidence identifies any remaining UAT work.

### MIG-18 — Prove rollback and reconcile work executed on v3

**Owner:** Airflow engineer • **Depends on:** MIG-02, MIG-03, MIG-17

Test both an abort before business execution and rollback after v3 has produced results. Return to the preserved v2 stack rather than downgrading the migrated schema.

**Acceptance criteria**

- [ ] The runbook stops v3 scheduling/execution before restarting v2 paused, restores the correct v2 image/DAG release/traffic/caller settings, and prevents both stacks processing the same production work.
- [ ] A drill exports v3 run outcomes and reconciles processed/missed intervals, side effects and configuration changes before affected v2 DAGs resume.
- [ ] Critical test outputs have no unexplained duplicates or gaps; risky replays require a DAG-owner decision rather than blanket marking runs successful.
- [ ] Another engineer completes the drill within the agreed service recovery target, with separate timing for business reconciliation and all runbook corrections captured.

---

## EPIC E5 — Validate business workflows and operational readiness in UAT

**Description:** Validate the target with production-like data, load and workflow patterns. Obtain evidence that the Airflow and Python changes preserve outputs and that integrations and recovery are ready for production.

**Completion criteria:** All critical DAGs pass business checks, representative remaining patterns pass, agreed service thresholds are met, and owners accept UAT evidence.

**Milestone:** M2 • **Label:** `phase::validation`

### MIG-19 — Validate DAG outputs and scheduling on the target runtime

**Owner:** QA lead • **Depends on:** MIG-17, MIG-18

Build UAT from the proven configuration and migrate its database using the tested procedure. Execute workflows against safe destinations on Airflow 3.3.2/Python 3.14.

**Acceptance criteria**

- [ ] UAT uses the recorded image/DAG/configuration versions, passes database validation and has zero retained-DAG import errors.
- [ ] Every critical DAG passes end-to-end output, completion-time and schedule checks against expected v2/business results.
- [ ] Representative remaining DAGs cover each provider/operator/custom-code pattern in use, including retries, sensors/deferral, mapping, callbacks, XCom and asset/dataset triggering where applicable.
- [ ] Weekly/month-end or other infrequent critical behavior is observed or reproduced using controlled intervals; owners accept results and dependency-related output differences are resolved.

### MIG-20 — Validate peak load and component recovery

**Owner:** Platform engineer • **Depends on:** MIG-19

Use representative peak concurrency and controlled restarts to validate the sizing and recovery behavior of the existing executor.

**Acceptance criteria**

- [ ] Scheduling/queue delay, critical completion times and resource/database pressure meet MIG-03 limits at representative peak load.
- [ ] Scheduler/API server/worker/triggerer recovery is tested where deployed, including node drain or worker interruption for long-running tasks.
- [ ] Recovery produces no unexplained lost work or duplicate business writes; retry/replay behavior is documented for affected patterns.
- [ ] Final production sizing and database/broker limits are recorded, with evidence of capacity for fallback.

### MIG-21 — Validate access, integrations, logging and alerts in UAT

**Owner:** QA lead • **Depends on:** MIG-13, MIG-14, MIG-19

Verify the operational features used by real users and systems on the target release.

**Acceptance criteria**

- [ ] User and service-account login/permission tests pass, including denied access and expired/invalid credentials.
- [ ] Every retained external caller passes its required operations and safe retries against UAT.
- [ ] Running, successful, failed and retried task logs are readable within the agreed visibility limit; retained historical logs have a verified access route.
- [ ] Secret lookups, required network restrictions, TLS and representative failure alerts pass with evidence accepted by Operations and the access-control owner.

### MIG-22 — Complete the UAT cutover/rollback drill and approve UAT exit

**Owner:** Change owner • **Depends on:** MIG-18, MIG-19, MIG-20, MIG-21

Run the corrected procedure with production-like workload/data and approve the evidence needed to proceed to production rehearsal.

**Acceptance criteria**

- [ ] Timed cutover and post-execution rollback drills pass, including service recovery and critical-work reconciliation checks.
- [ ] All critical business and agreed release gates pass; remaining nonblocking defects have owners and an accepted disposition.
- [ ] DAG owners, integration owners and Operations accept the relevant evidence, with a recorded UAT exit decision.
- [ ] The versioned runbook and production rehearsal inputs reflect UAT findings.

---

## EPIC E6 — Cut over production, stabilize and retire v2

**Description:** Rehearse production migration, execute an approved cutover and confirm stable business operation. Keep the preserved v2 stack available for the agreed fallback period, then retire it with approved retention.

**Completion criteria:** Production runs the agreed target, critical outputs and service gates pass throughout stabilization, and v2 is retired after an explicit acceptance decision.

**Milestones:** M3–M5 • **Labels:** phase labels set per issue

### MIG-23 — Prepare production and rehearse on a fresh database clone

**Owner:** Platform engineer • **Depends on:** MIG-22 • **Milestone:** M3 • **Label:** `phase::validation`

Prepare the isolated production target and rehearse using a recent production-sized clone without executing production business work.

**Acceptance criteria**

- [ ] Production target resources, secret references, image/DAG digests and configuration match the accepted release; v2 remains operationally unaffected.
- [ ] A fresh clone passes the tested upgrade/validation procedure; target scheduling and business writes remain blocked during rehearsal.
- [ ] Timings cover drain assumptions, backup/clone, migration, checks, traffic switch and recovery; the approved window includes measured contingency and rollback reserve.
- [ ] Rehearsal findings are resolved and the change owner accepts the runbook; any later release/configuration drift is reviewed and affected checks rerun.

### MIG-24 — Confirm production go/no-go and fallback readiness

**Owner:** Change owner • **Depends on:** MIG-23 • **Milestone:** M3 • **Label:** `phase::cutover`

Make the final release decision using a concise readiness checklist and the agreed thresholds.

**Acceptance criteria**

- [ ] Change approval, cutover window, staffing/deputies, critical DAG owners and stakeholder notifications are confirmed.
- [ ] Backups and preserved v2 image/configuration/DAG release, database, broker, secrets and restart capacity are available for the full fallback period.
- [ ] DNS/ingress, TLS/SSO and external caller switch/fallback settings are verified; release freeze rules and any needed TTL change are recorded.
- [ ] The go/no-go decision confirms no blocking defect, current drill evidence, numeric triggers and a latest safe rollback decision time.

### MIG-25 — Execute production cutover and controlled DAG resume

**Owner:** Change owner • **Depends on:** MIG-24 • **Milestone:** M4 • **Label:** `phase::cutover`

Execute the approved runbook with checkpoint evidence. Stop and follow the rehearsed abort/rollback procedure if a mandatory gate fails.

**Acceptance criteria**

- [ ] Original pause/run state is captured; v2 is paused, drained and stopped, with every unfinished/deferred item resolved or assigned an approved recovery action before target activation.
- [ ] A fresh consistent backup/clone is migrated and validated within the window; the original v2 database remains preserved and target DAGs stay paused until checks pass.
- [ ] Target component health, login, imports, smoke workflow and caller handover pass before restoring only previously enabled DAGs in owner-approved batches, critical DAGs first.
- [ ] Critical outputs, first-run intervals and release metrics meet the initial gate; phase timings, exceptions and the continue/rollback decision are recorded.

### MIG-26 — Stabilize production and approve migration acceptance

**Owner:** Operations engineer • **Depends on:** MIG-25 • **Milestone:** M5 • **Label:** `phase::stabilization`

Monitor the target for a proposed 14-day fallback period, extending it when critical business cycles or defects require more evidence. Keep v2 restartable and preserve compatible fallback artifacts.

**Acceptance criteria**

- [ ] Critical DAG outputs and reliability/performance gates pass throughout the agreed observation period, including required infrequent cycles or accepted controlled tests.
- [ ] Incidents have dispositions; retirement requires no migration-related Sev1 for the preceding 7 days and no open migration-related Sev2, unless stricter agreed gates apply.
- [ ] Capacity and alert tuning are completed, and support runbooks/ownership are accepted by Operations.
- [ ] Change and critical DAG owners record acceptance and authorize ending the fallback period; any extension has a reason and review date.

### MIG-27 — Retire the Airflow 2 stack and close the migration

**Owner:** Platform engineer • **Depends on:** MIG-26 • **Milestone:** M5 • **Label:** `phase::stabilization`

Remove unused v2 infrastructure after acceptance while preserving required history and audit evidence.

**Acceptance criteria**

- [ ] Final v2 backup, DAG/configuration artifacts and historical-log access have an approved retention/access plan before resources are removed.
- [ ] Approved v2 releases, database/broker resources, storage, hostnames and secret references are retired without affecting v3; retained items have a deletion owner/date.
- [ ] CI and deployment target v3 only after fallback ends; obsolete credentials, access mappings and temporary rehearsal resources are removed as appropriate.
- [ ] Operations documentation, actual costs and migration evidence are updated, with actionable lessons and remaining work recorded.

---

## Deferred improvements

Keep these outside the migration milestones. Create separate follow-up work only after stabilization and a demonstrated need:

| Candidate | Reason to proceed | Minimal acceptance |
| --- | --- | --- |
| Replace SMB DAG storage with NFS | Measured storage/parse performance cannot meet the service target | Demonstrate an agreed improvement in UAT, verify permissions/release delivery and rehearse return to SMB before production rollout. |
| Move task logs to Azure Blob | A defined retention, availability or cost requirement justifies the change | Verify running/completed/failed/retried logs, access controls and retention in UAT; rehearse fallback and preserve historical-log access. |

## Changes from the sample

- Consolidated 12 epics and 94 issues into 6 delivery epics and 27 issues, plus two deferred improvement candidates.
- Changed the runtime target from Python 3.13 to the requested Python 3.14 and added explicit dependency/native-package and runtime-behavior checks.
- Combined repeated environment builds, repeated test tickets and cutover microsteps into outcome-based issues and one executable runbook.
- Removed references to an unavailable external plan and unnamed numbered tests; each issue states what must be proven.
- Replaced conflicting committed/target dates and unsupported fixed resource/pool sizes with owner-confirmed milestones and measured gates.
- Preserved backup/restore, isolated clone migration, tested rollback, work reconciliation, access controls and business acceptance.

## Official references

Verified 12 October 2026. These establish product behavior; they do not verify the sample's deployment details or your dependencies.

- **S1 — Supported runtime/platform versions:** https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html
- **S2 — Airflow 3 upgrade guide:** https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/upgrading_to_airflow3.html
- **S3 — Release changes through 3.3.2:** https://airflow.apache.org/docs/apache-airflow/3.3.2/release_notes.html
- **S4 — Installation and versioned constraints:** https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/installing-from-pypi.html
- **S5 — Python 3.14 changes and porting guidance:** https://docs.python.org/3.14/whatsnew/3.14.html
- **S6 — Official Airflow Helm chart:** https://airflow.apache.org/docs/helm-chart/stable/index.html
