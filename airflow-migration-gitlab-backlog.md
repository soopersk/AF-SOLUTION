# Airflow 2.10.5 → 3.2.2 — QA-first blue/green migration

GitLab delivery backlog • 7 epics • 37 issues • Revised 22 October 2026

## Delivery approach

Roll out in this order: **QA → DEV → UAT → PROD**. QA is a new environment for technical feasibility, the migration POC and the blue/green swimlane design. Build a working Airflow 2.10.5 blue lane there first, then a second Airflow 2.10.5 green lane, and upgrade only green to **Airflow 3.2.2**. Promote the proven configuration and procedure through DEV, UAT and PROD, with an exit gate for each environment.

The lanes use the **same AKS nodepool, different namespaces, the same PostgreSQL server with different databases, the same Azure file share with separate lane directories, and separate Redis instances**. In QA, provision the first PostgreSQL server, Azure file share/PV/PVC and blue Redis before installing blue. Green reuses that server, nodepool and file share; it needs its own namespace, Helm release, database/roles, Redis, service accounts/federation, secret references and volume bindings. Reuse existing registry, Nexus, Key Vault, network and identity services where applicable.

The reviewed sources are [the migration PDF](<Airflow 2.10.5 to 3.3.2 Migration Plan (Blue Green on AKS).pdf>), especially sections 2–3, 6–12 and appendix A, and [AF_B_G.md](AF_B_G.md), which describes the same original design. This backlog records the user's revised implementation requirements; the source documents retain their original content. The architecture review and differences below take precedence for this backlog. Deployment configuration has not been inspected in a live Azure/AKS environment.

**Runtime planning assumption:** retain Python 3.11 for blue and green's initial 2.10.5 installation. Use Python 3.13 for green's 3.2.2 image as proposed in the source plan, subject to MIG-01/MIG-06 confirmation in QA. The previous backlog's Python 3.14 requirement was not present in the current request. Airflow 3.2.2 lists both 3.13 and 3.14 as supported [S1], but providers, internal libraries and native packages still need proof. Record the selected Python patch version before publishing the target image; propagate any agreed change through CI, values and this backlog.

Airflow's upgrade guide permits starting from 2.7 or later and recommends the latest 2.x [S2]. Prove direct 2.10.5 → 3.2.2 on a clone in QA. Add an intermediate supported 2.x step only if evidence requires it, on green's clone only. Never upgrade blue's database to rehearse the migration.

### Architecture review and revised decisions

| Area | Source plan / previous backlog | Revised implementation and required proof | Work |
| --- | --- | --- | --- |
| Version and sequence | 3.3.2; DEV → UAT → PROD | 3.2.2; QA → DEV → UAT → PROD; QA proves feasibility before DEV migration | MIG-01–02, QA-10 |
| QA baseline | Existing v2 environment assumed | Provision QA infrastructure and deploy blue 2.10.5 before creating green | QA-01–02 |
| Green lifecycle | Build a new v3 stack directly | Deploy the second installation at 2.10.5, prove its isolation, then upgrade that release in its namespace to 3.2.2 | QA-09–10, MIG-17, MIG-19, MIG-23 |
| Compute | Dedicated green nodepool | Reuse blue's nodepool; size for overlap, shared failure impact and rollback capacity | QA-01, MIG-03, MIG-20 |
| PostgreSQL | Clone, optionally on a separate server | Same server, separate blue/green databases and roles; green migration/result backend must never target blue | QA-07, MIG-10, MIG-15–16 |
| Redis | Separate broker | Two instances in QA: first for blue, new second for green; green queue must be empty before the major-version upgrade | QA-01, QA-07, QA-09 |
| Azure Files | Separate shares | Same physical share; separate DAG release roots and writable log paths; separate PV/PVC bindings per namespace | QA-08, MIG-08, MIG-10 |
| Identity | Mostly existing secrets; federation discussed for later Blob logging | Namespace-specific federated credentials and service-account setup are required now for green's existing Azure access | QA-03, MIG-11 |
| Delivery | Nexus delivery to two shares | Azure DevOps (ADO) pipeline targets environment and lane; deploy Nexus artifacts to lane directories on one share | QA-04–06, MIG-08 |
| Installation safety | No explicit second-install guard | Detect existing releases/workloads, fail unexpected duplicates, allow only the registered two-lane migration pair, prevent dual business activation | QA-06 |
| Rollback and retirement | Retain/delete dedicated v2 resources | Retain blue database, Redis, DAG release and logs; shared nodepool/server/share stay in service when blue is retired | MIG-18, MIG-24, MIG-27 |

**Isolation has limits.** Sharing a nodepool, database server and file share creates common capacity and failure risks. Separate namespaces, databases and directories provide operational separation; directories on a share mounted with common credentials are not a security boundary. QA must prove safe deployment and selective recovery; UAT must prove capacity and workload recovery. Do not claim a physically independent green stack or an untouched shared filesystem.

### Architecture within each environment

Names below are examples; inventory the real existing blue identifiers in DEV/UAT/PROD. Reuse is within an environment, not a shared QA/DEV/UAT/PROD metadata database or share.

| Resource | Blue lane | Green lane | Rule |
| --- | --- | --- | --- |
| AKS cluster / nodepool | Environment's existing pool; QA capacity provisioned if absent | Same pool | Match node selectors/tolerations; no new green pool by default |
| Namespace / Helm release | e.g. `airflow-qa-blue` / `airflow-qa-blue` | e.g. `airflow-qa-green` / `airflow-qa-green` | Distinct, registered identities; retain lane names across the version upgrade |
| Runtime | Airflow 2.10.5 / Python 3.11 | 2.10.5 / 3.11 first, then 3.2.2 / selected Python | Same green installation is upgraded; blue remains at 2.10.5 |
| Metadata database | e.g. `airflow_qa_blue` | e.g. `airflow_qa_green` | Same PostgreSQL server, separate DB roles/ownership and runtime grants |
| Celery result backend | Blue database | Green database | Match lane database even if a runtime uses PgBouncer |
| Celery broker | QA blue Redis | New QA green Redis | Separate service instance, endpoint and credentials; not Redis logical DB numbers on one instance |
| Azure file share | One environment share | Same share | Preserve existing blue paths in later environments |
| DAGs | e.g. `blue/dags/releases/<version>/` | e.g. `green/dags/releases/<version>/` | Runtime mounts read-only; independent selection of an immutable release |
| Logs | e.g. `blue/logs/` | e.g. `green/logs/` | Writable only through intended lane mounts; historical access tested |
| PV / PVC | Blue PV(s), PVC(s) in blue namespace | New PV(s), PVC(s) in green namespace pointing to the existing share | PV names and CSI volume handles unique; one claim per binding, `Retain` [S7, S9] |
| Workload identity | Existing/environment identity and blue service-account subjects | Reuse suitable managed identity if allowed, add green namespace/service-account subjects | Namespace changes the federation subject; validate each component that needs Azure access [S8] |
| Secrets | Blue references/credentials | Green DB, Redis and auth references | Preserve Fernet key value for cloned encrypted data; use lane-specific references and new green API/JWT keys |
| Ingress and external callers | Active service hostname | Separate test hostname until cutover | Switch UI/API routes and caller configuration only after scheduling handover |

Keep the current Celery executor, SMB storage, Nexus DAG packaging and logging approach during migration. PostgreSQL 17, PgBouncer, the official Helm chart and Key Vault are source-plan assumptions to confirm in MIG-01. Do not combine this work with a PostgreSQL major upgrade or NFS/Blob adoption.

```mermaid
flowchart LR
  ADO[ADO pipeline and deployment guard]
  subgraph ENV[One environment: same AKS nodepool]
    B[Blue namespace: Airflow 2.10.5]
    G[Green namespace: Airflow 2.10.5 then 3.2.2]
  end
  ADO --> B
  ADO --> G
  B --> RB[Blue Redis]
  G --> RG[Green Redis]
  subgraph PG[Same PostgreSQL server]
    DBB[Blue database]
    DBG[Green database: migrated clone]
  end
  B --> DBB
  G --> DBG
  DBB -. controlled clone .-> DBG
  subgraph FILES[Same Azure file share]
    FB[Blue DAG and log paths]
    FG[Green DAG and log paths]
  end
  B --> FB
  G --> FG
```

### Swimlane lifecycle and scheduling ownership

| Step | Blue lane | Green lane | Platform / DBA / release gate |
| --- | --- | --- | --- |
| 1. QA baseline | Deploy and validate 2.10.5 | Absent | QA blue infrastructure and safe test connections ready |
| 2. Second v2 installation | Remains available at 2.10.5 | Deploy 2.10.5 with its own DB/Redis and lane mounts; business DAGs paused | Guard recognizes exactly the registered pair; prove v2 parity using sandbox smoke work |
| 3. POC / rehearsal | Supplies consistent source snapshot; no simultaneous execution of the same business work | Stop green v2; replace its disposable bootstrap DB with a fresh blue clone, then migrate and deploy 3.2.2 | Capture paused/run states and Fernet key reference; block copied production destinations; scheduling held throughout replacement/migration |
| 4. Cutover | Pause/drain; stop scheduling, workers and triggerers; preserve DB, Redis and artifacts | Use a fresh cutover clone; validate, start paused, run harmless smoke, then become active | Confirm no blue execution remains; switch callers/ingress and restore only previously enabled DAGs in tiers |
| 5. Rollback drill / fallback | Restart 2.10.5 paused; reconcile intervals before resuming | Stop all scheduling/execution; retain migrated DB and evidence | Switch caller/traffic settings back; export/reconcile work already performed by green |
| 6. Accepted migration | Standby for agreed window, then retire lane-owned resources | Remains active at 3.2.2 | Keep shared nodepool, PostgreSQL server and file share |

Only one lane processes an environment's business workload at a time, including scheduled, manual, API, backfill and event-triggered work. Both installations may be present for comparison. Any simultaneous POC execution must use explicitly disjoint sandbox data/destinations. Pausing DAGs or switching the hostname alone does not fence all execution; control workers, triggerers, callers, autoscalers and deployment reconciliation as well. Never point a 2.x process at a migrated 3.x database.

### GitLab setup

- Create each epic with its description and completion criteria. Create the issues beneath it; the text under each issue title is its description.
- IDs such as MIG-01 and QA-01 are planning references. Preserve MIG-01–27 when updating existing issues; add E0 and QA-01–10. Replace dependency references with actual GitLab issue links and blocking relationships after creation.
- Apply `airflow-migration` to all items. Use one phase label: `phase::readiness`, `phase::build`, `phase::validation`, `phase::cutover` or `phase::stabilization`. Use `env::qa`, `env::dev`, `env::uat`, `env::prod` and lane labels where applicable. QA is an environment; a QA lead's UAT work still uses `env::uat`.
- Each issue needs one accountable assignee. Suggested owner roles below are placeholders to resolve during planning.
- Use the milestone gates below. Set dates after the QA feasibility/POC timings and inventory; dates and freeze windows in the older documents are not commitments for the revised scope.
- Split an issue by DAG owner team or external caller only when that allows independent delivery. Keep cutover commands and checkpoints in one runbook rather than separate tickets for every operational step.

### Milestones

| Milestone | Exit condition | Epics |
| --- | --- | --- |
| M0 — Readiness agreed | Inventory, scope, owners and measurable release gates agreed | E1 |
| MQ0 — QA blue ready | New QA infrastructure and first Airflow 2.10.5 installation pass baseline checks | E0: QA-01–02, QA-04–06 |
| MQ1 — QA second v2 ready | Green 2.10.5 works on the same pool/server/share with a separate DB/Redis and verified federation/guard | E0: QA-03, QA-07–09 |
| MQ2 — QA POC and design accepted | Green upgraded to 3.2.2 on a clone; failure recovery, cutover and rollback proven; reusable swimlane design/runbook accepted | E0: QA-10; E2–E4 QA proof work |
| M1 — DEV migration proven | MQ2 accepted; existing DEV blue retained, green installed at 2.10.5 then upgraded; DEV cutover and rollback pass | E4: MIG-17–18 repeated in DEV |
| M2 — UAT accepted | Workflow, integration, security, load and recovery evidence accepted | E5 |
| M3 — Production ready | Production rehearsal passes and change is approved | E6: MIG-23–24 |
| M4 — Production migrated | Controlled cutover and initial validation pass | E6: MIG-25 |
| M5 — Migration closed | Stabilization accepted and v2 retirement completed | E6: MIG-26–27 |

Readiness/code/template work may proceed in parallel. Environment deployment and migration follow QA → DEV → UAT → PROD; no later environment migration starts before the previous exit gate passes. Clone each environment's own blue database, never promote QA metadata or credentials. Repeat the registered second-v2 setup, clone/migrate, cutover and rollback checks per environment; only QA requires creation of the first blue stack from scratch. E0 owns reusable setup automation; MIG-17, MIG-19 and MIG-23 own its DEV/UAT/PROD executions.

### Shared completion rule

Close an issue when its acceptance criteria pass, relevant configuration/code is reviewed and merged, and evidence is linked. Record an explicit “not applicable” reason for unused features. A known blocker to the agreed target is not an accepted exception; unresolved nonblocking risks need an owner and approval from the change owner.

Keep secret values and sensitive production records out of issue attachments. Link to access-controlled evidence instead.

---

## EPIC E1 — Establish migration scope and release gates

**Description:** Agree what must migrate, who owns it, and how the team will judge readiness. Establish measurable gates for QA feasibility, DEV migration, UAT validation and production.

**Completion criteria:** The retained DAGs and integrations have owners, the target stack is confirmed, and cutover/rollback thresholds and authority are agreed.

**Milestone:** M0 • **Label:** `phase::readiness`

### MIG-01 — Inventory the current Airflow estate and confirm scope

**Owner:** Airflow engineer • **Depends on:** None

Capture the configuration and workload needed to migrate the existing service without losing required behavior.

**Acceptance criteria**

- [ ] Inventory includes DAGs, schedules, owners, criticality, providers, custom code, API callers, authentication, secrets, executor, database, storage and deployment versions.
- [ ] Every retained DAG and external caller has an owner; unused or unowned items have a keep/retire decision.
- [ ] Critical DAGs, external side effects and recovery constraints are identified for testing and rollback reconciliation.
- [ ] Scope pins Airflow 3.2.2, confirms blue/initial-green Python 3.11 and records the target Python decision (planning default 3.13), with supported image/provider/platform prerequisites [S1].
- [ ] An environment/lane resource register identifies cluster/pool, namespaces/releases, PostgreSQL server/databases/roles, Redis instances, Azure share/paths/PVs/PVCs, identities/federation subjects, hostnames and pipeline targets. Mark each resource create/reuse and its owner.
- [ ] Existing DEV/UAT/PROD blue deployments and QA gaps are documented; the same-pool/server/share design and any environment-specific constraints are resolved before promotion.

### MIG-02 — Agree responsibilities, milestones and the change window

**Owner:** Change owner • **Depends on:** MIG-01

Define delivery ownership and a practical release sequence, including business availability and production freeze constraints.

**Acceptance criteria**

- [ ] Every issue has an accountable owner; go/no-go and rollback decision makers have named deputies.
- [ ] QA blue setup, second-v2 setup, QA POC/design, DEV, UAT, rehearsal, cutover and stabilization dates follow the gated sequence and respect confirmed freeze periods.
- [ ] Cutover support, escalation contacts, stakeholder communications and change approval requirements are recorded.
- [ ] A cutover/rollback runbook has an owner and a review location; detailed timings will be completed after rehearsal.

### MIG-03 — Capture the baseline and agree release/rollback thresholds

**Owner:** Airflow engineer • **Depends on:** MIG-01

Use current operational data to define objective acceptance limits. Avoid arbitrary resource sizes or an unexplained percentage tolerance.

**Acceptance criteria**

- [ ] Capture an existing-v2 business baseline for readiness: task success, critical DAG completion times, scheduling/queue delay, DAG parse duration, component resource use, nodepool headroom, database/pool pressure and share I/O. Define the equivalent QA-blue measurements for QA-02 to add after bootstrap; QA's small POC is not a production-load baseline.
- [ ] A gate table specifies numeric performance/reliability limits, observation periods and the required critical workflow checks.
- [ ] Rollback triggers, the decision deadline and maximum service recovery time are agreed; start with the sample's 30-minute recovery target and validate feasibility in drills.
- [ ] DAG owners and Operations accept the thresholds, including any expected maintenance delay.

---

## EPIC E0 — Build QA blue and green v2 lanes and prove the migration design

**Description:** Establish the new QA environment, create the second Airflow 2.10.5 installation on reused infrastructure, then use that green installation for the Airflow 3.2.2 POC. Deliver reusable Helm/ADO automation and a tested architecture/runbook for later environments.

**Completion criteria:** MQ0, MQ1 and MQ2 pass in order; blue remains recoverable at 2.10.5; green reaches 3.2.2 on a migrated clone; isolation, installation/activation guards and rollback have evidence.

**Milestones:** MQ0–MQ2 as assigned below • **Environment:** QA • **Label:** `phase::build` unless overridden

### QA-01 — Provision the first QA Airflow infrastructure

**Owner:** Cloud engineer • **Depends on:** MIG-01, MIG-02 • **Milestone:** MQ0

Create the infrastructure missing from the new QA environment using reviewed infrastructure as code. This is the baseline infrastructure for blue, not a request for two copies of every Azure service.

**Acceptance criteria**

- [ ] QA AKS access and the intended nodepool exist; provision initial capacity if absent and record capacity for both lanes on that same pool. OIDC/workload identity, Azure Files CSI, ingress/DNS, network routes and required provider registrations are available.
- [ ] One QA PostgreSQL server, blue metadata database and roles are provisioned with TLS, private connectivity, backups and recorded restore responsibility; confirm PostgreSQL version and PgBouncer applicability.
- [ ] One QA Azure file share is provisioned with blue DAG/log directories, blue static PV/PVC bindings, `ReadWriteMany`, `Retain`, suitable SMB mount permissions and a tested backup/retention policy [S7].
- [ ] Blue's Redis instance is provisioned with required TLS/network access and broker-compatible persistence/eviction settings. Record the currently available Azure Redis service/tier and Celery compatibility; do not assume an old service SKU remains available.
- [ ] Existing registry/Nexus/Key Vault/network services are reused where suitable; QA secret references and blue identity/federation are configured without copying production write credentials.
- [ ] Infrastructure plan/apply and rerun evidence identify create/reuse operations; shared resources have lifecycle ownership outside an individual Helm uninstall.

### QA-02 — Deploy and establish the QA blue Airflow 2.10.5 baseline

**Owner:** Airflow engineer • **Depends on:** QA-01, QA-04, QA-05, QA-06 • **Milestone:** MQ0

Deploy the first installation using the existing 2.10.5 image, chart/configuration and representative DAG package, parameterized for QA blue.

**Acceptance criteria**

- [ ] Blue deploys through ADO into its registered namespace/release on the intended nodepool and reports Airflow 2.10.5/Python 3.11; image/chart/DAG digests and sanitized values are recorded.
- [ ] Metadata DB, result backend, broker, mounts, secrets and workload identity resolve to QA blue resources; no production business destinations are reachable through test configuration.
- [ ] UI/login, scheduler/worker/triggerer health, DAG imports, safe DAG execution, XCom, connection/variable lookup and task logs pass. Populate representative run history/pools/paused states for clone-validation tests.
- [ ] Restart/mount persistence and a backup/restore check pass; MIG-03 records the QA baseline and blue is accepted before green setup starts.

### QA-03 — Create green namespace, service accounts and federated credentials

**Owner:** Platform engineer • **Depends on:** QA-02 • **Milestone:** MQ1

Create the second namespace on the same nodepool. Reuse an existing managed identity if its permissions are suitable, but add federation for the green service-account subjects; blue's namespace subject cannot authenticate green [S8].

**Acceptance criteria**

- [ ] Green namespace, RBAC, quota/LimitRange, network policies and node placement are configured; blue resources are retained. Service-account names are explicit for runtime components and deployment/migration jobs that need Azure access.
- [ ] Required federated credentials use the cluster OIDC issuer, `system:serviceaccount:<green-namespace>:<service-account>` subject and `api://AzureADTokenExchange` audience; service-account client-ID annotations and workload-identity pod labels are rendered correctly [S8].
- [ ] Tokens and authorized Key Vault/Azure operations succeed from representative green pods; wrong namespace/subject tests fail. Separate component service accounts receive their own federation entries where needed.
- [ ] Existing blue federation remains functional. ADO service-connection federation/permissions, if used, are verified separately from pod workload identity.
- [ ] Reused CSI mount authentication is validated for the deployed driver/AKS version; workload identity for application Azure calls is not assumed to supply SMB mount authentication automatically.

### QA-04 — Parameterize Helm for environment, lane and Airflow version

**Owner:** DevOps engineer • **Depends on:** MIG-01 • **Milestone:** MQ0

Refactor the existing deployment into reusable base values plus environment/lane overrides. Support the initial second 2.10.5 installation and the later green-only 3.2.2 upgrade without editing hardcoded blue identifiers.

**Acceptance criteria**

- [ ] Parameters cover environment, lane, cluster/context, namespace, release/resource names, Airflow/image/chart versions, node selectors/tolerations, DB name and secret references, broker/result backend, service accounts/client IDs, share/PV/PVC/subpaths, DAG version, ingress/base URL, replicas and migration/activation mode.
- [ ] Namespace-scoped names and cluster-scoped PV names/CSI handles cannot collide. Shared resources are referenced, not recreated/adopted by a second Helm release; no `fullnameOverride` or generated endpoint accidentally addresses blue.
- [ ] Blue-v2, green-v2 and green-v3 configurations render/lint against pinned compatible charts; validate namespace/release routing and the migration-job DB target. Record a chart transition if one chart version cannot support both versions safely [S6].
- [ ] Deploying/updating green changes only its owned resources and approved lane directories. Blue upgrade/render comparison shows no unintended change.
- [ ] Idle mode holds scheduling/execution and disables or constrains migration hooks until the correct green database is selected. Autoscalers and ADO reconciliation cannot silently reactivate the inactive lane.

### QA-05 — Update ADO infrastructure, Helm and Nexus deployment pipelines

**Owner:** DevOps engineer • **Depends on:** QA-04, QA-06 • **Milestone:** MQ0

Implement explicit environment/lane selection for infrastructure, Airflow and DAG deployments. GitLab hosts the work backlog; ADO remains the deployment system and Nexus remains the DAG artifact source.

**Acceptance criteria**

- [ ] Pipeline inputs validate the registered environment/lane, cluster/context, namespace/release, chart/image/DAG versions, DB/broker secret references, share paths, identity and operation (`bootstrap`, `install`, `upgrade`, `migrate`, `activate`, `rollback`). Missing or inconsistent inputs fail before writes.
- [ ] Service connections, agent network reachability, namespace RBAC, variable groups and Key Vault references work for QA blue and green. Higher-environment targets use the same templates and environment-specific permissions.
- [ ] Infrastructure/deploy stages run QA-06 preflight under a per-environment exclusive lock before creating or changing lane resources; migration and activation have explicit steps, not implicit effects of any Helm upgrade.
- [ ] Nexus DAG packages are checksum-verified and published to the selected lane release directory with a tested atomic publish/selection method on SMB. A green deployment never updates blue's selected release or cleans its files.
- [ ] Sanitized deployment manifests record all artifact versions, resolved target identifiers and guard results. Reruns update the intended existing release; failure/recovery instructions cover partially completed deployments.
- [ ] QA evidence demonstrates both lane targets, rejected wrong-target operations and safe reruns. DEV/UAT/PROD migration stages require the previous environment's recorded exit gate.

### QA-06 — Implement duplicate-install and active-lane guards

**Owner:** DevOps engineer • **Depends on:** QA-04 • **Milestone:** MQ0

Warn and fail when an unexpected Airflow installation already exists. An intentional blue/green migration must be distinguishable from an accidental duplicate. A generic `allowSecond=true` bypass is insufficient.

**Acceptance criteria**

- [ ] Preflight inventories Helm release records (including failed/pending/scaled-down releases), registered namespaces, Airflow workloads/jobs and ownership labels across the selected cluster. Compare with the environment/lane register; legitimate other environments are recognized, unexpected installations are reported with namespace/release details.
- [ ] Default bootstrap/install fails with a clear warning and nonzero exit when a conflicting installation exists. Controlled green creation requires an explicit migration operation, exact expected blue/green identifiers, no unregistered extra installation, distinct DB/broker and nonoverlapping writable paths; it starts inactive. Updating the exact registered release remains idempotent.
- [ ] Green migration jobs check the resolved server/database/role and schema against the allowlisted green target, explicitly reject blue, and stop if any incompatible green v2 process is still running. Credentials cannot grant green runtime writes to blue.
- [ ] A per-environment lock spans inspection and deployment to prevent two pipelines passing simultaneously; preflight rechecks immediately before mutation. Inspection/RBAC failures, drift and unresolved partial installations fail closed with actionable recovery guidance.
- [ ] Activation/cutover/rollback checks verify the other lane's scheduling/execution is stopped or safely fenced, including workers, triggerers, queued work, API/manual triggers, autoscalers and GitOps/ADO reconciliation. Deployment presence is allowed; duplicate business execution is blocked.
- [ ] Tests cover: empty environment/first install; expected blue → controlled green; same-release rerun; unexpected second/third install; conflicting DB/Redis/paths; failed/scaled-down existing release; insufficient discovery rights; concurrent pipeline runs; and attempt to activate both lanes. Guard evidence is attached without secrets.

### QA-07 — Create green database and a separate Redis on reused QA infrastructure

**Owner:** DBA / Cloud engineer (assign one accountable owner) • **Depends on:** QA-02, MIG-03 • **Milestone:** MQ1

Reuse the QA PostgreSQL server, create another database for green and provision a second Redis instance. Initially green's database supports the second 2.10.5 installation; later it is replaced by a controlled blue clone.

**Acceptance criteria**

- [ ] Green database/owner/application role and credentials are distinct from blue on the same PostgreSQL server; explicit grants and negative connection/write tests prove green runtime cannot write blue. Privileged clone/migration credentials are separate from runtime credentials.
- [ ] A new green Redis instance, credentials, TLS and private routing are ready; using another logical DB or key prefix in blue Redis does not satisfy isolation.
- [ ] Metadata and Celery result-backend settings both select green's database; PgBouncer DB/user routing and direct admin/migration endpoints are verified if PgBouncer is used.
- [ ] Shared DB storage/IOPS/connections and nodepool capacity support both v2 installations and clone/migration activity without breaching the baseline; monitor the combined budget.
- [ ] Bootstrap DB recreation and clone ownership/ACL reapplication are scripted with blue explicitly protected. No retained green history is silently discarded; export any required bootstrap/POC evidence before replacement.

### QA-08 — Bind the same Azure file share safely in both namespaces

**Owner:** Platform engineer • **Depends on:** QA-02, QA-03 • **Milestone:** MQ1

Reuse the physical QA share while keeping independent DAG deployment selection and writable log paths. A PVC is namespaced and a PV binds exclusively to one claim; create separate green bindings to the existing share [S9].

**Acceptance criteria**

- [ ] Static green PV(s) with unique names/CSI volume handles and green PVC(s) point to the existing share, with `ReadWriteMany`, explicit binding and `Retain`; no dynamic provisioning silently creates a second share [S7].
- [ ] Create lane directories before using `subPath`; mount only the intended DAG/log directory into each component. DAGs are runtime-read-only, log roots do not overlap, and release jobs write only their selected lane paths.
- [ ] SMB UID/GID/mount settings are tested with the actual images. Green initialization/cleanup performs no recursive ownership/delete operation over the share root or blue directories; subdirectories are operational separation, not proof of storage ACL isolation.
- [ ] Both namespaces mount successfully on the same node and on different nodes in the same pool, including restart/rescheduling; no CSI staging/handle collision, log overwrite or DAG publication cross-effect occurs.
- [ ] Selective lane artifact/log recovery is demonstrated. A whole-share snapshot restore is not used for routine lane rollback because it could rewind the active lane.
- [ ] Uninstall/delete tests on disposable bindings preserve the shared share and blue files; storage lifecycle is managed independently of lane Helm releases.

### QA-09 — Deploy the second Airflow 2.10.5 installation in green

**Owner:** Airflow engineer • **Depends on:** QA-03, QA-04, QA-05, QA-06, QA-07, QA-08 • **Milestone:** MQ1

Deploy green at 2.10.5 first using the parameterized pipeline, same nodepool, new namespace, separate database/Redis and shared file share. This is a required intermediate deployment, not an empty v3 install.

**Acceptance criteria**

- [ ] Green reports Airflow 2.10.5/Python 3.11 and uses the intended existing v2 image/chart/DAG release, with the green namespace/release and actual pod nodepool recorded.
- [ ] Runtime identity, DB/result-backend routing, separate Redis, service/ingress/SSO test hostname and DAG/log mounts pass positive/negative isolation checks; green bootstrap schema is 2.10.5-compatible.
- [ ] Green starts with business DAGs paused and automatic scheduling held; a tightly scoped smoke test runs only against harmless sandbox resources. No same business interval is processed by both lanes.
- [ ] Compare blue and green v2 behavior: imports, connection/variable lookup, pools, XCom, worker/triggerer operation, callbacks, UI/API and task logs. Resolve setup defects before introducing v3.
- [ ] Capture blue health/configuration/schema/file checksums before and after, plus both v2 manifests. Rerunning the second-install pipeline targets green without warning suppression or blue changes; MQ1 evidence is accepted.

### QA-10 — Complete the green 3.2.2 POC and accept the swimlane design

**Owner:** Airflow platform lead • **Depends on:** QA-09, MIG-08, MIG-12, MIG-13, MIG-14, MIG-16, MIG-18 • **Milestone:** MQ2 • **Label:** `phase::validation`

Upgrade the second installation and turn QA findings into the implementation contract for DEV/UAT/PROD. MIG-15–16 own clone/upgrade proof and MIG-18 owns recovery proof; link their evidence instead of duplicating their execution tickets.

**Acceptance criteria**

- [ ] Green v2 is stopped; a fresh blue clone replaces its disposable bootstrap database with target ownership/grants reapplied. Only green's clone is migrated. Upgrade the same green Helm release to the pinned 3.2.2 image/configuration and prove no v2 process accesses the migrated DB.
- [ ] Component health, zero retained-DAG import errors, execution API/worker communication, auth/API caller smoke checks, Fernet decryption, logs, representative schedules and selected Python/dependency tests pass on the migrated clone.
- [ ] Controlled cutover, failed-migration rebuild and rollback to blue 2.10.5 pass with timings, scheduling fencing and sample side-effect reconciliation. Preserve blue schema/artifacts and demonstrate lane-only storage recovery.
- [ ] Publish the revised architecture/resource register and blue/green swimlane runbook, including ADO stages/parameters, federation subjects, guard behavior, clone method/path, startup order, traffic switch, rollback/reconciliation, shared-resource risks and retirement ownership.
- [ ] Platform, DBA, DevOps and Operations accept technical feasibility and MQ2 exit; remaining DEV/UAT validation is named, no feasibility blocker remains, and the reusable release/template versions are frozen for DEV promotion.

---

## EPIC E2 — Make DAGs and the Python 3.13 runtime compatible

**Description:** Produce a reproducible Airflow 3.2.2 image using the Python version selected in QA (planning default 3.13) and a DAG release tested on both versions. Release compatible fixes on v2 first and preserve the exact blue fallback artifacts.

**Completion criteria:** The target dependency set installs cleanly, affected code and schedules are tested, CI checks the required runtime combinations, and both target and fallback releases are recorded.

**Milestone:** MQ2 • **Label:** `phase::build`

### MIG-04 — Fix Airflow 3 incompatibilities in DAGs and custom code

**Owner:** DAG engineering lead • **Depends on:** MIG-01

Scan and review DAGs, operators, hooks, plugins, callbacks, templates and policies for removed or changed Airflow behavior. Review applicable release changes from 3.0 through 3.2.2 [S2, S3].

**Acceptance criteria**

- [ ] Ruff AIR301/AIR302 findings are resolved; manual review covers direct metadata DB access in tasks, removed features/context keys, custom XCom handling and plugins.
- [ ] Task code uses supported SDK/provider/API interfaces; required standard-provider imports and custom plugin replacements are implemented.
- [ ] All retained DAGs import without errors on the target runtime, and tests for affected custom code and callbacks pass.
- [ ] The migration DAG release also passes on 2.10.5/Python 3.11. Defer v3-only imports/syntax/features until the relevant blue fallback is retired; a required incompatibility needs an explicit release/rollback design decision before promotion.

### MIG-05 — Preserve scheduling and data interval behavior

**Owner:** DAG engineering lead • **Depends on:** MIG-01

Make intended scheduling behavior explicit so the upgrade does not create missed intervals, duplicate processing or an unexpected catchup burst.

**Acceptance criteria**

- [ ] Retained DAGs have reviewed schedules/timetables, timezone, catchup, start dates and active-run/task limits; behavior-sensitive defaults are explicit.
- [ ] Tests compare expected next runs and data intervals for each scheduling pattern in use, including timezone/DST boundaries where applicable.
- [ ] Critical DAG owners accept expected behavior for scheduled, manually triggered, backfilled and asset/dataset-triggered runs where used.
- [ ] Any intentional behavior change is documented, and the cutover runbook defines the expected first run and missed-interval handling.

### MIG-06 — Resolve providers and dependencies for Python 3.13

**Owner:** Airflow engineer • **Depends on:** MIG-01

Resolve Airflow, providers, internal libraries and DAG dependencies using the Airflow 3.2.2 constraints for the selected Python (planning default 3.13) as the starting point [S4]. Review Python changes across 3.12–3.13 if that default is retained.

**Acceptance criteria**

- [ ] A clean Python 3.13 installation resolves the full target package set with Airflow pinned to 3.2.2; `pip check` passes and exact versions are recorded.
- [ ] Native dependencies have compatible wheels for the deployment architecture or a tested, reproducible build; required internal libraries install and pass their relevant tests.
- [ ] Removed standard-library usage and relevant runtime changes are reviewed against Python 3.12/3.13 porting guidance and tested where used; shared DAG source stays compatible with Python 3.11 [S5].
- [ ] Dependency changes that could affect outputs have regression coverage; no unresolved package blocker remains for the agreed target.

### MIG-07 — Build and publish the Airflow 3.2.2/Python 3.13 image

**Owner:** DevOps engineer • **Depends on:** MIG-06

Build one immutable target image using a verified compatible base. Promote the same image through environments rather than rebuilding for production.

**Acceptance criteria**

- [ ] The image reports Airflow 3.2.2 and the pinned CPython 3.13 patch version; its base image, build inputs and package manifest are recorded.
- [ ] Build, package checks and vulnerability scan pass the team's release policy; exceptions are approved and owned.
- [ ] Required component entrypoints and provider/plugin imports pass smoke checks using the deployment user.
- [ ] The published digest is used by target components and migration jobs; the v2 image remains available for initial green installation and rollback. DAGs remain Nexus-delivered on the share, so DAG-only releases do not require an image rebuild.

### MIG-08 — Add runtime compatibility checks and release the migration DAG package

**Owner:** DevOps engineer • **Depends on:** MIG-04, MIG-05, MIG-07

Enforce the required compatibility in CI and publish traceable DAG releases using the existing delivery pipeline.

**Acceptance criteria**

- [ ] CI checks Airflow 2.10.5/Python 3.11 for the fallback release and Airflow 3.2.2/Python 3.13 for the target release, including import tests, affected unit tests and Ruff breaking-change rules.
- [ ] Required checks block merging/deployment on failure. The shared migration DAG package passes both runtime combinations; release to independent lane directories with matching checksums, preserving each lane's selected version and blue's known-good artifact.
- [ ] Compatible fixes are released to QA v2 with no blocking regression over its agreed representative cycle. Reusable promotion steps require the same QA → DEV → UAT → PROD v2 checks before each environment's cutover; MIG-17/19/23 own those later executions, so MQ2 does not depend on a later-environment deployment.
- [ ] Target/fallback DAG artifact versions and checksums are recorded; deploying the previous release and changing only green's selection is demonstrated in QA and repeated in DEV. Test the chosen publication/selection mechanism on SMB rather than assuming symlink behavior.

---

## EPIC E3 — Prepare the Airflow 3 platform and integrations

**Description:** Adapt the parameterized two-lane platform for Airflow 3 components, execution API communication, access control and operational visibility. Preserve lane separation while explicitly reusing the nodepool, PostgreSQL server and file share.

**Completion criteria:** The target configuration deploys, runtime resources and credentials are correctly isolated, users/callers can integrate, and operators can observe and diagnose the service.

**Milestone:** MQ2 • **Label:** `phase::build`

### MIG-09 — Update Helm values and Airflow configuration for v3

**Owner:** Platform engineer • **Depends on:** MIG-01, MIG-07, QA-04

Select and pin a compatible Helm chart. Update the configuration for the API server, standalone DAG processor, scheduler, workers and triggerer where used [S2, S6].

**Acceptance criteria**

- [ ] The pinned chart supports 3.2.2 and the deployed AKS/Helm versions; QA, DEV, UAT and PROD green values render/lint and pass validation, while the preserved blue-v2 chart/values remain available [S6].
- [ ] Removed/renamed configuration is resolved using target-version tooling; required executor/provider and DAG source settings are included.
- [ ] The task execution API URL, API server, standalone DAG processor, scheduler, Celery workers and triggerer commands/probes are verified in QA; no generated URL or secret reference addresses blue. Account for Celery worker broker/result-backend access as well as task execution API access.
- [ ] Initial requests, limits and database connection budgets fit measured capacity; automatic migration/startup behavior cannot change the v2 database or start business DAGs during an idle build.

### MIG-10 — Define and verify the reusable lane resource contract

**Owner:** Platform engineer • **Depends on:** MIG-01, MIG-03, QA-01

Define the resource validation used in QA and each later environment. QA-01/07/08 implement the QA resources; this issue owns their reusable isolation/capacity checklist and later-environment mappings, avoiding a second provisioning ticket for the same resources.

**Acceptance criteria**

- [ ] Every environment maps green to the same PostgreSQL server as blue but a different database/roles; metadata and result-backend routes, optional PgBouncer pools and direct migration access reach green only. Reapply/verify ACLs after each clone restore.
- [ ] Each lane has its own Redis instance/credentials; no blue queue message can reach green workers and green queues are drained/empty before changing major version.
- [ ] Each environment reuses its existing nodepool and file share; namespace-specific PV/PVC bindings and nonoverlapping DAG/log paths preserve existing blue files and selected release. Do not add a dedicated green share or nodepool by default.
- [ ] Shared nodepool, database server and share budgets support overlap, migration and restart capacity without breaching agreed limits; environment prerequisites and owners are recorded for DEV/UAT/PROD execution tickets. Rehearsal tasks cannot write to production business systems.

### MIG-11 — Configure secrets, execution API access and network controls

**Owner:** Platform engineer • **Depends on:** MIG-09, MIG-10, QA-03, QA-07, QA-08

Supply the target with the keys and connectivity needed to run tasks securely, including internal execution API communication.

**Acceptance criteria**

- [ ] Migrated encrypted connections/variables decrypt with the retained Fernet key; required auth/JWT/API secrets are supplied consistently to the appropriate components.
- [ ] Secret references resolve from the existing secret store, and no secret values are committed to Git or exposed in logs.
- [ ] Green service accounts/federated credentials work after the v2 → v3 component/chart changes, including new DAG processor/API server accounts where used. Reused Azure/registry/SSO access is tested without changing blue subjects or rotating blue secrets.
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
- [ ] Caller tests cover the operations actually used, token expiry/reacquisition and relevant 3.2.2 authentication/backfill response changes [S3].
- [ ] Each caller has a cutover and fallback configuration with a named owner and a verified safe trigger/retry behavior.
- [ ] Caller owners accept QA contract/smoke evidence and the planned switch/fallback procedure; MIG-21 obtains UAT execution sign-off before production promotion.

### MIG-14 — Update monitoring, logs and a reusable smoke test

**Owner:** Operations engineer • **Depends on:** MIG-03, MIG-09, MIG-11

Provide an end-to-end check and the operational signals needed for release gates and incident response.

**Acceptance criteria**

- [ ] Health checks and dashboards cover deployed v3 components, execution API failures, task failures/queue delays, resource use and database/pool pressure.
- [ ] Alerts implement the agreed gates and route to the responsible team; representative failures demonstrate delivery.
- [ ] A harmless smoke DAG proves task execution, connection/variable lookup, pool use, XCom, callback and readable logs.
- [ ] Runbook checks also verify login and an authenticated REST API operation; instructions identify where to find component and task logs.

---

## EPIC E4 — Prove clone migration and recovery in QA, then migrate DEV

**Description:** Establish a repeatable same-server backup, clone, migrate and validate procedure in QA. Prove controlled QA cutover and rollback before DEV promotion, then repeat against DEV's own blue source, including reconciliation of work performed while green was active.

**Completion criteria:** QA proof is accepted at MQ2; DEV passes cutover and rollback at M1; each original blue database is preserved and another engineer can execute the corrected recovery procedure.

**Milestones:** MQ2 for MIG-15–16/initial MIG-18; M1 for MIG-17 and DEV repeat of MIG-18 • **Label:** `phase::build`

### MIG-15 — Verify database backup, restore and clone procedures

**Owner:** DBA • **Depends on:** MIG-10, QA-02, QA-07

Verify backups and select a method that clones blue into another database on the same PostgreSQL server. Test `pg_dump`/`pg_restore` or an authorized database-template copy against actual Azure permissions and measured downtime. A service PITR that restores to a new server is a recovery test or explicit architecture exception, not the normal same-server green setup.

**Acceptance criteria**

- [ ] A recorded backup restores successfully in an isolated environment, with timing and retention sufficient for the agreed fallback window.
- [ ] The selected clone method produces a consistent, usable copy; required permissions, session handling and storage headroom are documented.
- [ ] Green runtime and migration credentials cannot modify blue. The separate DBA clone identity has only the source-read/administrative rights needed by the selected method; destructive target operations verify the exact green/rehearsal DB and reject blue.
- [ ] The runbook specifies consistent-snapshot semantics and source quiescence: a database-template copy requires zero source sessions, including pooled sessions; dump/restore uses the tested consistency procedure. The final cutover snapshot follows business pause/drain regardless of clone method.
- [ ] Stop all green v2 processes and migration hooks before replacing the disposable bootstrap/rehearsal DB; preserve required evidence, restore ownership/grants and sanitized test connection settings, and verify Fernet decryption. No service starts on stale/partially restored metadata.
- [ ] Capture blue schema/state and artifact checksums before/after QA rehearsals; clone/migration writes are confined to green. Record capacity/backup scope for the shared server without changing its major version.

### MIG-16 — Rehearse and validate the metadata database upgrade

**Owner:** DBA • **Depends on:** MIG-07, MIG-09, MIG-15, QA-09

Run `airflow db migrate` from a pinned 3.2.2 Kubernetes Job against green's fresh clone of the 2.10.5 blue database. Prove the path in QA and repeat with each environment's source; never treat a successful empty-database initialization as migration evidence [S2].

**Acceptance criteria**

- [ ] The selected direct or intermediate upgrade path completes on the clone, with migration logs, duration and schema readiness checks recorded.
- [ ] Retained DAG/run/task history, XCom samples, pools, connection/variable decryption and applicable auth records pass migration-aware checks; counts are reconciled to intentional schema/retention changes.
- [ ] Source paused states and active/deferred/retry work are captured; target startup prevents accidental scheduling and each pending item has a drain/recovery decision.
- [ ] Any history cleanup has an approved retention rule and is tested on the clone; failed migration recovery recreates the clone from a known source, preserving v2.
- [ ] Preflight confirms server, green database, application/migration role, direct endpoint and expected schema before writes; use direct PostgreSQL for migration/admin jobs and validated pooled endpoints only for runtime where applicable. Schema upgrade hooks cannot bypass these checks.
- [ ] Source/green states and timings are recorded per attempt: backup, clone, optional approved cleanup, migrate, validate and recovery. A failed/partial migration is rebuilt from a fresh clone; restarting green v2 on the migrated DB or using Helm rollback as schema downgrade is prohibited.

### MIG-17 — Repeat the proven two-lane migration and rollback in DEV

**Owner:** Airflow engineer • **Depends on:** QA-10, MIG-08, MIG-12, MIG-14, MIG-16, MIG-18 • **Milestone:** M1 • **Environment:** DEV

Use the accepted QA templates/runbook against existing DEV blue. Build DEV green at 2.10.5 first, then upgrade that second installation to 3.2.2 on a clone of DEV blue; execute cutover and repeat the MIG-18 rollback drill with safe test targets.

**Acceptance criteria**

- [ ] The pinned target image/configuration and selected DAG release deploy with healthy components and zero retained-DAG import errors.
- [ ] QA exit is recorded before DEV migration. DEV green starts at 2.10.5 in a new namespace on DEV's same nodepool/server/share, with a separate database/Redis, namespace federation, lane PV/PVC paths and passing ADO guards; v2 parity evidence precedes its v3 upgrade.
- [ ] The runbook covers pause/drain, stop v2, capture state, clone/migrate, start v3 safely, smoke checks and controlled resume; timings and checkpoints are recorded.
- [ ] Smoke tests and representative DAGs pass, including the first scheduled run, without duplicate external processing.
- [ ] Abort-before-activation and rollback-after-execution drills from MIG-18 pass again in DEV with caller/ingress reversal, fencing and reconciliation; shared resource contents/capacity and blue restartability remain valid.
- [ ] Defects and unclear steps are corrected in the runbook; platform/Operations record M1 DEV exit and identify any remaining UAT work. UAT migration stays blocked until this gate passes.

### MIG-18 — Prove rollback in QA and reuse the drill in later environments

**Owner:** Airflow engineer • **Depends on:** MIG-02, MIG-03, QA-09, MIG-08, MIG-12, MIG-14, MIG-16 • **Milestone:** MQ2 initially; repeat at M1/M2

First in QA, deploy upgraded green with safe test targets, execute a controlled handover and test both an abort before business execution and rollback after green produces results. Repeat the corrected drill in DEV (MIG-17) and UAT (MIG-22). Return to preserved blue rather than downgrade green's schema.

**Acceptance criteria**

- [ ] The runbook stops v3 scheduling/execution before restarting v2 paused, restores the correct v2 image/DAG release/traffic/caller settings, and prevents both stacks processing the same production work.
- [ ] A drill exports v3 run outcomes and reconciles processed/missed intervals, side effects and configuration changes before affected v2 DAGs resume.
- [ ] Critical test outputs have no unexplained duplicates or gaps; risky replays require a DAG-owner decision rather than blanket marking runs successful.
- [ ] Another engineer completes the drill within the agreed service recovery target, with separate timing for business reconciliation and all runbook corrections captured.
- [ ] Blue's database has not been schema-migrated; green queues/processes and automatic restarts are fenced before blue resumes. Restore only blue's selected DAG artifact/configuration, not the entire shared file share or server; preserve green evidence and historical-log access.
- [ ] The service RTO measures restored UI/API/smoke service separately from full DAG/business recovery. Export green history/configuration/side effects for reconciliation; do not claim new green run history is merged back into blue or that external writes are undone.

---

## EPIC E5 — Validate business workflows and operational readiness in UAT

**Description:** Validate the target with production-like data, load and workflow patterns. Obtain evidence that the Airflow and Python changes preserve outputs and that integrations and recovery are ready for production.

**Completion criteria:** All critical DAGs pass business checks, representative remaining patterns pass, agreed service thresholds are met, and owners accept UAT evidence.

**Milestone:** M2 • **Label:** `phase::validation`

### MIG-19 — Validate DAG outputs and scheduling on the target runtime

**Owner:** QA lead • **Depends on:** MIG-17, MIG-18 • **Environment:** UAT

After DEV exit, repeat the registered second-2.10.5 setup on UAT's same pool/server/share, with a new namespace, separate database/Redis and working identity/pipeline/guard. Record v2 parity, then upgrade UAT green using a fresh clone of UAT blue and the tested procedure. Execute workflows against safe destinations on Airflow 3.2.2 and the QA-selected Python (planning default 3.13).

**Acceptance criteria**

- [ ] UAT second-v2 setup and green-only upgrade evidence uses the accepted resource/identity/guard contract and recorded image/DAG/chart/configuration versions, passes database validation and has zero retained-DAG import errors.
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
- [ ] Measure shared-nodepool contention/eviction and share I/O during overlap, clone migration and peak execution; node drain accounts for both namespaces on the same pool. UAT probes actual common failure behavior rather than assuming a separate green pool.
- [ ] Final production sizing and database/broker/share limits are recorded, with evidence of shared capacity for fallback. Load/connection budgets cover both databases and new v3 API/DAG processor components; remediation stays on the agreed shared architecture unless an explicit design exception is accepted.

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

After UAT exit, prepare production green on the same pool/server/share as production blue. Deploy green at 2.10.5 in its new namespace with separate DB/Redis, prove safe setup parity, then rehearse the upgrade to 3.2.2 on a fresh production-sized clone without executing production business work.

**Acceptance criteria**

- [ ] Production target resources, namespace federation, lane storage bindings, ADO parameters/guard, secret references and image/DAG/chart versions match the accepted design; second-v2 parity is recorded before green's upgrade. Blue remains healthy within agreed limits on shared infrastructure.
- [ ] A fresh clone passes the tested upgrade/validation procedure; target scheduling and business writes remain blocked during rehearsal.
- [ ] Timings cover drain assumptions, backup/clone, migration, checks, traffic switch and recovery; the approved window includes measured contingency and rollback reserve.
- [ ] Rehearsal findings are resolved and the change owner accepts the runbook; any later release/configuration drift is reviewed and affected checks rerun.
- [ ] Green is left inactive after rehearsal. Preserve required evidence and discard/recreate only its rehearsal database for the final clone; no previously migrated rehearsal state or POC side effects become production cutover state.

### MIG-24 — Confirm production go/no-go and fallback readiness

**Owner:** Change owner • **Depends on:** MIG-23 • **Milestone:** M3 • **Label:** `phase::cutover`

Make the final release decision using a concise readiness checklist and the agreed thresholds.

**Acceptance criteria**

- [ ] Change approval, cutover window, staffing/deputies, critical DAG owners and stakeholder notifications are confirmed.
- [ ] Backups and preserved v2 image/configuration/DAG release, database, broker, secrets and restart capacity are available for the full fallback period.
- [ ] DNS/ingress, TLS/SSO and external caller switch/fallback settings are verified; release freeze rules and any needed TTL change are recorded.
- [ ] The shared nodepool, database server and file share have capacity and remain protected for green operation and blue restart; lane-only restoration/cleanup is proven. Neither cutover nor fallback restores the whole shared server/share.
- [ ] The go/no-go decision confirms no blocking defect, current drill evidence, numeric triggers and a latest safe rollback decision time.

### MIG-25 — Execute production cutover and controlled DAG resume

**Owner:** Change owner • **Depends on:** MIG-24 • **Milestone:** M4 • **Label:** `phase::cutover`

Execute the approved runbook with checkpoint evidence. Stop and follow the rehearsed abort/rollback procedure if a mandatory gate fails.

**Acceptance criteria**

- [ ] Original pause/run state is captured; v2 is paused, drained and stopped, with every unfinished/deferred item resolved or assigned an approved recovery action before target activation.
- [ ] ADO environment lock, guard and activation fence pass immediately before handover; broker queues and task/process states are checked, and autoscaling/reconciliation cannot restart blue execution during green activation.
- [ ] A fresh consistent backup/clone is migrated and validated within the window; the original v2 database remains preserved and target DAGs stay paused until checks pass.
- [ ] Target component health, login, imports, smoke workflow and caller handover pass before restoring only previously enabled DAGs in owner-approved batches, critical DAGs first.
- [ ] Critical outputs, first-run intervals and release metrics meet the initial gate; phase timings, exceptions and the continue/rollback decision are recorded.
- [ ] Initial validation and named continue/rollback decisions occur at T+1h, T+4h and T+24h as applicable to the measured runbook; failure follows the rehearsed abort/rollback path, not an attempted schema downgrade.

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
- [ ] Retire only blue-owned releases/namespaces, database/roles, Redis instance, PV/PVC bindings, permitted lane directories, hostnames, federation entries and secret references after retention checks; retain anything green still uses, including the Fernet key/managed identity if reused.
- [ ] Keep the shared AKS nodepool, PostgreSQL server, Azure file share/storage account and shared network/identity/registry services. Removing a blue PVC/PV or namespace cannot delete the physical share or green data; retained blue directories/backups have a deletion owner/date.
- [ ] CI and deployment target v3 only after fallback ends; obsolete credentials, access mappings and temporary rehearsal resources are removed as appropriate.
- [ ] Operations documentation, actual costs and migration evidence are updated, with actionable lessons and remaining work recorded.

---

## Deferred improvements

Keep these outside the migration milestones. Create separate follow-up work only after stabilization and a demonstrated need:

| Candidate | Reason to proceed | Minimal acceptance |
| --- | --- | --- |
| Replace SMB DAG storage with NFS | Measured storage/parse performance cannot meet the service target | Demonstrate an agreed improvement in UAT, verify permissions/release delivery and rehearse return to SMB before production rollout. |
| Move task logs to Azure Blob | A defined retention, availability or cost requirement justifies the change | Verify running/completed/failed/retried logs, access controls and retention in UAT; rehearse fallback and preserve historical-log access. |

## Decisions to finalize during QA

These are implementation decisions with owners, not reasons to defer QA setup. Record the selected values and evidence before MQ2; measured production capacity/window is finalized after UAT and rehearsal.

| Decision | Planning position / required result | Owner and issue |
| --- | --- | --- |
| Python, providers and image | Blue/initial-green 3.11; target planning default 3.13; prove constraints/dependencies and pin image/chart digests | Airflow engineer, MIG-01/06/07/09 |
| Real resource identifiers and identity reuse | Environment/lane register replaces example names; add namespace-specific federation even when managed identity is reused | Platform engineer, MIG-01, QA-03/04 |
| Same-server clone technique | Prove permissions, consistency, ownership/grants, recovery and timings; choose dump/restore or template copy from evidence | DBA, MIG-15/16 |
| Shared SMB release/log layout | Preserve existing blue paths, choose green roots and test publish/selection, CSI mounts and selective restore | Platform/DevOps, QA-08, MIG-08 |
| Guard/fencing implementation | Explicit registered pair plus serialized deployment/activation checks; define how autoscalers, jobs and callers are fenced | DevOps engineer, QA-05/06 |
| Shared capacity and change window | QA feasibility evidence, DEV validation, UAT load proof and production-sized rehearsal; do not inherit fixed pool sizes/dates | Cloud/DBA/change owner, MIG-03/20/23/24 |

## Revision summary and source traceability

- Retained MIG-01–27 and added E0/QA-01–10: 7 epics and 37 issues. The older `af-v3-gitlab-backlog.md` remains a historical sample, not the revised implementation backlog.
- Changed the target from 3.3.2 to the requested 3.2.2; added the new QA environment and first-v2/second-v2/green-v3 gates ahead of DEV/UAT/PROD.
- Replaced dedicated-green nodepool/share assumptions with explicit reuse, separate namespace bindings, lane directories and shared-failure/capacity checks (source sections 2–3, 6 and appendix A).
- Added current-phase federation, ADO/Helm parameterization and tested installation/activation guards; the second installation must pass at 2.10.5 before the upgrade (source sections 5–6, revised by user requirements).
- Preserved clone-only schema migration, backup/restore, paused startup, timed rollback and business reconciliation, with QA proof and environment-specific repetitions (source sections 7–12).
- Corrected shared-resource retirement and retained NFS/Blob as deferred work (source sections 13–14). Kept Python 3.13 as an explicit source-plan assumption to confirm rather than the previous backlog's unsupported requirement to use 3.14.

## Official references

Reviewed 22 October 2026. Versioned Airflow sources below cover 3.2.2, not the 3.3.x examples in the older plan. Product documentation does not verify the actual Azure/AKS deployment, installed driver/chart versions or application dependencies. Pin implementation versions during QA; the Helm/Azure/Kubernetes links track current documentation.

- **S1:** [Airflow 3.2.2 supported runtime/platform versions](https://airflow.apache.org/docs/apache-airflow/3.2.2/installation/prerequisites.html).
- **S2:** [Airflow 3.2.2 upgrade guide](https://airflow.apache.org/docs/apache-airflow/3.2.2/installation/upgrading_to_airflow3.html).
- **S3:** [Release changes through Airflow 3.2.2](https://airflow.apache.org/docs/apache-airflow/3.2.2/release_notes.html).
- **S4:** [Airflow 3.2.2 installation and constraints](https://airflow.apache.org/docs/apache-airflow/3.2.2/installation/installing-from-pypi.html) and [Python 3.13 constraints file](https://raw.githubusercontent.com/apache/airflow/constraints-3.2.2/constraints-3.13.txt).
- **S5:** [Python 3.13 changes and porting guidance](https://docs.python.org/3.13/whatsnew/3.13.html) and [Python 3.12 porting guidance](https://docs.python.org/3.12/whatsnew/3.12.html).
- **S6:** [Official Airflow Helm chart requirements and documentation](https://airflow.apache.org/docs/helm-chart/stable/index.html).
- **S7:** [Azure Files CSI volume provisioning and static existing-share mounts](https://learn.microsoft.com/en-us/azure/aks/create-volume-azure-files).
- **S8:** [AKS workload identity: service accounts and federated credentials](https://learn.microsoft.com/en-us/azure/aks/workload-identity-deploy-cluster).
- **S9:** [Kubernetes PV/PVC binding, namespaces and reclaim policies](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).
