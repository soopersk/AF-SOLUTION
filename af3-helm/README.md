# af3-helm — Airflow 3.3.2 / Python 3.14 v3 stack

Helm chart and image inputs for the v3 stack in
[Airflow 2.10.5 to 3.3.2 Migration Plan (Blue Green on AKS).pdf](<../Airflow 2.10.5 to 3.3.2 Migration Plan (Blue Green on AKS).pdf>).
§ references below and in the files are to that plan. Dependency input: [af3-helm-req.md](../af3-helm-req.md),
copied unchanged to [image/requirements.af3.in](image/requirements.af3.in).

```
af3-helm/
├── airflow-v3/                     Helm chart: release + namespace `airflow-v3`
│   ├── Chart.yaml, Chart.lock      depends on the official chart, vendored below
│   ├── charts/airflow-1.22.0.tgz   official Apache Airflow chart (unmodified)
│   ├── values.yaml                 base values, every environment
│   ├── env/{dev,uat,prod}.yaml     per-environment values (<placeholders> to fill)
│   ├── profiles/*.yaml             staged start / standby replica sets (§10.2)
│   └── templates/                  what the official chart does not cover:
│       ├── storage.yaml              v3 Azure Files PV + PVC (§6.6)
│       ├── storage-init-job.yaml     one-time share preparation (§6.6)
│       ├── db-migrate-job.yaml       explicit migration on the direct port (§7.6)
│       └── validate.yaml             render-time guards
├── image/
│   ├── Dockerfile                  runtime image, FROM the official slim 3.3.2-python3.14 image
│   ├── requirements.af3.in         direct dependencies (= af3-helm-req.md)
│   ├── requirements.af3.lock       full pinned closure, linux/amd64, Python 3.14
│   └── scripts/resolve-af3-lock.sh regenerates / checks the lock
└── vendor/
    ├── constraints/constraints-3.3.2-python3.14.txt   official constraints (unmodified)
    └── helm-chart/airflow-1.22.0.tgz.{sha512,asc}     chart checksum and Apache signature
```

## Versions and provenance

| Artifact | Version | Source | Verified |
|---|---|---|---|
| Airflow | 3.3.2 | — | `apache-airflow==3.3.2` pinned in the lock |
| Python | 3.14 | — | constraints and lock resolved for 3.14 |
| Official Helm chart | 1.22.0 | `downloads.apache.org/airflow/helm-chart/1.22.0/` | sha512 matches Apache's; GPG signature good (Jarek Potiuk, key `B84DC54D`, in Apache Airflow KEYS); same digest as the `airflow.apache.org` index |
| Constraints | `constraints-3.3.2/constraints-3.14.txt` | `raw.githubusercontent.com/apache/airflow` | stored as `constraints-3.3.2-python3.14.txt`, the name the requirements file uses |
| Base image | `apache/airflow:slim-3.3.2-python3.14` | Docker Hub | pinned by index digest `sha256:35528ff1…2491` in the Dockerfile |

To re-verify the chart: `cd airflow-v3/charts && sha512sum -c ../../vendor/helm-chart/airflow-1.22.0.tgz.sha512`,
then `gpg --verify` the `.asc` against https://downloads.apache.org/airflow/KEYS.

Only the dependencies this stack needs are kept. The official chart is the only chart; its own bundled
bitnami `postgresql` subchart is left inside the signed package but disabled. Of the three 3.3.2/Python 3.14
constraints files, only `constraints-3.14.txt` is kept: it is the one for installing released providers.

## Where this differs from the plan

| Plan says | Here | Why |
|---|---|---|
| Python 3.13 (D14, §5.5) | **Python 3.14** | As requested and as `af3-helm-req.md` specifies. Airflow 3.3.2 supports 3.10–3.14. D8 still applies: DAG releases must keep running on v2's Python 3.11, so the §5.5 language rules and the CI job become 3.11 + **3.14**. |
| "chart version that supports Airflow 3" (§6.7, appendix D #2) | Chart **1.22.0**, `airflowVersion: 3.3.2` | 1.22.0 is the newest released chart (no 1.23 RC exists). It ships 3.2.2 as its default and supports Airflow 3.1.0+. Pointed at a 3.3.2 image, it renders the 3.x layout: api-server, standalone dag-processor, FAB auth manager. Confirm in dev (appendix D #2). |
| Skeleton `existingClaim: airflow-v3-dags` / `airflow-v3-logs` (§6.7) | One PVC `airflow-v3-files` with sub-paths `dags/`, `logs/`, `plugins/` | §6.6 specifies one PV/PVC with sub-paths; the §6.7 skeleton "shows intent, not final syntax". |
| Secret `airflow-fernet` | `airflow-v3-fernet` (same v2 key value) | Every v3 secret carries the `airflow-v3-` prefix, so the isolation checklist (appendix A) can be read from the names. |

## Prerequisites per environment

- **Helm 3.19.0 or newer.** Chart 1.22.0 requires it; the local `helm 3.2.4` cannot render it.
- **Nodepool** with taint `workload=airflow-v3:NoSchedule` **and** label `workload=airflow-v3` (§6.1).
- **Namespace** `airflow-v3`. PostgreSQL and Redis firewalls must allow the v3 nodepool subnet (§6.2).
- **Ingress controller** namespace: the api-server NetworkPolicy admits the `ingress-nginx` namespace.
  If yours is different, override `airflow.apiServer.networkPolicy.ingress.from` and keep its first
  `podSelector: {}` peer, which lets workers reach `/execution/`.
- **Secrets** in `airflow-v3`, created the way v2's are today (Key Vault CSI or Kubernetes Secrets, §6.3):

| Secret | Key | Value |
|---|---|---|
| `airflow-v3-db` | `connection` | `postgresql://<v3-app-role>:<pw>@<server>:6432/airflow_v3?sslmode=require` (pooled; runtime) |
| `airflow-v3-db-direct` | `connection` | same database on port **5432**: migration Job only |
| `airflow-v3-result-backend` | `connection` | `db+postgresql://<v3-app-role>:<pw>@<server>:6432/airflow_v3?sslmode=require`: v3 DB, never v2's |
| `airflow-v3-broker` | `connection` | `rediss://:<key>@<new-redis>.redis.cache.windows.net:6380/0`: the new Redis |
| `airflow-v3-fernet` | `fernet-key` | **v2's** Fernet key (D3) |
| `airflow-v3-jwt` | `jwt-secret` | new, random, 32+ bytes (D4) |
| `airflow-v3-api-secret-key` | `api-secret-key` | new, random (D4) |
| `airflow-v3-storage` | `azurestorageaccountname`, `azurestorageaccountkey` | the v3 share's storage account |
| `airflow-v3-tls` | `tls.crt`, `tls.key` | certificate for the test hostname |

## Build the image (§5.4)

```bash
cd af3-helm/image
docker build --platform linux/amd64 -t <registry>/airflow:3.3.2-python3.14-<build> .
# scan, push, then record the pushed digest in env/<env>.yaml -> airflow.images.airflow.digest
```

The lock pins all 269 packages. Every pin comes from the constraints file except these, which the
constraints do not cover and were resolved to the newest compatible release:
`azure-monitor-opentelemetry` 1.8.10 with its closure (`azure-monitor-opentelemetry-exporter`
**1.0.0b57**, a beta; `opentelemetry-instrumentation-*` 0.65b0), and `nanoid` 2.0.0. Review them; if any needs
a floor, add it to `requirements.af3.in` (that is the only place pins belong).

After editing `requirements.af3.in` or the constraints, regenerate the lock and check it in CI:

```bash
image/scripts/resolve-af3-lock.sh           # rewrite the lock, list non-constraint pins
image/scripts/resolve-af3-lock.sh --check   # CI: fail if the lock is stale
# behind a TLS-intercepting proxy: export UV_NATIVE_TLS=1
```

## Deploy, by plan step

All commands run from `af3-helm/airflow-v3`, with `ENV=dev|uat|prod`. Every replica change goes through a
profile, so a later `helm upgrade` cannot undo it (do not use `kubectl scale` for this).

```bash
H="helm upgrade --install airflow-v3 . -n airflow-v3 -f env/$ENV.yaml"

# Render and check first (§6 build exit criteria). Use `helm template`, not `helm lint`:
# lint skips the validate.yaml guards.
helm template airflow-v3 . -n airflow-v3 -f env/$ENV.yaml -f profiles/standby.yaml > /tmp/airflow-v3.yaml

# §6 build day: whole stack, nothing running. The post-install Job prepares the share.
$H -f profiles/standby.yaml

# §10.2 Phase C step 5: migrate the clone on the direct port. Each run is a new, kept Job.
$H -f profiles/standby.yaml --set migration.enabled=true --set migration.runId=$(date +%Y%m%d%H%M)
kubectl logs -n airflow-v3 -f job/airflow-v3-db-migrate-<runId>      # keep the full log (§7.6)

# §10.2 Phase D (step 2: the DB secret now points at the migrated clone)
$H -f profiles/validate.yaml       # step 3: api-server + dag-processor; check SSO, DAG count, import errors
$H -f profiles/no-scheduler.yaml   # step 4: workers + triggerer register with the broker
$H                                 # step 4: scheduler last; fully active

# §12 rollback: v3 back to zero (then start v2 and switch traffic back)
$H -f profiles/standby.yaml
```

The chart never migrates on its own: its built-in `migrateDatabaseJob` is disabled, and `validate.yaml`
refuses to render if it is switched back on. Components keep their wait-for-migrations init
container, so they cannot start against an unmigrated schema.

Keep-annotated resources: the PV, the PVC and every `db-migrate` Job survive `helm uninstall` on
purpose. Delete migration Jobs by hand after their logs are in the change record.

## Still to fill from the v2 deployment

These depend on v2's live `helm/values.yaml`, which is not in this repository:

1. **`airflow.airflowLocalSettings`.** Port v2's `airflow_local_settings.py`: `LOGGING_CONFIG` (Azure Monitor
   OpenTelemetry handler), `azure_graph_api_client`, any cluster policies. Review it for Airflow 3, then
   uncomment `config.logging.logging_config_class`.
2. **`airflow.apiServer.apiServerConfig`.** Port v2's `webserverConfig` (AAD / FAB security manager) to the FAB
   provider 3.9.0 imports. Add the v3 redirect URI to the app registration.
3. **Azure identity** for the `orchestration` plugin (Key Vault, Graph): carry over v2's service-account
   annotations / workload-identity labels to each v3 service account.
4. **`storage.mountOptions`**: replace with v2's exact SMB mount options. **`plugins/`**: confirm the Nexus
   deployer publishes `orchestration` to `plugins/` on the v3 share (§6.8). The chart assumes that path.
5. **Sizing**: worker count, `resources` per component, `quotas` / `limits` mirrored from v2 (§6.1). Set the
   scheduler log-groomer `retentionDays` to v2's value.
6. **statsd**: disabled, because it is not in the §2 pod set. Re-enable if v2's dashboards read statsd metrics.
7. **Plan text**: D14 and §5.5 still say Python 3.13. Update them, or record the change to 3.14 as an amendment.

Checked here: `helm lint --strict` passes, and `helm template` renders every env × profile combination
plus the migration Job. Each guard was triggered and fails the render. The lock resolves and
`--check` passes. Not checked here: `docker build` (no Docker daemon was available) and any
server-side dry run against a cluster.
