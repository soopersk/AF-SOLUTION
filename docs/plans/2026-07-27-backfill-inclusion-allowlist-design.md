# Observability Backfill — Inclusion Allowlist Re-design

**Status:** Implemented 2026-07-28 against two stated assumptions (§9). Not yet run against a live event service.
**Scope:** `observability_backfill_dag` and `obs_backfill_helpers.py` only.
**Date:** 2026-07-27

---

## Why invert — the exclusion mechanism does not work today

This is the lead argument, not a footnote. The current filter at
[`observability_backfill_dag.py:152`](../../old-orchestration/dags/observability_backfill_dag.py#L152)
compares `event.additionalData.taskId` — a **UUID**
([START sample line 26](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L26)) —
against a list documented at
[line 32](../../old-orchestration/dags/observability_backfill_dag.py#L32)
as holding *"calculator class names"*. Names never equal UUIDs, so the exclusion set
never matches and **nothing is ever excluded**.

The failure is silent: the DAG reports a plausible `starts_after_exclusion` count and
replays everything. An operator reading the logs cannot tell the filter is inert.

Beyond fixing that, inversion is the correct posture on its own merits. An allowlist is
**fail-closed**: a calculator newly appearing in the event stream is not replayed into
observability until someone opts it in. An exclusion list is fail-open — every new
calculator is replayed by default. For a job whose side effect is POSTing historical runs
into a live observability API, fail-closed is the right default.

---

## Current state — the DAG is a draft and cannot parse

[`observability_backfill_dag.py`](../../old-orchestration/dags/observability_backfill_dag.py)
imports nine names ([lines 1-9](../../old-orchestration/dags/observability_backfill_dag.py#L1-L9))
and uses roughly twenty. Undefined: `DAG_DEFAULT_ARGUMENTS`, `CAPITAL_TAGS`, `UTC`,
`get_config`, `fetch_events`, `START_EVENT`, `COMPLETE_EVENT`, `index_latest_by_run_id`,
`extract_start_run_id`, `extract_complete_run_id`, `assign_run_numbers`, `obs_start_run`,
`obs_complete_run`, `calculator_catalogue_provider`, `decorated_message`, `logger`, `Logger`.

[Line 24](../../old-orchestration/dags/observability_backfill_dag.py#L24) calls
`datetime.now(UTC)` at module scope, so the failure is at **parse** time — Airflow never
imports this DAG.

Sibling DAGs carry full import blocks and a module-level logger
([`usrg_ihc_dag.py:1-6`](../../old-orchestration/dags/usrg_ihc_dag.py#L1-L6),
[`orchestration_control_dag_capital.py:1-12`](../../old-orchestration/dags/orchestration_control_dag_capital.py#L1-L12)),
so this is a genuine draft state rather than an extraction artifact.

**Consequences that shape this document:**

- The task rename is free. No task history, no external XCom consumer, no live Variable to migrate.
- XCom key names are preserved anyway (cheap, and they may be consumed later), but not because of an existing contract.
- The import block is **in scope**. A design that omits it produces an implementation that still does not parse.

---

## Decisions locked

| Question | Decision |
|---|---|
| Config direction | Inclusion allowlist replaces exclusion list |
| Variable name | `calculator_backfill_inclusion_ids` |
| Param name | `included_calculator_ids` (renames and reuses `excluded_calculators`) |
| `calculator_id` param | Dropped — redundant with a one-element allowlist |
| Override semantics | `env ∩ override`; entries outside the env list are dropped with a warning |
| Fetch shape | **N+1 per date** — starts scoped per calculator, completions fetched once unscoped |
| Completion scoping | Client-side, via `search_dict(event, "taskId")` against the allowlist |
| Mapped unit | Stays `date` (`.partial(data=…).expand(reporting_date=…)`) — **not** date×calculator |
| Calculator iteration | Sequential within each date, one log section per calculator |
| Name resolution | `context.data.class` off the calculator's first START event |
| Zero-event calculators | No section; counted as `calculators_with_no_events` in the per-date summary |
| `orphan_complete` | Retains exact original meaning (see §3) |
| Run numbering | Prefer `context.data.run_number`; fall back to `assign_run_numbers` when absent |
| `run_number` range | 1 or 2 only — the two-bucket model holds |
| FAILED runs | **In scope** — now receive `/complete` |
| `frequency` M/W bug | **In scope** |
| Import block + logger | **In scope** |

---

## 1. Config surface

| Today | New |
|---|---|
| Variable `calculator_backfill_exclusions` | Variable `calculator_backfill_inclusion_ids` — flat JSON array of calculator UUIDs, set per deployment (DEV/UAT/PROD each hold their own value; no env-detection code) |
| Param `excluded_calculators` | Param `included_calculator_ids` — per-run override |
| Param `calculator_id` | **Removed** |

The `_ids` suffix on both names is deliberate. The bug in §1 above existed precisely because
a config key said "calculators" while the code compared UUIDs; the key itself should now say
which one it holds.

### Workflow regression to state plainly

Today `calculator_id` lets an operator backfill *any* calculator ad hoc. Under `env ∩ override`,
a calculator absent from the Variable is **unreachable by param** — the Variable must be edited.
That is the intended safety property of a fail-closed allowlist, but it is a real change to
ad-hoc operations and belongs in the runbook, not discovered during an incident.

If an escape hatch is ever wanted, it is a separately-named `bypass_allowlist` boolean whose
name makes the risk legible — not a reinstated `calculator_id`.

---

## 2. Resolution (in `validate_params`)

1. Read Variable → `[]` default, `deserialize_json=True`. Reject non-list or non-string-element with the existing shape error.
2. **Canonicalize each entry via `uuid.UUID(x)`**, then emit `str(uuid.UUID(x))` — canonical lowercase hyphenated form. This also normalizes braced and `urn:uuid:` inputs.
3. On any parse failure, raise naming **every** offending value, not just the first.
4. Dedupe **preserving first-occurrence order** from the Variable, so log sections appear in the order the operator wrote them.
5. Empty override → use the env list as-is.
6. Non-empty override → `env ∩ override`, preserving env order. Entries in the override but not in env are dropped and logged at WARNING, naming each.
7. Empty result → `ValueError`, with distinct messages for *"no inclusions configured for this environment"* vs *"override has no overlap with the environment allowlist"*. The no-overlap message includes both lists (truncated) so the mismatch is visible without a second run.

### Why canonicalize rather than `.lower()`

The resolved UUID is sent to the event service as `query["taskId"]`
([`obs_backfill_helpers.py:56-57`](../../old-orchestration/obs_backfill_helpers.py#L56-L57)).
If that endpoint is case-sensitive on `taskId`, blanket-lowercasing operator input could break
the match; `uuid.UUID` round-tripping normalizes form without inventing a casing policy. Step 2
is also the single highest-value item in this redesign — it converts the most likely rollout
failure (a Variable still holding class names, exactly what exists today) from *"silently fetches
nothing"* into an immediate, precise error.

---

## 3. Fetch and correlate — N+1 per date

Completions pair to starts by **`runId`**, never by calculator. Once starts are filtered to the
allowlist, only *their* run_ids are consulted in the completions map. Calculator scoping of the
completion fetch is therefore unnecessary, and the design avoids depending on whether `taskId`
filters the `type=CALC_EVENT` query shape server-side.

Since completion events do carry `taskId` (§9 item 3), completions are grouped by calculator
**client-side**, which restores `orphan_complete` to its exact original meaning at zero extra
API calls. The N+1 shape is thus equivalent to a 2N shape in counter fidelity, with none of the
server-side filtering risk and roughly half the HTTP calls.

```
per reporting_date:

  completes_raw = fetch_events(config, date, COMPLETE_EVENT, frequency=frequency)   # 1 call, unscoped
  completes_by_calc = group(completes_raw, key=search_dict(ev, "taskId"))
                      filtered to the resolved allowlist

  for calculator_uuid in resolved_allowlist:            # sequential, one log section each
      starts_raw = fetch_events(config, date, START_EVENT,
                                calculator_id=calculator_uuid, frequency=frequency)   # N calls, scoped

      if not starts_raw:
          calculators_with_no_events += 1
          continue                                       # no section, per decision

      starts_by_run    = index_latest_by_run_id(starts_raw, extract_start_run_id)
      calc_completes   = completes_by_calc.get(calculator_uuid, [])

      completes_by_run = index_latest_by_run_id(calc_completes, extract_complete_run_id)     # ALL states
      finished_by_run  = index_latest_by_run_id(finish_only(calc_completes), extract_complete_run_id)

      success_run_ids  = {rid for rid in finished_by_run if rid in starts_by_run}
      inferred_numbers = assign_run_numbers(starts_by_run, success_run_ids)   # fallback only, see §4
      orphan_complete += count(rid for rid in completes_by_run if rid not in starts_by_run)

      replay(...)

= N+1 HTTP calls per date
```

### `taskId` and `runId` extraction — the asymmetry is total

Verified against the real payloads (2026-07-27). Both correlation fields live at **opposite paths in
the two event types**, and neither exists at the other's path:

| Field | START | COMPLETE |
|---|---|---|
| `taskId` | `event.additionalData` ([line 26](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L26)) | `context.data` ([line 68](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L68)) |
| `runId` | `event.additionalData` ([line 23](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L23)) | `context.data` ([line 66](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L66)) |

There is therefore **no fixed path that works for both**. A hardcoded `context.data.taskId` returns
`None` for every START; a hardcoded `additionalData.taskId` returns `None` for every completion.
`search_dict(event, "taskId")` is load-bearing, not a convenience — this is the single strongest
constraint the real samples imposed on the design.

Same conclusion for `runId`: [`extract_complete_run_id`](../../old-orchestration/obs_backfill_helpers.py#L68-L77)
already tries `context.data.runId` first, which is correct, but it should move to `search_dict` for
symmetry with `extract_start_run_id` rather than relying on two hand-maintained fallback chains.

### Unmatched-completion tripwire (required)

The client-side filter matches `search_dict(ev, "taskId")` against the canonical allowlist by exact
string equality. A systematic mismatch — malformed UUID, differing form, field renamed upstream —
produces **zero completions for that calculator**, so every run posts start-only and the backfill
writes a uniformly wrong picture into observability. Nothing in the happy path distinguishes that
from "these runs genuinely never completed."

So: count completions whose `taskId` fails to parse **or** matches no allowlist entry, and log at
WARNING per date. This is cheap and converts the design's worst silent failure into a first-run
signal.

> The 2026-07-27 samples originally disagreed on `taskId` by a two-character transposition
> (`…6f6b66bda9c0` vs `…6f66b6bda9c0`), confirmed a transcription slip and corrected in the fixture
> against the START value, which is attested twice — `additionalData.taskId` and the `event.key`
> suffix. That near-miss is precisely the failure this tripwire catches.

### `assign_run_numbers` is already per-calculator — do not claim otherwise

The group key at [`obs_backfill_helpers.py:124`](../../old-orchestration/obs_backfill_helpers.py#L124)
is `(additionalData.taskId, ctx["reporting-date"], ctx["frequency"])`. It keys on `taskId`, which is
present and correct on START events. Grouping is therefore **already** correct per calculator, and
calling `assign_run_numbers` inside the per-calculator loop is *equivalent* to one call on a merged
dict — not a bug fix.

The per-calculator call is still adopted, because it keeps each calculator's section self-contained
and lets the run-number counts be reported per section. The key itself should switch to
`search_dict(event, "taskId")` for consistency with the extraction rule above.

### Fetch errors

A fetch error fails the date. The DAG is idempotent per the upsert pattern, so retry is safe. The
log names which `calculator_id` was in flight. Note that
[`_safe_get_events`](../../old-orchestration/obs_backfill_helpers.py#L14-L28) already absorbs
empty-result `RuntimeError` and 404 into `[]`, so "no events" is not an error path.

---

## 4. Replay — FAILED runs now complete

Today [line 145](../../old-orchestration/dags/observability_backfill_dag.py#L145) builds
`completes_by_run` from `completes_finish_raw`, so only `STATE=FINISH` completions are ever posted.
A FAILED run posts `/start` and never `/complete`, leaving it perpetually in-flight in observability.
Note the query already asks for both states — `COMPLETE_EVENT = "FINISH|FAILED"`
([`obs_backfill_helpers.py:11`](../../old-orchestration/obs_backfill_helpers.py#L11)) — so the data
is fetched and then discarded.

**Change:** `completes_by_run` is built from **all** completion states. `success_run_ids` stays
**FINISH-only**.

These are two distinct concepts and conflating them would be a regression: a failed run must not
satisfy the "first successful pair" rule in
[`assign_run_numbers`](../../old-orchestration/obs_backfill_helpers.py#L105-L138), or run-numbering
silently shifts. Keeping them separate preserves run-number semantics exactly while fixing the
posting gap.

Incidental cleanup: [line 145](../../old-orchestration/dags/observability_backfill_dag.py#L145) and
[line 163](../../old-orchestration/dags/observability_backfill_dag.py#L163) currently build the
identical FINISH index twice. Collapse to one.

### Run numbering — the event now states it

The real START sample carries
[`context.data.run_number = 2`](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L70)
as an integer. [`assign_run_numbers`](../../old-orchestration/obs_backfill_helpers.py#L105-L138)
exists solely to *infer* that value from START timestamp ordering plus first-successful-pair
position — an inference that can now contradict an authoritative value sitting in the same event.

**Resolution: prefer the field, fall back to the heuristic.**

```python
run_number = ctx_data.get("run_number")
if run_number is None:
    run_number = inferred_numbers[run_id]
    run_number_source = "inferred"
else:
    run_number_source = "event"
```

The fallback is not defensive padding — a 30-day backfill window can straddle the date
`run_number` was introduced upstream, so historical STARTs will legitimately lack it while recent
ones carry it. Deleting the heuristic would silently mis-number exactly the old runs a backfill
exists to repair.

`run_number_source` is logged per run (§5) so a run whose number was guessed is distinguishable
from one the platform asserted. Where both are available, log at WARNING when they disagree —
that is direct evidence about whether the heuristic was ever correct, and it is the cheapest
route to eventually deleting it.

`run_number` is confirmed to be **1 or 2 only**, so the two-bucket model, the existing `run1`/`run2`
XCom keys, and the `sla_for(frequency, run_number)` lookup in
[`resolve_sla_time`](../../old-orchestration/obs_backfill_helpers.py#L141-L162) all remain valid.
Note the field is an `int` while much of `context.data` is stringly-typed — coerce defensively and
treat an unparseable value as absent rather than crashing the date.

---

## 5. Logging

Phase-structured, with names that state what they count. Every line carries both the UUID and — once
a START event has been seen — the calculator name from `context.data.class`, because config is UUIDs
and humans triage by name.

```
[allowlist]  env_inclusions=5 override=2 resolved=2 dropped_not_in_env=['<uuid>']

[fetch]      date=2026-05-01 completion_events_fetched=214 matched_to_allowlist=40
             unmatched_taskid=174 unparseable_taskid=0
[calculator] date=2026-05-01 calculator_id=<uuid> calculator=capitalcalcmedium
[fetch]      date=2026-05-01 calculator=capitalcalcmedium run_start_events=14
             run_completion_events_all_states=13 run_completion_events_successful=11
[correlate]  date=2026-05-01 calculator=capitalcalcmedium runs_started=14
             runs_paired_with_completion=13 runs_never_completed=1
             completions_without_matching_start=0
[replay]     date=2026-05-01 calculator=capitalcalcmedium run_id=… run_number=2
             run_number_source=event posted=start+complete sla=PT2H30M
[summary]    date=2026-05-01 calculators=2 calculators_with_no_events=3
             posted_both=38 start_only=2 failed=2 run1=30 run2=12
```

The date-level `[fetch]` line is the §3 tripwire. `unparseable_taskid > 0`, or an
`unmatched_taskid` count that swallows nearly everything, is the signature of a systematic
`taskId` mismatch — visible on the first run rather than after the backfill has finished writing.

Renames from the current single line at
[lines 136-141](../../old-orchestration/dags/observability_backfill_dag.py#L136-L141):
`starts` → `run_start_events`, `completes_total` → `run_completion_events_all_states`,
`completes_FINISH` → `run_completion_events_successful`.

Also:

- The orphaned `CALC_METADATA_FETCHED` line at [line 189](../../old-orchestration/dags/observability_backfill_dag.py#L189) folds into the per-run `[replay]` line — date, run_id, run_number, calculator, metadata on one greppable line.
- **A success log on the real POST path.** Today only exceptions log ([line 227](../../old-orchestration/dags/observability_backfill_dag.py#L227)), so a normal replay leaves no trace whatsoever.
- The failure `logger.exception` gains `calculator_id` and `run_number`.
- **A per-date summary line.** Today only the cross-date aggregate logs ([lines 274-286](../../old-orchestration/dags/observability_backfill_dag.py#L274-L286)), so one date's outcome cannot be read without arithmetic.

XCom dict keys are unchanged (`started_total`, `posted_both`, `orphan_complete`, …). `orphan_complete`
retains its original meaning under §3, so no key requires a semantic caveat.

---

## 6. Task rename

`backfill_one_date` → `replay_runs_for_date`, both `task_id` and function name. Free, per §2 of
current state.

---

## 7. Adjacent fixes in scope

1. **Full import block + module-level `logger`.** Matches the sibling DAGs. Includes the parse-time `datetime.now(UTC)` failure at [line 24](../../old-orchestration/dags/observability_backfill_dag.py#L24) and the undefined `Logger` at [line 99](../../old-orchestration/dags/observability_backfill_dag.py#L99) and [line 155](../../old-orchestration/dags/observability_backfill_dag.py#L155). Line 99 sits in `build_reporting_dates`, which runs on every DAG run — this is a `NameError`, not a casing nit.
2. **`frequency` validation.** [Line 55](../../old-orchestration/dags/observability_backfill_dag.py#L55) accepts `D, W, DAILY, MONTHLY` while [line 26](../../old-orchestration/dags/observability_backfill_dag.py#L26) advertises *"D or M"* — an operator passing `M` gets a `ValueError` today. Reconcile the accepted set with the documented set.
3. **FAILED runs receive `/complete`** — see §4.

---

## 8. Out of scope

- Any change to `obs_start_run` / `obs_complete_run` internals beyond the call site.
- Any change to the observability API contract.
- The `(date × calculator)` mapped fan-out. Considered and **rejected**: it would give per-calculator retry granularity and parallelism, but the date-level mapped unit is retained deliberately. Recorded here so it is not re-litigated.

---

## 9. Open items

Items 1 and 2 were **designed around** rather than waited on — the implementation no longer
blocks on either, but each rests on a stated assumption that should still be confirmed.

1. **~~`af_utils.search_dict` semantics~~ — DESIGNED AROUND 2026-07-28.** Two corrections to
   this item as originally written. It is **not** in `af_utils`: it is
   `orchestration.common.collection_utils.search_dict`
   ([`base_calculator.py:17`](../../old-orchestration/common/base_calculator.py#L17),
   [`generic_calculator.py:11`](../../old-orchestration/common/generic_calculator.py#L11)). And its
   miss-behaviour is settled by its call sites — `if not search_dict(event, key="jobLink")`
   ([`generic_calculator.py:125`](../../old-orchestration/common/generic_calculator.py#L125))
   proves it returns falsy rather than raising.

   Precedence and list-descent remain unknown, so the implementation does not depend on them.
   `extract_task_id` / `extract_start_run_id` / `extract_complete_run_id` try the **observed
   explicit paths first**, in the order correct for their event type, and fall back to
   `search_dict` only for shapes we have not seen. Extraction is therefore deterministic
   wherever the shape is known, and still survives a field moving upstream. Verified against
   both real samples under a *list-descending* stub — the pessimistic reading of this item.

2. **`obs_complete_run` derives terminal state from the event — ASSUMPTION, not verified.**
   The call site passes the whole `event`, so §4 is implemented as pure index widening with
   **no signature change**: `completes_by_run` now spans all terminal states while
   `success_run_ids` stays FINISH-only. The `[replay]` line logs `terminal_state=FINISH|FAILED`
   per run, so if `obs_complete_run` in fact ignores the event's state and needs an explicit
   argument, the first dry run shows FAILED runs being posted and the gap is caught before it
   reaches observability. This is the one assumption that could still require a real code
   change rather than a config fix.
3. **~~`runId` on completion events~~ — RESOLVED 2026-07-27.** Present at [`context.data.runId`](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L66), which is the first path [`extract_complete_run_id`](../../old-orchestration/obs_backfill_helpers.py#L68-L77) already tries.

4. **`runId` equality across a matched pair — still unverified, and it is the load-bearing assumption.** The two 2026-07-27 samples are **not a matched pair**: same calculator (`capitalcalcmedium`) and same reporting date (`2026-07-24`), but different `contextId`s, different business dates, and different `runId`s (`22a75fd9-…` vs `ef5216b4-…`) — consistent with being run 1 and run 2 of the same calculator, not one run's start and finish. So the samples confirm the field *exists* on both sides but cannot confirm that a given run's START `runId` equals its COMPLETE `runId`.

   Everything downstream of the fetch pairs on that equality. If the two are different identifier spaces, `posted_both` is zero across the board and every run posts start-only. **One matched pair — a single run's START and COMPLETE — settles it, and should be obtained before implementation starts.**

---

## 9a. Field-shape hazards observed in the real payloads

None of these break the design as written, but each is a live trap for anyone extending it.

1. **The reporting-date key differs by event type.** START uses `reporting-date` (kebab,
   [line 56](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L56));
   COMPLETE uses `reportingDate` (camel,
   [line 65](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L65)). Harmless
   today because the DAG reads it only from starts
   ([line 185](../../old-orchestration/dags/observability_backfill_dag.py#L185),
   [helpers line 124](../../old-orchestration/obs_backfill_helpers.py#L124)) — but it rules out a
   naive single-key `search_dict` for date across both types.

2. **Frequency carries two vocabularies inside one event — half resolved 2026-07-28 by
   `af_utils.normalize_frequency`** (see §12 item 4). The completion has
   `additionalData.FREQUENCY = "DAILY"`
   ([line 13](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L13)) *and*
   `context.data.frequency = "D"`
   ([line 38](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L38)) — differing
   in both case and vocabulary — which is why anything that *groups or looks up* by frequency must
   normalise first, or it treats one frequency as two. That half is now closed: the group key and
   the SLA lookup both go through `normalised_frequency`.

   **The other half is still open.** The `frequency` param is passed to **both** fetch queries
   ([`obs_backfill_helpers.py:53-54`](../../old-orchestration/obs_backfill_helpers.py#L53-L54)),
   and the DAG now always sends the short code. If the `type=CALC_EVENT` completion query in fact
   matches on `additionalData.FREQUENCY` (`"DAILY"`) rather than `context.data.frequency` (`"D"`),
   passing `frequency` will return **zero completions** and the date posts start-only. Normalising
   does not fix this — but it does make the behaviour single and testable rather than dependent on
   what the operator typed. **Verify in the first dry run by running one date with and without
   `frequency` and comparing `completion_events_fetched`.** Until then, omitting `frequency`
   entirely is the safe choice; it is an optional filter.

3. **`search_dict` precedence is now a concrete risk, not a theoretical one.** The completion's
   `context.data.inputDataFilter.INTERIMPOSTING[0].contextId`
   ([line 75](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L75)) is a
   *different* value from `event.contextId`
   ([line 22](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L22)) — an input
   data context, not the event's own. `inputVolumes`/`outputVolumes`/`inputDataFilter` are now real
   nested objects and arrays rather than the `"{ 27 keys }"` placeholder strings the earlier sample
   carried. Not a problem for `taskId` or `runId` (one occurrence each per event), but it makes §9
   item 1 sharp: **if `search_dict` descends into lists, key collisions at depth are reachable.**

4. **`event.key` is shaped differently per event type.** START uses `root,<taskId>`
   ([line 5](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L5));
   COMPLETE uses `root,<class name>`
   ([line 5](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L5)). Do not treat
   `event.key` as a uniform identifier source. Its one use here is corroboration: on START it
   independently attests `taskId`, which is what identified the fixture transposition in §3.

---

## 10. Grounding checkpoints (verify against code, not this doc)

- Exclusion filter compares UUID to name → [`observability_backfill_dag.py:152`](../../old-orchestration/dags/observability_backfill_dag.py#L152) vs [line 32](../../old-orchestration/dags/observability_backfill_dag.py#L32); UUID confirmed at [START sample line 26](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L26).
- Two query shapes and `taskId` applied to both → [`obs_backfill_helpers.py:40-57`](../../old-orchestration/obs_backfill_helpers.py#L40-L57).
- `taskId` is a live query param on the CALC finish-event path → [`http_deferrable_completion_sensor.py:114`](../../old-orchestration/sensor/http_deferrable_completion_sensor.py#L114).
- Run-number group key already uses `taskId` → [`obs_backfill_helpers.py:124`](../../old-orchestration/obs_backfill_helpers.py#L124).
- FINISH-only completion index → [`observability_backfill_dag.py:130-134`](../../old-orchestration/dags/observability_backfill_dag.py#L130-L134) and [line 145](../../old-orchestration/dags/observability_backfill_dag.py#L145).
- Catalogue lookup is **name**-keyed, not UUID-keyed → [`group.py:191`](../../old-orchestration/common/group.py#L191), [`obs_backfill_helpers.py:161`](../../old-orchestration/obs_backfill_helpers.py#L161). This is why §5 resolves names from START events rather than from the catalogue.
- Empty-result and 404 absorbed into `[]` → [`obs_backfill_helpers.py:14-28`](../../old-orchestration/obs_backfill_helpers.py#L14-L28).
- `taskId`/`runId` live at opposite paths per event type → START [lines 23, 26](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L23-L26) vs COMPLETE [lines 66, 68](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L66-L68). This is what forces `search_dict`.
- `run_number` is stated by the platform on START events → [sample line 70](../../old-orchestration/dags/sample_MEG_STARTED_context_enriched_event.json#L70).
- Completion state and event type for the query shape → [`STATE`/`type` lines 15-16](../../old-orchestration/dags/sample_calculator_COMPLETE_event.json#L15-L16).

---

## 11. Rollout

There is no migration. The DAG has never parsed (§2), so `calculator_backfill_exclusions` has never
been read and no deployment holds meaningful state for it.

1. Provision `calculator_backfill_inclusion_ids` in DEV/UAT/PROD as a flat JSON array of calculator UUIDs. Each environment holds its own value; there is no env-detection code.
2. First run with `dry_run=true` and a short `n_days`. Confirm the `[allowlist]` line reports the expected `resolved` count and that each expected calculator produces a section.
3. The UUID validation in §2 turns a names-instead-of-UUIDs Variable into an immediate `ValueError` at `validate_params`, before any fetch or POST. This is the intended first line of defence and should be verified deliberately in DEV by setting a bad value once.
4. Delete `calculator_backfill_exclusions` from any environment where it was speculatively provisioned.

---

## 12. Implementation notes (2026-07-28)

Deviations from the design as written, each deliberate:

1. **Allowlist resolution lives in `obs_backfill_helpers`, not the DAG file.**
   `resolve_inclusion_allowlist` / `canonicalise_calculator_ids` were moved out of
   `validate_params` because Airflow is not importable in this environment, and logic sitting in
   the DAG module cannot be exercised at all. The §2 UUID guard is the single highest-value
   change in this redesign; it should not also be the least testable. The DAG now calls one
   function and logs the result.

2. **The tripwire warns on a signature, not on any unmatched completion.** §3 says to WARN on
   completions that match no allowlist entry — but under an unscoped fetch, *most* completions
   legitimately belong to non-allowlisted calculators, so warning on every one would be constant
   noise and would defeat the purpose. The INFO counter line is unconditional; the WARNING fires
   on `unparseable_taskid > 0` **or** `fetched > 0 and matched == 0`, which are the two shapes
   that actually indicate systematic breakage.

3. **`end_date` defaults to `None`, resolved at runtime.** §7 item 1 treats the module-scope
   `datetime.now(UTC)` as an import-block fix, but importing `UTC` would leave a value frozen at
   DAG-parse time. `validate_params` already had a today-fallback, so the Param default is now
   `None` and module scope evaluates no clock at all.

4. **`frequency` is normalised via `af_utils.normalize_frequency`, which resolves §9a item 2.**
   The platform already has a canonical normaliser (`FREQ_TO_CODE`: `DAILY→D`, `MONTHLY→M`), so
   the two vocabularies are collapsed to the short code at the DAG boundary and everything
   downstream — both fetch queries, the run-number group key, the SLA lookup — sees one form.
   That is strictly better than passing the operator's spelling through verbatim, which is what
   this doc originally proposed.

   Two consequences worth stating plainly. **`W` is no longer accepted**: the old validation set
   allowed it, but `FREQ_TO_CODE` has no `W` entry, so it normalises to `None` and was never
   meaningful downstream — this removes a value that only ever looked supported. And the accepted
   set is derived from `sorted(FREQ_TO_CODE)` rather than restated in the DAG, so adding `WEEKLY`
   upstream makes the DAG accept it with no change here.

   `normalised_frequency` in the helpers wraps this for grouping and SLA use, with one deliberate
   difference: an unrecognised value falls back to its upper-cased raw form instead of `None`, so
   two genuinely different unknown frequencies do not collapse into a single run-number group.

5. **Calculator metadata is fetched once per calculator, not once per run.** §5 folds the orphaned
   `CALC_METADATA_FETCHED` line into `[replay]`; metadata is constant per calculator, so it is
   fetched once and logged on `[calculator]`, while the per-run `[replay]` line carries `sla=`,
   which is the part that actually varies by run number. SLA lookup is wrapped — it is a log
   field and must never fail a replay.

### What was verified, and what was not

Both files byte-compile and are pyflakes-clean. 31 assertions cover extraction, terminal state,
UUID canonicalisation, completion grouping and run-number resolution against the **real** sample
payloads; 23 more cover allowlist resolution. Both suites pass.

Not verified, and not verifiable here: every `orchestration.*` import, since those modules are
absent from this snapshot. `obs_start_run` / `obs_complete_run` are imported from
`orchestration.observability.obs_run_tasks` — inferred from
[`group.py:39`](../../old-orchestration/common/group.py#L39), which imports the *task factories*
`create_obs_start_task` / `create_obs_complete_task` from that module. The run-level functions
may live elsewhere; this import is a guess and should be the first thing checked on deployment.
`CAPITAL_TAGS.MAINTENANCE` is likewise unconfirmed — sibling DAGs use `.CAPITAL` and `.CONTROL`.

§9 item 4 — that a single run's START `runId` equals its COMPLETE `runId` — remains the
load-bearing unverified assumption, and no amount of local testing can settle it. A dry run over
one date shows it immediately: if `posted_both` is zero while `start_only` equals the run count,
the two are different identifier spaces.
