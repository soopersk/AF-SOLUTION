# Registry rules — `dags/dag_trigger_criteria_map.json`

The registry is the Tier-2 routing table: it maps a `dag_id` to the event condition(s)
that should trigger it. One entry is all that connects your DAG to the event stream.

---

## Grammar

```
TYPE.IDENT[.REGION]
```

- **Dot-separated.** Segments are `A-Z 0-9 _` only — **UPPERCASE**.
- **`TYPE`** ∈ `{SOURCE, CALC, DATASET, PIPELINEID}` (`trigger_conditions.py:15-17`,
  `TRIGGER_CONDITION` namedtuple). Anything else is rejected.
- **`IDENT`** is the source / calculator / dataset / pipeline identifier. It **may
  contain underscores** — `SOURCE.AQUA_RISK_PLATFORM` is a valid two-segment entry.
- **`REGION`** is optional and only meaningful when the identifier is region-partitioned.

| Type | Meaning | Live examples |
|---|---|---|
| `SOURCE` | an upstream *system* published an event | `SOURCE.MERIVAL.USRG`, `SOURCE.AQUA_RISK_PLATFORM`, `SOURCE.RWA` |
| `CALC` | another *calculator* finished | `CALC.CAPITALCALC`, `CALC.FLOORSCALC`, `CALC.GRPORTFOLIOCALC` |
| `DATASET` | a *dataset* completed ingestion/curation | `DATASET.OUTPUTPOSTING` |
| `PIPELINEID` | a named pipeline reported completion | (no live capital example) |

### Known region codes

`AMER, ASIA, AUNZ, EURO, LDNL, WMAP, WMCH, WMDE, WMUS, ZURI, USRG`

An unrecognised region is a **warning**, not an error — the set above is what the live
registry uses, and a genuinely new region is possible. But check first: two region
codes are known to drift between artifacts (`LDNL` vs `LONL`, `WMDE` vs `WMGE` — see
`pitfalls.md`). A "new" region is more often a typo than a new region.

### Value forms

A value is either one condition string or a list of them:

```jsonc
{
  "usrg_ihc_dag": "SOURCE.MERIVAL.USRG",              // single
  "output_floor_monthly_dag": [                        // list = OR
    "CALC.FLOORSCALC",
    "DATASET.OUTPUTPOSTING"
  ]
}
```

The list form is an **OR**: the control DAG builds one `CHECK` task per condition and
triggers the DAG when any one matches (discovery §6.2). It is **not** an AND — there is
no way to express "wait for both" in the registry. Cross-condition conjunction belongs
in `datasets=[...]` / `calc_deferrable_preconditions=[...]` inside the DAG.

### A separator inconsistency worth knowing about

Three separator conventions appear across the artifacts:

| Source | Form | Status |
|---|---|---|
| `dags/dag_trigger_criteria_map.json` (live registry) | `SOURCE.MERIVAL.USRG` | **canonical — write this** |
| `system_discovery_updated.md` §6.3 | `SOURCE.MERIVAL_USRG` | transcription drift; do not copy |
| `trigger_conditions.py:165` `get_trigger_condition` | splits on `", "` | the analysis snapshot of that function is partial/reconstructed and disagrees with the file it parses; the live registry file wins |

Write the dot form. `scripts/update_registry.py` and `scripts/validate_calculator_dag.py`
both enforce it, and the validator specifically flags `TYPE.IDENT_REGION` with a
"did you mean `TYPE.IDENT.REGION`?" error when `IDENT` is a known identifier and the
tail is a known region.

---

## Full routing chain — where your entry sits

```
EDF event
   │
   ▼
Tier 1 — event-orchestration (Scala)
   post_filter_control_dag_map (DB table)
   matches the enriched event (event + context merged JSON)
   POSTs to the matched control DAG's Airflow REST endpoint
   │
   ▼
Tier 2 — control DAG (Python)
   dags/dag_trigger_criteria_map.json          ← YOUR ENTRY IS HERE
   one CHECK task per condition; on match, a TRIGGER task fires
   │
   ▼
your calculator DAG run
   OTHER_PRE_CONDITIONS → dataset waits → CALC_INIT → MAPPED_GROUP → RESULT
```

Source: `system_discovery_updated.md` §6.1.

### The Tier-1 precondition — the manual touchpoint that bites

**A registry entry only fires for events that Tier 1 already forwards.** The DB table
`post_filter_control_dag_map` decides which events reach the control DAG at all;
currently 8 enabled rows, all pointing at the capital control DAG.

Therefore:

- **New condition on an event class already routed** (e.g. another `CALC.*` on the
  capital stream) → registry entry alone is sufficient.
- **New event *source*** (a system not currently in `post_filter_control_dag_map`) →
  a Tier-1 DB row must be added too. This is **not automatable from this skill** — it is
  a database change in a different repository's operational domain. It must be reported
  as a manual touchpoint in the run summary, and the DAG will silently never trigger
  until it is done.

The liquidity tenant is the live demonstration: its registry has 8 perfectly valid
entries and is completely dormant, because the single Tier-1 row
(`mfr_data_update_ACTL`) is `enabled=FALSE` (discovery §4.1, §6.3.1).

---

## Skip-churn — why conditions must be as narrow as possible

Every registry entry is evaluated on **every** control-DAG run, for **every** event.
`create_control_tasks` builds one `DagTriggerWithConditionTaskGroup` per entry, with one
`CHECK` task per condition; almost all of them raise `AirflowSkipException`
(discovery §6.2).

Measured cost per control-DAG run (2026-08-09):

| Registry | Entries | Conditions | Task instances per run |
|---|---|---:|---:|
| Capital prod | 30 | 41 | 73 |
| Capital dev | 37 | 48 | 87 |
| Liquidity prod | 8 | 8 | 18 |

Each skip is a real, state-tracked scheduler/DB lifecycle. So:

1. **Adding an entry is a permanent, per-event cost** paid by every other DAG's events,
   not just yours. One entry ≈ +2 task instances per condition per control-DAG run.
2. **Prefer the narrowest condition the grammar allows.** `SOURCE.MERIVAL.AMER` costs
   the same as `SOURCE.MERIVAL` per event, but the narrow one triggers your DAG only for
   AMER — avoiding the far more expensive Level-2 waste of a triggered calculator DAG
   run that self-skips in `OTHER_PRE_CONDITIONS`.
3. **Prefer one condition to several.** Each extra condition in a list adds another
   `CHECK` task instance to *every* control-DAG run forever.

Level-2 waste is the expensive kind: a triggered run that skips still creates a DAG run,
a task group, sensors and a scheduler footprint. Portfolio DAGs waste 9 of every 10 runs
this way (discovery §6.2).

---

## Editing the registry

Use the guarded editor — never hand-edit:

```
python ai-skills/new-calculator-dag/scripts/update_registry.py \
    --registry dags/dag_trigger_criteria_map.json \
    --dag-id   <dag_id> \
    --condition TYPE.IDENT[.REGION] [--condition ...]
```

It refuses duplicate `dag_id` keys, refuses bad grammar, writes a `.bak` alongside the
file before touching it, preserves existing entries, and emits 2-space-indented JSON.
Repeat `--condition` to produce the list form.

**If validation later fails and cannot be made to pass, restore from the `.bak`.** Never
leave the registry pointing at a DAG that does not validate — the control DAG will
trigger it and it will fail on every matching event.

### Invariants the validator enforces

- key exists for the `dag_id` being validated; a near-miss existing key is reported by
  name (this is how the live `amer_b3f_dag` / `amer_d_b3f_dag` drift is caught)
- no duplicate keys (JSON allows them; the last one silently wins)
- valid JSON
- every condition matches the grammar above
