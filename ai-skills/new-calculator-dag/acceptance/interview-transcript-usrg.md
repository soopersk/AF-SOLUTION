# Acceptance transcript — re-deriving `usrg_ihc_dag` from scratch

> **This is the manual acceptance test for any future edit to this skill.** Any change
> to `SKILL.md`, `reference/`, `templates/` or `examples/01-single-invocation/` must be
> followed by running this procedure. Differences beyond whitespace and comment wording
> are regressions until proven otherwise.

It is a *canned interview*: a fixed set of answers that a human would give when
onboarding the USRG IHC calculator with no prior artifacts. Because exemplar 01 is the
known-good output for exactly these answers, the skill's end-to-end behaviour can be
checked by diffing.

---

## Procedure

1. Create an empty output directory, e.g. `/tmp/acceptance-usrg/`.
2. Seed a registry there containing **only** unrelated entries, so the DAG is genuinely
   unregistered at the start:

   ```json
   { "portfolio_daily_dag": "CALC.CAPITALCALC" }
   ```

3. Run the skill against the transcript below. Feed each batch's answers **verbatim** —
   do not let the agent infer answers it should be asking for. Write all outputs into the
   output directory.
4. Diff each generated file against `examples/01-single-invocation/`:

   ```
   diff /tmp/acceptance-usrg/usrg_ihc_dag.py            examples/01-single-invocation/usrg_ihc_dag.py
   diff /tmp/acceptance-usrg/usrg_ihc_calculator.py     examples/01-single-invocation/usrg_ihc_calculator.py
   diff /tmp/acceptance-usrg/test_usrg_ihc_calculator.py examples/01-single-invocation/test_usrg_ihc_calculator.py
   diff /tmp/acceptance-usrg/fixture_merival_usrg.json  examples/01-single-invocation/fixture_merival_usrg.json
   ```

5. Run the validator on the generated artifacts. Expected: **`0 error(s), 0 warning(s)`**.

### What counts as a pass

| Difference | Verdict |
|---|---|
| whitespace, line wrapping | pass |
| comment wording, as long as each teaching comment's *point* survives | pass |
| identifier names, import layout, structure | **fail — regression** |
| a missing `push_xcom`, a changed `group_id` suffix, `.seconds` instead of `total_seconds()` | **fail — regression** |
| the validator reporting anything other than 0/0 | **fail — regression** |
| the agent inventing a component list, company code or dataset name not given below | **fail — the most serious kind** |
| the agent skipping the validator step | **fail — regression** |

---

## The canned interview

### Batch 1 — Identity

| Question | Answer |
|---|---|
| Calculator name (MEG catalogue key) | resolved at runtime by `get_regional_capital_calc_name()`; for this region it is `capitalusrgihccalc` |
| `dag_id` | `usrg_ihc_dag` |
| Description | `USRG IHC Dag` |
| Tags | `CAPITAL_TAGS.CAPITAL` |
| Domain / logic package | `dags/logic/capital/usrg_ihc_calculator.py` |

*Expected agent behaviour:* reads back `usrg_ihc_dag` == filename stem == registry key.

### Batch 2 — Triggering

| Question | Answer |
|---|---|
| Registry condition | `SOURCE.MERIVAL.USRG` |
| Is this event source already routed by Tier 1? | Yes — MERIVAL is an existing source |
| Pre-conditions | frequency `M`; run types `BATCH`, `INTRA`; event-date window days 5–31 |
| Dataset waits | `INTERIMCOLLATERAL` and `PARENTRATING`, both MERIVAL |
| Waits on another calculator? | No |

*Expected agent behaviour:* confirms `SOURCE.MERIVAL.USRG` is narrower than
`SOURCE.MERIVAL` and notes the skip-churn cost. Confirms the two datasets are readiness
waits (`datasets=`), **not** pre-conditions.

### Batch 3 — Execution

| Question | Answer |
|---|---|
| Run params to set | `enabledComponents = IHC_COMPONENTS` (imported from the capital commons module — do not inline it); `DerivedAttributesAdvancedApproachOff = "true"`; `cumulus = "false"` |
| Timeout | 5 hours (the default) |
| Completion scheme | legacy CALC_EVENT (the default) |
| Observability | **disabled** for this DAG (deviates from the default — the agent must ask, not assume) |

### Batch 4 — Fan-out

| Question | Answer |
|---|---|
| Shape | single invocation |
| Per-unit rules | one lane, fixed company `company-code-1 = "B615"` |

*Expected agent behaviour:* returns a single-element `list[dict]`, and says why that is
one lane.

### Batch 5 — Downstream triggering

| Question | Answer |
|---|---|
| Does this DAG relay to other DAGs? | **No** |

*Expected agent behaviour:* records "no relay" and asks nothing further in this batch.

---

## Expected generated artifacts

| File | Matches |
|---|---|
| `usrg_ihc_dag.py` | `examples/01-single-invocation/usrg_ihc_dag.py` |
| `usrg_ihc_calculator.py` | `examples/01-single-invocation/usrg_ihc_calculator.py` |
| `test_usrg_ihc_calculator.py` | `examples/01-single-invocation/test_usrg_ihc_calculator.py` |
| `fixture_merival_usrg.json` | `examples/01-single-invocation/fixture_merival_usrg.json` |
| registry | gains `"usrg_ihc_dag": "SOURCE.MERIVAL.USRG"`, plus a `.bak` of the original |

### Expected run summary

Must include the manual touchpoints block, and specifically must state:

- calculator catalogue Variable entry for the USRG IHC calculator — **required**;
- Tier-1 routing row — **not required**, MERIVAL is an existing source (this is the
  discriminating case: an agent that reports it as required has not understood
  `registry-rules.md`);
- company-region mapping for USRG, if the calculator resolves companies;
- connection ids — none new.

---

## Known acceptable deviations

- The exemplar's teaching comments are deliberately dense because it is a teaching
  artifact. Generated output is expected to be more sparsely commented; only the
  *why*-comments on the envelope push, the `_CALC` suffix and the timeout arithmetic are
  required to survive.
- `logger` naming, import ordering within a group, and the exact wording of log messages
  are free.
