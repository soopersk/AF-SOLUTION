# `new-calculator-dag`

An AI skill — and a human onboarding pack — for authoring a production-correct
calculator DAG against the **current** orchestration framework.

Authoring one calculator means writing three artifacts joined only by string identity,
against an untyped implicit contract, with no validation anywhere. Cross-artifact drift
in this repository is live, not hypothetical: the registry registers `amer_b3f_dag`
while the DAG declares `amer_d_b3f_dag`, and even the two "standard" samples disagree
about whether to use `timedelta.total_seconds()` or `timedelta.seconds`.

So this skill is knowledge-heavy and software-light. Its value is (a) writing down the
tribal knowledge that exists nowhere else, and (b) a deterministic gate that enforces
the consistency the framework never checks.

**Everything here also works without any AI.** The `reference/` docs are human onboarding
material and the scripts are ordinary CLI tools.

---

## One-command validation

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
    --dag-id       <dag_id> \
    --dag-file     dags/<dag_id>.py \
    --logic-module dags/logic/<domain>/<name>_calculator.py \
    --registry     dags/dag_trigger_criteria_map.json \
    --test-file    <test path> \
    --fixture      <fixture path>
```

Exit codes: **0** = pass (warnings allowed) · **1** = at least one ERROR · **2** =
usage/IO problem. `--with-dagbag` adds a real Airflow import check in CI (WARNs
explicitly, never silently passes, when `airflow` is not on PATH).

Registry edits go through the guarded editor, which refuses duplicates and bad grammar
and writes a `.bak` first:

```
python ai-skills/new-calculator-dag/scripts/update_registry.py \
    --registry dags/dag_trigger_criteria_map.json \
    --dag-id <dag_id> --condition TYPE.IDENT[.REGION]
```

Both scripts are **Python 3.10+ stdlib only** — AST-based, no Airflow import, no
install, runs on any dev box or CI runner.

---

## Directory map

```
ai-skills/new-calculator-dag/
  SKILL.md                          the entry point: mission, interview, workflow, guardrails
  README.md                         this file
  reference/
    authoring-contract.md           the framework contract; every rule with its runtime consequence
    trigger-conditions.md           pre-condition criteria catalogue and allowed value sets
    registry-rules.md               condition grammar + the full Tier-1 -> Tier-2 routing chain
    relay-triggering.md             relay pattern: payload sources, boundary rule, idempotency
    pitfalls.md                     observed failure modes, each anchored to real evidence
  examples/
    01-single-invocation/           usrg_ihc — the minimal complete shape
    02-parametrized-fanout/         B3F capital — company-group fan-out with per-unit rules
    03-dataset-gated/               output floor — dataset waits + deferrable calc preconditions
    04-relay-fanout/                hdl_process — BUILD_TRIGGER_CONF + mapped TriggerDagRunOperator
  templates/
    dag_file.py.tmpl                thin DAG file (pure boilerplate)
    test_calculator.py.tmpl         pytest scaffold
    fixture_event.json.tmpl         EnrichedEvent skeleton
  scripts/
    condition_grammar.py            TYPE.IDENT[.REGION] grammar, shared by both scripts
    validate_calculator_dag.py      the hard gate
    update_registry.py              guarded registry editor
  tests/                            the skill's own pytest suite (dev-time only)
  acceptance/
    interview-transcript-usrg.md    manual acceptance test for any future skill edit
  adapters/
    copilot-prompt-example.md       GitHub Copilot (VS Code / IntelliJ) usage
```

Each `examples/*/README.md` opens with **"Imitate me when: …"** so the right one is
findable in one pass.

---

## Using it

- **Claude Code** — skills are discovered under `.claude/skills/`, not at arbitrary
  paths: add a thin pointer skill there (frontmatter + "follow
  `ai-skills/new-calculator-dag/SKILL.md`"), or place this directory under
  `.claude/skills/` directly. See `adapters/copilot-prompt-example.md`.
- **GitHub Copilot / IntelliJ / anything else** — see `adapters/copilot-prompt-example.md`.
- **No AI at all** — read `reference/` as onboarding docs, copy the nearest exemplar by
  hand, and run the validator. The gate is the same either way.

---

## Testing the skill itself

```
python -m pytest ai-skills/new-calculator-dag/tests -v
```

Covers the condition grammar, every validator check, the registry editor, and a golden
suite asserting that all four exemplars stay validator-green **and** that the live
`amer_b3f_dag` drift bug is still caught end to end. pytest is a development dependency
of this directory only — the shipped scripts stay stdlib-only.

`acceptance/interview-transcript-usrg.md` is the manual end-to-end test: re-derive
`usrg_ihc_dag` from a canned interview and diff against exemplar 01. Run it after any
change to `SKILL.md`, `reference/`, `templates/` or exemplar 01.

**CI-only checks.** The exemplar unit-test files (`examples/*/test_*.py`) import the
real `orchestration` framework and therefore only run where it is installed — put them
on the CI list next to `--with-dagbag`. Exemplar 04's templated `trigger_run_id` is
likewise marked CI-VERIFY in the file: it has never executed against a live Airflow.

---

## Shelf life

**This skill targets the current framework and is deliberately temporary.** It is
superseded at the F2 migration waves, when calculator authoring moves to the Phase-2
`calculator_dag(spec, plan)` framework. Migration is explicitly **out of scope** here —
that is a separate skill, planned for a later quarter.

That bounded life is why the architecture is contract-plus-exemplars rather than a code
generator: minimal custom software for an authoring surface with a known end date.

What survives the migration:

- **`reference/`** — the pitfalls, the routing chain and the registry grammar describe
  the *system*, not the authoring API, and become the migration skill's input.
- **`scripts/`** — registry grammar and drift detection are framework-independent; the
  cross-artifact checks port to the new spec format.
- **`examples/`** — the "before" side of every migration example, and the corrected
  record of what two defective production files were actually trying to do.

The parts with a real expiry date are `SKILL.md`'s generation workflow, `templates/`,
and the logic-module checks in the validator.

---

## Self-contained by design

Everything lives under this one directory, with no dependency on anything outside it, so
it can be dropped into the `orchestration-dags` repository unchanged. Paths in the
documentation are written relative to the repository root.
