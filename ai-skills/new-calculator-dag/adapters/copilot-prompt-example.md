# Using this skill outside Claude Code

`SKILL.md` is plain imperative markdown with no tool-specific syntax, so any assistant
that can read repository files can follow it. The adapters below are thin pointers — all
the content lives in `SKILL.md` and the files it references, so there is nothing to keep
in sync.

---

## Claude Code

Claude Code discovers skills under `.claude/skills/` (project) or `~/.claude/skills/`
(user) — it does not scan arbitrary directories. Create a thin pointer skill at
`.claude/skills/new-calculator-dag/SKILL.md`:

```markdown
---
name: new-calculator-dag
description: Author a new calculator DAG for the current orchestration framework via a structured interview, golden exemplars and a deterministic validator gate.
---

Read and follow `ai-skills/new-calculator-dag/SKILL.md` end-to-end, including every
file it tells you to read first. Conduct the interview before writing any code.
Do not skip the validator.
```

Same rule as the Copilot prompt below: the pointer stays thin so there is no second
copy of the contract to drift out of date.

---

## GitHub Copilot (VS Code)

Create `.github/prompts/new-calculator-dag.prompt.md` in the repository root:

```markdown
---
mode: agent
description: Author a new calculator DAG (DAG file, logic module, registry entry, tests, fixture)
---

You are authoring a new calculator DAG for this repository's orchestration framework.

Read and follow `ai-skills/new-calculator-dag/SKILL.md` end-to-end, including every
file it tells you to read first. Conduct the interview before writing any code.
Do not skip the validator.
```

Invoke it from the Copilot Chat input with `/new-calculator-dag`.

**That is the whole prompt file, deliberately.** Restating any of the contract here
would create a second copy to drift out of date — the exact failure mode this skill
exists to prevent. If the prompt file grows past a role line and a pointer, that is a
smell.

If your Copilot version does not support prompt files, paste the same three lines into
chat and attach `ai-skills/new-calculator-dag/SKILL.md` as context.

---

## IntelliJ IDEA (GitHub Copilot plugin)

There is no prompt-file mechanism in the JetBrains plugin, so attach the file instead:

1. Open Copilot Chat.
2. Attach `ai-skills/new-calculator-dag/SKILL.md` as context (the paperclip / "Add
   context" control, or drag the file into the chat input).
3. Send:

   > You are authoring a new calculator DAG for this repository's orchestration
   > framework. Read and follow the attached `SKILL.md` end-to-end, including every file
   > it tells you to read first. Conduct the interview before writing any code. Do not
   > skip the validator.

The assistant will need to open the `reference/` and `examples/` files itself as it
works; keep the `ai-skills/new-calculator-dag/` directory open in the project view so
those reads resolve.

---

## Anything else

> Follow `ai-skills/new-calculator-dag/SKILL.md`.

---

## What to check regardless of tool

The scripts are the backstop, and they do not care which assistant produced the files:

```
python ai-skills/new-calculator-dag/scripts/validate_calculator_dag.py \
    --dag-id <dag_id> --dag-file <...> --logic-module <...> --registry <...> \
    --test-file <...> --fixture <...>
```

Exit 0 with the reported error count at zero is the acceptance bar. A weaker model that
runs the validator and fixes what it reports will produce a better DAG than a stronger
one that skips it — which is why the validator, not the prose, is the actual gate.

Two failure modes to watch for in any tool:

- **Invented domain values.** Component lists, company codes, dataset names and
  catalogue names must come from the author or from an existing module. The validator
  cannot detect a plausible-looking invented value; a human must.
- **A skipped interview.** An assistant that starts writing code before asking batch 1
  is guessing. Stop it and restart.
