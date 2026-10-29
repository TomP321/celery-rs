# 13d — Documentation in English

Read `specs/13-english-style.md` (mandatory rules).

Scope: `README.md`, `docs/DIFFERENZE.md` (rename to `docs/DIFFERENCES.md` with `git mv` and fix
every reference in the repo), `PLAN.md`, `AGENTS.md`, `CLAUDE.md` if present, every file in
`specs/`, the Markdown reports in `bench/results/`, `tools/selector/README.md`, and any other
`*.md` outside `celery/` (the reference source is read-only).

- README: English, minimal, same structure and numbers (installation line, RAM table,
  compatibility, limits, tips, layout, development, licence). Keep the GIF embeds and write
  English alt texts.
- `AGENTS.md`: translate faithfully — it is the instruction file for agents; keep every rule.
- `specs/` and `bench/results/*.md`: faithful translation (historical record), file names
  unchanged.
- Do not touch code. Verification: `grep` for leftover Italian in all Markdown outside `celery/`,
  check every relative link and image path resolves (`ls` them).
