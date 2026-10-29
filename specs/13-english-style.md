# 13 — English, minimal style (shared rules for 13a–13d)

The repository becomes English-only. Every comment, docstring, log/error message we wrote,
test name, tool output and document is translated and rewritten to be **minimal**: say what the
code cannot say, nothing more. **No behaviour change**: same public API, same wire format, same
CLI, same exit codes. Identifiers stay as they are unless they are Italian *and* private (tests,
tools, internal helpers): rename those to English.

## Rust — follow tokio / hyper / axum / serde
- `//!` module docs: one short paragraph (what the module is, at most the key invariant).
- `///` on public items: a summary sentence ending with a period, third person
  ("Returns …", "Parses …"). Add `# Errors`, `# Panics`, `# Safety` sections only when they apply.
  No `///` on private items unless the reason is non-obvious.
- `//` comments only for the *why* (invariants, protocol quirks, references to Celery/kombu
  behaviour). Sentence case, end with a period. Delete comments that restate the code, banner
  separators and commented-out code.
- `// SAFETY:` comments stay, in English, stating why the block is sound.
- Error and log messages: lowercase start, no trailing period (Rust API guidelines),
  e.g. `"frame truncated"`. `thiserror` messages likewise.
- Idiomatic cleanups are welcome when obviously behaviour-preserving (clippy-clean, `rustfmt`,
  dead code removal, simpler control flow); nothing that changes allocation patterns in hot
  paths or public signatures.

## Python — keep the Celery developers' style
The shell copies Celery 5.7: code we added must read as if Celery's maintainers wrote it.
- Module docstring: `"""Short summary."""` (Celery style, e.g. `"""Internal worker state (global)."""`).
- Docstrings: imperative summary line ending with a period; reST/Celery sections when useful
  (`Arguments:`, `Returns:`, `Raises:`), as in `celery/app/base.py`.
- `#` comments: short, sentence case, only for the why.
- Max line length 117 (Celery's flake8). Single quotes as in Celery. Same import grouping.
- Lines identical to the reference stay identical: only touch what we added or changed
  (compare with `celery/celery/`).
- Messages raised/logged by our code: English, Celery tone. Messages copied from Celery: unchanged.

## Everything else
- Markdown: short sentences, no filler, tables where they help. English headings.
- Tool output (compat, gates, bench): English, terse (`api: 0 differences in 137 modules`).
  If a tool parses another tool's output, update both together.

## Verification (every block)
`cargo build --release`, `cargo clippy --all-targets -- -D warnings`, `tools/dev/with-brokers cargo test`,
`tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q`, `./compat api`, and
`grep` for leftover Italian in the files of your block (report any intentional exception).
Do **not** run `tools/dev/gates`, `./compat upstream|integration` or soak: the orchestrator does.
Blocking commands, no waiting messages, no push. Commit on your branch (`13x: …`) and end with a
short report: files touched, leftover Italian (should be none), commands and real results.
