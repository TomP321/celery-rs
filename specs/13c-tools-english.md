# 13c — Tools, benchmarks and README GIFs in English

Read `specs/13-english-style.md` (mandatory rules) and `AGENTS.md`. For this spec only, editing
`tools/compat/` is **allowed** (output text and comments only; its checks and verdicts must not
change).

Scope: `tools/` (compat, dev, agent, selector, docs), `bench/` code (`*.py`, scripts; not the
historical `bench/results/` reports, that is 13d), `infra/`, `.github/` if present, `Makefile`
or scripts at the root.
- Tool output in English and terse; `tools/dev/gates` and anything that parses compat/bench output
  must keep working (update both sides together). Keep `target/gates/<name>.log` names.
- The README GIFs: translate every label in `tools/docs/make_readme_gif.py`,
  `tools/docs/make_memory_gifs.py` and `tools/docs/conversion_map.toml`/`conversion-report.md`,
  regenerate `docs/assets/*.gif` (`uvx --with pillow python tools/docs/<script>.py`), and look at
  3–4 frames of each to check that nothing is cut or overlapping. Keep file names, sizes
  (1920×1080 for the memory GIFs) and numbers.
- Verification also includes: `./compat api`, `./compat cli` (output must still report 0
  differences), `tools/dev/lanes status`, and running one bench probe
  (`tools/dev/with-brokers .venv-rs/bin/python bench/memprobe/loadpeak.py $PWD/.venv-rs 10 $PWD/bench/memprobe 3000`).
