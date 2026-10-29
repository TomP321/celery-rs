# 10d — Zygote: Don't close descriptors opened by user code

Orchestrator review. `python/celery/concurrency/_rs_zygote.py::_main` imports the application modules (`app.loader.import_default_modules()`, preloads) and **then** calls `_close_inherited_fds`, which closes all descriptors except stdio, control, and logger streams. Thus it also closes files, sockets and connections opened by user code at the module level (e.g. `DATA = open('dati.txt')`, a client created on import). In the reference, the prefork children (billiard, fork from the master) inherit all the descriptors of the master and can use them in tasks: here they find them closed. It is a difference in behavior that must be eliminated.

Read `AGENTS.md`, `specs/05b-worker-pool.md`, `specs/09a-integration.md` (logging fd), `specs/10c-ram-budget.md`. Constraints unchanged.

## Work

1. The zygote (and children) should close **only** descriptors that belong to the master's Rust runtime (epoll, eventfd/waker, Rust broker and backend sockets, internal pipes), not those opened by the user's Python code or libraries. Possible strategies, to be chosen with numbers and security: the Rust runtime exposes the list of its fds (or opens them with `O_CLOEXEC` and marks them), or you register the set of fds before starting the Rust runtime and close only those created afterwards by the runtime. Inherited Rust fds must never be used or closed twice by the child (fork safety already verified in 09b).
2. Differential test in `python/tests_rs` (worker rs and worker ref, `-P prefork -c 2`): a task module opens a file and a loopback socket to import; a task reads/uses them; the outcome must be identical to the reference.
3. 10c invariant test: after booting, the zygote does not open new fd (`/proc/<zygote>/fd` count equal after N spawns and respawns of children with `max_tasks_per_child`); if the invariant were to break, the children would inherit fd that no longer close.
4. No increase in RAM (`bench/bench.py --pool prefork --concurrency 10 --task pure --prefill`, before/after) and no regression in logging fd (09a test).

## Check

Targeted tests yours, blockers. **Do not** run `tools/dev/gates`, soak, or `./compat integration` - the orchestrator does them.

## Work Rules (model quota limited)

Blocking commands, no waiting messages or background commands. Brief report: strategy chosen and why, files touched, tests performed, RAM before/after. Commit to the branch (`10d: …`).
