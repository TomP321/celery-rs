# 11d — RAM before speed

Interrupted by orchestrator (codex share); consolidated and measured by the orchestrator. Measurements: `bench/memprobe/loadpeak.py`, Redis, `-c 10`, 30.000 tasks, PSS in MiB.

| Scenario | Base 11c rest/peak | 11d rest/peak | task/s 11d |
|---|---|---|---|
| Python 3.11, integers | 76,5 / 88,9 | 72,5 / 84,3 | 2668 |
| Python 3.11, 16 KiB | — / ~89 | 72,6 / 85,9 | 1701 |
| Python 3.13, integers | 50,9 / 54,8 | 51,2 / 56,2 | 2723 |
| Python 3.13, 16 KiB | 50,9 / 58,4 | 51,0 / 55,7 | 1771 |

Original Celery floor (same size): 834 whole task/s, 695 with 16 KiB.

## Held

- `gc.collect()` in the master before the zygote and `gc.freeze()` just before the fork.
- `_release_task_memory(force=True)` after each task in the child.

## Rejected (measurements in `bench/results/11d-*.log`)

- Resharing pages between siblings (copies in memfd): rest 3.11 until 50,9 MiB, but the cost of copies negates the advantage under load.
- Comparison with the image before the fork: recovers little and adds memory.
- Filling/normalizing pymalloc pools: limited benefit.
- Adaptive interpreter caches turned off: gets worse (up to 90,7 MiB 3.11, 64,9 MiB 3.13).
- Tracers pre-built in the zygote: gets worse.
- Only one glibc arena: −1,7 MiB to peak 3.11 in one trial, no advantage over 3.13; not consolidated.
- Reduced lapin internal buffers (fork in `vendor/`): not measured at the end of the job, discarded due to the cost of maintaining a copy of the dependency.
