# 10b — RAM dei figli prefork

Unità: MiB (PSS/1024), come nei benchmark. Redis; 10000 task, prefill, 20 campioni di latenza; concurrency 10/50. Ogni misura è terminata entro 300 s; i batch successivi al primo c10 pure hanno timeout esplicito. Le soglie sono verificate anche dai quattro nuovi test, su nuove istanze del worker.

## Prima/dopo senza profiler

| c / task | figlio medio prima → dopo | massimo prima → dopo | totale prima → dopo | master prima → dopo | zygote prima → dopo |
|---|---:|---:|---:|---:|---:|
| 10 / pure | 9.96 → 8.07 | 9.97 → 8.09 | 129.7 → 105.9 | 20.12 → 17.18 | 9.98 → 8.07 |
| 10 / bench-toplevel | 12.41 → 11.37 | 12.42 → 11.38 | 153.0 → 141.0 | 18.09 → 17.43 | 10.83 → 9.83 |
| 50 / pure | 6.47 → 5.39 | 6.96 → 5.88 | 349.7 → 291.6 | 19.56 → 16.93 | 6.50 → 4.97 |
| 50 / bench-toplevel | 10.14 → 9.00 | 10.56 → 9.39 | 536.3 → 473.5 | 20.43 → 15.87 | 8.75 → 7.62 |

| c / task | task/s prima → dopo | p50 ms prima → dopo | p99 ms prima → dopo |
|---|---:|---:|---:|
| 10 / pure | 2749 → 3108 | 2.41 → 2.21 | 4.01 → 4.61 |
| 10 / bench-toplevel | 2751 → 2779 | 1.99 → 2.03 | 4.47 → 4.57 |
| 50 / pure | 2892 → 2977 | 3.70 → 4.32 | 4.25 → 4.86 |
| 50 / bench-toplevel | 2834 → 2847 | 4.24 → 3.69 | 4.90 → 4.35 |

Campioni brevi su host condiviso: la latenza ha varianza, senza una conclusione statistica su differenze piccole.

## Cause e correzioni

| causa | MiB privati per figlio c10 pure | intervento / evidenza |
|---|---:|---|
| pagine libere glibc dopo initializer e riscaldamento | circa 0,59 recuperati | variante solo trim: Private_Dirty 6,89 → 6,31; PSS 9,96 → 9,17 |
| parser CFFI/prctl e pagine ereditate scritte durante initializer | ulteriori 1,08 recuperati | syscall nativa: Private_Dirty 6,31 → 5,22; PSS 9,17 → 8,07 |
| erosione CoW e inizializzazione del trace dopo idle | circa 1,68 residui | Private_Dirty finale 5,22 contro idle 3,54; nessun nuovo modulo nel percorso pure |
| nuovi oggetti Python vivi, con sonda attiva | ≤ 0,20 prima; ≤ 0,16 dopo | tracemalloc a 2000 task, comprende gli oggetti della sonda |
| heap Rust nel figlio pure | 0,0043, costante | build memory-probe, 4424 byte vivi e picco 4608 dal fork a 2000 task |

Il percorso nativo imposta lo stesso PR_SET_PDEATHSIG/SIGKILL di billiard, preservandone il ritorno anche per un segnale non valido. Nessuna classe kombu/billiard sostituita. Il parser CFFI non viene più precaricato nello zygote né costruito dall’initializer. libc era già nel workspace: la dipendenza diretta evita parser e oggetti temporanei per una sola syscall.

Le pagine libere vengono restituite dopo gc.freeze nello zygote, dopo initializer e dopo i task 1/10/100/1000. Restano invariati GC, logging, segnali dei task e codice utente. Il recupero dei payload grandi già esistente resta attivo. Non servono riduzioni dei buffer IPC: il contatore Rust non mostra ritenzione crescente.

Varianti scartate: solo trim (9,17 MiB, ancora sopra soglia); GC esplicito (nessun recupero nel primo esperimento, con aumento delle pagine private). Nessun GC aggiunto al percorso normale.

## Diagnosi con profiler

30000 task pure, c10, feature memory-probe; media dei dieci figli che completano almeno 2000 task. Le statistiche complete per heap, mapping anonimi (incluse arene pymalloc), librerie e stack sono in 10b-diagnosis.json. La sonda salva anche snapshot tracemalloc e malloc_info per processo in /tmp/10b-diagnosis-v2-{before,after}.

| task per figlio | PSS prima → dopo | Private_Dirty prima → dopo | Python vivo KiB prima → dopo |
|---|---:|---:|---:|
| 0 | 7.99 → 8.17 | 5.21 → 5.17 | 16.6 → 16.6 |
| 1 | 12.67 → 10.89 | 10.17 → 8.38 | 137.5 → 109.4 |
| 10 | 13.65 → 11.28 | 11.15 → 8.77 | 181.7 → 138.1 |
| 100 | 13.93 → 11.52 | 11.43 → 9.02 | 190.9 → 153.6 |
| 1000 | 14.07 → 11.64 | 11.58 → 9.14 | 194.6 → 160.3 |
| 2000 | 14.16 → 12.74 | 11.67 → 10.23 | 199.7 → 163.4 |

Il profiler altera il residente: snapshot, lettura smaps e serializzazione lasciano memoria temporanea nell’allocatore; non usare questa PSS per le soglie di accettazione. In particolare il campione 2000 segue 1000 task senza trim forzato, mentre i campioni 1/10/100/1000 seguono un trim. Nessuna importazione pigra aggiuntiva osservata nei figli pure.

## Flotta e processi esterni al pool

Flotta: budget 2048 MiB, c10, 10000 task pure completati in entrambi i casi. I numeri di worker sotto carico sono stime dal residente medio del campione, come implementato da fleet.py.

| | prima | dopo |
|---|---:|---:|
| worker idle entro 2 GiB | 22 | 26 |
| worker sotto carico stimati entro 2 GiB | 20 | 23 |
| MiB per worker sotto carico | 97.9 | 85.8 |
| worker sotto carico stimati in 8 GiB | 83 | 95 |
| beat redis, 3 voci, MiB | 25.39 | 24.73 |
| beat redis, 1000 voci, MiB | 27.32 | 27.25 |
| beat amqp, 3 voci, MiB | 24.55 | 24.54 |
| beat amqp, 1000 voci, MiB | 28.55 | 27.77 |
| client redis dopo get, MiB | 22.01 | 22.24 |
| client redis, Pss_Anon MiB | 19.438 | 19.453 |
| client amqp dopo get, MiB | 22.55 | 22.45 |
| client amqp, Pss_Anon MiB | 19.523 | 19.523 |

Client: 333 moduli in entrambi i casi; differenza anonima Redis di 16 KiB, PSS totale +0,23 MiB contro +0,12 MiB del riferimento misurato contemporaneamente; AMQP in lieve calo. Nessun aumento materiale osservato fuori dal pool, con rumore delle pagine condivise.

## Verifiche

- cargo build --release: PASS.
- cargo clippy --all-targets -- -D warnings: PASS.
- tools/dev/with-brokers cargo test: 243 PASS, 4 ignorati dalla suite.
- tools/dev/build-package e tools/dev/build-native: PASS, ripetuti sulla versione finale.
- ./compat api: 0 differenze su 137 moduli e settings/signals/CLI.
- tools/dev/with-brokers .venv-ref/bin/python -m pytest crates/celery-py/tests -q: 81 PASS.
- tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/{test_child_memory,test_memory_growth,test_worker_c10,test_zygote_logging_fds,test_master_beat_restart,test_kombu_identity}.py -q: 35 PASS.
- test_worker_pool.py -k "process_signals or zygote_preserves_user_gc_configuration": 4 PASS; test_worker_prefork.py::test_zygote_lost_shuts_down_with_failure: 1 PASS (sempre sotto with-brokers).
- Benchmark worker prima/dopo: bench/bench.py --venv .venv-rs --pool prefork --concurrency {10,50} --task {pure,bench-toplevel} --prefill --n 10000 --latency-n 20; flotta: bench/fleet.py --venv .venv-rs --budget-mb 2048 --tasks 10000. Tutti completati sotto lock; batch successivi al primo con timeout 300 s; baseline fleet con --source /tmp/10b-baseline-source, ricostruita da HEAD.
- Ripetizione finale sulla build installata: test_child_memory.py, test_memory_growth.py e test_worker_c10.py, 15 PASS in 71,81 s. Verificato che la build installata non esponga i contatori memory-probe.
- Controllo aggiuntivo cargo fmt --check: segnala differenze anche in numerosi file non modificati; nessuna formattazione globale applicata. git diff --check pulito.

Non eseguiti tools/dev/gates, soak e compat integration, come richiesto dalla spec: restano all’orchestratore. Nessun criterio di memoria aperto.

File modificati: bridge PyO3 worker/lib/Cargo e Cargo.lock, celery-backend/redis.rs, platforms.py, concurrency/_rs_child.py/_rs_zygote.py/_rs_memory_probe.py, bench/fleet.py, test_child_memory.py e test_memory_growth.py, dati e questo rapporto. Sorgente di riferimento, tools/compat e test_kombu_identity.py invariati.
