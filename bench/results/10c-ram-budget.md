# 10c — bilancio e riduzione della RAM

**Stato parziale: i criteri di accettazione NON sono tutti soddisfatti.**
La flotta scende da 78,1/86,0 a 73,3/81,4 MiB per worker idle/carico;
restano da recuperare 8,3/9,4 MiB per raggiungere 65/72. Nessuna modifica a
Celery di riferimento, tools/compat, test_kombu_identity.py o dipendenze.

## Modifiche adottate

- Lo zygote chiude già tutti i fd del master prima del primo fork. Ogni figlio
  deve chiudere solo il controllo, conservando il proprio socket e i log:
  eliminata la seconda scansione di tutti i logger, che erodeva il CoW.
- Il consumer Rust restituisce le pagine glibc libere dopo `on_ready`, anche
  alla riconnessione. Nessun GC, modifica di soglie, segnali o ambiente.
- `bench.py` salva anche smaps per mappatura e numero di thread, senza
  aggiungere letture smaps al campionatore frequente del picco.
- `ram_budget_probe.py`: diagnostica esplicita per master, zygote, figli e beat;
  moduli, tracemalloc per file, code object/docstring raggiungibili, mallinfo2,
  statistiche pymalloc e heap Rust della build `memory-probe`.

## Prima/dopo

MiB, PSS Linux. Flotte: budget 2048, c10, 10000 task pure completati;
`fleet-10c-{before,after}.json`. Gli 8 GiB sono stime da 8192/PSS media,
non una nuova flotta da 8 GiB.

| misura | prima | dopo | limite |
|---|---:|---:|---:|
| flotta, worker idle | 78,1 | 73,3 | 65: FALLITO |
| flotta, worker dopo carico | 86,0 | 81,4 | 72: FALLITO |
| worker idle effettivi entro 2 GiB | 26 | 27 | — |
| worker carichi stimati entro 2 GiB | 23 | 25 | — |
| worker stimati in 8 GiB, idle/carico | 104/95 | 111/100 | — |
| singolo c10 pure, idle/dopo latenza | 85,36/102,60 | 76,65/97,10 | — |
| master pure, idle/dopo latenza | 17,66/16,78 | 14,21/15,30 | 15: solo idle PASS |
| zygote pure, idle/dopo latenza | 6,34/7,53 | 5,65/7,06 | — |
| figlio medio pure, idle/dopo latenza | 6,14/7,83 | 5,68/7,48 | — |
| singolo c10 bench-toplevel, idle/dopo latenza | 85,69/137,55 | 78,01/137,76 | — |
| figlio medio bench-toplevel dopo latenza | 11,20 | 11,08 | — |
| beat Redis, 3 voci | 23,36 | 23,71 | 24: PASS |
| beat AMQP, 3 voci | 22,64 | 24,31 | 24: FALLITO |
| client Redis dopo get, PSS / anonima | 24,15/19,445 | 21,66/19,441 | nessun aumento |
| client AMQP dopo get, PSS / anonima | 24,22/19,523 | 21,49/19,523 | nessun aumento |
| pure p50 ms / task al secondo | 2,472/2459 | 2,216/2858 | ref 3,150/864: PASS |
| bench-toplevel p50 ms / task al secondo | 2,257/2828 | 2,124/2682 | ref non rimisurato |

La PSS dei file condivisi dipende dagli altri processi sull'host: la baseline
iniziale pure girava durante la flotta e misurava 78,58/96,70; la ripetizione
con smaps è `10c-before-physical.json`. Non attribuire tutto il calo della PSS
singola al codice. Più stabili i privati: master idle 11,98→10,79, figlio idle
3,50→3,12, figlio dopo latenza 5,10→4,77. Client: identici 333 moduli e memoria
anonima invariata. Beat non modificato: i numeri non dimostrano un cambiamento.

## Dove va ogni MiB

PSS fisica prima delle modifiche, senza sonda nel worker; figlio medio c10.
File `10c-before-physical.json`, sezioni idle_processes/after_processes.

| ruolo/stato | heap glibc | anonime, incluse arene Python e stack nativi | libpython/eseguibile | estensione Rust | altre librerie | stack principale | totale | Private_Dirty |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| master idle | 4,42 | 8,30 | 0,46 | 3,07 | 1,28 | 0,04 | 17,66 | 11,98 |
| master carico | 4,50 | 9,27 | 0,42 | 1,28 | 1,18 | 0,04 | 16,78 | 12,93 |
| zygote idle | 1,13 | 4,32 | 0,35 | 0,06 | 0,40 | 0,01 | 6,34 | 3,13 |
| zygote carico | 1,62 | 5,04 | 0,32 | 0,07 | 0,38 | 0,03 | 7,53 | 4,38 |
| figlio idle | 1,16 | 4,49 | 0,29 | 0,07 | 0,06 | 0,01 | 6,14 | 3,50 |
| figlio carico | 1,72 | 5,37 | 0,31 | 0,26 | 0,08 | 0,03 | 7,83 | 5,10 |

Smaps conserva anche Private_Clean e Shared_Clean/Dirty per ciascuna mappatura.
Heap e anonime ereditati sono condivisi CoW solo nello stesso worker; gli
oggetti Python di worker distinti non sono condivisi. Le pagine pulite dei
file sono condivisibili anche fra worker distinti. Allocazioni nuove, pagine
CoW scritte, TLS e stack sono privati. Il master ha 13 thread; zygote 1;
figlio 1 idle, 2 dopo la sonda di latenza che usa il backend. Non è isolata la
PSS degli stack pthread dalle altre mappature anonime.

Beat baseline A/B Redis: 24,02 MiB, di cui 21,10 anonimi e 21,13 Private_Dirty;
smaps completo in `10c-beat-ab.json`. La schedule persistente carica anche
sqlite3/dbm. La distribuzione per file non va sommata all'heap Python:
sono viste diverse delle stesse pagine.

Diagnostica separata, **con overhead della sonda**, in
`10c-{heap,physical}-diagnosis.json`. Tracemalloc parte all'avvio dell'interprete;
la build diagnostica espone i contatori Rust. Nessuna soglia di accettazione
è valutata con questa build o con tracemalloc attivo.

| ruolo | Python tracciato idle/carico | code object / docstring raggiungibili | Rust vivo idle/carico | glibc libero idle/carico senza tracemalloc | arene pymalloc idle/carico | blocchi+pool liberi idle/carico |
|---|---:|---:|---:|---:|---:|---:|
| master | 19,04/22,54 | 3,40/0,82 | 0,190/0,275 | 2,04/2,13 | 17/18 | 1,06/0,91 |
| zygote | 27,25/— | 5,02/1,32 | 0,0042/— | 0,70/— | 23/— | 0,26/— |
| figlio | 30,95/34,41 | 5,02/1,32 | 0,0042/0,0042 | 0,69/0,68 | 25/26 | 0,74/3,15 |
| beat | 17,29/— | 3,03/0,76 | 0,0044/— | 0,61/— | 15/— | 0,62/— |

Le righe tracemalloc per file e i moduli sono nei JSON; il loader attribuisce
molta memoria a importlib, quindi **non è un'attribuzione esatta dell'intero
heap a ciascun modulo importato**. I code object/docstring sono una stima
degli oggetti raggiungibili dai namespace, deduplicati. Principali costi
oltre al loader: typing, enum, collections, defaults, click; nello zygote si
aggiungono redis/commands, redis/cluster, redis/asyncio e kombu.transport.redis.
I campioni carichi includono gli oggetti temporanei lasciati dalla prima
diagnosi: non inferire una crescita naturale dal loro delta.

## Esperimenti

Private_Dirty medio del figlio dopo latenza (MiB); stessa famiglia di prove
c10 pure, 10000 task. Dati `10c-<variante>.json`.

| variante | privato figlio | decisione |
|---|---:|---|
| ripetizione baseline | 5,17 | confronto |
| collect master prima del fork | 5,20 | scartato |
| trim master prima del fork | 5,20 | scartato |
| PYTHONMALLOC=malloc | 5,56 | scartato, peggiora |
| glibc arena_max=1, trim_threshold=0, mmap_threshold=16384 | 5,17 | scartato, nessun vantaggio affidabile |
| collect+freeze master | 5,20 | scartato |
| preload figli/errori nel master | 5,19 | scartato, carica moduli vietati nel master |
| niente seconda scansione fd | 4,77 | adottato |
| scansione eliminata + warm build_tracer | 4,82 | ulteriore warm scartato |
| scansione eliminata + collect zygote | 4,99 | collect scartato |
| scansione eliminata + environment logging idempotente | 4,78 | ulteriore modifica scartata |
| scansione eliminata + trim a fine avvio master | 4,80 | trim adottato: master privato idle 12,11→10,85 |

Trim in beat: prova alternata, due campioni per broker. Anonimi invariati
21,10 MiB; PSS totale circa 24,0→25,7. La mappatura dell'estensione cresce
da 608 a 2344 KiB alla prima chiamata nativa. **Scartato**: nessun recupero,
nuove pagine di codice residenti. Stack più piccoli e rimozione docstring
non adottati: stack principalmente virtuali; docstring/API devono restare
intatti. Nessuna rimozione di moduli dall'ambiente dei task.

## Verifiche eseguite

- `cargo build --release`: PASS; anche build diagnostica con
  `--features celery-py/memory-probe`, copiata fuori dal percorso normale.
- `cargo clippy --all-targets -- -D warnings`: PASS.
- `tools/dev/with-brokers cargo test`: 243 PASS, 4 ignorati dalla suite.
- `tools/dev/build-package`: PASS, prima delle misure; installazione editable.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest
  python/tests_rs/{test_ram_budget,test_child_memory,test_memory_growth,
  test_worker_c10,test_zygote_logging_fds,test_master_beat_restart,
  test_kombu_identity}.py -q`: 38 PASS, 157,10 s.
- Primo giro mirato ram_budget + zygote_logging_fds: 6 PASS.
- Benchmark obbligatori, sotto `with-brokers timeout 300`, tutti exit 0:
  `bench/fleet.py --venv .venv-rs --budget-mb 2048 --tasks 10000` prima/dopo;
  `bench/bench.py --venv .venv-rs --pool prefork --concurrency 10
  --task pure|bench-toplevel --prefill --n 10000 --latency-n 30|100` prima/dopo;
  `bench/bench_beat.py --venv .venv-rs` e `bench/bench_client.py` prima/dopo.
  Pure del riferimento rimisurato con 100 campioni di latenza.
- `git diff --check`: PASS. Nessun gates, soak o compat integration eseguito.

Restano aperti: limiti della flotta, beat AMQP stabile entro 24 MiB, master
entro 15 MiB anche dopo il carico; attribuzione completa dell'heap per modulo
e degli stack pthread. Il commit contiene risparmi verificati e diagnostica,
non certifica il completamento della spec 10c.
