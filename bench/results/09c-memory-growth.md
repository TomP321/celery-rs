09c — diagnosi e correzione dei cicli dei traceback (27 ottobre 2026)

Correzioni: `bench/soak.py` sceglie sempre `.venv-<impl>/bin/python`, anche senza `--compare` e con lock già acquisito. Nel figlio, dopo la serializzazione e l'invio di READY, si liberano i frame del traceback e i loro chiamanti già terminati. Nessuna modifica a soglie/abilitazione del GC, nessun nuovo preload o dipendenza. Diagnostica opzionale tramite `CELERY_RS_MEMORY_PROBE=<directory>`: RSS/PSS/USS/Shared/Private, snapshot tracemalloc aggiornato ogni 30 secondi e diff per traceback, contatori Rust se compilato con `memory-probe`, XML di `malloc_info`. Il percorso normale non importa la diagnostica.

Causa: `ExceptionInfo.exception → traceback → frame trace_task → f_back fast_trace_task → R → ExceptionInfo`. Celery tenta la pulizia mentre il tracer sta ancora eseguendo; inoltre `traceback.clear_frames()` percorre `tb_next`, non `f_back`. Le liste di posizioni del codice costruite da `billiard.einfo._Code` restano quindi in cicli fino al GC, con allocazioni e frammentazione. Una prima correzione limitata a `clear_frames()` lasciava ancora `ExceptionInfo` referenziati da `fast_trace_task`: verificato con `gc.get_referrers`. La correzione finale percorre anche `f_back`, fermandosi al primo frame ancora attivo.

Il test sul vero `fast_trace_task`, con GC disabilitato, falliva prima di quest'ultima modifica e passa dopo: i weakref a `ExceptionInfo` si svuotano immediatamente per failure/retry/reject/ignore. Un altro test verifica identità dei byte pickle prima/dopo la pulizia. I test esistenti confrontano inoltre il pool Rust con billiard.

Tabella causa → MiB/h: scomposizione dei baseline c10 forniti dall'orchestratore, campioni a 300 e 870 secondi; medie per figlio. Le componenti sono misure di pagine, non una stima indipendente del costo dei singoli siti di allocazione.

| Componente osservata | Redis rs | AMQP rs |
|---|---:|---:|
| Nuove pagine anonime/RSS (include cicli traceback, cache e frammentazione) | circa 10,0 | circa 16,4 |
| Shared_Dirty perse, limite indicativo dell'erosione COW | circa 1,8 | circa 0,8 |
| PSS complessiva del figlio | circa 11,4 | circa 17,0 |

Il problema dominante è quindi nuova memoria, non erosione COW. Nella prova diagnostica Redis da 2 minuti, l'heap Rust vivo oscilla fra circa 47 KiB e 2,09 MiB dopo l'avvio (buffer I/O), senza crescita progressiva; picco circa 8,1 MiB. Il diff Python indica soprattutto `billiard/einfo.py:28` (`list(code.co_positions())`). Nella prova AMQP con interventi diagnostici, il GC all'ultimo campione recupera circa 1,55 MiB Python e `malloc_trim` circa 7,37 MiB RSS; `malloc_info` registra circa 13,6 MiB liberi nelle arene. Questi dati includono l'overhead di tracemalloc/snapshot: NON sono pendenze naturali né motivano un nuovo trim o GC periodico in produzione.

Prove A/B finali: c1, stesso carico misto di `soak_app`/`soak.load`, senza beat, senza tracemalloc, 240 secondi, riavvio al secondo 120, lock esclusivo per broker. Prima = stessa build con sola pulizia disabilitata nel runner; dopo = correzione completa. Native compilato con `memory-probe` in entrambi i gruppi. I numeri sono MiB, campioni circa 30 → 240 secondi. Le quattro prove terminano con exit 0 (il runner verifica sopravvivenza dei processi, non certifica le soglie del soak).

| Misura | Redis prima | Redis dopo | AMQP prima | AMQP dopo |
|---|---:|---:|---:|---:|
| PSS figlio | 27,05 → 30,24 | 23,79 → 24,95 | 27,59 → 30,43 | 24,16 → 24,39 |
| RSS finale figlio | 53,16 | 48,47 | 53,72 | 48,18 |
| USS finale figlio | 18,68 | 13,65 | 19,01 | 13,20 |
| Crescita PSS worker + zygote + figlio | +5,45 | +1,13 | +5,87 | +0,97 |
| Cicli completati | 66 | 66 | 56 | 56 |
| Delta PSS totale circa 110 → 131/132 s, attorno al riavvio | −1,60 | −1,64 | +0,86 | +0,98 |

La riduzione della crescita del figlio su questa finestra equivale a circa 35 MiB/h Redis e 45 MiB/h AMQP per c1; è un confronto breve che include warm-up e riconnessione, NON una previsione a un'ora. Dopo la correzione AMQP tende al plateau, Redis conserva crescita residua. Anche le prove precedenti da 120 secondi mostravano riduzione della PSS finale, ma la pulizia incompleta non risolveva il ciclo del chiamante.

Dati conservati in `bench/results/09c-memory-growth.json`: serie per ruolo/processo delle quattro prove da 240 s, cicli, tracce Python/Rust, statistiche malloc. Log e dettagli locali in `target/09c/`. Runner riproducibile:

```
tools/dev/with-brokers .venv-rs/bin/python bench/memory_growth.py --broker redis --seconds 240 --output target/09c/new-redis
# Aggiungere --without-cleanup per A/B, --trace per diagnosi.
# --reclaim con --trace misura separatamente GC e malloc_trim; solo diagnosi.
```

Verifiche eseguite: `cargo build --release` OK; `cargo clippy --all-targets -- -D warnings` OK; `tools/dev/with-brokers cargo test` OK, 240 test passati e 4 fuzz ignorati. Suite Python finale sotto lock (`test_worker_pool.py`, `test_redis_memory.py`, `test_memory_growth.py`, `bench/test_soak.py`): 39 passati, 52 warning di deprecazione fork da processo multithread. Le prove esplorative e A/B sono tutte bloccanti, al massimo 4 minuti ciascuna. Nessun gate o soak da 15/60 minuti eseguito.

Ripristinata e verificata la build normale con `tools/dev/build-package` (OK, `_allocator_stats` assente); i 10 test diagnostici/soak passano anche senza feature.

Da misurare dall'orchestratore: dopo `tools/dev/build-package`, soak `--compare --minutes 15` per PSS totale c10 e per figlio a 5 minuti/fine, pendenze relative al riferimento, delta attorno al riavvio e ripresa del carico; poi durata lunga se il confronto breve è soddisfacente. Verificare anche latenza e throughput rappresentativi: qui è verificata soltanto la parità dei cicli, oltre ai test del pool. La soglia della spec su 60 minuti e il salto totale c10 ≤3 MiB restano NON certificati.
