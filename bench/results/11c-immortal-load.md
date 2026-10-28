# 11c — oggetti immortali e carico prefork

CPython 3.13.9, Linux x86_64, `-P prefork -c 10`, 30.000 task `memapp.add`,
Redis e RabbitMQ sulle corsie di `tools/dev/with-brokers`. Data: 2026-10-28.

## Implementazione

`_celery_rs._immortalize(gc.get_objects())` visita i referenti tramite
`PyType_GetSlot(Py_tp_traverse)`, comprese le tuple non tracciate. Non usa un
insieme di visitati: marca prima della ricorsione e salta i refcount già
immortali. Stack limitato a 128 livelli, dieci puntatori ai tipi nativi per le
esclusioni, nessuna struttura proporzionale al numero di oggetti visitati.
Le radici tracciate vengono visitate singolarmente anche se raggiunte da
percorsi profondi. Una catena di soli contenitori non tracciati oltre il limite
può rimanere parzialmente normale.

Esclude `tp_finalize`, `tp_del` e istanze dei dieci tipi esportati dal core,
anche quando le esportazioni Python vengono sostituite. Scrive soltanto i
32 bit bassi del refcount e conserva i flag alti. I valori provengono dagli
header CPython: `UINT32_MAX` su
[3.12](https://github.com/python/cpython/blob/3.12/Include/object.h) e
[3.13](https://github.com/python/cpython/blob/3.13/Include/object.h), `3 << 30` su
[3.14](https://github.com/python/cpython/blob/3.14/Include/refcount.h).

La visita avviene nel master prima del fork dello zygote e nello zygote prima
del freeze finale. Segue il rilascio delle pagine libere dell'allocatore.
`CELERY_RS_IMMORTALIZE=0` disattiva l'ottimizzazione prima di creare la lista GC.
Le build senza GIL, con `Py_TRACE_REFS`, gli altri interpreti, le architetture
non supportate e le versioni diverse da 3.12–3.14 non vengono modificate.
Nessuna dipendenza aggiunta. I compromessi sui weakref e sul ciclo di vita
sono descritti in `docs/DIFFERENZE.md`.

## Diagnosi sotto carico

`sd.py` usa il pool Rust reale con un figlio e il tracing Celery di `add`.
Dopo il task 10 azzera `clear_refs` nel figlio e nel master; dopo il task 2000
legge `pagemap`, la memoria privata e i tipi dominanti nelle pagine scritte.
La classificazione, che alloca, avviene soltanto dopo le letture. Non usa
tracemalloc. Il master di questa sonda misura IPC e supervisione; il percorso
completo broker/consumer viene misurato separatamente da `loadpeak.py`.

| Argomenti | Immortali | Pagine scritte figlio | Pagine scritte master | Crescita privata figlio (KiB) | Crescita privata master (KiB) |
|---|---|---:|---:|---:|---:|
| interi | no | 260 | 252 | 16 | 80 |
| interi | sì | 144 | 170 | 44 | 64 |
| 16 KiB | no | 281 | 233 | 44 | 48 |
| 16 KiB | sì | 131 | 159 | 40 | 28 |

Le pagine scritte non sono tutte nuove copie: molte erano già private al task
10. Tutte le pagine rilevate appartengono a regioni già presenti al task 10.
Con gli immortali restano dominanti gli oggetti `code` (50/51 pagine), poi
`tuple` (24/17), `str` (21/16); senza immortali sono molto più frequenti
`function`, `type` e descrittori. I contatori dell'interprete adattivo restano
scrivibili anche quando il refcount è immortale.

La riduzione delle scritture sotto carico è del 45% con gli interi e del 53%
con 16 KiB. La crescita privata tardiva è di poche decine di KiB. Non è stata
introdotta una compattazione o un riempimento preventivo di pymalloc: queste
misure non dimostrano un beneficio sufficiente a giustificare nuove arene
trattenute. Non sono stati cambiati il protocollo IPC o i buffer dei task.

## Riproduzione

Ogni riga della matrice usa un processo nuovo, una coda univoca e una corsia
esclusiva. I confronti finali vengono eseguiti in sequenza, dopo build e test,
per evitare la contesa con il linker e con i worker della suite. Prima =
`CELERY_RS_IMMORTALIZE=0`; dopo = `CELERY_RS_IMMORTALIZE=1`.

```bash
for broker in redis amqp; do
  for bytes in 0 16384; do
    for enabled in 0 1; do
      tools/dev/with-brokers env CELERY_RS_IMMORTALIZE="$enabled" \
        .venv-rs/bin/python bench/memprobe/loadpeak.py \
        "$PWD/.venv-rs" 10 "$PWD/bench/memprobe" 30000 \
        --broker "$broker" --payload-bytes "$bytes"
    done
  done
done
```

`loadpeak.py` controlla che i dieci figli esistano prima della misura a riposo.
Campiona PSS ogni 200 ms, termina il carico quando il produttore ha finito e
la coda è vuota, misura nuovamente dopo tre secondi. Il throughput comprende
produzione e consumo, come la sonda originale; il limite della misura è la
piccola finestra dei task già in esecuzione alla lettura della coda vuota.
Il picco riguarda l'intervallo del carico, non i tre secondi successivi.
I valori sono MiB (KiB da `/proc` divisi per 1024), benché la spec li chiami MB.
La PSS dipende anche dai processi esterni che condividono librerie e bytecode.

Sonda soft-dirty (ripetere con `enabled=0/1` e `bytes=0/16384`):

```bash
tools/dev/with-brokers env CELERY_RS_IMMORTALIZE="$enabled" \
  .venv-rs/bin/python bench/memprobe/sd.py --payload-bytes "$bytes"
```

## Verifiche eseguite

| Comando | Esito |
|---|---|
| `tools/dev/build-package` | installazione release nel venv rs riuscita |
| `cargo build --release` | OK |
| `cargo clippy --all-targets -- -D warnings` | OK, nessun warning |
| `tools/dev/with-brokers cargo test` | 245 passati, 4 ignorati |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs -q` | 274 passati, 57 warning, 928,02 s |
| `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_immortal.py -q` | 6 passati sulla versione finale |
| `./compat api` | 0 differenze su 137 moduli + settings/signals/CLI |
| `git diff --check` | OK |

La suite completa era già partita quando sono stati aggiunti gli ultimi tre
casi di no-op (versione non supportata, free-threaded e trace-refs): la raccolta
finale ha 277 casi. Tutti i sei test mirati finali sono stati rieseguiti dopo
le ultime modifiche, coprendo anche questi tre casi e le nuove asserzioni sulle
esclusioni native. I 57 warning della suite comprendono le deprecazioni di
`fork()` da processi con thread nei test del pool.

Gli stessi sei test mirati sono stati eseguiti sul modulo ABI3 finale con:

```bash
env PYTHONPATH="$PWD/python" uv run --python 3.12 --no-project --with pytest \
  python -m pytest -o addopts= python/tests_rs/test_immortal.py -q
env PYTHONPATH="$PWD/python" uv run --python 3.14 --no-project --with pytest \
  python -m pytest -o addopts= python/tests_rs/test_immortal.py -q
env PYTHONPATH="$PWD/python" uv run --python 3.11 --no-project --with pytest \
  python -m pytest -o addopts= python/tests_rs/test_immortal.py -q
```

Risultati: CPython 3.12.3 e 3.14.5, 6 passati ciascuno; CPython 3.11.15,
5 passati e 1 saltato (la misura delle pagine private richiede PEP 683).
Su CPython 3.13.9 passano tutti e sei. Sono verificati il marcatore, i
referenti non tracciati, i cicli, le esclusioni con finalizzatore e native
(compresa `CronParseError` e la sostituzione delle esportazioni), la normale
vita degli oggetti nuovi, il flag di disattivazione e il risparmio di pagine
dopo un fork con margine del 40% rispetto al caso senza immortali.

Non sono stati lanciati gates, upstream, integration o soak: restano le
verifiche successive affidate all'orchestratore dalla spec. Non sono stati
modificati il riferimento, i tool di conformità o i test upstream.

## Risultati finali

Matrice sequenziale di 30.000 task dopo la conclusione della suite; ogni
cella riporta **prima → dopo**, in MiB PSS sommata su master, zygote e dieci figli.

| Broker / argomenti | Riposo | Picco carico | Dopo carico | Task/s |
|---|---:|---:|---:|---:|
| Redis / interi | 66,3 → 49,5 | 73,1 → 53,7 | 73,1 → 53,7 | 2829 → 2822 |
| Redis / 16 KiB | 66,0 → 49,6 | 74,8 → 57,3 | 74,8 → 57,3 | 1828 → 1825 |
| RabbitMQ / interi | 66,3 → 49,8 | 74,7 → 56,5 | 74,7 → 56,5 | 4219 → 3993 |
| RabbitMQ / 16 KiB | 66,5 → 49,8 | 77,8 → 58,1 | 77,8 → 58,2 | 2723 → 2979 |

I limiti della spec si riferiscono a Redis con argomenti interi: 49,5 ≤ 53
MiB a riposo, 53,7 ≤ 58 di picco, 53,7 ≤ 56 dopo il carico. Il throughput
è il 99,75% del caso precedente (2822 contro 2829 task/s). La crescita dal
riposo al picco scende da 6,8 a 4,2 MiB. Al picco il master scende da 13,5 a
10,9 MiB e la somma dei figli da 54,5 a 38,9 MiB.

RabbitMQ con interi ha mostrato una riduzione del 5,4% nel primo confronto
breve. Due ulteriori coppie di 30.000 task hanno prodotto 4107 → 4096 e
4266 → 3998 task/s; le mediane sono 4219 → 3998 (−5,24%). Per misurare la
capacità su un intervallo maggiore e ridurre la quantizzazione di 200 ms,
è stata eseguita una coppia di **300.000 task**, sempre in sequenza:

| RabbitMQ / interi, 300.000 task | Prima | Dopo |
|---|---:|---:|
| Riposo (MiB) | 66,0 | 50,1 |
| Picco / dopo (MiB) | 74,8 / 74,8 | 57,3 / 57,3 |
| Task/s | 4382 | 4292 |

Sul carico prolungato il rapporto è **97,95%** (−2,05%), entro il limite del
5%. Il confronto breve conserva una variabilità significativa vicino alla
soglia; non è stato omesso dal rapporto. Non è un soak: si tratta di una
misura finita della capacità su 300.000 task (circa 70 secondi di carico per
worker), senza restart del broker.

Il comando è quello della matrice, sostituendo `30000` con `300000` e usando
`--broker amqp --payload-bytes 0`. I log reali sono:

- [Matrice 30.000 task](11c-loadpeak.log)
- [Due ripetizioni RabbitMQ](11c-amqp-repeats.log)
- [RabbitMQ 300.000 task](11c-amqp-long.log)
- [Soft-dirty tra task 10 e 2000](11c-softdirty.log)

## File modificati e limiti

- `crates/celery-py/src/immortal.rs`, `lib.rs`: visita nativa, guardie ABI,
  esclusioni, test unitario e registrazione della funzione privata.
- `python/celery/concurrency/_rs_zygote.py`: chiamate nel master e nello
  zygote, trim prima del fork, disattivazione senza creare la lista GC.
- `python/tests_rs/test_immortal.py`: test isolati e misura delle pagine private.
- `bench/memprobe/loadpeak.py`, `sd.py`: payload da 16 KiB, RabbitMQ e sonda
  soft-dirty sul percorso reale del pool.
- `docs/DIFFERENZE.md`: durata degli oggetti, weakref, receiver e introspezione.
- Questo rapporto e i quattro log delle misure.

Restano il limite documentato di profondità della visita e la variabilità
del benchmark RabbitMQ breve. Le soglie RAM della spec sono verificate sul
caso Redis/interi richiesto; con payload più grandi e RabbitMQ sono riportate
le misure reali, senza applicare loro implicitamente le stesse soglie RAM.
