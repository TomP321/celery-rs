# 08f — RAM di `celery beat` (e `worker -B`) al minimo

Priorità del progetto: **la RAM di worker e beat rispetto al Celery originale**; le prestazioni
vengono dopo. Questa spec è dedicata a beat. Leggi `AGENTS.md`, `PLAN.md`, `specs/06-beat.md`,
`specs/08b-memory.md` (decisioni su import pigri onesti) e `specs/08b-review.md`.
Vincoli invariati: API, comportamento, traffico sul broker e formato del file di schedule
(`celerybeat-schedule`, shelve) identici al riferimento; **oggetti kombu/billiard veri, nessun
`__module__`/`__qualname__`/`__name__` riscritto, nessuna classe sostitutiva**
(`python/tests_rs/test_kombu_identity.py` non si modifica); `celery/`, `celery/t/`,
`tools/compat/` non si modificano.

## Misura di partenza (main @ 8ca8eae, `bench/bench_beat.py`, PSS dopo l'avvio)
| | ref | rs |
|---|---|---|
| 3 voci, Redis | 50,8 MB | 31,7 MB |
| 3 voci, AMQP | 43,0 MB | 31,6 MB |
| 1000 voci, Redis | 58,1 MB | 42,6 MB |
| 1000 voci, AMQP | 50,2 MB | 42,9 MB |
| invii/s Redis / AMQP | 433 / 470 | 481 / 561 |
| `Scheduler.tick` 1000 voci | 1130 µs | ~1167 µs |

rs cresce di **~11 KB per voce** (31,7 → 42,6 MB). Il processo beat a 3 voci pesa ancora 31,7 MB.

## Obiettivi
- beat 3 voci ≤ **24 MB** su Redis e AMQP;
- crescita per voce ≤ **3 KB** (1000 voci ≤ base + 3 MB); misura anche 10 000 voci;
- PSS stabile nel tempo: dopo 30 minuti di invii (o equivalente accelerato) nessuna crescita;
- `worker -B` (beat embedded, oggi in thread): misura la PSS del worker con e senza `-B`
  (`-P solo` e prefork c10) e riduci il costo aggiuntivo;
- invii/s e tick non peggiorano.

## Dove guadagnare (misura prima, adotta solo ciò che il bench conferma)
1. **Scomposizione** della PSS del processo beat: moduli Python caricati (elenco e costo,
   `python -X importtime`, `tracemalloc`), heap Python, librerie mappate, heap Rust. Riporta la
   tabella prima/dopo.
2. **Import**: ciò che beat carica senza usarlo (CLI, worker, canvas, backend, pygments, click
   oltre al necessario, ecc.) → import pigri onesti come in 08b.
3. **Per voce**: `ScheduleEntry` e gli oggetti `schedule`/`crontab`/`solar` tengono molti attributi
   Python; il core Rust ha già crontab (`crates/celery-beat`). Valuta di tenere lo stato di
   pianificazione (prossima scadenza, campi crontab espansi) in strutture Rust compatte, con
   `ScheduleEntry`/`crontab` Python che restano gli oggetti pubblici identici (stessi attributi,
   `repr`, pickle, confronti) ma senza duplicare dati; `__slots__` solo dove il riferimento non
   espone `__dict__` in modo osservabile. Verifica il costo per voce di: entry, schedule, shelve
   (cache in memoria di `PersistentScheduler`), dizionari `options`/`kwargs`.
4. **Persistenza**: `PersistentScheduler` sincronizza lo shelve; verifica che non tenga copie
   doppie delle voci in memoria (cache del modulo `dbm`/`shelve` con `writeback`).
5. **`-B`**: il beat embedded condivide il processo con il worker; ciò che beat carica pesa sul
   master. Valuta l'avvio dallo zygote come processo separato solo se riduce la PSS totale.

## Test
- `python/tests_rs/test_beat.py` (esistente) resta verde; aggiungi test per ogni cambio:
  attributi/`repr`/pickle/uguaglianza di `ScheduleEntry` e `crontab` identici al riferimento,
  file di schedule leggibile da ref e da rs nei due sensi, nessuna crescita di PSS oltre una soglia.
- `./compat wire-beat` 0 differenze.

## Gate
`tools/dev/gates` (tutti verdi; il comando esegue tutto in parallelo sulle corsie in ~10 minuti).
Per le verifiche intermedie usa `tools/dev/gates --only <nome>` o singoli test sotto
`tools/dev/with-brokers`.

## Regole di lavoro (quota del modello limitata)
Esegui i comandi lunghi **una volta e in modo bloccante** (un solo comando che aspetta la fine),
non in background con controlli periodici: ogni controllo costa una chiamata al modello.
Rapporto finale breve: tabella ref vs rs prima/dopo (3, 1000, 10 000 voci; Redis/AMQP; `-B`),
scomposizione della PSS, cosa hai cambiato e perché, varianti scartate con i numeri, gate con esito
reale, cosa resta aperto. Committa nel branch (`08f: …`).
