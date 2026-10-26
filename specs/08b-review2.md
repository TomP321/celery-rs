# 08b — revisione 2 (sessione 3, codex)

Leggi `AGENTS.md`, `specs/08b-memory.md` e `specs/08b-review.md` (decisione vincolante: oggetti
kombu/billiard veri, nessun `__module__`/`__qualname__`/`__name__` riscritto, nessuna classe
sostitutiva; `python/tests_rs/test_kombu_identity.py` non si modifica).

## Stato (commit `61699ed`, sessioni 1–2 di un altro agente)
- Gate verdi all'ultima esecuzione: `compat api` 0, `wire` 0/40, `wire-worker` solo 0/22,
  `wire-beat` 0/2, `cli` 0/190, `upstream` 0 regressioni, `test_kombu_identity.py` 3/3,
  `crates/celery-py/tests` 81. `tests_rs` completo: l'ultimo giro aveva 2 falliti
  (`test_master_does_not_import_broker_modules`), poi corretti ma non rieseguito tutto.
  `wire-worker --pool prefork` non rieseguito.
- mimalloc provato e scartato (solo 34→40 MB, prefork 71→77 MB); arene glibc: nessun guadagno.
  Riporta questi numeri nel messaggio finale, non ripetere la prova.

Misure attuali (`bench/results/rs-08b-final-*.json`, `client.json`, `beat-rs-beat-*.json`):

| cosa | ref | main (prima della 08b) | ora | obiettivo |
|---|---|---|---|---|
| client PSS dopo `get()` | 34,6 MB | 30,6 | 27,2 | ≤ 26 |
| master prefork c4 idle | 29 | 20,9 | 19,4 | ≤ 16 |
| `-P solo` Redis idle | 49,7 | 37,0 | 35,4 | ≤ 30 |
| prefork c4 `--task pure` picco | — | — | 73 | il più basso possibile |
| beat 3 voci | 51 / 43 | 45,7 | 32 | ≤ 38 ✅ |
| beat invii/s Redis | 406 | 497 | 430–456 | ≥ main |

## Da fare
1. **Client ≤ 26 MB** in modo onesto. Moduli ancora caricati dal client oltre a quelli imposti
   dagli oggetti kombu veri (`kombu.utils` → `amqp`/`billiard`/`importlib.metadata`,
   `kombu.entity`/`kombu.serialization` → `yaml`/`msgpack`): verifica con `bench/bench_client.py`
   e `python -X importtime` chi porta `click` (es. `CeleryCommandException` in
   `celery.exceptions`), `greenlet`, `dateutil`, `tzlocal`, `kombu.transport.*`, `redis`, e caricali
   solo dove servono. Ammesso: `__getattr__` di modulo (PEP 562) che crea/importa l'oggetto al primo
   accesso, purché l'oggetto risultante viva davvero al nome dichiarato (verificato dal test di
   identità e da `./compat api`). Aggiorna `python/tests_rs/test_import_diet.py` di conseguenza.
2. **Beat**: gli invii/s su Redis sono scesi del ~10% rispetto a `main` (497). Trova la causa con
   un profilo (`bench/bench_beat.py`, `cProfile` del ciclo di invio) e correggila, o dimostra con 3
   misure alternate main/branch che è rumore.
3. **Master del worker**: scomposizione della PSS del master (prefork e solo) per moduli Python
   (`tracemalloc`/`sys.modules`), heap Rust, librerie mappate (`/proc/<pid>/smaps`); riduci ciò che
   si può in modo onesto. Se ≤ 16 / ≤ 30 MB non è raggiungibile con gli oggetti veri, scrivi nel
   messaggio finale cosa pesa e perché.
4. **Gate completi** di `specs/08b-memory.md` più `test_kombu_identity.py`, ognuno con l'esito
   reale; tabella bench finale ref / main / branch (client, solo e prefork Redis+AMQP con
   `--task pure` e normale, beat, CLI).
5. Committa nel branch (`08b: …`) e chiudi con il rapporto: cosa hai fatto, file toccati, comandi
   ed esiti, cosa resta aperto.

Regole: test che usano Redis/RabbitMQ sempre sotto `tools/dev/with-brokers`; non modificare
`celery/` (riferimento), `tools/compat/`, `celery/t/`; dopo ogni modifica agli import `pyflakes`
sui file toccati (`uvx pyflakes …`).
