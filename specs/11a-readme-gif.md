# 11a — GIF per il README: cosa è in Rust e cosa no

Obiettivo: una GIF animata per il README di GitHub che mostri, a colpo d'occhio, quali componenti
di Celery sono stati portati in Rust, quali restano in Python per scelta (API pubblica) e cosa non
è coperto. Leggi `AGENTS.md`, `README.md`, `docs/DIFFERENZE.md`. **Non modificare codice di
celery-rs** (`python/celery/`, `crates/`): solo documentazione e lo script della GIF.

## Contenuto (classificazione già verificata dall'orchestratore; controllala sul codice e
segnala nel rapporto eventuali discrepanze, senza inventare)

**In Rust** (arancione):
- Protocollo dei messaggi: task v1/v2, header, JSON, compressione — `celery-proto`
- Trasporto Redis, trasporto RabbitMQ (AMQP 0-9-1), trasporto `memory://` — `celery-transport`
- Producer: `delay()`/`apply_async()` (serializzazione e invio)
- Backend dei risultati Redis e RPC — `celery-backend`
- Consumer del worker: QoS/prefetch, ack, eventi, heartbeat, timer ETA, rate limit, revoche —
  `celery-worker`
- Controllo remoto: pidbox, mingle, gossip (trasporto in Rust; i comandi restano Python)
- Pool prefork: supervisore, zygote, IPC con i figli
- Beat: calcolo delle scadenze (crontab) e invio — `celery-beat`
- Client di controllo della CLI (`inspect`, `control`)

**In Python per scelta** (blu: API pubblica identica a Celery, estendibile dall'utente):
- App, configurazione, registry e classe `Task`
- Canvas (chain, group, chord), `AsyncResult`
- Segnali, bootsteps, comandi di controllo (`Panel`)
- Esecuzione dei task nei figli (trace) e codice utente
- CLI (click), loader, fixup Django
- Persistenza della schedule di beat (shelve)

**Percorso di Celery originale, non convertito** (grigio):
- Altri broker (SQS, …) tramite kombu
- Altri backend dei risultati (database/SQLAlchemy, cache, S3, MongoDB, DynamoDB, …)

**Non supportato / non verificato** (rosso):
- Pool `eventlet` e `gevent`
- macOS e Windows (solo Linux)

**Numeri da mostrare alla fine** (riepilogo): 23.300 righe di Rust in 6 crate, guscio Python
52.600 righe; worker `-c 10`: 300 → 73 MB (−76%); worker `-c 10` in 8 GB: 27 → 96–105;
throughput `-c 10` Redis ×3; verificato con l'API pubblica (137 moduli), messaggi sul broker,
190 comandi CLI, 31.691 unit test e la suite `t/integration` di Celery.

## Forma
- Script riproducibile `tools/docs/make_readme_gif.py` (Pillow, eseguibile con
  `uvx --with pillow python tools/docs/make_readme_gif.py`), output
  `docs/assets/celery-rs-conversione.gif`.
- Larghezza 960 px, testo leggibile su GitHub (font DejaVu Sans, già sul sistema), sfondo chiaro,
  colori accessibili e una legenda. Animazione: i componenti compaiono/si colorano per gruppi
  (Rust, poi Python, poi non convertito, poi non supportato), poi il riepilogo; pausa lunga
  sull'ultimo fotogramma; ciclo infinito. Dimensione del file ≤ 1,5 MB, durata 10–20 s.
- Etichette in italiano, come il resto del README.
- Inserisci la GIF nel `README.md` subito dopo l'introduzione, con un testo alternativo.
- Guarda tu il risultato (estrai 3–4 fotogrammi in PNG e leggili) prima di dichiarare finito:
  niente testo tagliato o sovrapposto.

## Regole
Comandi bloccanti, niente messaggi di attesa. **Non fare push.** Committa nel branch (`11a: …`)
e chiudi con un rapporto breve (file creati, dimensione e durata della GIF, eventuali discrepanze
nella classificazione).
