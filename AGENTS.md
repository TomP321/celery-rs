# celery-rs — regole per gli agenti

Stai lavorando a celery-rs: Celery riscritto con core in Rust, API Python identica. Leggi `PLAN.md`.

## Struttura
- `celery/` — **sorgente di riferimento Celery 5.7.0rc1. SOLA LETTURA. Non modificarlo mai.**
  È la specifica: quando un comportamento non è chiaro, il riferimento ha ragione.
- `crates/` — workspace Rust (`celery-proto`, `celery-transport`, `celery-backend`, `celery-worker`,
  `celery-beat`, `celery-py` = modulo PyO3 `celery._rs`).
- `python/celery/` — guscio Python drop-in (stessa struttura di `celery/celery/`).
- `tools/compat/` — tool di conformità (`./compat api|upstream`). **Non modificarlo** e non
  modificare i test in `celery/t/` per farli passare.
- `specs/` — una spec per task. Implementa solo ciò che chiede la spec che ti è stata assegnata.

## Ambiente
- Venv: `.venv-ref` (Celery originale), `.venv-rs` (celery-rs). Python 3.13.
- Broker di test: Redis `127.0.0.1:6390`, RabbitMQ `127.0.0.1:5673` (guest/guest).
  Avvio: `docker compose -f infra/docker-compose.yml up -d`.
- Rust: `cargo build --release`, `cargo test`, `cargo clippy --all-targets -- -D warnings`.

## Regole di codice
- Memoria e prestazioni sono un requisito, non un extra: niente clone/alloc inutili, preferisci
  `bytes::Bytes` e prestiti (`&str`) a `String`/`Vec` copiati; niente `serde_json::Value` come
  struttura intermedia nei percorsi caldi; strutture dati limitate (nessuna crescita illimitata).
- Niente `unwrap()`/`expect()` nel codice di libreria (solo nei test). Errori con `thiserror`.
- `unsafe` solo se indispensabile, con commento `// SAFETY:` che spiega perché è corretto.
- Dipendenze: usa quelle in `[workspace.dependencies]`; per aggiungerne una nuova motivala nel
  messaggio finale.
- Commenti in italiano, brevi, solo dove spiegano il *perché*. Nomi e API come nel riferimento.
- Ogni modulo Rust ha test unitari; i comportamenti verificabili contro il riferimento hanno un
  test che confronta l'output con quello prodotto da `.venv-ref`.

## Prima di dichiarare finito
1. `cargo build --release` e `cargo clippy --all-targets -- -D warnings` puliti.
2. `cargo test` verde.
3. I comandi di verifica elencati nella spec passano.
4. Nel messaggio finale: cosa hai fatto, file toccati, comandi eseguiti con il loro esito reale,
   cosa resta aperto. Non dichiarare test passati che non hai eseguito.
