# 10d — descrittori dello zygote

Il master espone il registro dei descrittori posseduti da Rust. Waker e socket
IPC usano registrazioni RAII rimosse alla distruzione; le entry non crescono
oltre le risorse vive. Un registro nuovo per PID evita mutex ereditati nel
figlio. Per tokio una sonda privata identifica esattamente gli epoll che la
contengono, comprese le copie del driver: nessuna attribuzione basata soltanto
sul momento di apertura. Socket broker/backend ed eventfd si ricavano dai loro
`fdinfo`. La feature tokio `net` è ora esplicita; nessuna nuova dipendenza.

La foto prima del fork e la lettura del driver nel figlio coprono i cambiamenti
delle connessioni durante il fork. Le identità device/inode proteggono i numeri
riciclati. Lo zygote chiude le copie locali prima degli import, senza modificare
le registrazioni epoll condivise col master. Gli oggetti Rust ereditati restano
protetti dai distruttori `ForkSafe`. File, socket, epoll e log Python restano
aperti. Nessuna scansione aggiuntiva nei figli.

Verifiche eseguite:

- `cargo build --release`: passato.
- `cargo clippy --all-targets -- -D warnings`: passato.
- `tools/dev/with-brokers cargo test`: 244 passati, 4 ignorati, 0 falliti.
- `tools/dev/build-package` e `tools/dev/build-native`: passati.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_zygote_user_fds.py python/tests_rs/test_zygote_logging_fds.py python/tests_rs/test_ram_budget.py -q`: 11 passati.
- `tools/dev/with-brokers .venv-rs/bin/python -m pytest python/tests_rs/test_worker_pool.py -q`: 27 passati; 52 avvisi Python sul fork multithread, anche nel pool billiard di riferimento.

Il differenziale avvia worker ref e rs con `-P prefork -c 2` e
`max_tasks_per_child=1`: 12 PID distinti per implementazione usano lo stesso
file e socket TCP loopback aperti all'import, con risultati identici. Nello
zygote l'insieme dei fd resta identico prima e dopo i ricambi. Gli altri test
verificano la chiusura dei soli fd Rust con Redis e AMQP, il funzionamento
successivo del master, i logger file/queue e un fd Python che il padre ricicla
in un socket registrato nell'epoll condiviso. Il test 10c conserva la verifica
della singola pulizia nello zygote e ora richiede che il fd Python sopravviva.

RAM, PSS totale master + zygote + 10 figli (MiB):

| Misura | Prima | Dopo |
|---|---:|---:|
| Riposo | 87,41 | 83,23 |
| Dopo carico / picco | 108,28 | 102,07 |

Nessun aumento osservato. Comando eseguito per entrambe le misure:

```
tools/dev/with-brokers .venv-rs/bin/python bench/bench.py --venv .venv-rs --pool prefork --concurrency 10 --task pure --prefill --n 10000 --latency-n 20 --label 10d-before
tools/dev/with-brokers .venv-rs/bin/python bench/bench.py --venv .venv-rs --pool prefork --concurrency 10 --task pure --prefill --n 10000 --latency-n 20 --label 10d-after
```

Dati aggregati e per processo in `10d-zygote-fds.json` (le mappature complete
sono escluse). Gates completi, soak e `compat integration` non eseguiti, come
richiesto dalla spec; restano all'orchestratore.

File modificati:

- `crates/celery-py/Cargo.toml`, `src/lib.rs`, `src/transport.rs`.
- `crates/celery-worker/src/fds.rs`, `lib.rs`, `wake.rs`, `pool/spawn.rs`, `pool/supervisor.rs`.
- `python/celery/concurrency/_rs_zygote.py`.
- `python/tests_rs/test_zygote_user_fds.py`, `test_zygote_logging_fds.py`, `test_ram_budget.py`.
- `bench/results/10d-zygote-fds.json` e questo rapporto.
