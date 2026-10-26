# Misure 08c

10.000 task per prova; PSS in MiB; capacità con coda piena, dalla ripresa del worker.

## Capacità

| Broker | c | Task | ref task/s | rs prima | rs dopo |
|---|---:|---|---:|---:|---:|
| redis | 4 | pure | 928 | 2645 | 2860 |
| redis | 4 | bench | 900 | 2749 | 2718 |
| redis | 4 | bench-toplevel | 898 | 2775 | 2846 |
| redis | 10 | pure | 895 | 2810 | 2819 |
| redis | 10 | bench | 880 | 2748 | 2754 |
| redis | 10 | bench-toplevel | 868 | 2828 | 2895 |
| amqp | 4 | pure | 2977 | 5911 | 5718 |
| amqp | 4 | bench | 2893 | 5048 | 4696 |
| amqp | 4 | bench-toplevel | 2947 | 4991 | 4757 |
| amqp | 10 | pure | 3349 | 6449 | 6273 |
| amqp | 10 | bench | 3126 | 5126 | 5161 |
| amqp | 10 | bench-toplevel | 3157 | 5369 | 5360 |

## PSS c10

| Broker | Task | ref idle/picco | rs prima idle/picco | rs dopo idle/picco | Risparmio picco |
|---|---|---:|---:|---:|---:|
| redis | pure | 307.8/310.2 | 84.3/101.9 | 83.9/102.4 | 207.8 (67.0%) |
| redis | bench | 307.2/312.2 | 83.5/230.7 | 83.6/231.4 | 80.8 (25.9%) |
| redis | bench-toplevel | 307.6/315.1 | 90.8/153.1 | 91.1/154.2 | 160.9 (51.1%) |
| amqp | pure | 306.1/309.7 | 83.9/103.4 | 83.5/103.4 | 206.3 (66.6%) |
| amqp | bench | 306.5/319.7 | 83.4/233.6 | 83.8/235.2 | 84.5 (26.4%) |
| amqp | bench-toplevel | 305.7/318.5 | 90.9/155.2 | 91.2/155.0 | 163.5 (51.3%) |

## Latenze AMQP prefork c10

| Worker | Misura | ref p50/p99 ms | rs prima | rs dopo |
|---|---|---:|---:|---:|
| ref | rpc_ms | 3.520/4.588 | 3.326/4.603 | 2.803/3.798 |
| ref | ping_ms | 8.928/10.777 | 9.858/12.865 | 8.682/10.187 |
| rs | rpc_ms | 3.348/4.638 | 3.059/4.207 | 3.044/3.913 |
| rs | ping_ms | 8.924/24.327 | 9.916/12.380 | 8.487/11.106 |

## Ping in-process verso worker AMQP solo

| Worker | ref p50/p99 ms | rs prima | rs dopo |
|---|---:|---:|---:|
| ref | 8.982/10.726 | 10.230/12.827 | 8.381/10.019 |
| rs | 9.321/11.331 | 9.820/11.517 | 8.168/11.094 |

## Beat PSS (3 voci)

| Broker | ref | rs prima | rs dopo |
|---|---:|---:|---:|
| redis | 47.32 | 28.34 | 28.28 |
| amqp | 39.41 | 28.26 | 28.29 |

## Scomposizione RPC dopo (client rs)

| Worker | delay() | Andata fino al figlio | Task | Risposta |
|---|---:|---:|---:|---:|
| ref | 0.7674 | 1.6663 | 0.0002 | 0.8382 |
| rs | 0.9221 | 2.2424 | 0.0003 | 1.0709 |

Tempi in ms, mediane di sonde distinte: delay() è incluso nell’andata.
Broker, master e IPC non sono separabili con questa sonda.

## PSS isolata prima/dopo

| Processo | Prima MiB | Dopo MiB | Delta MiB |
|---|---:|---:|---:|
| client rs, worker ref | 30.424 | 30.436 | +0.012 |
| worker ref rpc c10 | 256.633 | 256.898 | +0.266 |
| client rs, worker rs | 26.736 | 26.876 | +0.140 |
| worker rs rpc c10 | 110.599 | 110.706 | +0.107 |

## Metodo e limiti

La prima matrice Redis c10 aveva altre sonde attive: per il prima si usa
il secondo giro isolato. Il dopo c10 riporta la mediana dei due giri.
Le prove di latenza/PSS isolate ripristinano temporaneamente la libreria nativa
e i due moduli Python originali, poi misurano la versione finale senza altre sonde.
Profilazione separata dalle latenze; vedi README-08c.md per riprodurre le sonde.

## Gate (ultimo esito per gruppo)

| Gate | Esito | Sintesi |
|---|---|---|
| build | OK |     Finished `release` profile [optimized] target(s) in 53.76s |
| clippy | OK |    Compiling thiserror v2.0.21 ;    Compiling thiserror-impl v2.0.21 ;     Finished `dev` profile [unoptimized + debuginfo] target(s) in 24.97s |
| build-package | OK |     Finished `release` profile [optimized] target(s) in 54.30s |
| build-native | OK |     Finished `release` profile [optimized] target(s) in 0.12s |
| upstream | FAIL (1) | upstream unit [venv-rs]: 31691 test, 31603 ok al primo passo, 3 instabili, 1 falliti (0 noti anche nel riferimento) → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/upstream-unit-venv-rs.json |
| tests_rs-1 | OK | 65 passed; 0 failed |
| tests_rs-3 | OK | 65 passed; 0 failed |
| tests_rs-4 | OK | 27 passed; 0 failed |
| wire-worker-prefork | OK | wire-worker: 0 scenari diversi su 22 → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/wire-worker-report.md |
| tests_rs-2 | OK | 64 passed; 0 failed |
| wire-worker-solo | OK | wire-worker: 0 scenari diversi su 22 → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/wire-worker-report.md |
| cargo-test | OK | 235 passed; 0 failed |
| native-tests | OK | 81 passed; 0 failed |
| api | OK | api: 0 differenze su 137 moduli + settings/signals/cli → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/api-report.md |
| wire | OK | wire: 0 scenari diversi su 40 → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/wire-report.md |
| wire-beat | OK | wire-beat: 0 broker diversi su 2 → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/wire-beat-report.md |
| cli | OK | cli: 0 comandi diversi su 190 (tempo totale ref 139.7s, rs 108.1s) → /home/ubuntu/HDD/celery-rs-wt/08c-latency-capacity/tools/compat/out/cli-report.md |

Il giro completo iniziale ha fallito anche il nuovo test della reply queue
(setup esclusivo/durevole e sonda del mailbox copiato da correggere) e canvas
AMQP prefork. Dopo la correzione del test: 221 test Python verdi; canvas
mirato verde e ripetizione completa prefork 0 differenze su 22, senza cambi
di produzione. Upstream rimane rosso solo per il fallimento noto
`test_pytest_celery_marker_registration`, non indagato.
I JSON gate-* conservano tutti gli esiti iniziali e delle ripetizioni.
