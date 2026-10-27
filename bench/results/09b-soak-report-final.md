| Implementazione/broker | Processo | PSS iniziale MiB | Δ max MiB | MiB/h | fd | thread | Esito |
|---|---|---:|---:|---:|---|---|---|
| rs/redis | worker:2820220:29144334 | 27.91 | 1.84 | 2.326 | [43, 44] | [13, 13] | FAIL |
| rs/redis | zygote:2820235:29144356 | 11.00 | 0.59 | 0.824 | [4, 4] | [1, 1] | OK |
| rs/redis | worker-child:2820262:29144368 | 18.45 | 6.07 | 7.866 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820258:29144368 | 18.19 | 6.91 | 7.759 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820254:29144368 | 18.37 | 6.31 | 8.209 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820251:29144368 | 20.14 | 4.46 | 4.482 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820248:29144367 | 20.42 | 4.19 | 4.591 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820246:29144367 | 20.36 | 4.85 | 5.645 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820244:29144367 | 18.24 | 4.19 | 4.071 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820242:29144367 | 18.09 | 6.90 | 6.290 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820240:29144367 | 18.18 | 6.78 | 7.567 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2820238:29144366 | 20.50 | 4.65 | 4.416 | [8, 10] | [2, 2] | FAIL |
| rs/redis | beat:2820221:29144335 | 25.98 | 0.09 | 0.017 | [11, 11] | [2, 2] | OK |

soak-20261027-051343-rs-redis-efdd9bb085.json: complete=True, passed=False, error=None

| rs/amqp | worker:2820223:29144336 | 21.49 | 1.86 | 1.727 | [35, 38] | [15, 16] | FAIL |
| rs/amqp | zygote:2820237:29144358 | 11.37 | 0.30 | 0.304 | [4, 4] | [1, 1] | OK |
| rs/amqp | worker-child:2820278:29144370 | 20.59 | 3.84 | 3.565 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820276:29144370 | 18.46 | 7.77 | 9.995 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820274:29144370 | 19.15 | 5.28 | 4.374 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820272:29144369 | 17.34 | 7.21 | 7.417 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820270:29144369 | 18.78 | 5.74 | 5.927 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820268:29144369 | 20.81 | 4.90 | 6.467 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820265:29144369 | 18.59 | 7.80 | 9.890 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820260:29144368 | 19.01 | 5.47 | 6.525 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820256:29144368 | 20.64 | 5.72 | 5.802 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:2820250:29144368 | 19.12 | 12.89 | 6.499 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | beat:2820224:29144336 | 27.13 | 2.50 | 3.837 | [10, 13] | [3, 4] | FAIL |

soak-20261027-051343-rs-amqp-5d3de8f78f.json: complete=True, passed=False, error=None

