| Implementazione/broker | Processo | PSS iniziale MiB | Δ max MiB | MiB/h | fd | thread | Esito |
|---|---|---:|---:|---:|---|---|---|
| rs/redis | worker:2390547:28736063 | 29.09 | 0.19 | 1.163 | [43, 44] | [13, 13] | FAIL |
| rs/redis | zygote:2390623:28736082 | 9.86 | 2.69 | 23.381 | [4, 4] | [1, 1] | FAIL |
| rs/redis | worker-child:2390642:28736086 | 18.36 | 19.87 | 207.985 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390640:28736086 | 21.44 | 15.92 | 137.950 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390638:28736086 | 18.52 | 19.46 | 197.901 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390636:28736086 | 18.46 | 19.74 | 177.970 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390634:28736086 | 18.54 | 19.83 | 180.848 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390632:28736085 | 18.50 | 19.56 | 177.478 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390630:28736085 | 18.38 | 20.24 | 226.730 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390628:28736085 | 26.76 | 3.13 | 35.586 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390626:28736085 | 26.87 | 11.35 | 104.060 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2390624:28736085 | 27.12 | 10.74 | 122.396 | [10, 10] | [2, 2] | FAIL |
| rs/redis | beat:2390548:28736063 | 27.67 | 0.00 | -2.408 | [11, 11] | [2, 2] | OK |

soak-20261027-040541-rs-redis-390ea448d0.json: complete=True, passed=False, error=None

| rs/redis | worker:2702239:29032788 | 29.05 | 0.81 | 6.520 | [43, 44] | [13, 13] | FAIL |
| rs/redis | zygote:2702383:29032809 | 11.46 | 0.37 | 3.193 | [4, 4] | [1, 1] | FAIL |
| rs/redis | worker-child:2702404:29032821 | 18.36 | 3.81 | 48.097 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702402:29032821 | 19.11 | 3.06 | 36.607 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702400:29032821 | 20.75 | 1.13 | 11.401 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702398:29032820 | 18.48 | 1.82 | 19.565 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702396:29032820 | 19.65 | 0.42 | 8.640 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702394:29032820 | 21.66 | 0.65 | 5.009 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702392:29032820 | 18.61 | 1.45 | 18.500 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702390:29032820 | 18.43 | 1.41 | 14.144 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702388:29032819 | 19.42 | 2.45 | 17.303 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:2702386:29032819 | 20.78 | 1.24 | 10.822 | [10, 10] | [2, 2] | FAIL |
| rs/redis | beat:2702240:29032788 | 27.53 | 0.08 | 0.717 | [11, 11] | [2, 2] | OK |

soak-20261027-045508-rs-redis-90758db30b.json: complete=True, passed=False, error=None

