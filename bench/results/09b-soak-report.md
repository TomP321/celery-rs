| Implementazione/broker | Processo | PSS iniziale MiB | Δ max MiB | MiB/h | fd | thread | Esito |
|---|---|---:|---:|---:|---|---|---|
| ref/amqp | worker:1267864:27663779 | 27.35 | 1.69 | 2.194 | [72, 72] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268108:27663919 | 29.47 | 4.40 | 3.912 | [57, 57] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268106:27663915 | 36.66 | 4.73 | 4.128 | [56, 56] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268104:27663911 | 38.81 | 7.22 | 7.851 | [55, 55] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268102:27663907 | 32.01 | 0.30 | 0.420 | [53, 53] | [1, 1] | OK |
| ref/amqp | worker-child:1268100:27663903 | 32.10 | 0.87 | 1.283 | [53, 53] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268098:27663899 | 34.31 | 1.79 | 2.462 | [52, 52] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268096:27663895 | 34.05 | 6.10 | 4.852 | [51, 51] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268094:27663890 | 36.74 | 4.13 | 4.254 | [50, 50] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268089:27663886 | 36.85 | 3.88 | 3.385 | [49, 49] | [1, 1] | FAIL |
| ref/amqp | worker-child:1268080:27663881 | 42.36 | 6.29 | 6.701 | [48, 48] | [1, 1] | FAIL |
| ref/amqp | beat:1267865:27663779 | 47.90 | 2.95 | 4.756 | [7, 7] | [1, 1] | FAIL |

soak-20261027-010658-ref-amqp-bbf29d4905.json: complete=True, passed=False, error=None

| ref/redis | worker:1267867:27663779 | 27.03 | 1.30 | 1.724 | [84, 86] | [1, 1] | FAIL |
| ref/redis | worker-child:1268109:27663922 | 23.01 | 0.18 | 0.248 | [57, 57] | [1, 1] | OK |
| ref/redis | worker-child:1268107:27663918 | 31.14 | 0.08 | 0.055 | [57, 57] | [1, 1] | OK |
| ref/redis | worker-child:1268105:27663914 | 39.67 | 11.35 | 12.482 | [60, 60] | [1, 1] | FAIL |
| ref/redis | worker-child:1268103:27663909 | 39.20 | 10.92 | 12.052 | [59, 59] | [1, 1] | FAIL |
| ref/redis | worker-child:1268101:27663905 | 30.25 | 0.02 | 0.024 | [53, 53] | [1, 1] | OK |
| ref/redis | worker-child:1268099:27663901 | 30.25 | 0.02 | 0.024 | [52, 52] | [1, 1] | OK |
| ref/redis | worker-child:1268097:27663896 | 30.25 | 0.02 | 0.024 | [51, 51] | [1, 1] | OK |
| ref/redis | worker-child:1268095:27663892 | 30.25 | 0.02 | 0.023 | [50, 50] | [1, 1] | OK |
| ref/redis | worker-child:1268093:27663888 | 31.08 | 0.02 | 0.024 | [50, 50] | [1, 1] | OK |
| ref/redis | worker-child:1268084:27663883 | 39.87 | 2.89 | 3.535 | [53, 53] | [1, 1] | FAIL |
| ref/redis | beat:1267868:27663779 | 47.93 | 0.15 | 0.190 | [10, 10] | [1, 1] | OK |

soak-20261027-010658-ref-redis-174119c44c.json: complete=True, passed=False, error=None


soak-20261027-010658-rs-amqp-da88a10c6f.json: complete=False, passed=False, error=RuntimeError('worker exited: 1')


soak-20261027-010658-rs-redis-aec22fd5c8.json: complete=False, passed=False, error=RuntimeError('worker exited: 1')

| rs/amqp | worker:1655588:28037258 | 23.31 | 1.57 | 1.243 | [35, 38] | [15, 16] | FAIL |
| rs/amqp | zygote:1655603:28037302 | 14.20 | 0.51 | 0.527 | [4, 4] | [1, 1] | OK |
| rs/amqp | worker-child:1655643:28037311 | 30.64 | 5.97 | 6.488 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655638:28037310 | 22.85 | 4.54 | 4.825 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655633:28037310 | 21.57 | 3.44 | 4.663 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655628:28037310 | 21.27 | 6.82 | 8.300 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655624:28037309 | 20.88 | 4.28 | 5.268 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655620:28037309 | 31.44 | 2.35 | 2.376 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655616:28037309 | 29.65 | 4.14 | 4.260 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655612:28037309 | 20.14 | 12.04 | 4.623 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655608:28037309 | 32.19 | 3.73 | 4.052 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | worker-child:1655604:28037308 | 21.53 | 3.54 | 4.384 | [9, 12] | [3, 4] | FAIL |
| rs/amqp | beat:1655589:28037258 | 28.87 | 0.97 | 1.579 | [10, 13] | [3, 4] | FAIL |

soak-20261027-020913-rs-amqp-b4b5cd9ccd.json: complete=True, passed=False, error=None

| rs/redis | worker:1655585:28037257 | 45.53 | 0.28 | 0.303 | [43, 44] | [13, 13] | FAIL |
| rs/redis | zygote:1655602:28037302 | 14.22 | 5.08 | 7.950 | [4, 4] | [1, 1] | FAIL |
| rs/redis | worker-child:1655640:28037310 | 21.24 | 21.90 | 33.466 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655636:28037310 | 30.10 | 8.06 | 9.730 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655632:28037310 | 21.25 | 21.07 | 32.459 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655629:28037310 | 30.00 | 4.43 | 4.889 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655626:28037310 | 21.31 | 22.82 | 32.794 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655622:28037309 | 30.09 | 5.09 | 6.018 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655618:28037309 | 30.12 | 4.26 | 5.016 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655614:28037309 | 29.80 | 4.77 | 5.426 | [8, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655610:28037309 | 30.15 | 4.11 | 4.599 | [10, 10] | [2, 2] | FAIL |
| rs/redis | worker-child:1655605:28037308 | 21.14 | 22.96 | 35.614 | [8, 10] | [2, 2] | FAIL |
| rs/redis | beat:1655586:28037257 | 27.82 | 0.00 | -0.223 | [11, 11] | [2, 2] | OK |

soak-20261027-020913-rs-redis-31db50fcee.json: complete=True, passed=False, error=None

