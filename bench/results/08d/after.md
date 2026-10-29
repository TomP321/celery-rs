PSS in MiB; average and min–max range of at least two runs.

| Broker/task | impl (n) | idle | peak | after | task/s | p50/p99 ms |
|---|---|---:|---:|---:|---:|---:|
| amqp/bench | ref (2) | 307.0 (306.9–307.2) | 319.6 (319.5–319.8) | 319.6 (319.5–319.8) | 2921 (2886–2956) | 3.26/3.99 |
| amqp/bench | rs (2) | 83.9 (83.6–84.2) | 233.1 (232.5–233.7) | 233.1 (232.5–233.7) | 3642 (3618–3667) | 2.63/4.35 |
| amqp/pure | ref (2) | 306.7 (306.7–306.7) | 310.3 (310.3–310.4) | 310.3 (310.3–310.4) | 2970 (2953–2988) | 3.33/4.39 |
| amqp/pure | rs (2) | 83.5 (83.4–83.6) | 103.1 (102.9–103.4) | 103.1 (102.9–103.4) | 3706 (3662–3750) | 2.51/4.34 |
| redis/bench | ref (2) | 308.7 (308.7–308.8) | 313.2 (312.7–313.8) | 313.2 (312.7–313.8) | 897 (895–898) | 3.11/4.08 |
| redis/bench | rs (2) | 83.9 (83.8–84.0) | 231.4 (231.2–231.6) | 231.4 (231.2–231.6) | 2674 (2637–2712) | 1.91/4.28 |
| redis/pure | ref (2) | 308.6 (308.5–308.7) | 310.6 (310.6–310.7) | 310.6 (310.6–310.7) | 916 (909–923) | 3.13/4.34 |
| redis/pure | rs (2) | 84.2 (84.1–84.2) | 101.4 (101.0–101.9) | 101.4 (101.0–101.9) | 2553 (2517–2588) | 1.91/3.97 |

| Broker/task | idle savings MiB (%) | peak MiB (%) | after MiB (%) |
|---|---:|---:|---:|
| redis/bench | 224.8 (72.8%) | 81.9 (26.1%) | 81.9 (26.1%) |
| redis/pure | 224.4 (72.7%) | 209.2 (67.3%) | 209.2 (67.3%) |
| amqp/bench | 223.2 (72.7%) | 86.5 (27.1%) | 86.5 (27.1%) |
| amqp/pure | 223.2 (72.8%) | 207.2 (66.8%) | 207.2 (66.8%) |

PSS and Private_Dirty per process, every execution (idle → after, MiB).

| measure | role/pid | PSS | Private_Dirty |
|---|---|---:|---:|
| amqp/bench/ref/1 | master/1924929 | 26.0 → 27.9 | 11.2 → 14.5 |
| amqp/bench/ref/1 | child/1924954 | 20.7 → 22.4 | 10.5 → 13.1 |
| amqp/bench/ref/1 | child/1924953 | 28.9 → 29.8 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924952 | 28.9 → 30.1 | 27.0 → 28.2 |
| amqp/bench/ref/1 | child/1924951 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924949 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924948 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924946 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924945 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924937 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/1 | child/1924936 | 28.9 → 29.9 | 27.1 → 28.0 |
| amqp/bench/ref/2 | master/1928501 | 26.0 → 27.9 | 11.3 → 14.5 |
| amqp/bench/ref/2 | child/1928517 | 20.7 → 22.4 | 10.5 → 13.1 |
| amqp/bench/ref/2 | child/1928516 | 29.0 → 29.9 | 27.1 → 28.0 |
| amqp/bench/ref/2 | child/1928515 | 29.0 → 30.2 | 27.1 → 28.3 |
| amqp/bench/ref/2 | child/1928514 | 29.0 → 29.9 | 27.1 → 28.1 |
| amqp/bench/ref/2 | child/1928513 | 29.0 → 29.9 | 27.1 → 28.1 |
| amqp/bench/ref/2 | child/1928512 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1928511 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1928510 | 28.9 → 29.9 | 27.1 → 28.0 |
| amqp/bench/ref/2 | child/1928509 | 29.0 → 29.9 | 27.1 → 28.1 |
| amqp/bench/ref/2 | child/1928508 | 29.0 → 29.9 | 27.1 → 28.1 |
| amqp/bench/rs/1 | master/1925073 | 17.9 → 18.6 | 9.0 → 12.1 |
| amqp/bench/rs/1 | zygote/1925079 | 5.9 → 10.6 | 3.4 → 7.7 |
| amqp/bench/rs/1 | child/1925098 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925096 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925094 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925092 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925090 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925088 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925086 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925084 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925082 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/1 | child/1925080 | 6.0 → 20.3 | 3.9 → 18.3 |
| amqp/bench/rs/2 | master/1928323 | 17.9 → 18.5 | 9.1 → 12.0 |
| amqp/bench/rs/2 | zygote/1928329 | 6.0 → 10.7 | 3.4 → 7.8 |
| amqp/bench/rs/2 | child/1928348 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928346 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928344 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928342 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928340 | 6.0 → 20.4 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928338 | 6.0 → 20.4 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928336 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928334 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928332 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/bench/rs/2 | child/1928330 | 6.0 → 20.5 | 3.9 → 18.4 |
| amqp/pure/ref/1 | master/1925272 | 25.9 → 26.9 | 11.3 → 12.8 |
| amqp/pure/ref/1 | child/1925287 | 20.7 → 21.1 | 10.6 → 11.1 |
| amqp/pure/ref/1 | child/1925283 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/1 | child/1925282 | 28.9 → 30.0 | 27.0 → 28.0 |
| amqp/pure/ref/1 | child/1925281 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/1 | child/1925280 | 28.9 → 29.1 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1925279 | 28.9 → 29.1 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1925278 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1925277 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1925276 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1925275 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | master/1928941 | 25.9 → 26.8 | 11.3 → 12.7 |
| amqp/pure/ref/2 | child/1928986 | 20.7 → 21.0 | 10.6 → 11.0 |
| amqp/pure/ref/2 | child/1928985 | 28.9 → 29.1 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1928984 | 28.9 → 30.0 | 27.0 → 28.1 |
| amqp/pure/ref/2 | child/1928983 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | child/1928982 | 28.9 → 29.1 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1928981 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1928977 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1928954 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1928949 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1928944 | 28.9 → 29.1 | 27.1 → 27.2 |
| amqp/pure/rs/1 | master/1925869 | 17.8 → 18.6 | 9.0 → 12.0 |
| amqp/pure/rs/1 | zygote/1925875 | 5.9 → 7.2 | 3.4 → 4.6 |
| amqp/pure/rs/1 | child/1925894 | 6.0 → 7.7 | 3.8 → 5.5 |
| amqp/pure/rs/1 | child/1925892 | 6.0 → 7.8 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925890 | 6.0 → 7.8 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925888 | 6.0 → 7.8 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925886 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925884 | 6.0 → 7.8 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925882 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925880 | 6.0 → 7.8 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925878 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/1 | child/1925876 | 6.0 → 7.8 | 3.9 → 5.5 |
| amqp/pure/rs/2 | master/1928631 | 17.8 → 18.7 | 9.0 → 12.0 |
| amqp/pure/rs/2 | zygote/1928637 | 5.9 → 7.1 | 3.4 → 4.6 |
| amqp/pure/rs/2 | child/1928656 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928654 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928652 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928650 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928648 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928646 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928644 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928642 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928640 | 6.0 → 7.7 | 3.9 → 5.5 |
| amqp/pure/rs/2 | child/1928638 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/bench/ref/1 | master/1923186 | 27.3 → 27.6 | 13.6 → 14.2 |
| redis/bench/ref/1 | child/1923232 | 21.5 → 21.6 | 12.1 → 12.4 |
| redis/bench/ref/1 | child/1923231 | 28.9 → 29.8 | 27.0 → 27.9 |
| redis/bench/ref/1 | child/1923228 | 28.9 → 29.8 | 27.0 → 27.9 |
| redis/bench/ref/1 | child/1923227 | 28.9 → 30.2 | 27.0 → 28.3 |
| redis/bench/ref/1 | child/1923226 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/1 | child/1923225 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/1 | child/1923223 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/1 | child/1923212 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/1 | child/1923194 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/1 | child/1923193 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/bench/ref/2 | master/1926843 | 27.4 → 27.7 | 13.6 → 14.3 |
| redis/bench/ref/2 | child/1926855 | 21.5 → 21.6 | 12.1 → 12.4 |
| redis/bench/ref/2 | child/1926854 | 28.9 → 29.8 | 27.0 → 27.9 |
| redis/bench/ref/2 | child/1926853 | 28.9 → 29.8 | 27.0 → 27.9 |
| redis/bench/ref/2 | child/1926852 | 28.9 → 30.2 | 27.0 → 28.3 |
| redis/bench/ref/2 | child/1926851 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1926850 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1926849 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1926848 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1926847 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1926846 | 28.9 → 30.0 | 27.0 → 28.1 |
| redis/bench/rs/1 | master/1923607 | 17.9 → 17.0 | 8.9 → 10.4 |
| redis/bench/rs/1 | zygote/1923611 | 5.9 → 10.6 | 3.4 → 7.7 |
| redis/bench/rs/1 | child/1923630 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923628 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923626 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923624 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923622 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923620 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923618 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923616 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923614 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/1 | child/1923612 | 6.0 → 20.4 | 3.9 → 18.4 |
| redis/bench/rs/2 | master/1926296 | 17.9 → 16.9 | 8.9 → 10.4 |
| redis/bench/rs/2 | zygote/1926300 | 5.9 → 10.6 | 3.4 → 7.7 |
| redis/bench/rs/2 | child/1926319 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926317 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926315 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926313 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926311 | 6.0 → 20.3 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926309 | 6.0 → 20.3 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926307 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926305 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926303 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/bench/rs/2 | child/1926301 | 6.0 → 20.4 | 3.9 → 18.3 |
| redis/pure/ref/1 | master/1924146 | 27.3 → 27.7 | 13.6 → 14.3 |
| redis/pure/ref/1 | child/1924167 | 21.5 → 21.6 | 12.1 → 12.4 |
| redis/pure/ref/1 | child/1924166 | 28.9 → 29.1 | 27.0 → 27.1 |
| redis/pure/ref/1 | child/1924164 | 28.9 → 29.1 | 27.0 → 27.1 |
| redis/pure/ref/1 | child/1924163 | 28.9 → 30.0 | 27.0 → 28.0 |
| redis/pure/ref/1 | child/1924162 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1924161 | 28.8 → 28.9 | 26.9 → 26.9 |
| redis/pure/ref/1 | child/1924160 | 28.8 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1924159 | 28.8 → 28.9 | 26.9 → 26.9 |
| redis/pure/ref/1 | child/1924158 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1924157 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | master/1928053 | 27.3 → 27.7 | 13.5 → 14.3 |
| redis/pure/ref/2 | child/1928065 | 21.5 → 21.6 | 12.1 → 12.4 |
| redis/pure/ref/2 | child/1928064 | 28.9 → 29.1 | 27.0 → 27.1 |
| redis/pure/ref/2 | child/1928063 | 28.9 → 29.1 | 27.0 → 27.1 |
| redis/pure/ref/2 | child/1928062 | 28.9 → 30.0 | 27.0 → 28.0 |
| redis/pure/ref/2 | child/1928061 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1928060 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1928059 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1928058 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1928057 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1928056 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/rs/1 | master/1924750 | 17.9 → 17.2 | 8.9 → 10.4 |
| redis/pure/rs/1 | zygote/1924754 | 5.9 → 7.1 | 3.4 → 4.5 |
| redis/pure/rs/1 | child/1924773 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924771 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924769 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924767 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924765 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924763 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924761 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924759 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924757 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/1 | child/1924755 | 6.0 → 7.7 | 3.9 → 5.4 |
| redis/pure/rs/2 | master/1927869 | 17.9 → 17.2 | 8.9 → 10.4 |
| redis/pure/rs/2 | zygote/1927877 | 5.9 → 7.2 | 3.4 → 4.6 |
| redis/pure/rs/2 | child/1927896 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927894 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927892 | 6.0 → 7.8 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927890 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927888 | 6.0 → 7.8 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927886 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927884 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927882 | 6.0 → 7.8 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927880 | 6.0 → 7.7 | 3.9 → 5.5 |
| redis/pure/rs/2 | child/1927878 | 6.0 → 7.7 | 3.9 → 5.5 |
