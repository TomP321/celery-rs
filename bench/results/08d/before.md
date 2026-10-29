PSS in MiB; average and min–max range of at least two runs.

| Broker/task | impl (n) | idle | peak | after | task/s | p50/p99 ms |
|---|---|---:|---:|---:|---:|---:|
| amqp/bench | ref (2) | 307.1 (306.8–307.3) | 320.0 (319.6–320.5) | 320.0 (319.6–320.5) | 2888 (2815–2962) | 3.40/4.44 |
| amqp/bench | rs (2) | 87.4 (87.0–87.8) | 233.1 (232.2–234.1) | 233.1 (232.2–234.1) | 3567 (3443–3692) | 2.80/7.08 |
| amqp/pure | ref (2) | 306.8 (306.5–307.1) | 310.4 (310.1–310.8) | 310.4 (310.1–310.8) | 2949 (2947–2951) | 3.37/4.08 |
| amqp/pure | rs (2) | 87.1 (87.0–87.1) | 105.6 (105.4–105.8) | 105.6 (105.4–105.8) | 4050 (3979–4122) | 2.64/4.28 |
| redis/bench | ref (2) | 321.8 (309.3–334.3) | 325.8 (314.4–337.2) | 325.8 (314.4–337.2) | 893 (888–899) | 3.00/3.85 |
| redis/bench | rs (2) | 95.8 (87.4–104.2) | 246.7 (231.7–261.7) | 246.7 (231.7–261.7) | 2596 (2583–2610) | 1.96/4.49 |
| redis/pure | ref (2) | 309.2 (309.0–309.3) | 311.2 (311.0–311.4) | 311.2 (311.0–311.4) | 918 (915–922) | 2.94/3.94 |
| redis/pure | rs (2) | 87.5 (87.5–87.6) | 104.2 (104.0–104.4) | 104.2 (104.0–104.4) | 2535 (2448–2621) | 1.96/3.46 |

| Broker/task | idle savings MiB (%) | peak MiB (%) | after MiB (%) |
|---|---:|---:|---:|
| redis/bench | 226.0 (70.2%) | 79.1 (24.3%) | 79.1 (24.3%) |
| redis/pure | 221.6 (71.7%) | 207.0 (66.5%) | 207.0 (66.5%) |
| amqp/bench | 219.6 (71.5%) | 86.9 (27.2%) | 86.9 (27.2%) |
| amqp/pure | 219.7 (71.6%) | 204.8 (66.0%) | 204.8 (66.0%) |

PSS and Private_Dirty per process, every execution (idle → after, MiB).

| measure | role/pid | PSS | Private_Dirty |
|---|---|---:|---:|
| amqp/bench/ref/1 | master/1907975 | 26.1 → 28.0 | 11.3 → 14.4 |
| amqp/bench/ref/1 | child/1908005 | 20.7 → 22.5 | 10.6 → 13.2 |
| amqp/bench/ref/1 | child/1908004 | 29.0 → 30.0 | 27.1 → 28.1 |
| amqp/bench/ref/1 | child/1908003 | 29.0 → 30.2 | 27.1 → 28.3 |
| amqp/bench/ref/1 | child/1908002 | 29.0 → 30.0 | 27.1 → 28.1 |
| amqp/bench/ref/1 | child/1908001 | 28.9 → 30.0 | 27.0 → 28.1 |
| amqp/bench/ref/1 | child/1908000 | 28.9 → 29.9 | 27.0 → 28.1 |
| amqp/bench/ref/1 | child/1907995 | 28.9 → 29.9 | 27.0 → 28.1 |
| amqp/bench/ref/1 | child/1907994 | 28.9 → 30.0 | 27.0 → 28.1 |
| amqp/bench/ref/1 | child/1907993 | 28.9 → 30.0 | 27.0 → 28.1 |
| amqp/bench/ref/1 | child/1907992 | 29.0 → 30.0 | 27.1 → 28.1 |
| amqp/bench/ref/2 | master/1920004 | 26.1 → 28.0 | 11.3 → 14.5 |
| amqp/bench/ref/2 | child/1920018 | 20.7 → 22.4 | 10.6 → 13.2 |
| amqp/bench/ref/2 | child/1920016 | 28.9 → 29.8 | 27.0 → 27.9 |
| amqp/bench/ref/2 | child/1920014 | 28.9 → 30.1 | 27.0 → 28.2 |
| amqp/bench/ref/2 | child/1920013 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1920012 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1920011 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1920010 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1920009 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1920008 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/ref/2 | child/1920007 | 28.9 → 29.9 | 27.0 → 28.0 |
| amqp/bench/rs/1 | master/1913145 | 17.7 → 18.4 | 8.8 → 11.8 |
| amqp/bench/rs/1 | zygote/1913156 | 6.1 → 10.6 | 3.5 → 7.6 |
| amqp/bench/rs/1 | child/1913175 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913173 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913171 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913169 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913167 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913165 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913163 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913161 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913159 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/1 | child/1913157 | 6.3 → 20.3 | 4.2 → 18.3 |
| amqp/bench/rs/2 | master/1920343 | 17.9 → 18.6 | 8.9 → 11.8 |
| amqp/bench/rs/2 | zygote/1920349 | 6.2 → 10.6 | 3.6 → 7.6 |
| amqp/bench/rs/2 | child/1920368 | 6.4 → 20.5 | 4.3 → 18.4 |
| amqp/bench/rs/2 | child/1920366 | 6.4 → 20.5 | 4.3 → 18.5 |
| amqp/bench/rs/2 | child/1920364 | 6.4 → 20.5 | 4.3 → 18.4 |
| amqp/bench/rs/2 | child/1920362 | 6.4 → 20.5 | 4.3 → 18.5 |
| amqp/bench/rs/2 | child/1920360 | 6.4 → 20.5 | 4.3 → 18.4 |
| amqp/bench/rs/2 | child/1920358 | 6.4 → 20.5 | 4.3 → 18.5 |
| amqp/bench/rs/2 | child/1920356 | 6.4 → 20.5 | 4.3 → 18.4 |
| amqp/bench/rs/2 | child/1920354 | 6.4 → 20.5 | 4.3 → 18.5 |
| amqp/bench/rs/2 | child/1920352 | 6.4 → 20.5 | 4.3 → 18.4 |
| amqp/bench/rs/2 | child/1920350 | 6.4 → 20.5 | 4.3 → 18.5 |
| amqp/pure/ref/1 | master/1915222 | 26.0 → 26.9 | 11.2 → 12.7 |
| amqp/pure/ref/1 | child/1915381 | 20.6 → 21.0 | 10.5 → 11.0 |
| amqp/pure/ref/1 | child/1915380 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915379 | 28.9 → 30.0 | 27.0 → 28.0 |
| amqp/pure/ref/1 | child/1915378 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915377 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915376 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915375 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915374 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915373 | 28.9 → 29.0 | 27.0 → 27.1 |
| amqp/pure/ref/1 | child/1915372 | 28.9 → 29.1 | 27.0 → 27.1 |
| amqp/pure/ref/2 | master/1920529 | 26.1 → 27.0 | 11.3 → 12.7 |
| amqp/pure/ref/2 | child/1920545 | 20.7 → 21.0 | 10.5 → 11.0 |
| amqp/pure/ref/2 | child/1920544 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | child/1920543 | 28.9 → 30.0 | 27.0 → 28.1 |
| amqp/pure/ref/2 | child/1920542 | 28.9 → 29.1 | 27.1 → 27.2 |
| amqp/pure/ref/2 | child/1920541 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | child/1920540 | 28.9 → 29.1 | 27.0 → 27.1 |
| amqp/pure/ref/2 | child/1920539 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | child/1920538 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | child/1920537 | 28.9 → 29.1 | 27.0 → 27.2 |
| amqp/pure/ref/2 | child/1920536 | 29.0 → 29.1 | 27.1 → 27.2 |
| amqp/pure/rs/1 | master/1916505 | 17.8 → 18.5 | 8.8 → 11.6 |
| amqp/pure/rs/1 | zygote/1916511 | 6.1 → 7.3 | 3.5 → 4.7 |
| amqp/pure/rs/1 | child/1916530 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916528 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916526 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916524 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916522 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916520 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916518 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916516 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916514 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/1 | child/1916512 | 6.3 → 8.0 | 4.2 → 5.8 |
| amqp/pure/rs/2 | master/1920878 | 17.8 → 18.6 | 8.8 → 11.7 |
| amqp/pure/rs/2 | zygote/1920887 | 6.1 → 7.3 | 3.5 → 4.7 |
| amqp/pure/rs/2 | child/1920906 | 6.3 → 8.0 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920904 | 6.3 → 8.0 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920902 | 6.3 → 7.9 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920900 | 6.3 → 7.9 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920898 | 6.3 → 7.9 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920896 | 6.3 → 7.9 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920894 | 6.3 → 8.0 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920892 | 6.3 → 7.9 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920890 | 6.3 → 7.9 | 4.2 → 5.7 |
| amqp/pure/rs/2 | child/1920888 | 6.3 → 7.9 | 4.2 → 5.7 |
| redis/bench/ref/1 | master/1905536 | 29.1 → 29.4 | 14.8 → 15.5 |
| redis/bench/ref/1 | child/1905581 | 22.9 → 23.1 | 13.1 → 13.4 |
| redis/bench/ref/1 | child/1905580 | 30.6 → 30.6 | 28.6 → 28.6 |
| redis/bench/ref/1 | child/1905579 | 30.6 → 31.6 | 28.6 → 29.5 |
| redis/bench/ref/1 | child/1905576 | 30.6 → 31.8 | 28.6 → 29.7 |
| redis/bench/ref/1 | child/1905575 | 30.6 → 30.6 | 28.6 → 28.6 |
| redis/bench/ref/1 | child/1905574 | 30.6 → 30.6 | 28.6 → 28.6 |
| redis/bench/ref/1 | child/1905573 | 30.6 → 30.6 | 28.6 → 28.6 |
| redis/bench/ref/1 | child/1905572 | 30.6 → 30.6 | 28.6 → 28.6 |
| redis/bench/ref/1 | child/1905571 | 31.6 → 31.6 | 29.7 → 29.7 |
| redis/bench/ref/1 | child/1905570 | 36.6 → 36.6 | 34.7 → 34.7 |
| redis/bench/ref/2 | master/1916720 | 27.5 → 27.8 | 13.6 → 14.3 |
| redis/bench/ref/2 | child/1916764 | 21.5 → 21.7 | 12.1 → 12.4 |
| redis/bench/ref/2 | child/1916763 | 28.9 → 29.9 | 27.0 → 28.0 |
| redis/bench/ref/2 | child/1916762 | 28.9 → 29.9 | 27.0 → 28.0 |
| redis/bench/ref/2 | child/1916761 | 28.9 → 30.3 | 27.0 → 28.3 |
| redis/bench/ref/2 | child/1916760 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1916759 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1916758 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1916757 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1916756 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/bench/ref/2 | child/1916745 | 29.0 → 30.0 | 27.0 → 28.1 |
| redis/bench/rs/1 | master/1907071 | 21.8 → 20.0 | 12.5 → 13.2 |
| redis/bench/rs/1 | zygote/1907075 | 7.3 → 16.7 | 4.1 → 13.7 |
| redis/bench/rs/1 | child/1907094 | 7.5 → 22.5 | 4.8 → 20.4 |
| redis/bench/rs/1 | child/1907092 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907090 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907088 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907086 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907084 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907082 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907080 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907078 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/1 | child/1907076 | 7.5 → 22.5 | 4.9 → 20.4 |
| redis/bench/rs/2 | master/1917884 | 17.9 → 17.0 | 8.8 → 10.2 |
| redis/bench/rs/2 | zygote/1917888 | 6.1 → 10.5 | 3.5 → 7.5 |
| redis/bench/rs/2 | child/1917907 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917905 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917903 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917901 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917899 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917897 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917895 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917893 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917891 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/bench/rs/2 | child/1917889 | 6.3 → 20.4 | 4.2 → 18.4 |
| redis/pure/ref/1 | master/1907242 | 27.5 → 27.8 | 13.6 → 14.3 |
| redis/pure/ref/1 | child/1907260 | 21.5 → 21.7 | 12.2 → 12.4 |
| redis/pure/ref/1 | child/1907257 | 28.9 → 29.1 | 27.0 → 27.1 |
| redis/pure/ref/1 | child/1907256 | 28.9 → 29.1 | 27.0 → 27.1 |
| redis/pure/ref/1 | child/1907255 | 28.9 → 30.0 | 27.0 → 28.0 |
| redis/pure/ref/1 | child/1907254 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1907253 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1907252 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1907251 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1907250 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/1 | child/1907249 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | master/1918302 | 27.5 → 27.9 | 13.6 → 14.3 |
| redis/pure/ref/2 | child/1918320 | 21.5 → 21.6 | 12.1 → 12.4 |
| redis/pure/ref/2 | child/1918319 | 28.9 → 29.1 | 27.0 → 27.2 |
| redis/pure/ref/2 | child/1918318 | 28.9 → 29.1 | 27.0 → 27.2 |
| redis/pure/ref/2 | child/1918317 | 28.9 → 30.0 | 27.0 → 28.0 |
| redis/pure/ref/2 | child/1918316 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1918315 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1918314 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1918313 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1918312 | 28.9 → 28.9 | 27.0 → 27.0 |
| redis/pure/ref/2 | child/1918311 | 28.9 → 29.0 | 27.0 → 27.0 |
| redis/pure/rs/1 | master/1907693 | 17.9 → 17.1 | 8.7 → 10.2 |
| redis/pure/rs/1 | zygote/1907701 | 6.2 → 7.3 | 3.5 → 4.7 |
| redis/pure/rs/1 | child/1907720 | 6.3 → 8.0 | 4.2 → 5.8 |
| redis/pure/rs/1 | child/1907718 | 6.3 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907716 | 6.4 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907714 | 6.3 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907712 | 6.4 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907710 | 6.3 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907708 | 6.4 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907706 | 6.3 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907704 | 6.4 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/1 | child/1907702 | 6.4 → 8.0 | 4.3 → 5.8 |
| redis/pure/rs/2 | master/1919408 | 17.9 → 17.1 | 8.7 → 10.1 |
| redis/pure/rs/2 | zygote/1919412 | 6.2 → 7.3 | 3.5 → 4.7 |
| redis/pure/rs/2 | child/1919431 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919429 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919427 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919425 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919423 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919421 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919419 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919417 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919415 | 6.3 → 8.0 | 4.2 → 5.7 |
| redis/pure/rs/2 | child/1919413 | 6.3 → 8.0 | 4.3 → 5.7 |
