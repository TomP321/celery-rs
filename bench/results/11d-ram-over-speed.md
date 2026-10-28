# 11d — RAM prima della velocità

Interrotto dall'orchestratore (quota di codex); consolidato e misurato dall'orchestratore.
Misure: `bench/memprobe/loadpeak.py`, Redis, `-c 10`, 30.000 task, PSS in MiB.

| Scenario | Base 11c riposo / picco | 11d riposo / picco | task/s 11d |
|---|---|---|---|
| Python 3.11, interi | 76,5 / 88,9 | 72,5 / 84,3 | 2668 |
| Python 3.11, 16 KiB | — / ~89 | 72,6 / 85,9 | 1701 |
| Python 3.13, interi | 50,9 / 54,8 | 51,2 / 56,2 | 2723 |
| Python 3.13, 16 KiB | 50,9 / 58,4 | 51,0 / 55,7 | 1771 |

Pavimento del Celery originale (stessa misura): 834 task/s interi, 695 con 16 KiB.

## Tenuto
- `gc.collect()` nel master prima dello zygote e `gc.freeze()` subito prima del fork.
- `_release_task_memory(force=True)` dopo ogni task nel figlio.

## Scartato (misure in `bench/results/11d-*.log`)
- Ricondivisione delle pagine tra fratelli (copie in memfd): riposo 3.11 fino a 50,9 MiB, ma il
  costo delle copie annulla il vantaggio sotto carico.
- Confronto con l'immagine prima del fork: recupera poco e aggiunge memoria.
- Riempimento/normalizzazione dei pool pymalloc: beneficio limitato.
- Cache adattive dell'interprete spente: peggiora (fino a 90,7 MiB 3.11, 64,9 MiB 3.13).
- Tracer pre-costruiti nello zygote: peggiora.
- Una sola arena glibc: −1,7 MiB al picco 3.11 in una prova, nessun vantaggio su 3.13; non
  consolidato.
- Buffer interni di lapin ridotti (fork in `vendor/`): non misurato a fine lavoro, scartato per il
  costo di mantenere una copia della dipendenza.
