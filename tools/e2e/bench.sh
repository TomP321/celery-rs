#!/usr/bin/env bash
# Alternate reference and celery-rs on real projects, three rounds. Run under tools/dev/with-brokers
# with no other load: throughput is only comparable when the machine is otherwise idle.
set -euo pipefail
HERE="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"; OUT="$HERE/../../target/e2e"
R="${CELERY_RS_REDIS_URL:-redis://127.0.0.1:6390}"; R="${R%/}"
export E2E_RUN="$OUT/runs/bench" E2E_QUEUE="bq$RANDOM" E2E_DB=e2e_bench E2E_CACHE="$R/5" E2E_BROKER="$R/4" E2E_RESULT="$R/6" PYTHONPATH="$HERE"
mkdir -p "$E2E_RUN"
docker compose -f "$HERE/docker-compose.yml" up -d --quiet-pull --wait postgres >/dev/null
first=1
for round in 1 2 3; do for v in ${E2E_VENVS:-ref rs}; do for spec in "fastapi add" "django add" "django orm"; do
  set -- $spec
  E2E_CREATE=$([ $first = 1 ] && echo 1 || true) "$OUT/venv-$v/bin/python" "$HERE/bench.py" "$OUT/venv-$v" "$1" "$2" "${E2E_TASKS:-3000}" | tail -1
  first=0
done; done; done
