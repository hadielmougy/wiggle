#!/usr/bin/env bash
#
# Two standalone PostgreSQL instances (separate containers) for local testing. Both use database
# "wiggle", user "wiggle", password "wiggle".
#
#   ds_0   -> localhost:5442   jdbc:postgresql://127.0.0.1:5442/wiggle
#   ds_1   -> localhost:5443   jdbc:postgresql://127.0.0.1:5443/wiggle
#
# Usage:
#   scripts/two-dbs.sh up          # start all, wait until ready, print connection info
#   scripts/two-dbs.sh down        # stop and remove all
#   scripts/two-dbs.sh status      # show container + readiness state
#   scripts/two-dbs.sh psql 0      # psql shell into ds_0 (also: psql 1)
#
# Override via env: PG_IMAGE, PORT0, PORT1, DB, DB_USER, DB_PASSWORD, NAME0, NAME1.
set -uo pipefail
cd "$(dirname "$0")/.."

PG_IMAGE=${PG_IMAGE:-postgres:16-alpine}
DB=${DB:-wiggle}
DB_USER=${DB_USER:-wiggle}
DB_PASSWORD=${DB_PASSWORD:-wiggle}
PORT0=${PORT0:-5442}
PORT1=${PORT1:-5443}
NAME0=${NAME0:-wiggle-ds0}
NAME1=${NAME1:-wiggle-ds1}

command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }

start_one() {
  local name=$1 port=$2
  if [ -n "$(docker ps -aq -f name="^${name}$")" ]; then
    echo "== ${name} already exists; (re)starting =="
    docker start "$name" >/dev/null
    return
  fi
  echo "== starting ${name} on :${port} =="
  docker run -d --name "$name" \
    -e POSTGRES_DB="$DB" \
    -e POSTGRES_USER="$DB_USER" \
    -e POSTGRES_PASSWORD="$DB_PASSWORD" \
    -p "${port}:5432" \
    "$PG_IMAGE" >/dev/null
}

wait_ready() {
  local name=$1
  echo -n "== waiting for ${name} to accept connections "
  for _ in $(seq 1 60); do
    if docker exec "$name" pg_isready -U "$DB_USER" -d "$DB" >/dev/null 2>&1; then
      echo "-> ready"; return 0
    fi
    echo -n "."; sleep 1
  done
  echo " -> TIMED OUT" >&2; return 1
}

case "${1:-up}" in
  up)
    start_one "$NAME0" "$PORT0"
    start_one "$NAME1" "$PORT1"
    wait_ready "$NAME0" || exit 1
    wait_ready "$NAME1" || exit 1
    cat <<EOF

Postgres instances are up (db=${DB}, user=${DB_USER}, password=${DB_PASSWORD}):

  ds_0   ${NAME0}   jdbc:postgresql://127.0.0.1:${PORT0}/${DB}
  ds_1   ${NAME1}   jdbc:postgresql://127.0.0.1:${PORT1}/${DB}

Stop them with: scripts/two-dbs.sh down
EOF
    ;;
  down)
    echo "== removing ${NAME0} and ${NAME1} =="
    docker rm -f "$NAME0" "$NAME1" >/dev/null 2>&1
    echo "done"
    ;;
  status)
    docker ps -a --filter "name=${NAME0}" --filter "name=${NAME1}" \
      --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    for n in "$NAME0" "$NAME1"; do
      if [ -n "$(docker ps -q -f name="^${n}$")" ]; then
        docker exec "$n" pg_isready -U "$DB_USER" -d "$DB" 2>/dev/null | sed "s/^/${n}: /"
      fi
    done
    ;;
  psql)
    case "${2:-0}" in
      1)       name=$NAME1 ;;
      *)       name=$NAME0 ;;
    esac
    exec docker exec -it "$name" psql -U "$DB_USER" -d "$DB"
    ;;
  *)
    echo "usage: scripts/two-dbs.sh {up|down|status|psql [0|1]}" >&2
    exit 2
    ;;
esac
