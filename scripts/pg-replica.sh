#!/usr/bin/env bash
#
# A PostgreSQL primary and a hot standby streaming from it, for the read-replica tests.
#
#   scripts/pg-replica.sh up      # start both, wait until the standby streams, print the test env
#   scripts/pg-replica.sh down    # remove both
#
# Then run, with the printed env exported:
#   ./gradlew :tests:test --tests 'com.wiggle.postgres.PostgresReplicaTest'
#
# Override via env: PG_IMAGE, PRIMARY_PORT, STANDBY_PORT.
set -euo pipefail

PG_IMAGE=${PG_IMAGE:-postgres:16-alpine}
PRIMARY_PORT=${PRIMARY_PORT:-55440}
STANDBY_PORT=${STANDBY_PORT:-55441}
NET=wiggle-repl
PRIMARY=wiggle-pg-primary
STANDBY=wiggle-pg-standby

command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }

ready() {
  for _ in $(seq 1 60); do
    docker exec "$1" pg_isready -h 127.0.0.1 -U wiggle >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "$1 did not become ready" >&2; return 1
}

case "${1:-up}" in
  up)
    docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
    docker run -d --name "$PRIMARY" --network "$NET" -p "$PRIMARY_PORT:5432" \
      -e POSTGRES_USER=wiggle -e POSTGRES_PASSWORD=wiggle -e POSTGRES_DB=wiggle \
      "$PG_IMAGE" -c wal_level=replica -c max_wal_senders=5 -c hot_standby=on >/dev/null
    ready "$PRIMARY"
    docker exec "$PRIMARY" sh -c 'echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"'
    docker exec "$PRIMARY" psql -U wiggle -d wiggle -tAc "select pg_reload_conf()" >/dev/null
    docker run -d --name "$STANDBY" --network "$NET" -p "$STANDBY_PORT:5432" -e PGPASSWORD=wiggle \
      --entrypoint sh "$PG_IMAGE" -c 'mkdir -p "$PGDATA" && chown postgres "$PGDATA" && chmod 700 "$PGDATA" \
        && su-exec postgres pg_basebackup -h '"$PRIMARY"' -U wiggle -D "$PGDATA" -R -X stream \
        && exec su-exec postgres postgres -c hot_standby=on' >/dev/null
    ready "$STANDBY"
    cat <<OUT
primary and standby are up; the standby is streaming. Export:

  export WIGGLE_TEST_DB_URL=jdbc:postgresql://127.0.0.1:${PRIMARY_PORT}/wiggle
  export WIGGLE_TEST_PG_URL=jdbc:postgresql://127.0.0.1:${PRIMARY_PORT}/wiggle
  export WIGGLE_TEST_PG_REPLICA_URL=jdbc:postgresql://127.0.0.1:${STANDBY_PORT}/wiggle
  export WIGGLE_TEST_DB_USER=wiggle WIGGLE_TEST_DB_PASSWORD=wiggle
OUT
    ;;
  down)
    docker rm -f "$PRIMARY" "$STANDBY" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    echo "removed"
    ;;
  *)
    echo "usage: scripts/pg-replica.sh {up|down}" >&2; exit 2 ;;
esac
