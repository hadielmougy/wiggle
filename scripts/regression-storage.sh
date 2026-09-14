#!/usr/bin/env bash
#
# Storage regression: runs each supported store's opt-in integration test, one at a time.
# For every database-backed store it spins up a throwaway Docker container, waits for it to be
# ready, runs the store's test against it, then destroys the container -- pass or fail. At the end
# it prints a per-store PASS/FAIL summary and exits non-zero if any store failed.
#
#   scripts/regression-storage.sh            # both stores
#   scripts/regression-storage.sh postgres   # just one
#
# H2 is embedded (no container) and is for tests and local runs, not a deployment target; PostgreSQL
# is what a cluster runs on, and needs Docker.

set -uo pipefail
cd "$(dirname "$0")/.."

ALL_STORES=(h2 postgres)
STORES=("${ALL_STORES[@]}")
[ "$#" -gt 0 ] && STORES=("$@")

PREFIX="wiggle-regress"
LOGDIR="$(mktemp -d)"
SUMMARY=()          # one "store|RESULT|seconds" line per store (portable to bash 3.2)
FAILED=()
STARTED=()

c_red=$'\033[0;31m'; c_grn=$'\033[0;32m'; c_ylw=$'\033[0;33m'; c_blu=$'\033[1;34m'; c_off=$'\033[0m'
log()  { printf '%s==> %s%s\n' "$c_blu" "$*" "$c_off"; }
warn() { printf '%s%s%s\n' "$c_ylw" "$*" "$c_off"; }

cleanup() {
    for c in "${STARTED[@]:-}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT INT TERM

# wait_until <description> <timeout-seconds> <command...> -- polls until the command succeeds.
wait_until() {
    local desc="$1" timeout="$2"; shift 2
    local deadline=$((SECONDS + timeout))
    while (( SECONDS < deadline )); do
        "$@" >/dev/null 2>&1 && return 0
        sleep 3
    done
    warn "  timed out after ${timeout}s waiting for ${desc}"
    return 1
}

# run_tests <store> <test-fqn...> -- runs the given test classes; log goes to $LOGDIR/<store>.log.
run_tests() {
    local store="$1"; shift
    local args=(:tests:test)
    for t in "$@"; do args+=(--tests "$t"); done
    args+=(--rerun-tasks --console=plain)
    ./gradlew "${args[@]}" >"$LOGDIR/$store.log" 2>&1
}

# record <store> <PASS|FAIL|SKIP> <start-seconds> -- store the outcome + elapsed time.
record() {
    local dur=$(( SECONDS - $3 ))
    SUMMARY+=("$1|$2|$dur")
    [ "$2" = FAIL ] && FAILED+=("$1")
    local color=$c_grn; [ "$2" = FAIL ] && color=$c_red; [ "$2" = SKIP ] && color=$c_ylw
    printf '  %s%s%s (%ss)\n' "$color" "$2" "$c_off" "$dur"
}

require_docker() {
    if ! docker info >/dev/null 2>&1; then
        warn "  Docker is not available -- skipping $1"; return 1
    fi
}

# --- per-store drivers -------------------------------------------------------------------------

store_h2() {  # embedded; the H2-backed store tests run without a container
    local t0=$SECONDS
    if run_tests h2 com.wiggle.postgres.SchemaMigrationTest com.wiggle.tests.JdbcGraphTest; then
        record h2 PASS "$t0"; else record h2 FAIL "$t0"; fi
}

store_postgres() {
    local t0=$SECONDS name="$PREFIX-postgres"
    require_docker postgres || { record postgres SKIP "$t0"; return; }
    log "postgres: starting container"
    docker run -d --name "$name" -e POSTGRES_DB=wiggle -e POSTGRES_USER=wiggle -e POSTGRES_PASSWORD=wiggle \
        -p 55432:5432 postgres:16-alpine >/dev/null 2>&1 || { record postgres FAIL "$t0"; return; }
    STARTED+=("$name")
    wait_until "postgres" 90 docker exec "$name" pg_isready -U wiggle -d wiggle || { record postgres FAIL "$t0"; docker rm -f "$name" >/dev/null 2>&1; return; }
    export WIGGLE_TEST_PG_URL="jdbc:postgresql://localhost:55432/wiggle" WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle
    log "postgres: running com.wiggle.postgres.PostgresClaimTest"
    if run_tests postgres com.wiggle.postgres.PostgresClaimTest; then record postgres PASS "$t0"; else record postgres FAIL "$t0"; fi
    docker rm -f "$name" >/dev/null 2>&1
}





# --- run ---------------------------------------------------------------------------------------

log "storage regression: ${STORES[*]}"
for store in "${STORES[@]}"; do
    case "$store" in
        h2)        store_h2 ;;
        postgres)  store_postgres ;;
        *) warn "unknown store '$store' (known: ${ALL_STORES[*]})"; SUMMARY+=("$store|SKIP|0") ;;
    esac
done

# --- summary -----------------------------------------------------------------------------------

echo
printf '%s================ storage regression summary ================%s\n' "$c_blu" "$c_off"
printf '  %-12s %-6s %-8s\n' "STORE" "RESULT" "TIME"
for line in "${SUMMARY[@]:-}"; do
    [ -z "$line" ] && continue
    store="${line%%|*}"; rest="${line#*|}"; r="${rest%%|*}"; d="${rest##*|}"
    color=$c_grn; [ "$r" = FAIL ] && color=$c_red; [ "$r" = SKIP ] && color=$c_ylw
    printf '  %-12s %s%-6s%s %ss\n' "$store" "$color" "$r" "$c_off" "$d"
done
printf '%s============================================================%s\n' "$c_blu" "$c_off"
if [ "${#FAILED[@]}" -gt 0 ]; then
    echo "${c_red}${#FAILED[@]} store(s) failed: ${FAILED[*]}. Logs in $LOGDIR${c_off}"
    for store in "${FAILED[@]}"; do
        echo "--- $store (last 20 log lines) ---"; tail -n 20 "$LOGDIR/$store.log" 2>/dev/null
    done
    exit 1
fi
echo "${c_grn}all stores passed${c_off}  (logs in $LOGDIR)"
