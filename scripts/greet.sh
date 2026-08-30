#!/usr/bin/env bash
#
# Run the coordinator-aware greet worker / client against a local coordinator (see also two-dbs.sh and
# the coordinator + cells launch commands). Runs the built example classpath directly with java, so the
# WIGGLE_* env vars reliably reach the process (a Gradle JavaExec would not see inline env changes).
#
#   scripts/greet.sh worker [namespace]           # coordinator-aware NamespaceWorker (foreground)
#   scripts/greet.sh start  [namespace] [name]    # allocate + start one instance
#
# Env: WIGGLE_COORDINATOR_URL (default 127.0.0.1:8099).
set -uo pipefail
cd "$(dirname "$0")/.."

export WIGGLE_COORDINATOR_URL=${WIGGLE_COORDINATOR_URL:-127.0.0.1:8099}
command -v java >/dev/null || { echo "java is required" >&2; exit 2; }
[ -d example/build/install/example/lib ] || { echo "== building :example:installDist =="; ./gradlew :example:installDist -q; }
CP="example/build/install/example/lib/*"

cmd=${1:-}; shift || true
case "$cmd" in
  worker)
    export WIGGLE_NAMESPACE=${1:-ns1}
    echo "== greet worker -> namespace '$WIGGLE_NAMESPACE' via $WIGGLE_COORDINATOR_URL (Ctrl-C to stop) =="
    exec java -cp "$CP" com.wiggle.greet.GreetWorker
    ;;
  start)
    export WIGGLE_NAMESPACE=${1:-ns1}
    exec java -cp "$CP" com.wiggle.greet.GreetStart "${2:-ada}"
    ;;
  *)
    echo "usage: scripts/greet.sh {worker [namespace] | start [namespace] [name]}" >&2
    exit 2
    ;;
esac
