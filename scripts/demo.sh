#!/usr/bin/env bash
# Single-JVM demo: embedded server + worker + three orders.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d out/classes ] || scripts/build.sh
java -cp "out/classes:$(cat out/classpath.txt)" com.wiggle.example.Demo
