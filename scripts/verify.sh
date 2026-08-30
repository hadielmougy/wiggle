#!/usr/bin/env bash
# Runs the conformance suite (18 scenarios) without any test framework.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d out/classes ] || scripts/build.sh
java -cp "out/classes:$(cat out/classpath.txt)" com.wiggle.tests.Scenarios
