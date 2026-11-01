#!/usr/bin/env bash
# Builds the whole monorepo with plain javac. ./gradlew build is the normal path.
#
# This is not dependency-free: the server needs the Tomcat and JDBC jars.
# Run `./gradlew :server:installDist` at least once (with network access) so the
# generated sources and every correctly-resolved runtime jar land on disk; after that
# this script reuses what's already there and needs no network itself. (Globbing the
# Gradle module cache directly is unsafe -- it often holds several versions of the
# same artifact, and picking the wrong one produces baffling "cannot find symbol"
# errors.)
set -euo pipefail
cd "$(dirname "$0")/.."

LIB_DIR="server/build/install/server/lib"
if [ ! -d "$LIB_DIR" ]; then
  echo "no runtime jars found under $LIB_DIR" >&2
  echo "run './gradlew :server:installDist' once (needs network) before using this script" >&2
  exit 1
fi

CP="${WIGGLE_CLASSPATH:-$(find "$LIB_DIR" -name '*.jar' | paste -sd: -)}"

OUT=out/classes
rm -rf "$OUT" && mkdir -p "$OUT"
find core server client example tests/src/main -name '*.java' > out/sources.txt
javac -cp "$CP" -d "$OUT" @out/sources.txt
echo "$CP" > out/classpath.txt
echo "compiled $(wc -l < out/sources.txt) source files into $OUT"
