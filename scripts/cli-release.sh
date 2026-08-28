#!/usr/bin/env bash
#
# Packages the `wiggle` CLI as a self-contained distribution (a JVM app; needs Java 21 to run) and,
# optionally, uploads it to a GitHub Release. The `application` plugin produces a .zip and a .tar
# under cli/build/distributions/; this script builds them, prints their SHA-256 (which you paste into
# the Homebrew formula and the release notes), and can attach them to the release for the tag.
#
#   scripts/cli-release.sh                 # build archives + print paths and SHA-256
#   scripts/cli-release.sh 2.1.5           # explicit version
#   UPLOAD=true scripts/cli-release.sh     # also `gh release upload v<version> ...` (needs gh + an existing release)
#
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="${1:-$(grep -oE 'version = "[^"]+"' build.gradle.kts | head -1 | sed -E 's/.*"([^"]+)".*/\1/')}"
: "${VERSION:?could not determine version from build.gradle.kts; pass it explicitly}"
UPLOAD="${UPLOAD:-false}"

echo "==> building the wiggle CLI distribution (v${VERSION})"
./gradlew :cli:distZip :cli:distTar -q

ZIP="cli/build/distributions/wiggle-${VERSION}.zip"
TAR="cli/build/distributions/wiggle-${VERSION}.tar"
for f in "$ZIP" "$TAR"; do
    [ -f "$f" ] || { echo "expected artifact not found: $f" >&2; exit 1; }
done

sha() { shasum -a 256 "$1" 2>/dev/null || sha256sum "$1"; }

echo
echo "==> artifacts"
for f in "$ZIP" "$TAR"; do printf '  %s\n' "$f"; done
echo
echo "==> SHA-256 (paste the .tar digest into HomebrewFormula/wiggle.rb)"
sha "$ZIP"
sha "$TAR"

if [ "$UPLOAD" = "true" ]; then
    command -v gh >/dev/null || { echo "gh CLI not found; cannot upload" >&2; exit 1; }
    echo
    echo "==> uploading to release v${VERSION}"
    gh release upload "v${VERSION}" "$ZIP" "$TAR" --clobber
    echo "uploaded."
else
    echo
    echo "Not uploading (set UPLOAD=true to attach these to release v${VERSION})."
fi
