#!/usr/bin/env bash
#
# Bumps the Wiggle version in one shot: the Gradle project version (the published jars) and the
# Docker image tag, plus every version reference in the docs and compose files. The CURRENT version
# is read from build.gradle.kts -- the single source of truth -- so you never hand-edit versions.
#
#   scripts/bump-version.sh 2.1.5     # bump from whatever build.gradle.kts says to 2.1.5
#   scripts/bump-version.sh           # just print the current version
#
# After it runs: review the diff, commit, then build & push the image with scripts/docker-release.sh
# (which reads the new version straight from build.gradle.kts).
set -euo pipefail
cd "$(dirname "$0")/.."

CURRENT="$(grep -oE 'version = "[^"]+"' build.gradle.kts | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"
: "${CURRENT:?could not read current version from build.gradle.kts}"

NEW="${1:-}"
if [ -z "$NEW" ]; then
    echo "current version: $CURRENT"
    echo "usage: $(basename "$0") <new-version>"
    exit 0
fi

if [ "$NEW" = "$CURRENT" ]; then
    echo "already at $CURRENT; nothing to do."
    exit 0
fi

# A typo must not scribble junk across every file.
if ! printf '%s' "$NEW" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.]+)?$'; then
    echo "refusing to use '$NEW' as a version (expected e.g. 2.1.5)" >&2
    exit 1
fi

echo "bumping $CURRENT -> $NEW"

# The Gradle project version drives the jars: rewrite only that line, so a dependency that happens
# to share the version string is never touched.
perl -pi -e 's/^(\s*version = ")\Q'"$CURRENT"'\E(".*)$/${1}'"$NEW"'${2}/' build.gradle.kts

# Everywhere else the current version only appears as the Wiggle image tag, the Maven Central
# coordinates, or a docs badge -- all of which move together -- so an exact-string replace is safe.
# (Files listed explicitly rather than auto-discovered, so the blast radius is always obvious.)
DOC_FILES=(Dockerfile docker-compose.full.yml README.md docs/onboarding.md scripts/docker-release.sh
    docs/workflow-yaml.md HomebrewFormula/wiggle.rb scripts/cli-release.sh
    cli/src/main/java/dev/wiggle/cli/Wiggle.java)
for f in "${DOC_FILES[@]}"; do
    [ -f "$f" ] || { echo "  (skipping missing $f)"; continue; }
    perl -pi -e 's/\Q'"$CURRENT"'\E/'"$NEW"'/g' "$f"
done

echo
echo "changed files:"
git --no-pager diff --stat -- build.gradle.kts "${DOC_FILES[@]}" 2>/dev/null || true

# Safety net: flag any remaining reference to the old version anywhere outside build output, in case
# a new doc started mentioning it and isn't in the list above.
echo
leftover="$(grep -rIn -F "$CURRENT" . \
    --exclude-dir=.git --exclude-dir=build --exclude-dir=.gradle --exclude-dir=node_modules \
    2>/dev/null || true)"
if [ -n "$leftover" ]; then
    echo "NOTE: the old version ($CURRENT) still appears here -- update by hand if these are release refs:"
    echo "$leftover" | sed 's/^/  /'
else
    echo "no stray references to $CURRENT remain."
fi

echo
echo "Done. Next:"
echo "  1. review the diff and commit"
echo "  2. scripts/docker-release.sh          # builds & pushes hadielmougy/wiggle:$NEW"
