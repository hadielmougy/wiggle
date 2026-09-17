#!/usr/bin/env python3
"""Rewrite the Java in docs/ from compiled source in this repo.

Same idea as wiggle-site's scripts/snippets.py, for our own docs. A markdown fence is text nothing
checks; the moment an API moves, every example quoting it is wrong and the build stays green. The
site learned this the expensive way -- its saga page published a removed API for weeks -- and these
docs are vendored to that site, so a stale example here becomes a stale page there.

A region of real, compiled source owns the code instead:

    example/src/main/java/com/wiggle/cookbook/Cookbook.java

    // docs:begin handlers
    ...                       <- this, dedented, is what the page shows
    // docs:end handlers

Lines between `// docs:skip` and `// docs:resume` inside a region are dropped -- the scaffolding a
fixture needs to compile (injected fields, throwaway interfaces) is not what a page is teaching.

`// docs:elide` emits `...` in its place, and `// docs:elide <text>` emits that text. It is the
honest form of skip: use it where a doc deliberately abridges, so the reader sees that something was
left out instead of reading a class that looks complete and is not.

and the page marks where each region goes:

    <!-- snippet: saga/handlers -->
    ```java
    ...whatever is here is replaced...
    ```

A marker may name several regions, comma-separated, when one block reads better than two:
`<!-- snippet: saga/contract,topology -->` joins them with a blank line between.

Break the API and the main repo's build fails on the fixture. Edit the page's Java by hand and the
next sync puts it back. Either way the published code is code that compiles.

Run via scripts/docs-snippets.py. Exits non-zero on anything unexpected -- a missing source file, an unknown
region, a marker with no fence after it -- because a snippet step that fails quietly would leave
exactly the stale page it exists to prevent.
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

FIXTURES = ROOT / "example/src/main/java/com/wiggle"

# page marker name -> the file whose regions it draws from
SOURCES = {
    # the conformance cookbook: every spec the doc shows is one the test suite runs
    "cookbook": FIXTURES / "cookbook/Cookbook.java",
    "cookbook-contract": FIXTURES / "docs/CookbookContract.java",
    "versioning": FIXTURES / "docs/VersioningSnippet.java",
    "errors": FIXTURES / "docs/ErrorHandlingSnippet.java",
    "saga": FIXTURES / "docs/SagaSnippet.java",
    "saga-handlers": FIXTURES / "docs/BookingHandlers.java",
    "saga-doc": FIXTURES / "docs/SagaDocSnippet.java",
    "saga-doc-activity": FIXTURES / "docs/CapturePayment.java",
    "saga-doc-handlers": FIXTURES / "docs/SagaDocHandlers.java",
    "onboarding": FIXTURES / "docs/onboarding/OnboardingSnippet.java",
    "onboarding-handlers": FIXTURES / "docs/onboarding/OrderHandlers.java",
    "decode": FIXTURES / "docs/decode/OrderHandlers.java",
    "queues": FIXTURES / "docs/QueuesSnippet.java",
    "local-execution": FIXTURES / "docs/LocalExecutionSnippet.java",
    # docs that quote the implementation itself, so the quote cannot drift from it
    "id-codec": ROOT / "placement/src/main/java/com/wiggle/placement/IdCodec.java",
    "coordinated-connection": ROOT / "client/src/main/java/com/wiggle/client/CoordinatedConnection.java",
}

MARKER = re.compile(r"^(?P<indent>[ \t]*)<!-- snippet: (?P<source>[\w-]+)/(?P<regions>[\w,-]+) -->$")
FENCE = re.compile(r"^```")


def regions(path):
    """Every `// docs:begin X` .. `// docs:end X` block in a source file, dedented."""
    if not path.exists():
        sys.exit(f"snippets: no such source file: {path}\n"
                 f"  the fixture moved or was deleted; this page's Java has nowhere to come from")
    out, name, buf, skipping = {}, None, [], False
    for line in path.read_text().splitlines():
        begin = re.match(r"\s*// docs:begin (\S+)\s*$", line)
        end = re.match(r"\s*// docs:end (\S+)\s*$", line)
        if begin:
            if name:
                sys.exit(f"snippets: {path.name}: '{begin.group(1)}' opens inside '{name}'")
            name, buf, skipping = begin.group(1), [], False
        elif end:
            if end.group(1) != name:
                sys.exit(f"snippets: {path.name}: '{end.group(1)}' closes '{name}'")
            if skipping:
                sys.exit(f"snippets: {path.name}: docs:skip in '{name}' is never resumed")
            out[name] = dedent(buf)
            name = None
        elif name is not None:
            elide = re.match(r"(\s*)// docs:elide(?: (.*))?\s*$", line)
            if re.match(r"\s*// docs:skip\s*$", line):
                skipping = True
            elif re.match(r"\s*// docs:resume\s*$", line):
                skipping = False
            elif elide:
                if not skipping:
                    buf.append(elide.group(1) + (elide.group(2) or "..."))
            elif not skipping:
                buf.append(line)
    if name:
        sys.exit(f"snippets: {path.name}: region '{name}' is never closed")
    if not out:
        sys.exit(f"snippets: {path.name} has no docs:begin regions -- markers renamed?")
    return out


def dedent(lines):
    """Strip the common leading indentation, so a nested class reads as top-level on the page."""
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    body = [ln for ln in lines if ln.strip()]
    pad = min((len(ln) - len(ln.lstrip()) for ln in body), default=0)
    return [ln[pad:] if ln.strip() else "" for ln in lines]


def rewrite(page, cache):
    """Replace the fenced block after each `<!-- snippet: source/region -->` marker."""
    lines = page.read_text().splitlines()
    out, i, count = [], 0, 0
    while i < len(lines):
        m = MARKER.match(lines[i])
        out.append(lines[i])
        i += 1
        if not m:
            continue

        source = m.group("source")
        wanted = m.group("regions").split(",")
        if source not in SOURCES:
            sys.exit(f"snippets: {page.name}:{i}: unknown source '{source}'")
        if source not in cache:
            cache[source] = regions(SOURCES[source])
        for region in wanted:
            if region not in cache[source]:
                sys.exit(f"snippets: {page.name}:{i}: no region '{region}' in "
                         f"{SOURCES[source].name} (has: {', '.join(sorted(cache[source]))})")

        body = []
        for region in wanted:
            if body:
                body.append("")       # one blank line between joined regions
            body.extend(cache[source][region])

        if i >= len(lines) or not FENCE.match(lines[i]):
            sys.exit(f"snippets: {page.name}:{i + 1}: marker is not followed by a ``` fence")
        out.append(lines[i])          # opening fence, verbatim (keeps ```java)
        i += 1
        while i < len(lines) and not FENCE.match(lines[i]):
            i += 1                    # drop whatever the page used to hold
        if i >= len(lines):
            sys.exit(f"snippets: {page.name}: unterminated fence after "
                     f"{source}/{m.group('regions')}")
        out.extend(body)
        out.append(lines[i])          # closing fence
        i += 1
        count += 1
    return "\n".join(out) + "\n", count


def main():
    cache, total, touched = {}, 0, []
    for page in sorted(ROOT.glob("docs/**/*.md")):
        if "archive" in page.relative_to(ROOT).parts:   # retired pages keep their old snippets
            continue
        text = page.read_text()
        if "<!-- snippet:" not in text:
            continue
        new, n = rewrite(page, cache)
        total += n
        if new != text:
            page.write_text(new)
            touched.append(page.relative_to(ROOT))
    if total == 0:
        sys.exit("snippets: no snippet markers found in docs/ -- markers renamed or lost?")
    print(f"snippets: {total} block(s) from compiled source"
          + (f"; rewrote {', '.join(str(p) for p in touched)}" if touched else "; all current"))


if __name__ == "__main__":
    main()