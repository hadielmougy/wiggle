# Conformance fixtures

Language-neutral test cases for the behaviour that more than one client implements.

Wiggle has Java, Go and Python clients, and each carries its own copy of the instance-id codec and
the placement resolver. Three implementations of one rule is three chances to drift — and drift is
silent, because every implementation passes its own tests. These files are the shared answer.

| file | covers | Java runner |
|---|---|---|
| `placement-v1.json` | the instance-id format, and resolving an id to the cell holding it | `placement/src/test/java/com/wiggle/placement/ConformanceTest.java` |

## Using them from another client

Vendor the file beside the `.proto` files you already copy, and run it from your test suite.
`ConformanceTest` is the reference: it decodes the fixtures, calls the implementation, and compares
— roughly 150 lines with no framework beyond the test runner.

Each case carries a `name`, and most carry a `why` explaining the rule rather than restating the
assertion. Read those before porting; several exist because the obvious implementation is wrong.

## The four case kinds

- **`format`** — building an id from its parts. `error` means the call must be rejected, and the
  message must mention that string.
- **`parse`** — reading an id back. `legacy: true` means it must *not* parse, which is not a failure:
  it is how every id minted before namespaces looks, and callers route those to the genesis cell.
- **`resolve`** — a policy, an id and the set of live cells, to the cell holding that instance.
  `expect: null` means unresolvable.
- **`mintable`** — a policy and a cell, to the epoch and shards that cell may mint into, plus a
  reason. `STANDBY` (a ring exists and excludes this cell) and `NO_RING` (nobody has placed this
  namespace) are different answers on purpose.

`policies` holds named fixtures the `resolve` and `mintable` cases refer to, so a ring history is
written once. A null policy means the namespace has never been placed.

## Adding a case

Add it to the JSON. The Java runner is a `@TestFactory` that builds one dynamic test per case, so a
new case runs without touching Java — that is deliberate. It asserts the total case count, which
will fail and tell you to update it: the point is that nobody can add a case here and quietly not
run it.

Then port it to the other clients, or open an issue against them. A case that only Java runs is
worse than no case, because it looks like coverage.

## Versioning

The filename carries a version. Cases are only ever **added** to `placement-v1.json`; anything that
would change an existing expectation is a behaviour change and belongs in `placement-v2.json`
alongside it, so a client can state which contract it satisfies.
