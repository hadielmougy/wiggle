# Conformance fixtures

Language-neutral test cases for behaviour that more than one client implements. Wiggle has Java, Go
and Python clients; each one that mints or reads instance ids runs these files from its own test
suite, so the implementations cannot drift apart silently.

| file | covers | Java runner |
|---|---|---|
| `shard-ids-v1.json` | the shard-carrying id format, reading the shard back, and deriving one id from another | `tests/src/test/java/com/wiggle/core/ShardIdsConformanceTest.java` |

## The case kinds

- **`format`**: building `{prefix}.s{shard}.{rest}` from its parts. `error` means the call must be
  rejected with a message that mentions that string.
- **`parse`**: reading the shard back from an id. `shard: null` means the id carries no shard and
  belongs to the home shard: bare ids from before shards existed, and ids from the removed cell
  coordinator.
- **`inherit`**: minting an id for something another id owns (a token for its instance, a child
  instance for its parent's token). The new id is on the owner's shard; when the owner carries no
  shard, the new id is bare (`{prefix}_…`, `bare: true`) so it too belongs to the home shard.

Cases with a `why` exist because the obvious implementation gets them wrong. Read those before
porting.

## Adding a case

Add it to the JSON. The Java runner builds one dynamic test per case, and it checks the total case
count, which fails until you update it: nobody can add a case and quietly not run it. Then port it
to the other clients.

## Versioning

Cases are only ever **added** to a `-v1` file. Anything that would change an existing expectation is
a behaviour change and goes in a `-v2` file alongside it, so a client can state which contract it
satisfies.
