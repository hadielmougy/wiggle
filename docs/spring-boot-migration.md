# Spring Boot migration

The server moves to Spring Boot, tests included. Storage moves to Spring's `DataSource` and
transaction management, `Storage.inTx` and the `Tx` handle give way to repositories, and
`InMemoryStorage` is removed: tests run on H2 in its in-memory mode.

Each phase is one or more pull requests, stacked. The per-step statement-count tests, the no-table-scan
test and the live-database tests must pass unchanged at every step; they are what shows the store
still behaves the same.

## Decisions

| Decision | Choice |
|---|---|
| Spring Boot | 4.1.x (Spring Framework 7.0.x) on Java 21; 3.5 is the last 3.x line |
| Jackson | Boot 4.1 manages Jackson 2 at 2.21.x, so `jackson-core` stays on 2.21.x and follows Boot's patch level |
| Transaction boundaries | `@Transactional` only where the database never changes (accounts on the auth shard, definitions and cluster state on the home shard); `ShardTransactions`, one `TransactionTemplate` per shard, wherever the shard depends on the instance |
| Retry | Ours, around `TransactionTemplate`: replay only a rolled-back transient failure; a failure during commit is ambiguous and never retried |
| Nesting | A nested call on the same shard joins the open transaction; one on another shard is refused |
| Write buffer | `BufferedTx` becomes a buffer tied to the Spring transaction, flushed just before commit |
| In-memory storage | Removed; tests run on H2 (`jdbc:h2:mem:…;MODE=PostgreSQL`) |
| Migrations | Our forward-only runner stays, run as a Spring bean |
| Test ports | From `TestPorts`, never port 0 |

## Phases

0. Decisions and baselines (this document).
1. Replace `inTx` with Spring transactions and repositories, without Boot:
   - 1a. Each store runs its transactions through Spring's `DataSourceTransactionManager` and
     `TransactionTemplate`, over a `DataSource` it is given; `Storage.inTx` keeps its signature.
   - 1b. Store operations take their connection from Spring's thread binding instead of a passed-in `Tx`.
   - 1c. `Tx` splits into repositories on `JdbcTemplate`, with the same SQL.
   - 1d. Call sites move to `ShardTransactions` and the repositories, file by file.
   - 1e. `BufferedTx` becomes a buffer tied to the Spring transaction.
   - 1f. Services on one fixed database move to `@Transactional`.
   - 1g. `InMemoryStorage`, `Storage.inTx*`, `Tx` and `ReadTx` are removed; tests move to H2.
2. The `boot` module: configuration binding with the `WIGGLE_*` names kept, beans for the shard
   `DataSource`s and transaction managers, the API and portal servlets on Boot's Tomcat (two ports),
   Actuator in place of `HealthServer`, TLS, HTTP/2.
3. Test support: `@WiggleBootTest`, a few named profiles, `WiggleTestNodes` for multi-node tests, and a
   two-class pilot that decides whether every test runs in a Spring context.
4. Tests converted in batches.
5. Packaging: the image runs the Boot jar; `dist` and `HealthServer` are retired.
6. Measure against the baselines below and tune; HTTP/2 between client and server.

## Baselines

Taken on `31ebeeb1` (#320) on one laptop, which varies widely from run to run: compare against
interleaved runs, not against these numbers alone.

| Measure | Result |
|---|---|
| Test suite, `./gradlew test --rerun` | 111 s |
| The same with every in-memory server on H2 | 115 s |
| Start until `/v1/HealthCheck` answers, in-memory | 523–814 ms |
| The same on PostgreSQL 16 | 843–868 ms |

`:example:bench`, 20 steps per instance, steps per second:

| Storage, mode, instances | Runs |
|---|---|
| In-memory, SERVER, 6,000 | 21,233 · 28,673 · 28,605 |
| In-memory, LOCAL_ASYNC, 20,000 | 131,389 · 117,303 · 124,519 |
| PostgreSQL 16, SERVER, 1,000 | 1,948 · 2,466 · 2,431 |
| PostgreSQL 16, LOCAL_ASYNC, 6,000 | 27,893 · 23,584 · 27,965 |

Replacing in-memory storage with H2 costs the suite about 4 s, inside the noise. The in-memory
benchmark never touches the JDBC store, so the PostgreSQL rows are the ones phase 1 must hold.
