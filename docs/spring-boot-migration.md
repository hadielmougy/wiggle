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
| Transaction boundaries | `ShardTransactions` everywhere, one `TransactionTemplate` per shard. `@Transactional` was dropped even for services on one fixed database: it would bypass the replay of rolled-back transient failures, failure classification and shard-outage marking, and the bound `Repositories` |
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
   - 1f. Dropped (see Transaction boundaries).
   - 1g. `InMemoryStorage`, `Storage.inTx*`, `Tx` and `ReadTx` are removed; tests move to H2.
2. The `boot` module (Spring Boot 4.1.2):
   - 2a. A Boot application: settings read through Spring's `Environment` under the `WIGGLE_*` names,
     the store and the node as beans, the API servlet on Boot's Tomcat with virtual threads.
   - 2b. The portal on a second connector of the same Tomcat (`WIGGLE_PORTAL_PORT`), each port serving
     only its own paths.
   - 2c. Actuator on the probe port (`WIGGLE_DASHBOARD_PORT`) in place of `HealthServer`.
   - 2d. TLS and mTLS on both connectors from the `WIGGLE_TLS_*` settings.
   - 2e. HTTP/2 on the server side.
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

From 1g on, a run with no database configured is on H2 in memory: 9.6–10.0k steps/s in `SERVER` and
50.8–54.5k in `LOCAL_ASYNC`, against 20.9–27.6k and 112–115k on the removed in-memory store
(interleaved, same laptop).
