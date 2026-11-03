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
3. Test support, in `boot`'s test fixtures: `@WiggleBootTest` (one node, H2 in memory, ports from
   `TestPorts`, fast timings, the process's own `WIGGLE_*` variables hidden), the `portal`, `probe` and
   `postgres` profiles, and `WiggleTestNodes` for several nodes on one database. The pilot converted
   `HttpApiTest` and `CompetingConsumersTest` and added `ClusterNodesTest`; see the pilot below.
4. Tests converted in batches: every test that started a node now runs on a node of the Boot application
   (`@WiggleBootTest`, `WiggleTestNodes`, or `WiggleTestServer` on the test's own `ServerConfig`), except
   the four that test what phase 5 retires: `TlsTest`, `HealthzTest` and `ServerRoleTest` on a node's own
   servers, `PortalTest` on `Portal.fromEnvironment`. See the results below.
5. Packaging: the image runs the Boot jar and the release archive is Boot's distribution; `dist`,
   `HealthServer`, the node's own API server and the console's own Tomcat are retired. The server runs
   as its own process: embedding it is no longer supported, and the examples and tests run the Boot
   application in their JVM through `WiggleNode`. See the results below.
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

## Phase 3 pilot

| Measure | Result |
|---|---|
| `HttpApiTest`, 3 runs | 0.82–0.88 s with a server per test, 0.51–0.53 s on one shared node |
| `CompetingConsumersTest`, 3 runs | 0.62–0.63 s with a server per test, 0.49–0.52 s on one shared node |
| Starting a node's context | about 0.9 s, once per distinct test configuration |
| `ClusterNodesTest`, two nodes, two tests | 1.3–1.4 s |
| Whole suite with `tests` on Boot's managed versions (JUnit 6, Tomcat 11, Hikari 7, H2 2.4) | 112 s, against 110 s; nothing new fails |

Of 142 test classes, 57 start a node and 47 work on a store or an engine without one.

What the pilot found:

- Tests that share a node must not share a queue. A worker closed by one test leaves its long poll
  on the server, and that poll can still claim the next test's token, which then waits out its lease.
  Before, each test's own server stopped the poll. A worker stopping mid-poll strands tokens the same
  way in production; the server should release what it claimed when it cannot deliver it.
- Tests that set a different configuration per method need one context each, through `@Nested`
  classes or test properties. Spring caches up to 32 of them.

Recommended for phase 4: not every test runs in a Spring context. The 57 that start a node move to
`@WiggleBootTest` or `WiggleTestNodes`, each test on queues of its own. Tests of a store or an engine
stay plain JUnit on `PostgresStorageFactory.inMemory()`; a context would add a node they never call.

## Phase 4 results

| Measure | Result |
|---|---|
| Classes moved to the Boot application | 55 (23 + 27 in `tests`, 3 in `console`, 2 in the pilot), plus `ClusterNodesTest` |
| Test suite, `./gradlew test --rerun`, 2 runs | 145 s, against 112 s before |
| The same on live PostgreSQL 16 | 165 s, against 131 s |
| Starting a node, warm | about 85 ms as a Boot application, 15 ms as a `WiggleServer` |
| The same with Actuator's metrics, observations and JMX off | about 70 ms |

The suite starts about 350 nodes, and nearly all the added time is their start-up. Each converted test
still has a node of its own, as it had a server of its own. Sharing one node across a class's tests
would save most of it, test by test, once each test is on queues of its own.

## Phase 5 results

PostgreSQL 16, `:example:bench`, steps per second, three interleaved runs each, against 1g, whose
node still served its API on a Tomcat of its own:

| Mode | 1g | Boot application |
|---|---|---|
| `SERVER`, 1,000 instances | 2,550–2,593 | 2,343–2,443 |
| `LOCAL_ASYNC`, 6,000 instances | 28,294–29,389 | 26,853–27,643 |

About 6% and 4% lower. Boot puts its servlet filters in front of every call, MVC's per-request
observation among them; that is for phase 6 to measure and remove where the API does not need it.

