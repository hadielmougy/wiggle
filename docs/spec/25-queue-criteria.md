# 25 — Queue criteria (proposed)

← [Worker contract](20-worker.md) · [Index](00-index.md) · Next: [Engine semantics](30-engine.md)

**Status: proposed.** Nothing in this chapter is implemented yet. Criteria over task metadata and
the attributes of [§3](#3-attributes) are built in the two changes that follow this one; the
portal's three coverage states ([WGL-QC-081](#44-long-poll-wake-ups-and-the-backlog)) and the warning
of [WGL-QC-045](#3-attributes) are not. The benchmark of [WGL-QC-100](#6-cost) has run. Every requirement here is
*unverified* ([WGL-GEN-001](00-index.md)) until the change that builds it names its test. Where this
chapter and the rest of the suite disagree, the rest of the suite describes the code as it is today.

Today a worker says *which queues* it serves, and optionally which `(workflow, version)` pairs, and
the claim hands it the oldest ready tasks on those queues ([WGL-WRK-030](20-worker.md),
[WGL-STOR-021](80-storage.md)). This chapter adds **queue criteria**: a predicate, built with an API
shaped like the JPA Criteria API, that narrows which tasks of those queues a poll may claim. A
predicate can test the task's own metadata (workflow, version, step, attempt, correlation id, ...) and
the **business data** of the instance, through **attributes** the flow declares
([§3](#3-attributes)).

Polling by queue names stays exactly as it is. Criteria are a second, optional form of the same poll.

## 1. Model

| Term | Meaning |
|---|---|
| **criteria** | A non-empty set of queues plus an optional predicate. A poll that carries one claims only ready tasks on those queues for which the predicate holds. |
| **predicate** | A tree of comparisons joined by `and`, `or` and `not`. |
| **comparison** | One field, one operator, and literal arguments: `equal(Task_.STEP, "charge")`. |
| **field** | A token column from a fixed list ([§2.2](#22-fields)), or a declared attribute. |
| **attribute** | A named, typed value a flow declares as a path into its context. The server extracts it each time a task becomes ready ([§3](#3-attributes)). |

**WGL-QC-001** (MUST) A poll that carries no criteria MUST behave exactly as today: the claim, its
order, its statements, and the wake-ups are unchanged.

**WGL-QC-002** (MUST) A flow that declares no attributes MUST cost nothing extra when its steps are
dispatched: no added statement, column write, or extraction.

## 2. The API

### 2.1 Building criteria

```java
QueueCriteria eu = QueueCriteria.from("payments", "refunds")
        .where(cb -> cb.and(
                cb.equal(Task_.WORKFLOW, "orders"),
                cb.in(Task_.STEP, "charge", "refund"),
                cb.lessThan(Task_.ATTEMPT, 3),
                cb.equal(Task_.attr("region", String.class), "eu"),
                cb.greaterThanOrEqualTo(Task_.attr("amount", BigDecimal.class), new BigDecimal("1000")),
                cb.not(cb.like(Task_.CORRELATION_ID, "test-%"))));

Worker w = new Worker(client, "pay-eu-1", WorkerOptions.defaults().withCriteria(eu))
        .registerHandler(new PaymentHandlers())
        .start();
```

**WGL-QC-010** (MUST) `QueueCriteria.from(String... queues)` MUST be the only way to create criteria,
and MUST reject an empty or blank queue list. Criteria therefore never span every queue.

**WGL-QC-011** (MUST) `CriteriaBuilder` MUST provide `equal`, `notEqual`, `in`, `lessThan`,
`lessThanOrEqualTo`, `greaterThan`, `greaterThanOrEqualTo`, `between`, `like`, `isNull`, `isNotNull`,
`and`, `or` and `not`, with the JPA meanings.

**WGL-QC-012** (MUST) A field is a typed `Attribute<T>` (`Task_.WORKFLOW` is `Attribute<String>`,
`Task_.ATTEMPT` is `Attribute<Integer>`), and every comparison's arguments MUST have the field's type,
so a mismatch fails to compile. `Task_.attr(name, type)` creates a typed reference to a declared
attribute.

**WGL-QC-013** (MUST) Criteria MUST be an immutable value with value equality. A worker builds them
once, and the same criteria object goes on every poll.

**WGL-QC-014** (MUST) `WorkerOptions.withCriteria(...)` and `withQueues(...)` MUST be mutually
exclusive. Setting both MUST fail `start()`. The worker's served queue set
([WGL-WRK-030](20-worker.md)) is the criteria's queues.

**WGL-QC-015** (MUST NOT) Criteria MUST NOT have an `orderBy`. The claim keeps its order: oldest
instance first, then longest ready ([WGL-STOR-021](80-storage.md)).

### 2.2 Fields

| Field | Type | Source |
|---|---|---|
| `WORKFLOW` | `String` | `wf_token.workflow` |
| `VERSION` | `Integer` | `wf_token.version` |
| `QUEUE` | `String` | `wf_token.queue` (narrows within the criteria's queues) |
| `STEP` | `String` | the step's name, the part of `wf_token.activity` after `workflow#`; `equal`, `notEqual` and `in` only |
| `NODE_ID` | `String` | `wf_token.node_id` |
| `KIND` | `String` | `TASK` or `PREDICATE` |
| `ATTEMPT` | `Integer` | `wf_token.attempt` |
| `CORRELATION_ID` | `String` | `wf_token.correlation_id`, copied from the instance when the token is created ([§5](#5-storage)) |
| `INSTANCE_CREATED_AT` | `Instant` | `wf_token.inst_created_at` |
| `attr(name, type)` | `String`, `Long`, `BigDecimal`, `Boolean` | a declared attribute ([§3](#3-attributes)) |

**WGL-QC-020** (MUST) The list of fields is closed. A field is sent on the wire as an enum value, and
the server MUST map each value to a fixed column. A field name, or any other client-supplied text,
MUST NOT reach SQL as anything but a bound value.

### 2.3 Semantics

**WGL-QC-030** (MUST) Evaluation is two-valued. A comparison against an absent value (an attribute the
task does not carry, or a null correlation id) is **false**, and `not` of it is **true**. Only
`isNull` and `isNotNull` test for absence. Every backend MUST give the same answer: SQL's three-valued
`NOT (NULL)` MUST NOT leak through.

**WGL-QC-031** (MUST) `like` supports `%` and `_` only, with `\` as the escape character, and is
case-sensitive on every dialect.

**WGL-QC-032** (MUST) Numeric attributes compare numerically, `LONG` and `DECIMAL` with each other,
and strings compare by code point. A boolean supports `equal`, `notEqual` and `in` only. An
attribute compared with a value of another type is false: the attribute `"1000"` is not equal to the
number `1000`. The literals of one comparison MUST all have one type.

## 3. Attributes

Business data lives in the instance's context, which is opaque JSON to the claim query. The context
can't be queried at claim time: there is no portable JSON-path SQL (H2 has none), and an unindexed
JSON scan on the hottest query in the system is not acceptable. So a flow **declares** the values it
wants to route on, and the server copies them out of the context into an indexed table when a task
becomes ready.

```java
FlowSpec orders = FlowSpec.define("orders", 3, Order.class, OrderSteps.class, (f, s) -> f
        .attribute("region", AttrType.STRING, "customer.address.region")
        .attribute("amount", AttrType.DECIMAL, "total")
        .attribute("vip",    AttrType.BOOLEAN, "customer.vip")
        .thenApply(s::validate)
        .thenApply(s::charge, "payments"));
```

**WGL-QC-040** (MUST) An attribute is declared on the flow with a name, a type (`STRING`, `LONG`,
`DECIMAL`, `BOOLEAN`) and a dotted path into the context. Declarations are part of the topology:
they are compiled into the graph, stored with the definition version, and published with it. They
are not lambdas ([WGL-AUTH](10-authoring.md)).

**WGL-QC-041** (MUST) Names MUST match `[a-z][a-z0-9_]{0,62}` and be unique within a version. A flow
version MAY declare at most 16 attributes. Publishing MUST reject a violation of either rule.

**WGL-QC-042** (MUST) The server MUST extract a task's attributes from the context **that step would
receive**: the instance context, a fork arm's view, or a forEach item's value. It does this each
time the task's token becomes `READY` and when a local-chain continuation is leased
([§4.3](#43-local-execution-chains)). Attributes therefore follow the data as it changes from step to
step.

**WGL-QC-043** (MUST) A path that does not resolve, resolves to JSON null, or resolves to a value
that does not fit the declared type (an object, an array, a string under `LONG`, a number under
`STRING`, a fraction under `LONG`) MUST leave the attribute **absent**. It is never an error, and the
step still dispatches. Numbers are read from their decimal text, so every JSON reader agrees. A
`STRING` value longer than 256 characters, and a `DECIMAL` with more than 10 fraction digits or 28
integer digits, MUST also be absent, so every store keeps a value exactly. A decimal literal outside
the same range is `INVALID_ARGUMENT`.

**WGL-QC-046** (MUST) A token's attributes are read once, when it becomes ready or is leased as a
continuation. A retry keeps them: a failed attempt leaves the context as it was. A compensation task
carries no attributes.

**WGL-QC-044** (MUST) Only declared paths are copied out of the context. Undeclared data, and the
context itself, MUST NOT be written to the attribute table.

**WGL-QC-045** (SHOULD) A criteria that names an attribute none of its queues' current definitions
declare SHOULD be logged once per worker and shown as a warning on the portal's Backlog tab. It is
not rejected: the definition declaring it may be published later.

## 4. The poll

### 4.1 Wire

```proto
message PollRequest {
    string worker_id = 1;
    repeated string queues = 2;           // the queue-name form: unchanged
    int32 max = 3;
    int64 lease_millis = 4;
    int64 wait_millis = 5;
    repeated WorkflowVersion versions = 6;
    QueueCriteria criteria = 7;           // the criteria form
}

message QueueCriteria {
    repeated string queues = 1;           // required, non-empty
    CriteriaPredicate where = 2;          // absent = every task on those queues
}

message CriteriaPredicate {
    oneof p {
        CriteriaComparison cmp = 1;
        CriteriaJunction all_of = 2;      // and, or and not are Python keywords
        CriteriaJunction any_of = 3;
        CriteriaPredicate negated = 4;
    }
}

message CriteriaJunction { repeated CriteriaPredicate terms = 1; }

message CriteriaComparison {
    string field = 1;                     // a Field name, like every enum-valued string on this wire
    string op = 2;                        // an Op name
    repeated CriteriaLiteral args = 3;
    string attribute = 4;                 // set iff field == ATTRIBUTE (§3)
}

message CriteriaLiteral {                 // exact, unlike google.protobuf.Value's double
    oneof v {
        string text = 1;
        sint64 integer = 2;
        string decimal = 3;               // §3
        bool flag = 4;                    // §3
    }
}
```

**WGL-QC-050** (MUST) A poll MUST carry **either** `queues` **or** `criteria`. Both set is
`INVALID_ARGUMENT`. `versions` is allowed with either form and is ANDed with the criteria, so
version-scoped registration ([WGL-WRK-024](20-worker.md)) keeps working.

**WGL-QC-051** (MUST) The server MUST validate criteria before any claim and answer
`INVALID_ARGUMENT` on any of the following: an empty queue list, a tree deeper than 8, more than 64
nodes, an `in` with more than 256 arguments, an empty `and`/`or`, an unknown field or operator, an argument
count wrong for its operator, an argument whose literal type does not fit its field, an ordering or
`like` on `STEP`, a text argument longer than 256 characters, or a `like` pattern ending in an
escape.

**WGL-QC-052** (MUST) Criteria can only narrow a poll. They MUST NOT let a worker claim a task its
authorization ([gRPC authz](70-api.md)) would not let it claim by queue name.

### 4.2 The claim

**WGL-QC-060** (MUST) `Tx.claimTasks` takes a `ClaimFilter` in place of the queue and version sets.
The filter is the queues, the versions, and the predicate. The queue-name form is a filter with no
predicate.

**WGL-QC-061** (MUST) On JDBC the predicate MUST compile to a SQL fragment with named binds, added
to the existing candidate filter of both claim paths: the per-queue lateral pick with `SKIP LOCKED`,
and the compare-and-set fallback ([WGL-STOR-021](80-storage.md), [WGL-STOR-022](80-storage.md)). The
claim stays one statement on PostgreSQL. A comparison on an attribute compiles to `EXISTS` (or
`NOT EXISTS` for `isNull`) against the attribute table, which gives two-valued results
([WGL-QC-030](#23-semantics)) without extra handling. A nullable token column is wrapped so that an
absent value compares false.

**WGL-QC-062** (MUST) In memory, the same predicate MUST compile to a `Predicate<Token>` applied as
the claimable index is walked.

**WGL-QC-063** (MUST) Sharded storage MUST pass the filter unchanged to each shard's claim.

**WGL-QC-064** (MUST) One conformance suite MUST run the same table of tasks and criteria against
in-memory storage, H2, and every SQL dialect in its real container image. The cases MUST cover every
operator, nesting, absent values under `not`, `like` escapes, and numeric against string attributes.
Every backend MUST claim exactly the same task ids.

### 4.3 Local execution chains

In `LOCAL_SYNC`/`LOCAL_ASYNC` the worker decides whether to run the next step itself
(`LocalRun.advance`): it carries on when it serves the next step's queue, and the server leases it the
continuation (`Tokens.createLeased`) without the step ever being polled.

**WGL-QC-069** (MUST) A worker polling with criteria MUST hand back at a next step its criteria do not
match, evaluated on what the activation carries. A `TaskActivation` therefore carries the instance's
`correlation_id` and `instance_created_at`.

**WGL-QC-070** (MUST) When the reporting worker polled with criteria, the server MUST lease it a
continuation only if the continuation's queue is in the criteria's queues **and** the predicate holds
for the continuation, with its attributes freshly extracted ([WGL-QC-042](#3-attributes)). Otherwise
the continuation is parked `READY` for whoever matches. A run that reports a step after a
continuation its criteria do not match MUST be refused (400). Where the mismatch does not depend on
attributes it MUST be refused before any of the run is written; one that does is found as the
continuation is created, once its context is known, and the transaction is rolled back. This
extends [WGL-WRK-031](20-worker.md): a worker cannot run, through a chain, a step its criteria would
not claim.

**WGL-QC-071** (MUST) A report and the poll before it may reach different server nodes, so each
`ReportedRun` MUST carry the worker's criteria (`criteria = 5`). The server MUST NOT depend on per-node poller
state to make this check.

### 4.4 Long-poll wake-ups and the backlog

**WGL-QC-080** (MUST) Wake-ups stay per queue: a task becoming ready wakes the pollers of its queue,
including those whose predicate will reject it. Such a poller claims nothing and waits again. This
means spurious wake-ups, never missed ones.

**WGL-QC-081** (MUST) `PollerRegistry` MUST record each poller's criteria. Until the portal shows the
three states below, a slice counts as covered unless every live poller's predicate is false for it. The portal's Backlog tab
MUST evaluate them against each `(workflow, version, queue, step)` slice and show the slice as
*covered*, *conditionally covered* (only an attribute predicate stands between a poller and the
slice, and the slice's attribute values are not known) or *uncovered*. A task that no poller's
criteria match stays `READY` indefinitely, exactly like a task on a queue nobody serves.

## 5. Storage

```sql
ALTER TABLE wf_token ADD COLUMN correlation_id VARCHAR(200);  -- matches wf_instance

CREATE TABLE wf_token_attr (
    token_id  VARCHAR(128)   NOT NULL,
    name      VARCHAR(63)    NOT NULL,
    t         CHAR(1)        NOT NULL,   -- S (s_val), B ('true'/'false' in s_val), N (n_val)
    s_val     VARCHAR(256),
    n_val     DECIMAL(38, 10),
    PRIMARY KEY (token_id, name)
);
CREATE INDEX ix_token_attr_s ON wf_token_attr (name, s_val);
CREATE INDEX ix_token_attr_n ON wf_token_attr (name, n_val);
```

The primary key serves the claim's probe of each candidate row as it walks the queue. The value
indexes serve the plan PostgreSQL picks for a rare predicate: it starts from the matching attribute
rows instead ([§6](#6-cost)).

**WGL-QC-090** (MUST) A token's attribute rows MUST be written in the transaction that inserts or
readies the token, through the buffered write path, as one batch. For a flow with no attributes no
row and no statement is written ([WGL-QC-002](#1-model)). When a written token's attributes are read
again, its old rows MUST be replaced.

**WGL-QC-091** (MUST) Attribute rows MUST be deleted with their token by retention (found through the
token's instance, so the table needs no index of its own), and never on claim, so the claim adds no
statement.

**WGL-QC-092** (MUST) `correlation_id` MUST be copied onto every token when it is created, the same
way `inst_created_at` is, so the claim needs no join to `wf_instance`.

**WGL-QC-093** (MUST) The migrations add the column (34) and the table (35). On a dialect where
`ADD COLUMN` rewrites the table, it MUST follow the large-table guidance of
[WGL-STOR](80-storage.md). Existing tokens keep a null `correlation_id` and no attributes, which
match only `isNull`.

## 6. Cost

The claim walks each queue's `ix_token_ready_age` index in age order and tests the predicate on
each row it reaches, until it has `max` tasks. When the planner expects a predicate on an attribute
to be rare, it starts from the attribute table's value index instead, then sorts the matches by age.

**WGL-QC-100** (MUST) `postgres/ClaimCriteriaBench` MUST measure the claim on PostgreSQL over queue
backlogs of 10⁴, 10⁵ and 10⁶ rows: by queue name, with a 1% predicate spread evenly (task field and
attribute), with an attribute only the newest 1% carry, and with predicates that match nothing.
`StepStatementsTest` MUST show the per-step statement count unchanged for a flow without attributes.

Measured 2026-10-11 on PostgreSQL 17, default settings, on a laptop; a claim of 10, rolled back,
p50 in ms, two runs agreeing:

| Predicate | 10⁴ | 10⁵ | 10⁶ | 10⁶ without the value indexes |
|---|---|---|---|---|
| Queue name only | 0.8 | 0.7 | 0.7 | 0.8 |
| Task field, 1% spread evenly | 0.9 | 0.8 | 0.9 | 0.9 |
| Attribute, 1% spread evenly | 2.4 | 2.8 | 3.3 | 3.3 |
| Attribute, only the newest 1% match | 1.0 | 2.7 | 22 | 70 |
| Attribute, nothing matches | 0.7 | 0.6 | 0.7 | 79 |
| Task field, nothing matches | 2.7 | 22 | 255 | 256 |

A common predicate costs the same at any depth. A rare attribute predicate costs what its matches
cost, not what the queue costs. A rare **task-field** predicate reads the whole queue on every poll,
and an idle worker re-polls about every 100 ms. A version-scoped poll for a version with no ready
tasks, which predates criteria, does the same (154 ms at 10⁶). A partial index such as
`(queue, correlation_id text_pattern_ops) WHERE status='READY'` takes the no-match correlation case to
0.09 ms, but every token entering `READY` would pay for it, criteria or not.

**WGL-QC-101** (SHOULD) A value that is often absent from a deep queue SHOULD be routed on as an
attribute, not a task field. Indexes on task fields are not added until a measured workload needs one.

## 7. Out of scope

- `orderBy` and priority ([WGL-QC-015](#21-building-criteria)).
- Predicates over undeclared context data, or JSON-path queries at claim time ([§3](#3-attributes)).
- Joins to other tables, subqueries, functions, and arithmetic in predicates.
- Criteria on `PollEvents`.
- The Python and Go clients. They keep polling by queue names ([WGL-QC-001](#1-model)) and can add
  criteria later against the same wire contract.

## 8. Alternatives considered

- **Required criteria, `queues` field removed.** This is a wire break for every client, including
  the separately vendored Python client, and gains nothing a mutual-exclusion check doesn't already
  give. Rejected.
- **Handler-set attributes** (`Step.tag("region", "eu")`), in the style of Temporal's search
  attributes. Routing would depend on handler code having run, and the first step of an instance
  couldn't be routed. Declared paths need no handler code, and the topology states them. Rejected
  for now; a start-time attribute override could come later.
- **JSON column on `wf_token` with dialect JSON operators.** No extra table, but H2 has no JSON path
  support, each dialect's syntax and indexing differ, and an expression index per attribute would
  be a schema change per flow. Rejected.
- **Fixed attribute slot columns on `wf_token`** (`attr1..attr4`). Indexable, but a slot means
  different things in different workflows, the limit is tiny, and the columns widen every token.
  Rejected.
