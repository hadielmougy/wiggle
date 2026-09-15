# Error handling

Three things look like "it stopped" and are not the same event. Getting them confused is what makes
a workflow system noisy — alarms for orders that simply did not apply, and silence for the ones that
actually broke.

| what happened | how you say it | the instance ends | should it page someone |
|---|---|---|---|
| this might work on a retry | throw anything | retries, then `FAILED` | yes, once retries are exhausted |
| this will never work | `PermanentActivityException` | `FAILED`, immediately | yes |
| this one does not apply | a gate returning `false` | `COMPLETED` | **no** |

---

## 1. Throwing is how a step fails

There is no result type to return and no error channel to write to. A handler that throws has
failed, the engine catches it, and the step's retry policy decides what happens next:

<!-- snippet: errors/policies -->
```java
return FlowSpec.define("orders", Order.class, OrderSteps.class, (f, s) -> f
        // no policy: inherits the workflow default, which is retry forever
        .thenApply(s::validate)
        // a gate is not an error -- false ends the instance successfully
        .thenFilter(s::inStock)
        // this one talks to a payment gateway, so back off rather than hammer it
        .thenApply(s::charge, RetryPolicy.exponential(5, Duration.ofMillis(100)))
        // and this one is not worth retrying at all
        .thenApply(s::confirm, RetryPolicy.none()));
```

- `RetryPolicy.exponential(n, initial)` — n attempts, backing off. The one you want for anything
  across a network.
- `RetryPolicy.fixed(n, delay)` — n attempts, evenly spaced.
- `RetryPolicy.none()` — one attempt. The declarative way to say "do not retry this step".
- `RetryPolicy.forever()` — the default when a workflow sets none. Suited to a step that is
  genuinely idempotent and whose dependency will come back.

A step with no policy of its own inherits the workflow default given to `FlowSpec.define`. Retries
are **at-least-once**, so handlers must be idempotent: a retry can follow a step that actually
succeeded but whose result never got back.

## 2. `PermanentActivityException` when a retry cannot help

Some failures are not transient. A malformed order, a declined card, a 4xx: retrying burns the
budget and delays the inevitable.

<!-- snippet: errors/permanent -->
```java
@ForFlow("orders")
public static class OrderHandlers implements OrderSteps {

    @Override public Order validate(Order o) {
        if (o.quantity() <= 0) {
            // retrying cannot make this order valid: fail now, no attempts left on the clock
            throw new PermanentActivityException("order " + o.id() + " has no items");
        }
        return o.withStatus("VALIDATED");
    }

    @Override public boolean inStock(Order o) {
        return o.quantity() <= 50;      // false: nothing to do, end cleanly
    }

    @Override public Order charge(Order o) {
        gateway.charge(o);              // throws on a network blip -> retried per the policy
        return o.withStatus("CHARGED");
    }

    @Override public Order confirm(Order o) { return o.withStatus("CONFIRMED"); }
}
```

`PermanentActivityException` fails the instance on the **first** attempt whatever the step's policy
says. With `fixed(3, …)` on the step, an ordinary exception runs the handler three times and a
permanent one runs it once — same configuration, different outcome, chosen by the code that knows.

Wrap the cause when you have one; it ends up on the instance:

<!-- snippet: errors/wrap -->
```java
try {
    gateway.charge(o);
} catch (CardDeclinedException e) {
    throw new PermanentActivityException("declined for " + o.id(), e);
}
```

`RetryPolicy.none()` reaches the same place from the other side. Use the policy when *the step* is
never worth retrying, and the exception when *this particular failure* is not — a step that usually
should retry, but not for this input.

## 3. A gate is not a failure

`thenFilter` returning `false` ends the instance **successfully**. It is the difference between
"this order had nothing to ship" and "shipping broke", and it is worth being deliberate about,
because only one of those should wake someone up.

The steps after the gate do not run, the context keeps whatever the last step returned, and nothing
is recorded as an error. Reach for it when a flow legitimately does not apply; reach for an
exception when something is wrong.

## 4. Stopping an instance from outside

<!-- snippet: errors/cancel -->
```java
client.cancel(instanceId, "customer withdrew the order");
```

The instance ends `CANCELLED` and keeps the reason. In-flight work is abandoned; a running
sub-workflow is cancelled with its parent.

## 5. When a failure should undo what already happened

A `FAILED` instance stops where it is. It does **not** roll back — earlier steps touched other
systems, and the engine cannot know how to reverse a captured payment or a reserved pallet.

If you declare compensators, failure instead unwinds them in reverse order: the instance goes
`COMPENSATING`, each undo runs as a durable task under the step's own retry policy, and it settles
`COMPENSATED` with the original failure preserved. A compensator that exhausts its own retries lands
`COMPENSATION_FAILED` — loudly, because the one thing worse than a stuck saga is a stuck saga
reported as success. See [Saga / compensation](/patterns/saga/).

## 6. What is *not* an error

Two things look alarming in logs and are routine:

- **A lease expiring.** A worker that dies mid-step does not fail the instance. The lease runs out,
  the step becomes claimable again, and another worker picks it up from the last completed step.
  Nothing replays.
- **A step being dispatched twice.** That is the at-least-once guarantee doing its job, which is why
  handlers must be idempotent.

And one thing that is not an error but *is* a problem: work nobody can claim. A step on a queue no
worker polls, or on a version every worker has scoped itself out of, sits dispatchable forever. It
never fails, so nothing alerts. See
[backlog coverage](/docs/onboarding/#75-backlog-coverage-work-nothing-can-claim) and
[Versioning](/docs/versioning/).

## 7. The statuses

| status | meaning |
|---|---|
| `RUNNING` | in flight |
| `COMPLETED` | reached an end — including via a false gate |
| `FAILED` | a step exhausted its retries, or threw `PermanentActivityException` |
| `CANCELLED` | stopped from outside with `client.cancel` |
| `COMPENSATING` | failed, and is running the declared undos in reverse |
| `COMPENSATED` | the undos finished; the original failure is preserved on the instance |
| `COMPENSATION_FAILED` | an undo exhausted its own retries — needs a human |

Everything on this page is asserted by `ErrorHandlingTest`, which counts handler attempts rather
than only reading the final status: a page that got the retry count wrong would still look right if
all you checked was `FAILED`.
