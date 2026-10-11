# Cookbook: a multi-agent research team

A team of LLM agents writes a research brief, and every agent turn is a durable step. A supervisor
splits the topic into subtasks, specialists answer them in parallel, a writer and a critic revise
the draft until the critic approves, and a human editor signs it off. Read the source at
[`example/src/main/java/com/wiggle/agents/ResearchTeam.java`](../example/src/main/java/com/wiggle/agents/ResearchTeam.java)
alongside this page; run it end to end, offline, with:

```bash
./gradlew :example:runAgents --args="vector databases"
```

and against Claude with:

```bash
ANTHROPIC_API_KEY=... ./gradlew :example-claude:runAgents --args="vector databases"
```

Nothing here is an agent framework. The team is an ordinary workflow over operators the
[cookbook](cookbook.md) already covers — a spawning step and its combine, `repeatWhile`,
`thenAwait` with a deadline, `oneOf` — and an agent is an ordinary handler method that calls a model.

## The team

| Agent | Step | What it does |
|---|---|---|
| supervisor | `plan` | splits the topic into `search:` and `numbers:` subtasks; starts one branch per subtask |
| researcher | `research` | answers a `search:` subtask |
| analyst | `analyse` | answers a `numbers:` subtask with an estimate |
| fact checker | `factCheck` | checks the analyst's estimate — the second agent in the same branch |
| writer | `draft`, `revise` | writes the brief from every finding, then revises it against the critique |
| critic | `critique` | replies `APPROVED`, or says what to change |
| editor (human) | `editor-approval` signal | approves or rejects; silence past the deadline shelves the brief |

## The topology

<!-- snippet: agents/topology -->
```java
return FlowSpec.define(FLOW, 1, Brief.class, TeamSteps.class, (f, s) -> {
    var reviewed = f
            .thenApply(s::plan, MODEL_RETRY)       // starts one specialist branch per subtask
            .combine(s::collect)                   // runs once every specialist is done
            .thenApply(s::draft, MODEL_RETRY)
            .repeatWhile(s::needsRevision, loop -> loop
                    .thenApply(s::critique, MODEL_RETRY)
                    .thenApply(s::revise, MODEL_RETRY))
            // no worker is held while the editor reads
            .thenAwait(APPROVAL, approvalTimeout, late -> late.thenApply(s::shelveUnanswered));

    return Wiggle.oneOf(
            reviewed.when(s::approved).thenApply(s::publish),
            reviewed.otherwise().thenApply(s::shelve));
});
```

Read it top to bottom:

- **`plan` followed directly by `combine`** makes `plan` a spawning step: whatever branches its
  handler creates run in isolation, and `collect` runs once every one of them is done. How many
  specialists run, and which, is decided by the model at run time — the topology does not know.
- **`repeatWhile`** is do-while: the critic always reads the first draft. The condition caps the
  loop, so a critic that is never satisfied cannot spend tokens forever.
- **`thenAwait`** parks the instance until the editor's signal arrives. No worker is held while the
  editor reads; the deadline branch runs `shelveUnanswered` instead and rejoins.
- **`MODEL_RETRY`** on every agent step: a rate-limited or timed-out model call is retried by the
  engine, with backoff, not by the agent.

## The supervisor: a team shaped at run time

<!-- snippet: agents/supervisor -->
```java
public Brief plan(Brief b) {
    String reply = model.complete(new Model.Prompt(SUPERVISOR, """
            You lead a research team. Split the topic into at most %d subtasks, one per \
            line, each written as "search: <question>" or "numbers: <question>". \
            Reply with the lines only.""".formatted(MAX_SUBTASKS), b.topic()));

    for (Task t : parseTasks(reply)) {
        if (t.kind().equals("numbers")) {
            Step.create(t).thenApply(this::analyse).thenApply(this::factCheck);
        } else {
            Step.create(t).thenApply(this::research);
        }
    }
    return b;
}
```

`Step.create(t)` opens a branch whose whole context is the `Task`. A `numbers` subtask chains two
agents — the analyst's `Finding` is the fact checker's input — while a `search` subtask needs one.
The branches inherit `plan`'s retry policy and queue. They are buffered on the worker and committed
with `plan`'s report, so a supervisor that crashes half way through its loop has started no
specialist at all; the retried `plan` starts them all.

## The specialists and the combine

<!-- snippet: agents/specialists -->
```java
public Finding research(Task t) {
    return new Finding(RESEARCHER, t.question(), model.complete(new Model.Prompt(RESEARCHER,
            "You are a researcher. Answer the question in a short paragraph of facts.",
            t.question())));
}

public Finding analyse(Task t) {
    return new Finding(ANALYST, t.question(), model.complete(new Model.Prompt(ANALYST,
            "You are an analyst. Answer with a numeric estimate and how you reached it.",
            t.question())));
}

public Finding factCheck(Finding f) {
    return new Finding(FACT_CHECKER, f.question(), model.complete(new Model.Prompt(FACT_CHECKER,
            "You are a fact checker. Return the answer corrected where it is wrong, "
                    + "unchanged where it is right.",
            f.answer())));
}

public Brief collect(Brief base, List<Finding> findings) {
    return new Brief(base.topic(), List.copyOf(findings), null, null, 0, null, null);
}
```

A specialist sees its subtask and nothing else — not the topic, not the other findings. That is the
point of running branches on isolated copies: what an agent knows is exactly what the workflow
handed it. The combine's parameters bind by type, so `List<Finding>` collects every branch's
result, whichever agent produced it, and `Brief base` is the context `plan` returned.

## The critic loop

<!-- snippet: agents/revision -->
```java
/** Rewrites the draft against the critique; an approved draft passes through unchanged. */
public Brief revise(Brief b) {
    if (isApproved(b)) return b;
    return withDraft(b, model.complete(new Model.Prompt(WRITER,
            "You are a writer. Revise the brief to address the critique.",
            b.draft() + "\n\nCritique: " + b.critique())), b.revisions() + 1);
}

public boolean needsRevision(Brief b) {
    return !isApproved(b) && b.revisions() < MAX_REVISIONS;
}
```

`revise` passes an approved draft through unchanged, which is what lets the loop body stay a plain
`critique → revise` while the condition decides whether to go round again.

## The editor

The editor is a person, so the workflow waits for them by signal. The demo plays the editor:

<!-- snippet: agents-demo/editor -->
```java
String id = client.start(spec, new ResearchTeam.Brief(topic));
awaitEditor(client, id, Duration.ofMinutes(10));
System.out.println("brief is with the editor; approving");
client.signal(id, ResearchTeam.APPROVAL, Map.of("decision", "approved"));
```

The signal's payload merges into the context, so `decision` arrives as a field of the `Brief`, and
`approved` routes on it. `awaitEditor` polls the instance's tokens until one is parked on
`editor-approval`; a real editor would see the brief in the portal or your own UI and signal from
there.

## The model

Agents call one interface. The team never sees which model is behind it:

<!-- snippet: agents-model/model -->
```java
public interface Model {

    /**
     * @param agent  the calling agent's name, for routing, logging or a scripted reply
     * @param system the agent's standing instructions
     * @param user   this turn's input
     */
    record Prompt(String agent, String system, String user) {}

    String complete(Prompt prompt);
}
```

Keep an implementation stateless. Whatever an agent must remember travels in the context, which is
what makes a step re-runnable on another worker after a crash.

- **`ScriptedModel`** gives each agent a canned reply built from its input. The critic rejects the
  first draft and approves the second, so one revision always runs. It is what the demo and
  `ResearchTeamTest` use.
- **`ClaudeModel`**, in the unpublished `:example-claude` module, calls the Claude Messages API.

<!-- snippet: agents-claude/complete -->
```java
@Override
public String complete(Prompt p) {
    MessageCreateParams params = MessageCreateParams.builder()
            .model(model)
            .maxTokens(16000L)
            .system(p.system())
            .addUserMessage(p.user())
            .fallbacksDefault()
            .addBeta(AnthropicBeta.SERVER_SIDE_FALLBACK_2026_07_01)
            .build();
    BetaMessage reply;
    try {
        reply = client.beta().messages().create(params);
    } catch (AnthropicServiceException e) {
        int status = e.statusCode();
        if (status == 408 || status == 409 || status == 429 || status >= 500) throw e;
        throw new PermanentActivityException(p.agent() + ": " + e.getMessage(), e);
    }
    if (reply.stopReason().filter(BetaStopReason.REFUSAL::equals).isPresent()) {
        throw new PermanentActivityException(p.agent() + ": the model declined this turn");
    }
    return reply.content().stream()
            .flatMap(block -> block.text().stream())
            .map(text -> text.text())
            .collect(Collectors.joining());
}
```

The SDK's own retries are off, so the step's retry policy is the only retry loop. Which failures it
retries is the adapter's call:

| Failure | Thrown as | What the engine does |
|---|---|---|
| 408, 409, 429, 5xx | the SDK's exception | retries the step with backoff |
| a refusal | — | the API retries it on a fallback model (`fallbacks: "default"`) |
| a refusal that survives the fallback | `PermanentActivityException` | fails the instance, no retry |
| any other 4xx | `PermanentActivityException` | fails the instance, no retry |

The model defaults to `claude-opus-5-5`; set `WIGGLE_CLAUDE_MODEL` to change it.

## What durability buys an agent team

| Event | Without a workflow engine | Here |
|---|---|---|
| a worker dies mid-call | the run is lost, or replayed from the start | only that agent's turn re-runs |
| the model rate-limits | every agent needs its own retry loop | one retry policy, on the step |
| five specialists | your own thread pool | branches, spread across every worker polling the queue |
| a human must sign off | a process holding a connection open | a parked instance and a signal, with a deadline |
| what did each agent see and say? | logs, if you wrote them | each step's input and output, recorded and shown in the portal |

## Tests

`ResearchTeamTest` runs the team against a real server with the scripted model. It covers:

- the publish path: three findings, one fact-checked, one revision, nine model calls;
- an editor who never answers shelves the brief at the deadline;
- a critic that never approves stops the loop at `MAX_REVISIONS`;
- a failed model call is retried by the step;
- the supervisor's reply parses leniently — list markers, unknown kinds, junk lines, a cap.
