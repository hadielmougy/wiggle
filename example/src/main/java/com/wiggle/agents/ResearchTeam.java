package com.wiggle.agents;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Step;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A team of LLM agents writing a research brief, each agent a step:
 *
 * <ol>
 *   <li>the <b>supervisor</b> splits the topic into subtasks and starts one specialist per subtask —
 *       a {@link Step#create} branch, so the team's shape is decided by the model at run time;</li>
 *   <li>a <b>researcher</b> answers a search subtask; an <b>analyst</b> answers a numbers subtask and
 *       hands its estimate to a <b>fact checker</b> — two agents chained in one branch;</li>
 *   <li>the combine gathers every finding, and the <b>writer</b> drafts the brief;</li>
 *   <li>the <b>critic</b> and the writer loop until the critic approves, at most
 *       {@value #MAX_REVISIONS} rounds;</li>
 *   <li>a human editor approves or rejects by signal; with no answer in time the brief is shelved.</li>
 * </ol>
 *
 * <p>Every agent turn is a durable step: a worker that dies mid-call loses only that call, the
 * specialists run in parallel across whatever workers are polling, and a rate-limited model call is
 * retried by the step's policy rather than by the agent. Agents share nothing but the context, so
 * what one agent knows is exactly what the workflow handed it.
 *
 * <p>The model is a {@link Model}; {@link ScriptedModel} runs the team offline.
 */
public final class ResearchTeam {

    public static final String FLOW = "agents-research-team";
    public static final String APPROVAL = "editor-approval";

    public static final String SUPERVISOR = "supervisor";
    public static final String RESEARCHER = "researcher";
    public static final String ANALYST = "analyst";
    public static final String FACT_CHECKER = "fact-checker";
    public static final String WRITER = "writer";
    public static final String CRITIC = "critic";

    /** The critic's whole reply when it has nothing left to change. */
    public static final String APPROVED = "APPROVED";

    public static final int MAX_REVISIONS = 3;
    public static final int MAX_SUBTASKS = 6;

    /** Model calls fail transiently (rate limits, timeouts); every agent step retries them. */
    static final RetryPolicy MODEL_RETRY = RetryPolicy.exponential(4, Duration.ofSeconds(1));

    private ResearchTeam() {}

    /**
     * The brief as it moves through the team. {@code decision} is what the editor's signal sets;
     * {@code status} is {@code PUBLISHED} or {@code SHELVED} once the instance completes.
     */
    public record Brief(String topic, List<Finding> findings, String draft, String critique,
                        int revisions, String decision, String status) {
        public Brief(String topic) { this(topic, List.of(), null, null, 0, null, null); }
    }

    /** One subtask the supervisor hands to a specialist: {@code kind} picks the specialist. */
    public record Task(String kind, String question) {}

    /** What a specialist branch hands back to the combine. */
    public record Finding(String agent, String question, String answer) {}

    /** The steps the topology names; {@link Agents} implements them. */
    public interface TeamSteps {
        Brief plan(Brief b);
        Brief collect(Brief base, List<Finding> findings);
        Brief draft(Brief b);
        Brief critique(Brief b);
        Brief revise(Brief b);
        boolean needsRevision(Brief b);
        Brief shelveUnanswered(Brief b);
        boolean approved(Brief b);
        Brief publish(Brief b);
        Brief shelve(Brief b);
    }

    /** The topology, with the editor given {@code approvalTimeout} to answer. */
    public static FlowSpec spec(Duration approvalTimeout) {
        // docs:begin topology
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
        // docs:end topology
    }

    @ForFlow(FLOW)
    public static final class Agents implements TeamSteps {

        private final Model model;

        public Agents(Model model) { this.model = model; }

        // docs:begin supervisor
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
        // docs:end supervisor

        // docs:begin specialists
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
        // docs:end specialists

        public Brief draft(Brief b) {
            return withDraft(b, model.complete(new Model.Prompt(WRITER,
                    "You are a writer. Turn the findings into a one-page research brief.",
                    b.topic() + "\n\n" + findingsText(b))), b.revisions());
        }

        public Brief critique(Brief b) {
            String reply = model.complete(new Model.Prompt(CRITIC,
                    "You are a critic. If the brief needs no change, reply " + APPROVED
                            + " and nothing else; otherwise say what to change.",
                    b.draft()));
            return new Brief(b.topic(), b.findings(), b.draft(), reply.strip(), b.revisions(),
                    b.decision(), b.status());
        }

        // docs:begin revision
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
        // docs:end revision

        public Brief shelveUnanswered(Brief b) { return withDecision(b, "timed-out"); }

        public boolean approved(Brief b) { return "approved".equals(b.decision()); }

        public Brief publish(Brief b) { return withStatus(b, "PUBLISHED"); }

        public Brief shelve(Brief b) { return withStatus(b, "SHELVED"); }

        private static boolean isApproved(Brief b) {
            return b.critique() != null && b.critique().equals(APPROVED);
        }

        private static Brief withDraft(Brief b, String draft, int revisions) {
            return new Brief(b.topic(), b.findings(), draft, b.critique(), revisions, b.decision(),
                    b.status());
        }

        private static Brief withDecision(Brief b, String decision) {
            return new Brief(b.topic(), b.findings(), b.draft(), b.critique(), b.revisions(),
                    decision, b.status());
        }

        private static Brief withStatus(Brief b, String status) {
            return new Brief(b.topic(), b.findings(), b.draft(), b.critique(), b.revisions(),
                    b.decision(), status);
        }

        private static String findingsText(Brief b) {
            StringBuilder out = new StringBuilder();
            for (Finding f : b.findings()) {
                out.append("- ").append(f.question()).append(": ").append(f.answer()).append('\n');
            }
            return out.toString();
        }
    }

    /**
     * The supervisor's reply as subtasks: one {@code kind: question} per line, unknown kinds read as
     * {@code search}, blank and unparseable lines dropped, at most {@value #MAX_SUBTASKS} kept.
     */
    public static List<Task> parseTasks(String reply) {
        List<Task> tasks = new ArrayList<>();
        for (String line : reply.split("\n")) {
            int colon = line.indexOf(':');
            if (colon < 0) continue;
            String kind = line.substring(0, colon).strip().replaceFirst("^[-*\\d.\\s]+", "").toLowerCase();
            String question = line.substring(colon + 1).strip();
            if (question.isEmpty()) continue;
            tasks.add(new Task(kind.equals("numbers") ? "numbers" : "search", question));
            if (tasks.size() == MAX_SUBTASKS) break;
        }
        return tasks;
    }
}
