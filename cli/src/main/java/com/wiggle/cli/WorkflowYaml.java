package com.wiggle.cli;

import com.wiggle.client.dsl.Activity;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Case;
import com.wiggle.client.dsl.Predicate;
import com.wiggle.client.dsl.SideEffect;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.dsl.WorkflowStream;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads a declarative workflow YAML file (see {@code docs/workflow-yaml.md}) into a {@link Blueprint}
 * by translating it into calls on the existing {@link WorkflowStream} builder. Step logic is not in
 * the file: every {@code task}/{@code gate}/predicate becomes a named node with a <em>placeholder</em>
 * handler here, and the real handler is bound by name on a worker. The placeholders never leave the
 * process -- registration sends only the graph -- so this produces exactly the graph (and
 * content-hash version) the fluent DSL would, and reuses its fork/join/loop wiring and validations.
 */
public final class WorkflowYaml {

    /** A load-time error with a human-readable message; the CLI prints it without a stack trace. */
    public static final class WorkflowYamlException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public WorkflowYamlException(String message) { super(message); }
    }

    private static final Set<String> OPERATORS = Set.of(
            "task", "effect", "gate", "sleep", "await_signal", "sub_workflow",
            "fork", "fork_each", "choose", "do_while");

    // Placeholder handlers -- discarded at registration (only the graph is sent), replaced by name.
    private static final Activity<Map<String, Object>> ID = ctx -> ctx;
    private static final SideEffect<Map<String, Object>> NOOP = ctx -> { };
    private static final Predicate<Map<String, Object>> FALSE = ctx -> false;

    private static final Pattern DURATION = Pattern.compile("\\s*(\\d+)\\s*(ms|s|m|h)\\s*");
    private static final int MAX_VERSION = 0x7FFFFFFF;

    private WorkflowYaml() {}

    public static Blueprint<Map<String, Object>> load(Path file) throws IOException {
        return parse(Files.readString(file));
    }

    public static Blueprint<Map<String, Object>> parse(String yaml) {
        Object doc = new Yaml(new SafeConstructor(new LoaderOptions())).load(yaml);
        if (doc == null) throw err("the document is empty");
        Map<String, Object> root = asMap(doc, "document");

        String name = reqStr(root, "workflow");
        WorkflowStream<Map<String, Object>> s = Workflow.define(name);
        if (root.containsKey("defaultQueue")) s.defaultQueue(reqStr(root, "defaultQueue"));

        List<Object> steps = asList(root.get("steps"), "steps");
        if (steps.isEmpty()) throw err("'steps' must not be empty");
        buildInto(s, steps);

        Blueprint<Map<String, Object>> bp = s.build();
        if (root.containsKey("version")) bp = withVersion(bp, reqVersion(root.get("version")));
        return bp;
    }

    // ---- node dispatch ----------------------------------------------------------------------

    private static WorkflowStream<Map<String, Object>> buildInto(
            WorkflowStream<Map<String, Object>> s, List<Object> steps) {
        for (Object node : steps) s = apply(s, node);
        return s;
    }

    private static WorkflowStream<Map<String, Object>> apply(
            WorkflowStream<Map<String, Object>> s, Object nodeObj) {
        Map<String, Object> node = asMap(nodeObj, "step");
        String op = soleOperator(node);
        RetryPolicy retry = node.containsKey("retry") ? parseRetry(node.get("retry")) : null;
        String queue = optStr(node, "queue");
        return switch (op) {
            case "task" -> task(s, reqStr(node, "task"), retry, queue);
            case "effect" -> effect(s, reqStr(node, "effect"), retry, queue);
            case "gate" -> gate(s, reqStr(node, "gate"), retry, queue);
            case "sleep" -> sleep(s, node.get("sleep"));
            case "await_signal" -> awaitSignal(s, node);
            case "sub_workflow" -> subWorkflow(s, node.get("sub_workflow"));
            case "fork" -> fork(s, asMap(node.get("fork"), "fork"));
            case "fork_each" -> forkEach(s, asMap(node.get("fork_each"), "fork_each"));
            case "choose" -> choose(s, asList(node.get("choose"), "choose"));
            case "do_while" -> doWhile(s, asMap(node.get("do_while"), "do_while"));
            default -> throw err("unknown operator '" + op + "'");
        };
    }

    private static WorkflowStream<Map<String, Object>> task(
            WorkflowStream<Map<String, Object>> s, String name, RetryPolicy retry, String queue) {
        if (retry != null) return s.step(name, ID, retry, queue);
        if (queue != null) return s.step(name, queue);
        return s.step(name);
    }

    private static WorkflowStream<Map<String, Object>> effect(
            WorkflowStream<Map<String, Object>> s, String name, RetryPolicy retry, String queue) {
        if (retry != null) return s.effect(name, NOOP, retry, queue);
        if (queue != null) return s.effect(name, NOOP, queue);
        return s.effect(name);
    }

    private static WorkflowStream<Map<String, Object>> gate(
            WorkflowStream<Map<String, Object>> s, String name, RetryPolicy retry, String queue) {
        if (retry != null) return s.gate(name, FALSE, retry, queue);
        if (queue != null) return s.gate(name, FALSE, queue);
        return s.gate(name, FALSE);
    }

    private static WorkflowStream<Map<String, Object>> sleep(
            WorkflowStream<Map<String, Object>> s, Object value) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> m = asMap(value, "sleep");
            Duration d = parseDuration(reqStr(m, "for"));
            String name = optStr(m, "name");
            return name != null ? s.sleep(name, d) : s.sleep(d);
        }
        return s.sleep(parseDuration(String.valueOf(value)));
    }

    private static WorkflowStream<Map<String, Object>> awaitSignal(
            WorkflowStream<Map<String, Object>> s, Map<String, Object> node) {
        String name = reqStr(node, "await_signal");
        Duration timeout = node.containsKey("timeout") ? parseDuration(reqStr(node, "timeout")) : null;
        if (node.containsKey("escalation")) {
            List<Object> esc = asList(node.get("escalation"), "escalation");
            if (timeout == null) throw err("await_signal '" + name + "' escalation needs a timeout");
            return s.awaitSignal(name, timeout, sub -> buildInto(sub, esc));
        }
        return timeout != null ? s.awaitSignal(name, timeout) : s.awaitSignal(name);
    }

    private static WorkflowStream<Map<String, Object>> subWorkflow(
            WorkflowStream<Map<String, Object>> s, Object value) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> m = asMap(value, "sub_workflow");
            return s.subWorkflow(reqStr(m, "name"), reqStr(m, "workflow"));
        }
        String child = String.valueOf(value);
        return s.subWorkflow(child, child);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static WorkflowStream<Map<String, Object>> fork(
            WorkflowStream<Map<String, Object>> s, Map<String, Object> branches) {
        List<Branch<Map<String, Object>>> arr = new ArrayList<>();
        for (Map.Entry<String, Object> e : branches.entrySet()) {
            List<Object> body = asList(e.getValue(), "branch '" + e.getKey() + "'");
            arr.add(Branch.of(e.getKey(), sub -> buildInto(sub, body)));
        }
        return s.fork(arr.toArray(new Branch[0]));
    }

    private static WorkflowStream<Map<String, Object>> forkEach(
            WorkflowStream<Map<String, Object>> s, Map<String, Object> m) {
        String over = reqStr(m, "over");
        String as = reqStr(m, "as");
        String name = optStr(m, "name");
        List<Object> body = asList(m.get("body"), "fork_each body");
        if (body.isEmpty()) throw err("fork_each body must not be empty");
        return s.forkEach(name != null ? name : as, over, as, sub -> buildInto(sub, body));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static WorkflowStream<Map<String, Object>> choose(
            WorkflowStream<Map<String, Object>> s, List<Object> cases) {
        List<Case<Map<String, Object>>> arr = new ArrayList<>();
        for (Object co : cases) {
            Map<String, Object> c = asMap(co, "choose case");
            List<Object> then = asList(c.get("then"), "case 'then'");
            if (c.containsKey("when")) {
                arr.add(Case.when(reqStr(c, "when"), FALSE, sub -> buildInto(sub, then)));
            } else if (c.containsKey("otherwise")) {
                arr.add(Case.otherwise("otherwise", sub -> buildInto(sub, then)));
            } else {
                throw err("choose case needs 'when: <predicate>' or 'otherwise:'");
            }
        }
        return s.choose(arr.toArray(new Case[0]));
    }

    private static WorkflowStream<Map<String, Object>> doWhile(
            WorkflowStream<Map<String, Object>> s, Map<String, Object> m) {
        List<Object> body = asList(m.get("body"), "do_while body");
        if (body.isEmpty()) throw err("do_while body must not be empty");
        return s.doWhile(reqStr(m, "while"), FALSE, sub -> buildInto(sub, body));
    }

    // ---- value parsing ----------------------------------------------------------------------

    static Duration parseDuration(String value) {
        Matcher m = DURATION.matcher(value);
        if (!m.matches()) throw err("bad duration '" + value + "' (use <number> with unit ms|s|m|h)");
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(n);
            case "s" -> Duration.ofSeconds(n);
            case "m" -> Duration.ofMinutes(n);
            default -> Duration.ofHours(n);
        };
    }

    static RetryPolicy parseRetry(Object value) {
        if (value instanceof String preset) {
            return switch (preset) {
                case "none" -> RetryPolicy.none();
                case "forever" -> RetryPolicy.forever();
                default -> throw err("unknown retry preset '" + preset + "' (use none, forever, or a map)");
            };
        }
        Map<String, Object> m = asMap(value, "retry");
        int max = reqInt(m, "max");
        long backoff = parseDuration(reqStr(m, "backoff")).toMillis();
        double multiplier = m.containsKey("multiplier") ? num(m, "multiplier") : 1.0;
        long maxBackoff = m.containsKey("maxBackoff") ? parseDuration(reqStr(m, "maxBackoff")).toMillis() : 0;
        double jitter = m.containsKey("jitter") ? num(m, "jitter") : 0.0;
        return new RetryPolicy(max, backoff, multiplier, maxBackoff, jitter);
    }

    private static Blueprint<Map<String, Object>> withVersion(Blueprint<Map<String, Object>> bp, int version) {
        WorkflowDefinition d = bp.definition();
        WorkflowDefinition pinned = new WorkflowDefinition(
                d.name(), version, d.startNode(), d.nodes(), d.queues(), d.executionMode(), d.checkpoints());
        return new Blueprint<>(pinned, bp.handlers(), bp.codec());
    }

    // ---- helpers ----------------------------------------------------------------------------

    private static String soleOperator(Map<String, Object> node) {
        List<String> ops = node.keySet().stream().filter(OPERATORS::contains).toList();
        if (ops.isEmpty()) {
            throw err("step has no operator (one of " + new TreeSet<>(OPERATORS) + "); keys were " + node.keySet());
        }
        if (ops.size() > 1) throw err("step has multiple operators " + ops + "; use exactly one per step");
        return ops.get(0);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o, String what) {
        if (!(o instanceof Map<?, ?>)) throw err(what + " must be a mapping, got " + typeOf(o));
        return (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object o, String what) {
        if (o == null) throw err("'" + what + "' is required");
        if (!(o instanceof List<?>)) throw err("'" + what + "' must be a list, got " + typeOf(o));
        return (List<Object>) o;
    }

    private static String reqStr(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null || String.valueOf(v).isBlank()) throw err("'" + key + "' is required");
        return String.valueOf(v);
    }

    private static String optStr(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v == null ? null : String.valueOf(v);
    }

    private static int reqInt(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (!(v instanceof Number n)) throw err("'" + key + "' must be an integer");
        return n.intValue();
    }

    private static double num(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (!(v instanceof Number n)) throw err("'" + key + "' must be a number");
        return n.doubleValue();
    }

    private static int reqVersion(Object v) {
        if (!(v instanceof Number n)) throw err("'version' must be an integer");
        int version = n.intValue();
        if (version < 1 || version > MAX_VERSION) throw err("'version' must be in 1.." + MAX_VERSION);
        return version;
    }

    private static String typeOf(Object o) {
        return o == null ? "null" : o.getClass().getSimpleName();
    }

    private static WorkflowYamlException err(String message) {
        return new WorkflowYamlException(message);
    }
}