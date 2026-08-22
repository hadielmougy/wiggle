package dev.wiggle.tests;

import dev.wiggle.core.GraphTraversal;
import dev.wiggle.core.GraphTraversal.Handback;
import dev.wiggle.core.Node;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The pure traversal seam shared by the server's state machine and the worker's local driver.
 * Being the single source of truth for "what runs next / where must a chain hand back", every
 * branch deserves an explicit check.
 */
class GraphTraversalTest {

    private static final Set<String> SERVED = Set.of("main");

    @Test @DisplayName("successor: tasks follow next; predicates route by value")
    void successor() {
        Node task = Node.task("t", "t", "act", "main", null).withNext("after");
        assertEquals("after", GraphTraversal.successor(task, false), "task ignores the flag");

        Node predicate = Node.predicate("p", "p", "act", "main", null).withNext("yes").withAltNext("no");
        assertEquals("yes", GraphTraversal.successor(predicate, true));
        assertEquals("no", GraphTraversal.successor(predicate, false));
    }

    @Test @DisplayName("classify: same-queue worker steps run locally, everything else hands back")
    void classify() {
        assertNull(GraphTraversal.classify(Node.task("t", "t", "a", "main", null), SERVED));
        assertNull(GraphTraversal.classify(Node.predicate("p", "p", "a", "main", null), SERVED));

        assertEquals(Handback.OTHER_QUEUE,
                GraphTraversal.classify(Node.task("t2", "t2", "a", "elsewhere", null), SERVED));
        assertEquals(Handback.SLEEP, GraphTraversal.classify(Node.sleep("s", "s", 100), SERVED));
        assertEquals(Handback.FORK, GraphTraversal.classify(
                Node.fork("f", "f").withBranches(List.of("a", "b")), SERVED));
        assertEquals(Handback.JOIN, GraphTraversal.classify(Node.join("j", "j", 2), SERVED));
        assertEquals(Handback.USER_TASK, GraphTraversal.classify(Node.userTask("u", "u", 0), SERVED));
        assertEquals(Handback.TERMINAL, GraphTraversal.classify(Node.end("e", true, null), SERVED));
    }
}
