package dev.wiggle.server.store;

import dev.wiggle.core.NodeKind;
import dev.wiggle.server.store.Rows.Instance;
import dev.wiggle.server.store.Rows.InstanceStatus;
import dev.wiggle.server.store.Rows.Schedule;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Rows.Token;
import dev.wiggle.server.store.Rows.TokenStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the storage row structs in {@link Rows}. The rows are deliberately dumb -- the
 * engine owns their invariants -- so the only behaviour to pin down is {@link Token}'s join-stack
 * helpers and {@code isActive()}, the default field values, and that {@code clone()} yields an
 * independent copy (safe because every field is a {@code String} or primitive).
 */
class RowsTest {

    @Nested
    @DisplayName("Token")
    class TokenTests {

        @Test
        @DisplayName("a fresh token is READY")
        void defaults() {
            assertEquals(TokenStatus.READY, new Token().status);
            assertEquals("", new Token().joinStack);
        }

        @Test
        @DisplayName("currentJoinGroup returns the innermost (last) group, or null when empty")
        void currentJoinGroup() {
            Token t = new Token();
            assertNull(t.currentJoinGroup(), "empty stack has no current group");

            t.joinStack = "g1";
            assertEquals("g1", t.currentJoinGroup());

            t.joinStack = "g1,g2,g3";
            assertEquals("g3", t.currentJoinGroup(), "innermost is the last element");

            t.joinStack = null;
            assertNull(t.currentJoinGroup(), "a null stack is treated as empty");
        }

        @Test
        @DisplayName("popJoinStack drops the innermost group")
        void popJoinStack() {
            Token t = new Token();
            assertEquals("", t.popJoinStack(), "popping an empty stack stays empty");

            t.joinStack = "only";
            assertEquals("", t.popJoinStack(), "popping the sole group empties the stack");

            t.joinStack = "a,b,c";
            assertEquals("a,b", t.popJoinStack());

            t.joinStack = null;
            assertEquals("", t.popJoinStack());
        }

        @Test
        @DisplayName("pushJoinStack appends a group without mutating the token")
        void pushJoinStack() {
            Token t = new Token();
            assertEquals("first", t.pushJoinStack("first"), "pushing onto an empty stack is just the group");
            assertEquals("", t.joinStack, "push returns the new stack; it does not mutate the field");

            t.joinStack = "a";
            assertEquals("a,b", t.pushJoinStack("b"));

            t.joinStack = null;
            assertEquals("g", t.pushJoinStack("g"), "a null stack is treated as empty");
        }

        @Test
        @DisplayName("push then pop and current round-trip a nested stack")
        void joinStackRoundTrip() {
            Token t = new Token();
            t.joinStack = t.pushJoinStack("outer");
            t.joinStack = t.pushJoinStack("inner");
            assertEquals("outer,inner", t.joinStack);
            assertEquals("inner", t.currentJoinGroup());

            t.joinStack = t.popJoinStack();
            assertEquals("outer", t.joinStack);
            assertEquals("outer", t.currentJoinGroup());

            t.joinStack = t.popJoinStack();
            assertEquals("", t.joinStack);
            assertNull(t.currentJoinGroup());
        }

        @Test
        @DisplayName("isActive is true for live statuses and false for terminal ones")
        void isActive() {
            Set<TokenStatus> active = EnumSet.of(TokenStatus.READY, TokenStatus.RUNNING,
                    TokenStatus.WAITING, TokenStatus.AWAITING, TokenStatus.JOINED);
            for (TokenStatus s : TokenStatus.values()) {
                Token t = new Token();
                t.status = s;
                assertEquals(active.contains(s), t.isActive(), "isActive for " + s);
            }
        }

        @Test
        @DisplayName("clone is an independent copy")
        void cloneIndependence() {
            Token t = new Token();
            t.id = "t1";
            t.kind = NodeKind.TASK;
            t.status = TokenStatus.RUNNING;
            t.attempt = 2;
            t.joinStack = "a,b";

            Token c = t.clone();
            assertNotSame(t, c);
            assertEquals("t1", c.id);
            assertEquals(NodeKind.TASK, c.kind);
            assertEquals(TokenStatus.RUNNING, c.status);
            assertEquals(2, c.attempt);
            assertEquals("a,b", c.joinStack);

            c.id = "t2";
            c.attempt = 9;
            c.status = TokenStatus.DONE;
            assertEquals("t1", t.id, "mutating the clone must not touch the original");
            assertEquals(2, t.attempt);
            assertEquals(TokenStatus.RUNNING, t.status);
        }
    }

    @Nested
    @DisplayName("Instance")
    class InstanceTests {

        @Test
        @DisplayName("a fresh instance is RUNNING with an empty context")
        void defaults() {
            Instance i = new Instance();
            assertEquals(InstanceStatus.RUNNING, i.status);
            assertEquals("{}", i.contextJson);
        }

        @Test
        @DisplayName("clone is an independent copy")
        void cloneIndependence() {
            Instance i = new Instance();
            i.id = "i1";
            i.workflow = "wf";
            i.version = 7;
            i.contextJson = "{\"a\":1}";

            Instance c = i.clone();
            assertNotSame(i, c);
            assertEquals("i1", c.id);
            assertEquals("wf", c.workflow);
            assertEquals(7, c.version);
            assertEquals("{\"a\":1}", c.contextJson);

            c.id = "i2";
            c.status = InstanceStatus.COMPLETED;
            assertEquals("i1", i.id);
            assertEquals(InstanceStatus.RUNNING, i.status);
        }
    }

    @Nested
    @DisplayName("ServerNode")
    class ServerNodeTests {

        @Test
        @DisplayName("a fresh node is a non-leader")
        void defaults() {
            assertFalse(new ServerNode().leader);
        }

        @Test
        @DisplayName("clone is an independent copy")
        void cloneIndependence() {
            ServerNode n = new ServerNode();
            n.id = "node-1";
            n.name = "alpha";
            n.workers = 4;
            n.leader = true;

            ServerNode c = n.clone();
            assertNotSame(n, c);
            assertEquals("node-1", c.id);
            assertEquals("alpha", c.name);
            assertEquals(4, c.workers);
            assertTrue(c.leader);

            c.leader = false;
            c.workers = 0;
            assertTrue(n.leader, "mutating the clone must not touch the original");
            assertEquals(4, n.workers);
        }
    }

    @Nested
    @DisplayName("Schedule")
    class ScheduleTests {

        @Test
        @DisplayName("a fresh schedule has an empty context")
        void defaults() {
            assertEquals("{}", new Schedule().contextJson);
        }

        @Test
        @DisplayName("clone is an independent copy")
        void cloneIndependence() {
            Schedule s = new Schedule();
            s.id = "s1";
            s.workflow = "nightly";
            s.intervalMillis = 3_600_000;
            s.cron = "0 3 * * *";

            Schedule c = s.clone();
            assertNotSame(s, c);
            assertEquals("s1", c.id);
            assertEquals("nightly", c.workflow);
            assertEquals(3_600_000, c.intervalMillis);
            assertEquals("0 3 * * *", c.cron);

            c.cron = null;
            c.intervalMillis = 0;
            assertEquals("0 3 * * *", s.cron, "mutating the clone must not touch the original");
            assertEquals(3_600_000, s.intervalMillis);
        }
    }

    @Test
    @DisplayName("clone preserves the concrete row type")
    void clonePreservesType() {
        assertSame(Token.class, new Token().clone().getClass());
        assertSame(Instance.class, new Instance().clone().getClass());
        assertSame(ServerNode.class, new ServerNode().clone().getClass());
        assertSame(Schedule.class, new Schedule().clone().getClass());
    }
}
