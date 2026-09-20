package com.wiggle.server.engine;

import com.wiggle.core.InstanceView;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Holds {@link StateChart} -- and the docs generated from it -- to the code it describes. A state
 * added to either enum, a renamed entry point, a changed terminal rule or a hand-edited doc all
 * fail here rather than leaving a table that quietly lies.
 */
class StateChartTest {

    private static final Path DOC = repoRoot().resolve("docs").resolve("state-machines.md");

    /** The test's working directory is the :tests project, so walk up to the build root. */
    private static Path repoRoot() {
        Path p = Path.of("").toAbsolutePath();
        while (p != null && !Files.exists(p.resolve("settings.gradle.kts"))) p = p.getParent();
        if (p == null) throw new IllegalStateException("no settings.gradle.kts above " + Path.of("").toAbsolutePath());
        return p;
    }

    @Test
    @DisplayName("there is one TokenState per persisted TokenStatus, and one InstanceState per InstanceStatus")
    void everyPersistedStatusHasAState() {
        assertEquals(names(TokenStatus.values()),
                Arrays.stream(TokenState.values()).map(Enum::name)
                        .collect(Collectors.toCollection(LinkedHashSet::new)),
                "TokenStatus and TokenState have drifted apart");
        assertEquals(names(InstanceStatus.values()),
                Arrays.stream(InstanceState.values()).map(Enum::name)
                        .collect(Collectors.toCollection(LinkedHashSet::new)),
                "InstanceStatus and InstanceState have drifted apart");
    }

    @Test
    @DisplayName("every charted transition is one the state enum actually permits")
    void chartedTransitionsAreLegal() {
        for (StateChart.Transition t : StateChart.tokens().transitions()) {
            if (t.from().equals("(none)")) continue;
            TokenState from = TokenState.valueOf(t.from());
            assertTrue(t.from().equals(t.to()) || from.successors().contains(TokenStatus.valueOf(t.to())),
                    "the chart documents " + t.from() + " -" + t.event() + "-> " + t.to()
                            + ", but TokenState." + from + " only permits " + from.successors());
        }
        for (StateChart.Transition t : StateChart.instances().transitions()) {
            if (t.from().equals("(none)")) continue;
            InstanceState from = InstanceState.valueOf(t.from());
            assertTrue(t.from().equals(t.to()) || from.successors().contains(InstanceStatus.valueOf(t.to())),
                    "the chart documents " + t.from() + " -" + t.event() + "-> " + t.to()
                            + ", but InstanceState." + from + " only permits " + from.successors());
        }
    }

    @Test
    @DisplayName("every transition the state enums permit is documented by at least one event")
    void legalTransitionsAreCharted() {
        for (TokenState from : TokenState.values()) {
            for (TokenStatus to : from.successors()) {
                assertTrue(StateChart.tokens().transitions().stream()
                                .anyMatch(t -> t.from().equals(from.name()) && t.to().equals(to.name())),
                        "TokenState permits " + from + " -> " + to + ", but no charted event causes it");
            }
        }
        for (InstanceState from : InstanceState.values()) {
            for (InstanceStatus to : from.successors()) {
                assertTrue(StateChart.instances().transitions().stream()
                                .anyMatch(t -> t.from().equals(from.name()) && t.to().equals(to.name())),
                        "InstanceState permits " + from + " -> " + to + ", but no charted event causes it");
            }
        }
    }

    @Test
    @DisplayName("a settled token state and a terminal instance state declare no way out")
    void settledStatesDeclareNoSuccessors() {
        for (TokenState s : TokenState.values()) {
            if (s.active()) continue;
            assertTrue(s.successors().isEmpty(),
                    "TokenState." + s + " is settled but declares successors " + s.successors());
        }
        for (InstanceState s : InstanceState.values()) {
            if (s.live()) continue;
            assertTrue(s.successors().isEmpty(),
                    "InstanceState." + s + " is terminal but declares successors " + s.successors());
        }
    }

    @Test
    @DisplayName("only a leased token answers the lease and failure operations")
    void leaseOperationsBelongToRunningAlone() {
        Token t = new Token();
        t.id = "tok_1";
        for (TokenState s : TokenState.values()) {
            t.status = TokenStatus.valueOf(s.name());
            if (s == TokenState.RUNNING) continue;
            assertThrows(EngineException.class, () -> s.renewLease(t, 1),
                    s + " must not extend a lease it does not hold");
            assertThrows(EngineException.class, () -> s.requireLeasedBy(t, "w1"),
                    s + " must not accept a report against a lease it does not hold");
            assertThrows(EngineException.class, () -> s.requireLeasedBy(t, null),
                    s + " must not pass the leased check a failure report requires");
        }
    }

    @Test
    @DisplayName("releasing a lease is a no-op for every state that cannot hold one")
    void onlyRunningReleasesALease() {
        for (TokenState s : TokenState.values()) {
            Token t = new Token();
            t.leaseOwner = "w1";
            t.leaseExpiresAt = 99;
            s.releaseLease(t);
            if (s.holdsLease()) {
                assertEquals(null, t.leaseOwner, s + " should have dropped the lease");
                assertEquals(0, t.leaseExpiresAt);
            } else {
                assertEquals("w1", t.leaseOwner, s + " has no lease to drop and should not touch one");
            }
        }
    }

    @Test
    @DisplayName("an illegal transition is refused rather than persisted")
    void illegalTransitionsThrow() {
        assertThrows(IllegalStateException.class, () -> TokenState.DONE.moveTo(TokenStatus.READY),
                "a settled token must not come back to life");
        assertThrows(IllegalStateException.class, () -> TokenState.WAITING.moveTo(TokenStatus.RUNNING),
                "a sleeping token must be woken before it can be leased");
        assertThrows(IllegalStateException.class,
                () -> InstanceState.COMPLETED.moveTo(InstanceStatus.RUNNING),
                "a terminal instance must not restart");
        assertThrows(IllegalStateException.class,
                () -> InstanceState.COMPENSATING.moveTo(InstanceStatus.COMPLETED),
                "a compensating instance must not report success");
    }

    @Test
    @DisplayName("TokenState.active() and the row's own isActive() agree")
    void activeClassificationMatchesIsActive() {
        Set<String> fromRow = Arrays.stream(TokenStatus.values())
                .filter(s -> { Token t = new Token(); t.status = s; return t.isActive(); })
                .map(Enum::name).collect(Collectors.toCollection(LinkedHashSet::new));
        assertEquals(fromRow, kind(StateChart.tokens(), "active"),
                "Token.isActive() and TokenState.active() disagree");
    }

    @Test
    @DisplayName("only RUNNING holds a lease, and only RUNNING dispatches the reverse pass")
    void dispatchClassificationsAreSingular() {
        assertEquals(Set.of(TokenState.RUNNING), Arrays.stream(TokenState.values())
                .filter(TokenState::holdsLease).collect(Collectors.toSet()));
        assertEquals(Set.of(InstanceState.RUNNING), Arrays.stream(InstanceState.values())
                .filter(s -> s.dispatches(false)).collect(Collectors.toSet()),
                "only a RUNNING instance dispatches forward work");
        assertEquals(Set.of(InstanceState.COMPENSATING), Arrays.stream(InstanceState.values())
                .filter(s -> s.dispatches(true)).collect(Collectors.toSet()),
                "only a COMPENSATING instance dispatches the reverse pass");
    }

    @Test
    @DisplayName("InstanceState.live() and InstanceView.isTerminal() agree")
    void terminalClassificationMatchesInstanceView() {
        Set<String> fromCode = Arrays.stream(InstanceStatus.values())
                .filter(s -> view(s).isTerminal())
                .map(Enum::name).collect(Collectors.toCollection(LinkedHashSet::new));
        assertEquals(fromCode, kind(StateChart.instances(), "terminal"),
                "InstanceView.isTerminal() and the chart's terminal states disagree");
    }

    /**
     * The terminal rule is written down three times -- InstanceView, the JDBC purge query and the
     * in-memory purge. This pins the in-memory copy to the chart behaviourally, so a new status
     * that nobody classified is purged (or kept) loudly rather than silently.
     */
    @Test
    @DisplayName("the in-memory purge treats exactly the chart's terminal states as terminal")
    void inMemoryPurgeAgreesWithTheChart() {
        Storage storage = new InMemoryStorage();
        for (InstanceStatus s : InstanceStatus.values()) {
            storage.inTxVoid(tx -> {
                Instance i = new Instance();
                i.id = "wfi_" + s.name();
                i.workflow = "w";
                i.version = 1;
                i.status = s;
                i.createdAt = 0;
                i.updatedAt = 0;
                tx.insertInstance(i);
            });
        }
        storage.inTxVoid(tx -> tx.deleteTerminalInstancesBefore(1_000, 100));
        Set<String> survived = Arrays.stream(InstanceStatus.values())
                .filter(s -> storage.inTx(tx -> tx.findInstance("wfi_" + s.name()).isPresent()))
                .map(Enum::name).collect(Collectors.toCollection(LinkedHashSet::new));
        assertEquals(kind(StateChart.instances(), "live"), survived,
                "the in-memory purge and the chart disagree about which states are terminal");
    }

    @Test
    @DisplayName("every entry point a transition names is a real public method on WorkflowEngine")
    void entryPointsResolve() {
        Set<String> real = Arrays.stream(WorkflowEngine.class.getDeclaredMethods())
                .filter(m -> java.lang.reflect.Modifier.isPublic(m.getModifiers()))
                .map(Method::getName).collect(Collectors.toSet());
        for (StateChart.Chart c : List.of(StateChart.instances(), StateChart.tokens())) {
            for (StateChart.Transition t : c.transitions()) {
                if (t.entryPoint() == null) continue;
                assertTrue(real.contains(t.entryPoint()),
                        "transition " + t.from() + " -" + t.event() + "-> " + t.to()
                                + " names entry point '" + t.entryPoint()
                                + "', which is not a public method on WorkflowEngine");
            }
        }
    }

    @Test
    @DisplayName("every transition's endpoints are charted states")
    void transitionEndpointsAreCharted() {
        for (StateChart.Chart c : List.of(StateChart.instances(), StateChart.tokens())) {
            Set<String> states = charted(c);
            for (StateChart.Transition t : c.transitions()) {
                assertTrue(t.from().equals("(none)") || states.contains(t.from()),
                        c.title() + " transition leaves uncharted state '" + t.from() + "'");
                assertTrue(states.contains(t.to()),
                        c.title() + " transition enters uncharted state '" + t.to() + "'");
            }
        }
    }

    @Test
    @DisplayName("no transition leaves a terminal instance state")
    void terminalInstanceStatesAreFinal() {
        Set<String> terminal = kind(StateChart.instances(), "terminal");
        for (StateChart.Transition t : StateChart.instances().transitions()) {
            assertTrue(!terminal.contains(t.from()),
                    "instance state " + t.from() + " is terminal but has an outgoing " + t.event());
        }
    }

    @Test
    @DisplayName("no transition leaves a settled token state")
    void settledTokenStatesAreFinal() {
        Set<String> settled = kind(StateChart.tokens(), "settled");
        for (StateChart.Transition t : StateChart.tokens().transitions()) {
            assertTrue(!settled.contains(t.from()),
                    "token state " + t.from() + " is settled but has an outgoing " + t.event());
        }
    }

    /**
     * The generated doc is checked in, so a reader gets it from the repo rather than from a build.
     * Regenerate with {@code ./gradlew :tests:test -Dwiggle.statechart.write=true}.
     */
    @Test
    @DisplayName("docs/state-machines.md is the rendering of the chart")
    void docMatchesTheChart() throws Exception {
        String rendered = StateChart.render();
        if (Boolean.getBoolean("wiggle.statechart.write")) {
            Files.writeString(DOC, rendered);
            return;
        }
        assertTrue(Files.exists(DOC), DOC + " is missing; regenerate it with -Dwiggle.statechart.write=true");
        assertEquals(rendered, Files.readString(DOC),
                "docs/state-machines.md is stale; regenerate it with -Dwiggle.statechart.write=true");
    }

    private static Set<String> names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<String> charted(StateChart.Chart c) {
        return c.states().stream().map(StateChart.State::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Set<String> kind(StateChart.Chart c, String kind) {
        return c.states().stream().filter(s -> s.kind().equals(kind)).map(StateChart.State::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static InstanceView view(InstanceStatus s) {
        return new InstanceView("i", "w", 1, s.name(), null, null, null, 0, 0);
    }
}
