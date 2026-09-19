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
    @DisplayName("the token chart lists exactly the TokenStatus constants")
    void tokenStatesMatchTheEnum() {
        assertEquals(names(TokenStatus.values()), charted(StateChart.tokens()),
                "TokenStatus and the token chart have drifted apart");
    }

    @Test
    @DisplayName("the instance chart lists exactly the InstanceStatus constants")
    void instanceStatesMatchTheEnum() {
        assertEquals(names(InstanceStatus.values()), charted(StateChart.instances()),
                "InstanceStatus and the instance chart have drifted apart");
    }

    @Test
    @DisplayName("the chart's active token states are exactly the ones Token.isActive() accepts")
    void activeClassificationMatchesIsActive() {
        Set<String> fromCode = Arrays.stream(TokenStatus.values())
                .filter(s -> { Token t = new Token(); t.status = s; return t.isActive(); })
                .map(Enum::name).collect(Collectors.toCollection(LinkedHashSet::new));
        assertEquals(fromCode, kind(StateChart.tokens(), "active"),
                "Token.isActive() and the chart's active states disagree");
    }

    @Test
    @DisplayName("the chart's terminal instance states are exactly the ones InstanceView.isTerminal() accepts")
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
