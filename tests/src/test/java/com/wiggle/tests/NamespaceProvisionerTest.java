package com.wiggle.tests;

import com.wiggle.server.coord.CellDeployer;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import com.wiggle.server.coord.NamespaceProvisioner;
import com.wiggle.server.coord.NamespaceSpec;
import com.wiggle.server.coord.ProvisionState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T13: the provisioning state machine drives REQUESTED → MIGRATING_SCHEMA → STARTING → ACTIVE through
 * the {@link CellDeployer} seam, is idempotent, and leaves a resumable FAILED record on any error.
 */
class NamespaceProvisionerTest {

    /** A deployer that records its call order and can be told to fail a step a number of times. */
    static final class FakeDeployer implements CellDeployer {
        final List<String> calls = new ArrayList<>();
        int failDeployTimes = 0;
        boolean failMigrate = false;

        @Override public void migrateSchema(NamespaceSpec spec) {
            calls.add("migrate:" + spec.namespace());
            if (failMigrate) throw new IllegalStateException("boom-migrate");
        }

        @Override public Deployment deploy(NamespaceSpec spec) {
            calls.add("deploy:" + spec.namespace());
            if (failDeployTimes > 0) { failDeployTimes--; throw new IllegalStateException("boom-deploy"); }
            return new Deployment(spec.namespace(), "127.0.0.1:9000");
        }

        @Override public void teardown(String id) { calls.add("teardown:" + id); }
    }

    private static NamespaceSpec spec() {
        return NamespaceSpec.inMemory("shop", 8080);
    }

    @Test @DisplayName("happy path reaches ACTIVE, migrating the schema before deploying")
    void happyPath() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        FakeDeployer deployer = new FakeDeployer();
        CoordNamespace rec = new NamespaceProvisioner(store, deployer).create(spec());

        assertEquals(ProvisionState.ACTIVE, rec.state());
        assertEquals("127.0.0.1:9000", rec.endpoint(), "endpoint recorded");
        assertEquals(List.of("migrate:shop", "deploy:shop"), deployer.calls, "migrate precedes deploy");
        assertEquals(ProvisionState.ACTIVE, store.getNamespace("shop").orElseThrow().state(), "persisted");
    }

    @Test @DisplayName("provisioning an already-ACTIVE namespace is a no-op")
    void idempotent() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        FakeDeployer deployer = new FakeDeployer();
        NamespaceProvisioner p = new NamespaceProvisioner(store, deployer);

        p.create(spec());
        CoordNamespace second = p.create(spec());

        assertEquals(ProvisionState.ACTIVE, second.state());
        assertEquals(List.of("migrate:shop", "deploy:shop"), deployer.calls, "no re-migrate / re-deploy");
    }

    @Test @DisplayName("a failed step leaves a resumable FAILED record; a retry completes it")
    void resumableFailure() {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        FakeDeployer deployer = new FakeDeployer();
        deployer.failDeployTimes = 1;   // first deploy throws, second succeeds
        NamespaceProvisioner p = new NamespaceProvisioner(store, deployer);

        CoordNamespace failed = p.create(spec());
        assertEquals(ProvisionState.FAILED, failed.state());
        assertTrue(failed.error().contains("boom-deploy"), "error captured: " + failed.error());
        assertEquals(ProvisionState.FAILED, store.getNamespace("shop").orElseThrow().state(), "FAILED persisted");

        CoordNamespace resumed = p.create(spec());   // retry
        assertEquals(ProvisionState.ACTIVE, resumed.state(), "retry resumes to ACTIVE");
        assertEquals(List.of("migrate:shop", "deploy:shop", "migrate:shop", "deploy:shop"), deployer.calls);
    }
}
