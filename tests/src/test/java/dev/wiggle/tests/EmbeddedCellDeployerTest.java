package dev.wiggle.tests;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.IdCodec;
import dev.wiggle.server.coord.CoordNamespace;
import dev.wiggle.server.coord.EmbeddedCellDeployer;
import dev.wiggle.server.coord.InMemoryCoordinatorStore;
import dev.wiggle.server.coord.NamespaceProvisioner;
import dev.wiggle.server.coord.NamespaceSpec;
import dev.wiggle.server.coord.ProvisionState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * T13 acceptance: provision a namespace end-to-end with the embedded deployer -- a real in-process cell
 * comes up, its endpoint is recorded and usable, and teardown stops it. Only the deployer touches
 * {@code WiggleServer}; the provisioner drives it through {@link NamespaceProvisioner}.
 */
class EmbeddedCellDeployerTest {

    @Test @DisplayName("provisions an in-memory cell to ACTIVE and its endpoint serves work")
    void provisionsAndServes() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (EmbeddedCellDeployer deployer = EmbeddedCellDeployer.inMemory()) {
            NamespaceProvisioner provisioner = new NamespaceProvisioner(store, deployer);

            CoordNamespace ns = provisioner.create(NamespaceSpec.inMemory("shop", 0));   // ephemeral port
            assertEquals(ProvisionState.ACTIVE, ns.state());

            // the recorded endpoint is a live cell: register + start, and the id is epoch-aware for "shop"
            try (WiggleClient client = new WiggleClient(ns.endpoint())) {
                client.register(Workflow.define("wf").step("a", c -> c).build());
                String id = client.start("wf", Map.of());
                assertEquals("shop", IdCodec.parse(id)
                        .orElseThrow(() -> new AssertionError("expected an epoch-aware id, got " + id))
                        .namespace(), "the provisioned cell mints ids for its namespace");
            }

            // teardown stops the cell -> the endpoint no longer serves
            deployer.teardown("shop");
            try (WiggleClient dead = new WiggleClient(ns.endpoint())) {
                assertThrows(RuntimeException.class,
                        () -> dead.register(Workflow.define("wf").step("a", c -> c).build()),
                        "a torn-down cell should not answer");
            }
        }
    }
}
