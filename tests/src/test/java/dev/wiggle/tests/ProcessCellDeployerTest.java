package dev.wiggle.tests;

import dev.wiggle.server.coord.CellDeployer;
import dev.wiggle.server.coord.ProcessCellDeployer;
import dev.wiggle.server.coord.NamespaceSpec;
import dev.wiggle.server.coord.SecretResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T13: the forked-process deployer. Verified with a stand-in launch command (so the test does not need
 * the real distribution): a deploy forks the process and reports the endpoint, a repeat is idempotent,
 * and teardown stops it promptly. A bad command surfaces as a deploy failure the provisioner can catch.
 */
class ProcessCellDeployerTest {

    private static final NamespaceSpec SPEC = NamespaceSpec.inMemory("proc", 9500);

    @Test @DisplayName("deploy forks a process and teardown stops it promptly")
    void deployAndTeardown() {
        ProcessCellDeployer deployer =
                new ProcessCellDeployer(List.of("sleep", "30"), SecretResolver.ENV, null);
        try {
            CellDeployer.Deployment d = deployer.deploy(SPEC);
            assertEquals("127.0.0.1:9500", d.endpoint(), "endpoint from the base port");

            CellDeployer.Deployment again = deployer.deploy(SPEC);
            assertEquals(d.endpoint(), again.endpoint(), "a repeat deploy is idempotent");

            long start = System.currentTimeMillis();
            deployer.teardown("proc");
            assertTrue(System.currentTimeMillis() - start < 30_000, "teardown killed the process, did not wait it out");
        } finally {
            deployer.close();
        }
    }

    @Test @DisplayName("a bad launch command surfaces as a deploy failure")
    void badCommandFails() {
        try (ProcessCellDeployer deployer =
                     new ProcessCellDeployer(List.of("wiggle-no-such-binary-xyz"), SecretResolver.ENV, null)) {
            assertThrows(UncheckedIOException.class, () -> deployer.deploy(SPEC));
        }
    }
}
