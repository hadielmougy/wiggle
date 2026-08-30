// Coordinator persistence + provisioning SPI: the CoordinatorStore contract, domain records, epoch
// codec, in-memory reference store, and the CellDeployer/SecretResolver seams. Depends only on :core.
// Storage adapters (jdbc, cassandra) and the coordinator runtime depend on this; the engine never does.
dependencies {
    api(project(":core"))
}
