// The coordinator control plane: the CellCoordinator gRPC service, the reconcile/retire loop, the
// census, provisioning, and CoordinatorServer (self-hosted). It talks to cells only over gRPC (:proto)
// and NEVER depends on :server -- the cell engine and the coordinator are decoupled in source.
dependencies {
    api(project(":coordinator:spi"))
    api(project(":proto"))
}
