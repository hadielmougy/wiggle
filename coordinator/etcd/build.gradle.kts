// A consensus-backed coordinator store: CoordinatorStore over etcd (linearizable CAS via etcd Txn,
// leader lease over the same store). Depends only on the coordinator SPI + the jetcd client -- NOT on
// the engine or any engine database. This is the "control plane needs no external RDBMS" option: the
// coordinator's durable state lives in etcd (itself Raft-backed).
dependencies {
    api(project(":coordinator:spi"))
    implementation("io.etcd:jetcd-core:${property("jetcdVersion")}")
}
