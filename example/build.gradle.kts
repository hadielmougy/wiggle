plugins {
    application
}

dependencies {
    implementation(project(":client"))
    // Only the single-JVM Demo needs the server on its classpath.
    implementation(project(":server"))
    // So the benchmark can run against a real database (WIGGLE_JDBC_URL) to show LOCAL_ASYNC's
    // commit-batching win. Storage is an explicit StorageFactory (no ServiceLoader), so Benchmark
    // wires JdbcStorage + a dialect itself -- these must be on the compile classpath.
    implementation(project(":jdbc"))
    implementation(project(":postgres"))
}

application {
    mainClass.set("com.wiggle.order.Demo")
}

tasks.register<JavaExec>("seedDashboard") {
    group = "application"
    description = "Starts a dashboard-enabled server (:8090) seeded with data across every tab."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.DashboardSeed")
    systemProperty("wiggle.dashboard.port",
        project.findProperty("port")?.toString()
            ?: System.getenv("WIGGLE_DASHBOARD_PORT") ?: "8090")
}

tasks.register<JavaExec>("runWorker") {
    group = "application"
    description = "Runs a standalone worker against WIGGLE_URL (default localhost:8080)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.WorkerMain")
}

tasks.register<JavaExec>("submitOrders") {
    group = "application"
    description = "Submits a batch of orders. Pass a count with -Pcount=20."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.SubmitOrders")
    args = listOf(project.findProperty("count")?.toString() ?: "5")
}

tasks.register<JavaExec>("runBinding") {
    group = "application"
    description = "Name-only binding demo: a flow authored once, served by two independent workers by step name."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.binding.BindingDemo")
}

tasks.register<JavaExec>("runTypedBinding") {
    group = "application"
    description = "Name-only binding demo with a typed (record) context served by typed handlers."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.binding.typed.TypedBindingDemo")
}

tasks.register<JavaExec>("runCookbook") {
    group = "application"
    description = "Runs every DSL cookbook example (embedded server + worker, one JVM)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.cookbook.CookbookDemo")
}

tasks.register<JavaExec>("bench") {
    group = "application"
    description = "Throughput benchmark of a linear pipeline. Set WIGGLE_EXECUTION_MODE etc."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.Benchmark")
}

tasks.register<JavaExec>("namespaceWorker") {
    group = "application"
    description = "Runs the coordinator-routed namespace worker (one worker per active cell). " +
            "Set WIGGLE_COORDINATOR_URL/WIGGLE_NAMESPACE/WIGGLE_ENDPOINT_REWRITE."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.NamespaceWorkerMain")
}

tasks.register<JavaExec>("coordFailover") {
    group = "application"
    description = "Coordinator resiliency under load: fixed-rate starts + probes; kill the coordinator " +
            "mid-run. Set WIGGLE_COORDINATOR_URL/WIGGLE_NAMESPACE/WIGGLE_ENDPOINT_REWRITE, BENCH_RATE etc."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.CoordinatorFailoverBench")
}

tasks.register<JavaExec>("rateCeiling") {
    group = "application"
    description = "Find the sustainable start-rate ceiling of a deployment (needs a running worker). " +
            "Set WIGGLE_COORDINATOR_URL/WIGGLE_NAMESPACE/WIGGLE_ENDPOINT_REWRITE; tune BENCH_RATES etc."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.order.RateCeilingBench")
}
