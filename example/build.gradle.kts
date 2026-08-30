plugins {
    application
}

dependencies {
    implementation(project(":client"))
    // Only the single-JVM Demo needs the server on its classpath.
    implementation(project(":server"))
    // So the benchmark can run against a real database (WIGGLE_JDBC_URL) to show LOCAL_ASYNC's
    // commit-batching win; brings the StorageProvider SPI + Postgres/H2 drivers at runtime.
    runtimeOnly(project(":postgres"))
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
