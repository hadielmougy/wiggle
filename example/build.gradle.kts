plugins {
    application
}

dependencies {
    implementation(project(":client"))
    // Only the single-JVM Demo needs the server on its classpath.
    implementation(project(":server"))
}

application {
    mainClass.set("dev.wiggle.order.Demo")
}

tasks.register<JavaExec>("runWorker") {
    group = "application"
    description = "Runs a standalone worker against WIGGLE_URL (default localhost:8080)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("dev.wiggle.order.WorkerMain")
}

tasks.register<JavaExec>("submitOrders") {
    group = "application"
    description = "Submits a batch of orders. Pass a count with -Pcount=20."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("dev.wiggle.order.SubmitOrders")
    args = listOf(project.findProperty("count")?.toString() ?: "5")
}
