// Wires the Python client's pytest suite into the Gradle lifecycle: `./gradlew build` (or `check`)
// runs it. It skips cleanly -- with a warning, not a failure -- when Python 3 or its runtime deps
// (grpcio, protobuf, pytest) are not installed, or when run with -PskipPython. That keeps the Java
// build self-contained on machines without a Python toolchain.
//
// Regenerate the committed gRPC stubs after a proto change with: ./gradlew :clients:python:pyGenerateStubs

plugins { base }

val skipPython = providers.gradleProperty("skipPython").isPresent

/** True when `python3` can import the client's runtime deps and pytest. Probed lazily at execution. */
fun pythonReady(): Boolean {
    if (skipPython) return false
    return try {
        ProcessBuilder("python3", "-c", "import grpc, google.protobuf, pytest")
            .redirectErrorStream(true).start().waitFor() == 0
    } catch (e: Exception) {
        false
    }
}

val pyTest by tasks.registering(Exec::class) {
    group = "verification"
    description = "Runs the Python client's pytest suite (skips if Python/grpcio/pytest are absent)."
    workingDir = projectDir
    environment("PYTHONPATH", projectDir.absolutePath)
    commandLine("python3", "-m", "pytest", "-q")
    onlyIf {
        val ready = pythonReady()
        if (!ready) logger.warn(
            "Skipping Python tests: python3 with grpcio+protobuf+pytest not found " +
                "(pip install '${projectDir}[dev]'), or -PskipPython was set.")
        ready
    }
}

val pyGenerateStubs by tasks.registering(Exec::class) {
    group = "build"
    description = "Regenerates the committed Python gRPC stubs from proto/ (needs grpcio-tools)."
    workingDir = projectDir
    commandLine("bash", "codegen.sh")
}

tasks.named("check") { dependsOn(pyTest) }
