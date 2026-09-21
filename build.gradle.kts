import com.vanniktech.maven.publish.MavenPublishBaseExtension
import com.vanniktech.maven.publish.SonatypeHost
import java.time.Duration
import org.gradle.plugins.signing.Sign

plugins {
    java
    id("com.vanniktech.maven.publish") version "0.30.0" apply false
}

allprojects {
    group = "sh.wiggle"
    version = "0.0.8"

    repositories {
        mavenCentral()
    }
}

// Every module is a Java library EXCEPT `bom`, which is a `java-platform` (a BOM has no code and
// the java-library plugin is incompatible with java-platform).
configure(subprojects.filter { it.name != "bom" }) {
    apply(plugin = "java-library")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-parameters"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
            showStandardStreams = false
        }
        // The conformance suite starts real servers on ephemeral ports and waits on
        // timers, so it needs more headroom than a unit-test default.
        timeout.set(Duration.ofMinutes(10))
        // Forwarded to the forked test JVM: StateChartTest rewrites docs/state-machines.md
        // instead of asserting against it.
        systemProperty("wiggle.statechart.write",
                providers.systemProperty("wiggle.statechart.write").getOrElse("false"))
    }
}

// Only the reusable library modules are published to Maven Central; `example` and
// `tests` are excluded. The actual upload is a manual, credentialed step -- see
// RELEASING.md. Signing and Central-Portal credentials are read from properties or
// environment variables and are never stored in the repository.

// The BOM is published by the shared block below. `client-all` (the shaded client) publishes a
// SHADOW component with a dependency-free POM, so it wires its own publishing in client-all/build.gradle.kts.
// :election must be here even though nobody depends on it directly: :server has an `api`
// dependency on it, so it appears in wiggle-server's POM. Leaving it out published a POM
// pointing at sh.wiggle:election, which was never uploaded -- so every consumer of
// wiggle-server (and of wiggle-jdbc / wiggle-postgres, which bring it transitively) failed to
// resolve. An unpublished project dependency of a published module is always a broken POM.
val publishedModules = setOf("core", "proto", "client", "server", "jdbc", "postgres", "bom",
        "election", "placement")

val moduleDescriptions = mapOf(
    "core" to "Wiggle shared model: JSON, the compiled state-machine graph, retry policy, wire records.",
    "proto" to "Wiggle gRPC/protobuf stubs for the control-plane wire protocol.",
    "client" to "Wiggle client: the flow-authoring DSL, imperative builder, worker runtime, and cell resolver.",
    "server" to "Wiggle server: the durable state-machine engine, cluster manager, cell coordinator, and control-plane API.",
    "jdbc" to "Wiggle JDBC storage core: the dialect-aware, HikariCP-pooled store shared by every database module.",
    "postgres" to "Wiggle PostgreSQL storage: PostgreSQL and H2 dialects for multi-node clustering.",
    "bom" to "Wiggle BOM: a version-alignment platform for every wiggle module and its gRPC/protobuf stack.",
    "election" to "Wiggle leader election: announce-and-heartbeat election over a pluggable store, "
            + "shared by the server and the coordinator.",
    "placement" to "Wiggle placement rules: the ring model and the pure decisions that map an "
            + "instance id to the cell holding it, shared by the server and the coordinator.",
)

configure(subprojects.filter { it.name in publishedModules }) {
    apply(plugin = "com.vanniktech.maven.publish")

    // Sign with the local GnuPG agent (pinentry prompts for the passphrase) instead of
    // requiring an in-memory key. Vanniktech applies the signing plugin lazily, so we
    // hook it once registered. Override by setting the signingInMemoryKey properties.
    plugins.withId("signing") {
        extensions.configure<SigningExtension> {
            useGpgCmd()
        }
        // Signing is required only for the Central upload; never for a local install. Skip it for
        // publishToMavenLocal so `./gradlew publishToMavenLocal` works with no GPG key present.
        tasks.withType<Sign>().configureEach {
            onlyIf { !gradle.startParameter.taskNames.any { it.contains("MavenLocal") } }
        }
    }

    extensions.configure<MavenPublishBaseExtension> {
        publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL, automaticRelease = true)
        signAllPublications()

        coordinates(group.toString(), "wiggle-${project.name}", version.toString())

        pom {
            name.set("Wiggle ${project.name}")
            description.set(moduleDescriptions.getValue(project.name))
            url.set("https://github.com/hadielmougy/wiggle")
            inceptionYear.set("2026")

            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    distribution.set("repo")
                }
            }
            developers {
                developer {
                    id.set("hadielmougy")
                    name.set("Hadi Elmougy")
                }
            }
            scm {
                url.set("https://github.com/hadielmougy/wiggle")
                connection.set("scm:git:https://github.com/hadielmougy/wiggle.git")
                developerConnection.set("scm:git:ssh://git@github.com/hadielmougy/wiggle.git")
            }
        }
    }
}
