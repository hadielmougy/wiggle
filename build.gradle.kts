import com.vanniktech.maven.publish.MavenPublishBaseExtension
import com.vanniktech.maven.publish.SonatypeHost
import java.time.Duration

plugins {
    java
    id("com.vanniktech.maven.publish") version "0.30.0" apply false
}

allprojects {
    group = "io.github.hadielmougy"
    version = "2.1.6"

    repositories {
        mavenCentral()
    }
}

subprojects {
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
    }
}

// Only the reusable library modules are published to Maven Central; `example` and
// `tests` are excluded. The actual upload is a manual, credentialed step -- see
// RELEASING.md. Signing and Central-Portal credentials are read from properties or
// environment variables and are never stored in the repository.

val publishedModules = setOf("core", "proto", "client", "server", "jdbc", "postgres", "mysql", "oracle", "sqlserver", "cassandra")

val moduleDescriptions = mapOf(
    "core" to "Wiggle shared model: JSON, the compiled state-machine graph, retry policy, wire records.",
    "proto" to "Wiggle gRPC/protobuf stubs for the control-plane wire protocol.",
    "client" to "Wiggle client: the flow-authoring DSL, imperative builder, worker runtime, and cell resolver.",
    "server" to "Wiggle server: the durable state-machine engine, cluster manager, cell coordinator, and control-plane API.",
    "jdbc" to "Wiggle JDBC storage core: the dialect-aware, HikariCP-pooled store shared by every database module.",
    "postgres" to "Wiggle PostgreSQL storage: PostgreSQL and H2 dialects for multi-node clustering.",
    "mysql" to "Wiggle MySQL storage: the MySQL/MariaDB dialect for multi-node clustering.",
    "oracle" to "Wiggle Oracle storage: the Oracle Database dialect for multi-node clustering.",
    "sqlserver" to "Wiggle SQL Server storage: the Microsoft SQL Server dialect for multi-node clustering.",
    "cassandra" to "Wiggle Cassandra storage: a partition-correct, LWT-based store for multi-node clustering."
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
