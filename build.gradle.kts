import com.vanniktech.maven.publish.MavenPublishBaseExtension
import com.vanniktech.maven.publish.SonatypeHost
import java.time.Duration

plugins {
    java
    id("com.vanniktech.maven.publish") version "0.30.0" apply false
}

allprojects {
    group = "io.github.hadielmougy"
    version = "1.0.0"

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

// ------------------------------------------------------------------ publishing
//
// Only the reusable library modules are published to Maven Central; `example` and
// `tests` are excluded. The actual upload is a manual, credentialed step -- see
// RELEASING.md. Signing and Central-Portal credentials are read from properties or
// environment variables and are never stored in the repository.

val publishedModules = setOf("core", "proto", "client", "server")

val moduleDescriptions = mapOf(
    "core" to "Wiggle shared model: JSON, the compiled workflow graph, retry policy, wire records.",
    "proto" to "Wiggle gRPC/protobuf stubs for the control-plane wire protocol.",
    "client" to "Wiggle client: the workflow authoring DSL, imperative builder, and worker runtime.",
    "server" to "Wiggle server: the workflow engine, cluster manager, and control-plane API."
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
