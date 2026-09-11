import com.vanniktech.maven.publish.SonatypeHost
import org.gradle.plugins.signing.Sign

// wiggle-client-all: the client, fully shaded into ONE self-contained jar. gRPC, protobuf, Guava
// and friends are relocated under `com.wiggle.shaded`, and the published POM has NO dependencies —
// so this jar drops onto any classpath (even one already using a different gRPC/protobuf) without a
// single version pin, exclusion, or resolutionStrategy hack. The public API is unchanged: consumers
// use `com.wiggle.*` exactly as with wiggle-client.
//
// It publishes the Shadow plugin's `shadow` COMPONENT (artifact = the fat jar, dependencies = the
// empty `shadow` configuration -> a dependency-free POM). The vanniktech BASE plugin gives it the
// same Central-Portal upload + signing as every other module, so `./gradlew publishToMavenCentral`
// releases it alongside the rest.
plugins {
    `java-library`
    id("com.gradleup.shadow") version "9.0.0"
    id("com.vanniktech.maven.publish.base")
}

dependencies {
    // Swallowed whole into the shaded jar (client -> core, proto, grpc, protobuf, ...).
    implementation(project(":client"))
}

tasks.shadowJar {
    archiveClassifier.set("")            // this IS the module's main artifact
    mergeServiceFiles()                  // fold + rewrite META-INF/services (gRPC provider lookup)

    // Relocate every third-party package the client drags in (gRPC pulls protobuf, Guava, gson,
    // perfmark, the proto-google-common-protos tree — com.google.api/rpc/type/... — and a few
    // annotation libs). One com.google rule covers protobuf + guava + all the common-protos.
    // Nothing under these prefixes is part of wiggle's public API (that's com.wiggle.*), so this is
    // transparent to consumers. javax.annotation and android.annotation are left alone (harmless,
    // duplicate-safe annotations that don't clash at runtime).
    val shaded = "com.wiggle.shaded"
    relocate("com.google", "$shaded.com.google")
    relocate("io.grpc", "$shaded.io.grpc")
    relocate("io.perfmark", "$shaded.io.perfmark")
    relocate("org.checkerframework", "$shaded.org.checkerframework")
    relocate("org.codehaus.mojo", "$shaded.org.codehaus.mojo")

    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA", "module-info.class")
}

// The plain (empty) jar would collide with the shaded one on the same classifier; suppress it.
tasks.named<Jar>("jar") { enabled = false }
tasks.named("assemble") { dependsOn(tasks.shadowJar) }

// Maven Central requires -sources and -javadoc jars for every jar artifact. This module has no
// source of its own; the shaded jar's public API IS wiggle-client's (com.wiggle.client.*), so we
// ship :client's sources and javadoc (Maven republishes them under the wiggle-client-all
// coordinates). Force :client to configure first so its jar tasks are resolvable here.
evaluationDependsOn(":client")

// Publish the SHADOW component: artifact = shadowJar, dependencies = the (empty) `shadow`
// configuration, giving a dependency-free POM; plus the sources/javadoc jars Central mandates.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["shadow"])
            artifactId = "wiggle-client-all"
            artifact(project(":client").tasks.named("sourcesJar")) { classifier = "sources" }
            artifact(project(":client").tasks.named("mavenPlainJavadocJar")) { classifier = "javadoc" }
        }
    }
}

mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL, automaticRelease = true)
    signAllPublications()
    coordinates("sh.wiggle", "wiggle-client-all", version.toString())

    pom {
        name.set("Wiggle client (shaded)")
        description.set("Wiggle client as one self-contained jar: gRPC/protobuf/Guava relocated " +
                "under com.wiggle.shaded, zero transitive dependencies.")
        url.set("https://github.com/hadielmougy/wiggle")
        inceptionYear.set("2026")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers { developer { id.set("hadielmougy"); name.set("Hadi Elmougy") } }
        scm {
            url.set("https://github.com/hadielmougy/wiggle")
            connection.set("scm:git:https://github.com/hadielmougy/wiggle.git")
            developerConnection.set("scm:git:ssh://git@github.com/hadielmougy/wiggle.git")
        }
    }
}

// Sign with the local GnuPG agent for Central, but never for a local install.
plugins.withId("signing") {
    extensions.configure<SigningExtension> { useGpgCmd() }
    tasks.withType<Sign>().configureEach {
        onlyIf { !gradle.startParameter.taskNames.any { it.contains("MavenLocal") } }
    }
}
