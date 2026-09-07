dependencies {
    api(project(":core"))
    api(project(":proto"))
    // No storage dependency: the server core is storage-agnostic and builds its store from an
    // injected StorageFactory. The runnable, all-backends server lives in the :dist module.
}

// The dashboard SPA moved to the :console module (the server no longer serves a web UI, only a
// /healthz probe), so the ClojureScript compile now lives in console/build.gradle.kts.
