dependencies {
    api(project(":election"))
    api(project(":core"))
    // ApiServlet; the application's Tomcat supplies the servlet API at runtime
    compileOnly("jakarta.servlet:jakarta.servlet-api:${property("servletApiVersion")}")
    // No storage dependency: the server core is storage-agnostic and builds its store from an
    // injected StorageFactory. The runnable server is the :boot module.
}
