import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    id("org.springframework.boot")
    `java-test-fixtures`
}

// The server as a Spring Boot application: the node's parts as beans, the API on Boot's Tomcat.
// Not published; versions of everything it runs with come from Boot's BOM.
dependencies {
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))
    implementation(project(":server"))
    implementation(project(":postgres"))
    implementation(project(":console"))
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")   // Tomcat, and MVC for Actuator's endpoints
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // @WiggleBootTest, the test profiles and WiggleTestNodes, for any module's tests.
    testFixturesApi(platform(SpringBootPlugin.BOM_COORDINATES))
    testFixturesApi(project(":server"))
    testFixturesApi(project(":client"))
    testFixturesApi(project(":test-fixtures"))
    testFixturesApi("org.springframework.boot:spring-boot-starter-test")

    testImplementation(platform(SpringBootPlugin.BOM_COORDINATES))
    testImplementation(project(":client"))
    testImplementation(project(":test-fixtures"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

springBoot {
    mainClass.set("com.wiggle.boot.WiggleApplication")
}
