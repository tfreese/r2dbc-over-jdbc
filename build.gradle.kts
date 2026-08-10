plugins {
    id("java")
    id("de.freese.gradle.conventions")
    id("io.spring.dependency-management")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:" + property("version_springBoot"))
    }

    dependencies {
        val r2dbcSpiVersion = dependencyManagement.importedProperties["r2dbc-spi.version"]

        dependency("io.r2dbc:r2dbc-spi-test:$r2dbcSpiVersion")

//        dependencySet(group: "io.r2dbc", version: dependencyManagement.importedProperties["r2dbc-spi.version"]) {
//            entry "r2dbc-spi-test"
//        }
    }
}

val mockitoAgent = configurations.create("mockitoAgent")

dependencies {
    implementation("io.projectreactor:reactor-core")
    implementation("io.r2dbc:r2dbc-spi")
    implementation("org.slf4j:slf4j-api")

    testImplementation("com.zaxxer:HikariCP")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("io.projectreactor.addons:reactor-extra")
    testImplementation("io.r2dbc:r2dbc-spi-test")
    testImplementation("org.awaitility:awaitility")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.springframework:spring-jdbc")

    testRuntimeOnly("com.h2database:h2")
    testRuntimeOnly("org.apache.derby:derby")
    testRuntimeOnly("org.hsqldb:hsqldb")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple")

    // To avoid compiler warnings about @API annotations in Log4j Code.
    // testCompileOnly("com.github.spotbugs:spotbugs-annotations")

    // To avoid compiler warnings about @API annotations in JUnit Code.
    // testCompileOnly("org.apiguardian:apiguardian-api")

    testImplementation("org.mockito:mockito-junit-jupiter")
    mockitoAgent("org.mockito:mockito-core") {
        isTransitive = false
    }
}

tasks.withType<Test>().configureEach {
    val mockitoFiles = mockitoAgent.asPath

    doFirst {
        jvmArgs.add("-javaagent:$mockitoFiles")
    }
}
