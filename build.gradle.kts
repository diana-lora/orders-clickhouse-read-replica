plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.spring") version "2.3.21"
    id("org.springframework.boot") version "4.1.1"
    id("io.spring.dependency-management") version "1.1.7"
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"
description = "orders-clickhouse-read-replica"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")

    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")

    implementation("org.springframework.boot:spring-boot-starter-flyway:4.0.1")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    implementation("com.clickhouse:clickhouse-jdbc:0.11.0")
    implementation("org.apache.httpcomponents.client5:httpclient5")
    implementation("org.flywaydb:flyway-database-clickhouse:10.11.0")

    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("tools.jackson.module:jackson-module-kotlin")

    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:3.2.0")

    implementation("io.github.oshai:kotlin-logging-jvm:8.0.4")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("io.mockk:mockk:1.14.11")
    testImplementation("com.ninja-squad:springmockk:5.0.1")
    testImplementation("org.awaitility:awaitility-kotlin")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.testcontainers:testcontainers-clickhouse")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("com.lemonappdev:konsist:0.16.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
    }
}

ktlint {
    version.set("1.8.0")
    filter {
        exclude { it.file.path.contains("/build/") }
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
