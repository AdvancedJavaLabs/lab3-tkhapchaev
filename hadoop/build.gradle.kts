plugins {
    id("java")
}

group = "ru.sales.mapreduce"
version = "1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    implementation("org.apache.hadoop:hadoop-common:3.2.1")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.2.1")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-jobclient:3.2.1")

    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")

    testCompileOnly("org.projectlombok:lombok:1.18.30")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.30")
}

tasks.test {
    useJUnitPlatform()
}