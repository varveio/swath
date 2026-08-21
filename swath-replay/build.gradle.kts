import java.time.Duration
import org.gradle.jvm.application.tasks.CreateStartScripts

plugins {
    id("swath.java-conventions")
    application
    // testkit: ObjectEntries/ParquetFixtures/FakeListingStore — the fixture builders and the
    // range-only in-memory store this module's own suites use.
    // Published as test fixtures rather than kept in `src/test` so a sibling tool module can drive
    // the same capture shapes instead of re-deriving them (`swath-sim`'s differential suite).
    `java-test-fixtures`
}

val conformanceSourceSet = sourceSets.create("conformance") {
    compileClasspath += sourceSets.main.get().output + configurations.compileClasspath.get()
    runtimeClasspath += output + compileClasspath + configurations.runtimeClasspath.get()
}

dependencies {
    implementation(libs.picocli)
    annotationProcessor(libs.picocli.codegen)
    implementation(libs.slf4j.api)
    implementation(libs.micrometer.core)
    implementation(platform(libs.jetty.bom))
    implementation(libs.jetty.server)
    implementation(libs.duckdb.jdbc)
    // The swath-core io.varve.swath.sort API (sort-fixture engine, stamp detection, index derive):
    // main-scope so .fixture can call it directly. swath-core declares
    // parquet-hadoop/hadoop/zstd-jni as its OWN main-scope `implementation` deps, so those land on
    // this module's RUNTIME classpath only via this transitive edge — never on its COMPILE
    // classpath, which is the whole point: no io.varve.swath.replay source may import an
    // org.apache.parquet/org.apache.hadoop type.
    implementation(project(":swath-core"))
    runtimeOnly(libs.logback.classic)

    add(conformanceSourceSet.implementationConfigurationName, libs.jackson.databind)

    // testkit fixtures expose swath-core/swath-model types on their own surface (ParquetFixtures
    // returns a PartWriter, ObjectEntries an ObjectEntry), so consumers need them on the compile
    // classpath -- hence `api`. parquet-hadoop is a direct compile-time need of ParquetFixtures
    // (ParquetSchema.canonical() is MessageType-typed) and stays out of the MAIN compile classpath,
    // which verifyNoParquetOrHadoopOnCompileClasspath below still guards.
    testFixturesApi(project(":swath-core"))
    testFixturesImplementation(libs.parquet.hadoop)

    // SwathRoundTripIT starts the replay server and then drives it with swath's own real
    // S3PageFetcher/S3ClientFactory/S3Config (an end-to-end round trip) -- a genuine
    // test-only edge to swath-s3 (this module's MAIN code never depends on swath-s3).
    testImplementation(project(":swath-s3"))
    // Test-only edge to swath-core's LatencyModel/LatencyModels fixture-mode profiles, so a
    // replay test can reuse the same canned profiles MockPageFetcher uses — never on this
    // module's main compile classpath (see ReplayHandler/ReplayServer's doc notes).
    testImplementation(testFixtures(project(":swath-core")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.awssdk.bom))
    testImplementation(conformanceSourceSet.output)
    testImplementation(libs.awssdk.s3)
    testImplementation(libs.awssdk.apache.client)
    testImplementation(libs.parquet.hadoop)
    testImplementation(libs.zstd.jni)
    testImplementation(libs.hadoop.common) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
        exclude(group = "org.apache.hadoop", module = "hadoop-auth")
    }
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jqwik)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.jackson.databind)
    testRuntimeOnly(libs.junit.platform.launcher)
}

val replayRuntimeArtifacts = rootProject.layout.buildDirectory.file(
        "generated/legal/replay-runtime-artifacts.txt")

val writeReplayRuntimeArtifactCoordinates by tasks.registering {
    group = "reporting"
    description = "Writes the exact external artifacts packaged in the replay distribution."
    val runtimeClasspath = configurations.named("runtimeClasspath")
    inputs.files(runtimeClasspath)
    outputs.file(replayRuntimeArtifacts)
    doLast {
        val output = replayRuntimeArtifacts.get().asFile
        output.parentFile.mkdirs()
        val artifacts = runtimeClasspath.get().resolvedConfiguration.resolvedArtifacts
                .map {
                    "${it.moduleVersion.id.group}:${it.name}:${it.moduleVersion.id.version}\t${it.file.name}"
                }
                .distinct()
                .sorted()
        output.writeText(artifacts.joinToString("\n", postfix = "\n"))
    }
}

application {
    mainClass = "io.varve.swath.replay.server.ReplayServerApp"
    applicationName = "swath-replay"
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

val replayLegalFiles = listOf(
        rootProject.file("LICENSE"),
        rootProject.file("NOTICE"),
        project.file("THIRD_PARTY_NOTICES.md"))

val conformanceScriptsDir = layout.buildDirectory.dir("generated/scripts/replayConformance")

val conformanceJar by tasks.registering(Jar::class) {
    archiveClassifier.set("conformance")
    from(conformanceSourceSet.output)
}

val conformanceStartScripts by tasks.registering(CreateStartScripts::class) {
    applicationName = "swath-replay-conformance"
    mainClass.set("io.varve.swath.replay.conformance.ReplayConformanceApp")
    defaultJvmOpts = listOf("--enable-native-access=ALL-UNNAMED")
    outputDir = conformanceScriptsDir.get().asFile
    classpath = files(
            conformanceJar,
            tasks.named("jar"),
            configurations.runtimeClasspath,
            configurations.named(conformanceSourceSet.runtimeClasspathConfigurationName))
}

distributions {
    main {
        contents {
            from(replayLegalFiles)
            into("lib") {
                from(conformanceJar)
                from(configurations.named(conformanceSourceSet.runtimeClasspathConfigurationName))
            }
            into("bin") {
                from(conformanceScriptsDir)
                filePermissions {
                    unix("rwxr-xr-x")
                }
            }
        }
    }
}

listOf("installDist", "distZip", "distTar").forEach { taskName ->
    tasks.named(taskName) {
        dependsOn(conformanceStartScripts)
        dependsOn(rootProject.tasks.named("verifyReplayThirdPartyNotices"))
        mustRunAfter(rootProject.tasks.named("generateReplayThirdPartyNotices"))
    }
}

// Mechanically enforce the compile-classpath boundary: no io.varve.swath.replay source may import
// an org.apache.parquet/org.apache.hadoop type, because swath-core's parquet-hadoop/
// hadoop dependency reaches this module only via the `implementation(project(":swath-core"))` edge
// above, which by design lands those artifacts on this module's RUNTIME classpath but never its
// COMPILE classpath. A future accidental `api(...)` (or a new direct dependency) on either group here
// would silently widen that surface — this task catches it at build time instead of relying on code review.
val verifyNoParquetOrHadoopOnCompileClasspath by tasks.registering {
    group = "verification"
    description = "Fails if org.apache.parquet/org.apache.hadoop artifacts reach the main compile classpath."
    val compileClasspath = configurations.named("compileClasspath")
    inputs.files(compileClasspath)
    doLast {
        val offenders = compileClasspath.get().resolvedConfiguration.resolvedArtifacts
                .map { it.moduleVersion.id }
                .filter { it.group == "org.apache.parquet" || it.group == "org.apache.hadoop" }
                .map { "${it.group}:${it.name}:${it.version}" }
                .distinct()
                .sorted()
        if (offenders.isNotEmpty()) {
            throw GradleException(
                    "swath-replay's main COMPILE classpath must never carry " +
                    "org.apache.parquet/org.apache.hadoop artifacts (no io.varve.swath.replay " +
                    "source may import a parquet/hadoop type; those live only on the RUNTIME classpath " +
                    "via swath-core's own main-scope dependency). Found: " + offenders.joinToString(", "))
        }
    }
}

tasks.named("check") {
    dependsOn(verifyNoParquetOrHadoopOnCompileClasspath)
}

// Overrides the swath.java-conventions default (10 min) — the replay module's suite is
// smaller/faster; kept at its pre-reorg 5-minute budget.
tasks.withType<Test>().configureEach {
    timeout.set(Duration.ofMinutes(5))
}
