import java.util.jar.JarFile
import java.util.zip.ZipFile

// The `swath` binary: cli/* (App, ListCommand, ResumeCommand, ...),
// wiring S3PageFetcher/S3ClientFactory from swath-s3 into picocli commands.
plugins {
    id("swath.java-conventions")
    application
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":swath-core"))
    implementation(project(":swath-s3"))

    implementation(libs.picocli)
    annotationProcessor(libs.picocli.codegen)
    implementation(libs.slf4j.api)
    // micrometer-core is `implementation`-scoped inside swath-core, so it doesn't cross
    // the module boundary on its own; ListCommand directly calls ctx.metrics().registry()
    // (a MeterRegistry-typed return) when wiring JsonRunSummaryWriter.
    implementation(libs.micrometer.core)
    // commons-codec is `implementation`-scoped inside swath-core (arrives there only
    // transitively, via hadoop-common), so it doesn't cross the module boundary on its
    // own; ListCommand's resume path recomputes a finalized part's MD5 directly.
    implementation(libs.commons.codec)
    runtimeOnly(libs.logback.classic)

    // ITs (HardCrashSigkillResumeProcessIT, Int12BrokenPipeProcessIT, ...) drive the
    // testkit fixtures + LocalStackSupport from both library modules.
    testImplementation(testFixtures(project(":swath-core")))
    testImplementation(testFixtures(project(":swath-s3")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.localstack)
    // logback is the runtime SLF4J binding; SeedZeroProgressHeartbeatTest's heartbeat/first-request
    // checks capture those log lines via a logback ListAppender (mirrors swath-core's own testImplementation of this),
    // so it must be on the test compile classpath too.
    testImplementation(libs.logback.classic)
    // jackson-databind is `implementation`-scoped inside swath-core (arrives there only
    // transitively, via parquet-hadoop), so it doesn't cross the module boundary on its
    // own; the *SummaryJsonTest family asserts on the CLI's JSON summary output via
    // ObjectMapper/JsonNode directly.
    testImplementation(libs.jackson.databind)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass = "io.varve.swath.cli.App"
    applicationName = "swath"
    // JDK-25 FFM API (TerminalCapabilities' isatty probe) prints an
    // "illegal native access" advisory to stderr on every real invocation unless the JVM is
    // told the module doing it is trusted. Threads through the generated
    // start scripts (installDist/distZip) AND the `run` task (both consume this).
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

val releaseLegalFiles = listOf("LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md")

distributions {
    main {
        contents {
            from(releaseLegalFiles.map(rootProject::file))
        }
    }
}

// The checked-in notice file is also the output of the opt-in root generator. When both
// generation and packaging are requested in one invocation, make that ordering explicit
// without making normal packaging regenerate (and thereby hide) a stale notice file.
listOf("installDist", "distZip", "distTar").forEach { taskName ->
    tasks.named(taskName) {
        mustRunAfter(rootProject.tasks.named("generateThirdPartyNotices"))
    }
}

tasks.named<JavaExec>("run") {
    // Allow piping (broken-pipe handling exercised by Int12BrokenPipeProcessIT).
    standardInput = System.`in`
}

tasks.named<Test>("test") {
    // TuneOptionsTest reads docs/usage.md at runtime to check the `--tune` table against the
    // registry. Without this, Gradle only tracks compiled classes/resources as inputs, so a
    // docs-only edit leaves the task UP-TO-DATE and the doc/code parity check silently never runs.
    inputs.file(rootProject.file("docs/usage.md")).withPathSensitivity(PathSensitivity.RELATIVE)
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    // A single runnable fat jar, `java -jar swath.jar list ...`, no Docker/unzip.
    archiveBaseName.set("swath")
    archiveClassifier.set("")
    archiveVersion.set("")

    // Coalesce colliding META-INF/services entries (hadoop FileSystem, AWS SDK
    // providers) — without this, shading silently keeps only one jar's service file
    // and the fat jar throws ServiceConfigurationError / "No FileSystem for scheme:
    // file" at runtime.
    mergeServiceFiles()

    // Shadow otherwise retains an arbitrary one of each colliding legal resource.
    // Append preserves every upstream text without changing service-file or
    // multi-release handling.
    append("META-INF/LICENSE")
    append("META-INF/LICENSE.md")
    append("META-INF/LICENSE.txt")
    append("META-INF/LICENSE.zentus")
    append("META-INF/NOTICE")
    append("META-INF/NOTICE.md")
    append("META-INF/NOTICE.txt")

    // Use identical, discoverable paths in the runnable jar and distributions.
    from(releaseLegalFiles.map(rootProject::file))
    dependsOn(rootProject.tasks.named("verifyThirdPartyNotices"))

    // Strip signed-jar metadata from merged dependency jars — a leftover signature
    // file no longer matches the shaded manifest and JVM startup fails with
    // "Invalid signature file digest for Manifest main attributes".
    exclude("META-INF/*.SF", "META-INF/*.RSA", "META-INF/*.DSA")

    // dnsjava (pulled in transitively via hadoop-common) ships a multi-release jar
    // (its java.net.spi.InetAddressResolverProvider impl lives under
    // META-INF/versions/18/) but Shadow 8.3.x doesn't propagate the source jars'
    // `Multi-Release: true` manifest attribute (fixed upstream only in Shadow 9.x,
    // which isn't Gradle-9.0.0-compatible yet — see report). Without it the JVM
    // never looks in the versioned directory, the ServiceLoader entry resolves to
    // nothing, and DNS lookups blow up with ServiceConfigurationError at runtime.
    manifest {
        attributes(
            "Multi-Release" to "true",
            // Same JDK-25 native-access grant as applicationDefaultJvmArgs above, but for
            // `java -jar swath.jar ...` (the fat jar has no start script to carry a JVM flag) --
            // the JDK reads this manifest attribute itself and grants ALL-UNNAMED without
            // requiring the caller to pass --enable-native-access on the command line.
            "Enable-Native-Access" to "ALL-UNNAMED",
        )
    }

    // Deliberately no relocate(...): sqlite-jdbc extracts a native .so/.dll from its
    // packaged resource path at runtime, and relocation would break that lookup.
}

val verifyLegalArtifactContents by tasks.registering {
    group = "verification"
    description = "Checks that jar and application distributions carry the complete legal bundle."
    dependsOn("shadowJar", "installDist", "distZip")
    doLast {
        val shadowJar = tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar").get()
        val jarFile = shadowJar.archiveFile.get().asFile
        val missingFromJar = ZipFile(jarFile).use { jar ->
            releaseLegalFiles.filter { jar.getEntry(it) == null }
        }
        check(missingFromJar.isEmpty()) { "swath.jar is missing legal files: $missingFromJar" }

        // Assert actual text, not merely an arbitrary META-INF/NOTICE survivor. These
        // components exercise the Avro/Hadoop/AWS notice variants present in the CLI graph.
        val licenseReportDir = layout.buildDirectory.dir("reports/licenses").get().asFile
        listOf("avro-1.9.2.jar", "hadoop-common-3.4.1.jar", "annotations-2.31.78.jar").forEach { artifact ->
            val notice = licenseReportDir.resolve("$artifact/META-INF/NOTICE")
            val noticeTxt = licenseReportDir.resolve("$artifact/META-INF/NOTICE.txt")
            val source = listOf(notice, noticeTxt).firstOrNull { it.isFile }
                ?: error("License report did not extract a representative notice for $artifact")
            ZipFile(jarFile).use { jar ->
                val entryName = "META-INF/${source.name}"
                val entry = jar.getEntry(entryName)
                check(entry != null) { "swath.jar is missing merged notice entry: $entryName" }
                val merged = jar.getInputStream(entry).bufferedReader().readText()
                check(source.readText().trim() in merged) {
                    "swath.jar lost the $artifact ${source.name} text while shading"
                }
            }
        }

        val distribution = tasks.named<Sync>("installDist").get().destinationDir
        val missingFromInstall = releaseLegalFiles.filter { !distribution.resolve(it).isFile }
        check(missingFromInstall.isEmpty()) { "installDist is missing legal files: $missingFromInstall" }

        val distZip = tasks.named<Zip>("distZip").get()
        val missingFromZip = ZipFile(distZip.archiveFile.get().asFile).use { zip ->
            releaseLegalFiles.filter { legal -> zip.entries().asSequence().none { it.name.endsWith("/$legal") } }
        }
        check(missingFromZip.isEmpty()) { "distZip is missing legal files: $missingFromZip" }
    }
}

val verifyVersionedCliJars by tasks.registering {
    group = "verification"
    description = "Checks ordinary and shaded CLI jars carry the canonical Implementation-Version."
    dependsOn("jar", "shadowJar")
    doLast {
        val expectedVersion = project.version.toString()
        val ordinaryJar = tasks.named<Jar>("jar").get().archiveFile.get().asFile
        val shadowJar = tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar")
            .get().archiveFile.get().asFile
        listOf(ordinaryJar, shadowJar).forEach { archive ->
            val actualVersion = JarFile(archive).use { it.manifest.mainAttributes.getValue("Implementation-Version") }
            check(actualVersion == expectedVersion) {
                "${archive.name} has Implementation-Version '$actualVersion', expected '$expectedVersion'"
            }
        }
    }
}

val verifyContainerLegalCopies by tasks.registering {
    group = "verification"
    description = "Checks Dockerfile COPY instructions install the legal bundle at runtime paths."
    val dockerfile = rootProject.file("Dockerfile")
    inputs.file(dockerfile)
    doLast {
        val copyInstruction = Regex(
            """^COPY\s+--from=build\s+--chown=10001:10001\s+(\S+)\s+(\S+)\s*$""",
            RegexOption.MULTILINE,
        )
        val copies = copyInstruction.findAll(dockerfile.readText())
            .map { it.groupValues[1] to it.groupValues[2] }
            .toSet()
        releaseLegalFiles.forEach { legal ->
            val expected = "/src/$legal" to "/opt/swath/$legal"
            check(expected in copies) {
                "Dockerfile lacks required build-stage COPY: ${expected.first} -> ${expected.second}"
            }
        }
    }
}

tasks.named("check") {
    dependsOn(rootProject.tasks.named("verifyThirdPartyNotices"),
        verifyLegalArtifactContents, verifyContainerLegalCopies, verifyVersionedCliJars)
}
