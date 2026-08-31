import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.jar.JarFile
import java.util.zip.ZipFile
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.jvm.toolchain.JavaToolchainService

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
    // Text output supports Zstandard streams directly; swath-core's implementation-scoped
    // Parquet dependency does not cross the module boundary onto the CLI compile classpath.
    implementation(libs.zstd.jni)
    // Terminal geometry for ProgressDisplay's in-place redraw (TerminalGeometry). JLine is used for
    // the size query alone: it never receives this process's stderr, its streams or its signals.
    // jline-native is the JNI provider's bundled natives, dead weight next to the FFM provider a
    // JDK-25 toolchain always selects — and dropping it keeps platform .so/.dll files out of the
    // shaded jar and out of the third-party notices.
    implementation(libs.jline.terminal) {
        exclude(group = "org.jline", module = "jline-native")
    }
    runtimeOnly(libs.jline.terminal.ffm) {
        exclude(group = "org.jline", module = "jline-native")
    }
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

// distTar defaults to an UNCOMPRESSED .tar, which would ship a ~90 MB release asset
// next to a .zip of the same tree — pure waste on every download. Gzip is also what
// the wider ecosystem expects of a `.tar.gz`: Homebrew formulae, in particular, cannot
// consume a bare .tar url.
tasks.named<Tar>("distTar") {
    compression = Compression.GZIP
    archiveExtension = "tar.gz"
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
    // TuneOptionsTest reads docs/configuration.md at runtime to check the `--tune` table against the
    // registry. Without this, Gradle only tracks compiled classes/resources as inputs, so a
    // docs-only edit leaves the task UP-TO-DATE and the doc/code parity check silently never runs.
    inputs.file(rootProject.file("docs/configuration.md")).withPathSensitivity(PathSensitivity.RELATIVE)
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

val parquetStartupToolchains = extensions.getByType<JavaToolchainService>()

val parquetStartupBaseline by tasks.registering {
    group = "reporting"
    description = "Records fresh-process CLI and replay startup samples for the PR 7 baseline."
    dependsOn("shadowJar", ":swath-replay:installDist")
    val report = layout.buildDirectory.file("reports/parquet-linkability/startup-baseline.jsonl")
    outputs.file(report)
    outputs.upToDateWhen { false }
    doLast {
        val launcher = parquetStartupToolchains.launcherFor {
            languageVersion = JavaLanguageVersion.of(25)
        }.get()
        val javaBinary = launcher.executablePath.asFile.absolutePath
        val jar = tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar")
            .get().archiveFile.get().asFile.absolutePath
        val replay = project(":swath-replay").layout.buildDirectory.file(
            "install/swath-replay/bin/swath-replay").get().asFile.absolutePath
        fun json(fields: Map<String, String>): String = fields.entries.joinToString(
            prefix = "{", postfix = "}") { (key, value) ->
            val escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
            "\"$key\":\"$escaped\""
        }
        fun sample(app: String, mode: String, iteration: Int, command: List<String>): String {
            val builder = ProcessBuilder(command)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
            builder.environment()["JAVA_HOME"] = launcher.metadata.installationPath.asFile.absolutePath
            val start = System.nanoTime()
            val process = builder.start()
            check(process.waitFor(30, TimeUnit.SECONDS)) {
                "$app $mode startup timed out"
            }
            check(process.exitValue() == 0) { "$app $mode startup exited ${process.exitValue()}" }
            return json(linkedMapOf(
                "format" to "swath-parquet-startup-baseline-v1",
                "app" to app,
                "mode" to mode,
                "iteration" to iteration.toString(),
                "elapsed_ns" to (System.nanoTime() - start).toString(),
            ))
        }

        val lines = mutableListOf(json(linkedMapOf(
            "format" to "swath-parquet-startup-baseline-v1",
            "record" to "run",
            "java_home" to launcher.metadata.installationPath.asFile.absolutePath,
            "os" to "${System.getProperty("os.name")} ${System.getProperty("os.arch")}",
            "clock" to "fresh process wall time; filesystem cache uncontrolled",
            "recorded_at" to Instant.now().toString(),
        )))
        repeat(5) {
            lines += sample("cli", "version", it, listOf(javaBinary, "-jar", jar, "--version"))
        }
        repeat(5) {
            lines += sample("cli", "help", it, listOf(javaBinary, "-jar", jar, "--help"))
        }
        repeat(5) { lines += sample("replay", "help", it, listOf(replay, "--help")) }
        val output = report.get().asFile
        output.parentFile.mkdirs()
        output.writeText(lines.joinToString("\n", postfix = "\n"))
    }
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
        // components exercise the Parquet/AWS notice variants present in the CLI graph.
        // Named by module, resolved to the shipped version: spelling the version here made a
        // dependency bump fail this check for a reason that had nothing to do with legal text.
        val licenseReportDir = layout.buildDirectory.dir("reports/licenses").get().asFile
        val shippedVersions = configurations.runtimeClasspath.get().incoming.artifacts.artifacts
            .mapNotNull { it.id.componentIdentifier as? ModuleComponentIdentifier }
            .associate { it.module to it.version }
        val noticeBearingModules = listOf("parquet-hadoop", "annotations")
        noticeBearingModules.map { module ->
            val version = shippedVersions[module]
                ?: error("$module is no longer on the CLI runtime classpath; pick another notice sample")
            "$module-$version.jar"
        }.forEach { artifact ->
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
