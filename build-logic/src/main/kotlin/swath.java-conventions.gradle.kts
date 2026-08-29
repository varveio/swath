import java.time.Duration

// Shared build config for every swath-* module. Replaces the copy-pasted `subprojects {}`/duplicated
// root+replay build-file config that would otherwise 5x across modules: group, the
// JDK 25 LTS toolchain (`--release 25`, no `--enable-preview`), compiler args,
// and the shared `Test` task tuning (tag filtering, Testcontainers env, timeouts).
// Applied by every module via `plugins { id("swath.java-conventions") }`.

plugins {
    java
    id("com.diffplug.spotless")
    // Dependency audit, aggregated by the root's `buildHealth` task and run on demand. It is
    // the tool that answers "does this module still need that?", which is worth having as the
    // shipped closure gets pruned — but it is NOT wired into `check`, and should not be. Its
    // model is that a module declares everything it touches and exposes as `api` everything
    // that reaches its own signatures, and this build deliberately does the opposite in
    // places: swath-core keeps parquet-hadoop and hadoop-common `implementation`-scoped
    // precisely so no consumer can import those types, a boundary swath-replay enforces with
    // its own verifyNoParquetOrHadoopOnCompileClasspath task. Taken as a gate, the plugin
    // would demand that swath-core promote both to `api` and undo it. Read the report, apply
    // judgement.
    id("com.autonomousapps.dependency-analysis")
}

group = "io.varve.swath"
version = rootProject.version

repositories {
    mavenCentral()
}

java {
    // JDK 25 LTS, compile with --release 25, no --enable-preview.
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

// swath is a local-disk lister; it never talks to HDFS/YARN/ZooKeeper. parquet-hadoop
// drags in hadoop-common + hadoop-mapreduce-client-core, which in turn pull the whole
// HDFS-client/YARN/ZooKeeper/Curator/Kerberos stack transitively — none of it is ever
// exercised by swath's local Parquet read/write path. A per-dependency
// `exclude()` on one `implementation(libs.hadoop...)` declaration only prunes that one
// dependency's own subtree, so a module reachable through a *different* path (e.g.
// hadoop-auth via hadoop-mapreduce-client-core -> hadoop-yarn-common, even though
// swath-core also excludes hadoop-auth on its hadoop-common declaration) still lands on
// the resolved runtimeClasspath that `installDist` packages. A configuration-level
// exclude filters the whole resolved graph regardless of which edge introduced the
// module, so it reaches every module's runtimeClasspath (in particular swath-cli's and
// swath-replay's, the ones whose installDist actually ships a lib/ directory).
configurations.all {
    exclude(group = "org.apache.hadoop", module = "hadoop-auth")
    exclude(group = "org.apache.hadoop", module = "hadoop-hdfs-client")
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-api")
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-common")
    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-client")
    exclude(group = "org.apache.zookeeper", module = "zookeeper")
    exclude(group = "org.apache.zookeeper", module = "zookeeper-jute")
    exclude(group = "org.apache.curator", module = "curator-client")
    exclude(group = "org.apache.curator", module = "curator-framework")
    exclude(group = "org.apache.curator", module = "curator-recipes")
    exclude(group = "org.bouncycastle", module = "bcprov-jdk18on")
    exclude(group = "org.apache.commons", module = "commons-math3")
    // Netty arrives twice and is used neither time. hadoop-common wants it for the IPC and
    // HDFS-client paths already excluded above; the AWS SDK ships `netty-nio-client` for
    // `S3AsyncClient`, and swath builds only the synchronous client over `apache-client`
    // (S3ClientFactory) — no source in any module names an async client or a netty type.
    // It was not free: eleven jars, and 40 of the 62 advisories an OSV scan of the CLI
    // distribution reported, including a `netty-transport-native-epoll` left at 4.1.100
    // beside `netty-transport-classes-epoll` at 4.1.118 — a native/classes version skew
    // that would have failed at runtime had anything actually loaded epoll.
    //
    // The SDK's own adapter jar goes with it: dropping only `io.netty` would leave
    // netty-nio-client shipping with nothing behind it, which is strictly worse than either
    // state — an async HTTP provider that fails on use rather than being absent.
    exclude(group = "io.netty")
    exclude(group = "software.amazon.awssdk", module = "netty-nio-client")
}

// Floors for libraries no swath module declares. They arrive under hadoop-common and
// hadoop-mapreduce-client-core, whose POMs pin versions years behind their fixed releases
// (jackson 2.12.7, guava 27.0, avro 1.9.2 with a 9.8 RCE), and every one of them ships in
// the CLI and replay distributions. Constraints rather than `force`: a constraint raises a
// floor and still loses to anything that legitimately needs newer, where `force` would
// silently pin a future dependency DOWN to this line.
//
// The versions live in the version catalog, so Dependabot raises them like any other
// dependency. Applied per source set rather than to `implementation` alone so that a
// distribution assembled from a source set of its own — swath-replay's `conformance`
// launcher — is pinned by the same list.
//
// Not listed, and deliberately: commons-collections 3.2.2 is the release that removed the
// InvokerTransformer deserialization gadget, it is the last 3.x, and OSV reports nothing
// against it. It stays as hadoop-common resolves it.
val versionCatalog = extensions.getByType<VersionCatalogsExtension>().named("libs")
val shippedTransitiveFloors = listOf(
    "jackson-databind",
    "guava",
    "avro",
    "commons-compress",
    "commons-beanutils",
    "commons-configuration2",
    "commons-lang3",
).map { alias ->
    versionCatalog.findLibrary(alias).orElseThrow {
        IllegalStateException("libs.versions.toml has no library '$alias' to constrain")
    }
}

sourceSets.configureEach {
    val declarableConfiguration = implementationConfigurationName
    shippedTransitiveFloors.forEach { floor ->
        dependencies.constraints.add(declarableConfiguration, floor)
    }
}

// Gradle resolves a version conflict within one configuration, so a distribution built from a
// single runtime classpath cannot ship a module twice. A distribution that packs a SECOND
// closure into the same `lib/` can, and swath-replay does exactly that for its conformance
// launcher: it shipped jackson-core, -databind and -annotations at both 2.12.7 and 2.22, with
// nothing but start-script classpath order deciding which generation ran. Nothing failed, and
// nothing reported it. Assert on the shipped directory itself, which is the only place the
// union of the closures actually exists.
plugins.withId("application") {
    val verifyNoDuplicateModuleVersions by tasks.registering {
        group = "verification"
        description = "Checks the application distribution ships at most one version of each module."
        val installTask = tasks.named<Sync>("installDist")
        dependsOn(installTask)
        doLast {
            val libraryDirectory = installTask.get().destinationDir.resolve("lib")
            // Maven artifact names are `<module>-<version>[-<classifier>].jar`, and the version
            // always starts with a digit — so the split point is the last hyphen followed by one.
            val moduleAndVersion = Regex("""^(.+)-(\d[^-]*(?:-.+)?)\.jar$""")
            val versionsByModule = (libraryDirectory.listFiles() ?: emptyArray())
                .mapNotNull { moduleAndVersion.matchEntire(it.name)?.destructured }
                .groupBy({ (module, _) -> module }, { (_, version) -> version })
            // A trailing classifier is indistinguishable from a version qualifier by shape alone
            // (`0.2.5-SNAPSHOT-conformance` against `0.2.5-SNAPSHOT`), and a classified sibling is
            // a second ARTIFACT of one version, not a second version. Treat a group as one version
            // when its shortest member is a hyphen-bounded prefix of every other — which
            // `2.12.7` is not of `2.22.0`, the case this exists to catch.
            val duplicated = versionsByModule.filterValues { versions ->
                val shortest = versions.minBy { it.length }
                versions.any { it != shortest && !it.startsWith("$shortest-") }
            }
            check(duplicated.isEmpty()) {
                "${libraryDirectory} ships more than one version of: " +
                    duplicated.entries.sortedBy { it.key }
                        .joinToString("; ") { (module, versions) -> "$module ${versions.sorted()}" }
            }
        }
    }
    tasks.named("check") { dependsOn(verifyNoDuplicateModuleVersions) }
}

// Spotless enforces import hygiene and the SPDX license header on every first-party Java
// source. `spotlessCheck` is wired into `check` (below), so CI fails on an unused import, a
// mis-ordered import block, or a missing/malformed header; `./gradlew spotlessApply` fixes all
// three in place. The header text lives at the repo root (config/spotless/license-header.java)
// so every module resolves the same file.
spotless {
    java {
        target("src/*/java/**/*.java")
        removeUnusedImports()
        importOrder()
        licenseHeaderFile(rootProject.file("config/spotless/license-header.java"))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 25
    options.compilerArgs.add("-parameters")
    options.compilerArgs.add("-Xlint:deprecation")
    // No --enable-preview — shipped artifacts must run without a flag.
}

// Byte-for-byte reproducible archives. Without these, every Jar/Zip/Tar embeds the
// build's wall-clock timestamps and the filesystem's directory order, so two builds
// of the same commit produce different bytes. That matters here specifically: the
// container-promotion contract carries the tested uber-jar into the image and
// `cmp`s it, and the release pipeline's "the image ships exactly what was tested"
// claim is only as strong as the build being deterministic. It also removes the
// residual risk documented in ci.yml's docker-publish provenance note (a BuildKit
// cache miss between the smoke build and the pushed build could otherwise yield
// different bytes).
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

// The commit that produced this artifact. `Implementation-Version` alone cannot
// answer "which build am I running?" between releases — every merge on the 0.1.0
// -SNAPSHOT line reports the same string, and dev container images are now
// published per merge. Resolution order:
//   1. GITHUB_SHA — always set in Actions, and correct even in a detached-HEAD
//      checkout where `git rev-parse HEAD` would report the merge commit.
//   2. `git rev-parse HEAD` for local builds, guarded on .git existing so a build
//      from an extracted source archive degrades instead of failing.
//   3. "unknown".
val gitDirectory = rootProject.layout.projectDirectory.dir(".git").asFile
val commitFromGit: Provider<String> =
    if (gitDirectory.exists()) {
        providers.exec {
            workingDir = rootProject.layout.projectDirectory.asFile
            commandLine("git", "rev-parse", "HEAD")
            isIgnoreExitValue = true
        }.standardOutput.asText.map { it.trim() }
    } else {
        providers.provider { "" }
    }
val implementationCommit: Provider<String> =
    providers.environmentVariable("GITHUB_SHA")
        .orElse(commitFromGit)
        .map { it.trim().ifBlank { "unknown" } }
        .orElse("unknown")

// Every shipped Java archive carries the canonical Gradle version. The CLI uses
// this manifest entry for `--version`; applying it here also prevents ordinary
// jars and internal module jars from silently reporting an unstamped build.
tasks.withType<Jar>().configureEach {
    manifest {
        attributes(
            "Implementation-Version" to project.version,
            "Implementation-Commit" to implementationCommit.get(),
        )
    }
}

tasks.withType<Test>().configureEach {
    timeout.set(Duration.ofMinutes(10))
    useJUnitPlatform {
        // jqwik + jupiter both run on the platform.
        includeEngines("junit-jupiter", "jqwik")
        // Keep the default suite fast. Heavy scale/throughput tests (the gated
        // <=50M-key PERF tier, future endpoint matrices) are @Tag("perf") and are
        // OPT-IN only:  ./gradlew test -Pperf
        // Scale must never be seeded into a real store object-by-object — use the
        // in-memory MockPageFetcher (no puts) or a synthetic S3 Inventory manifest;
        // real-store ITs stay at modest counts. See docs/ops/dev/TESTING.md.
        // `-PonlyPerf` runs ONLY the @Tag("perf") tier (lean perf run, e.g. the nightly job) so it
        // doesn't drag the whole fast+integration suite along; `-Pperf` keeps the "run everything
        // INCLUDING perf" semantics; default excludes perf entirely.
        if (project.hasProperty("onlyPerf")) includeTags("perf")
        else if (!project.hasProperty("perf")) excludeTags("perf")
        // `deep` tier: schedule-sensitive probe-budget tests + latency-injecting
        // retry/AIMD/throttle/timeout tests. Demoted OFF the per-commit fast suite — they are
        // slow (real Thread.sleep) or flaky-under-contention, and the invariants they guard are
        // pinned per-commit by cheap deterministic smokes instead. `-Pdeep` runs ONLY the deep
        // tier (the CI deep job on main + a nightly cron), analogous to -PonlyIntegration; by
        // default the tier is excluded.
        if (project.hasProperty("deep")) includeTags("deep") else excludeTags("deep")
        // Docker-free quick inner loop (skip Testcontainers ITs): ./gradlew test -PnoIntegration
        if (project.hasProperty("noIntegration")) excludeTags("integration")
        // Integration-only tier for CI: run *only* the Testcontainers/LocalStack ITs,
        // so the merge-time integration job does not re-run the whole fast tier that
        // the fast-tests job already ran. Mutually exclusive with -PnoIntegration.
        if (project.hasProperty("onlyIntegration")) includeTags("integration")
    }
    // Per-module test parallelism. swath-core (~184 classes) is the sole critical path of the
    // fast tier — every other module's suite finishes inside its shadow (org.gradle.parallel
    // already overlaps the modules) — so intra-module test forks on swath-core are the only lever
    // that shortens fast-tier wall-clock. Measured: forks=4 roughly halves swath-core's
    // :test wall (~137s → ~66s), the single biggest lever on the critical-path module.
    //
    // Fork policy:
    //  * The FAST tier forks swath-core across `min(4, cores/2)` JVMs — defensive on small CI
    //    runners (a 4-core box → 2 forks, an 8-core dev box → 4). Other modules stay serial: they
    //    add nothing to the critical path, and their socket/Testcontainers ITs would only risk
    //    port/container contention under concurrent forks.
    //  * The `deep` and `perf` tiers stay SERIAL (forks=1): they are latency-injecting /
    //    real-`Thread.sleep`, wall-clock-observing tests whose timing is schedule-sensitive, so
    //    cross-fork CPU contention makes them flaky. They run as their own opt-in
    //    invocations (`-Pdeep` / `-Pperf`/`-PonlyPerf`), so this only gates those runs.
    //  * `-PtestMaxParallelForks=N` still overrides everything (local experiments / a future retune).
    // The one fast-tier test whose absolute wall-clock assertion flaked under forks
    // (RangeScannerReadaheadTest#readaheadOnIsByteExactAndFasterThanSerial) was converted to a
    // deterministic round-trip-structure guard alongside this change.
    val timingSensitiveTier = project.hasProperty("deep")
            || project.hasProperty("perf") || project.hasProperty("onlyPerf")
    val explicitForks = (project.findProperty("testMaxParallelForks") as String?)?.toIntOrNull()
    maxParallelForks = when {
        explicitForks != null -> maxOf(1, explicitForks)
        timingSensitiveTier -> 1
        project.name == "swath-core" -> maxOf(1, minOf(4, Runtime.getRuntime().availableProcessors() / 2))
        else -> 1
    }
    // Re-execution backstops: the timing-sensitive tiers above plus the `kill -9` ITs, whose
    // outcome depends on the machine and the schedule rather than on the sources alone, so a
    // green result recorded on an earlier run says nothing about this one. Gradle cannot know
    // that — the inputs are unchanged, so it is entitled to reuse the result — and the nightly
    // job restores a build cache (gradle/actions/setup-gradle), which served `:swath-core:test`
    // FROM-CACHE and let a nightly deep run "pass" in 2.4 min without executing a single deep
    // test. Opting these invocations out of both caching and up-to-date checks is what makes the
    // backstop a backstop; the fast tier keeps both, since that is where they pay.
    if (timingSensitiveTier || System.getProperty("swath.it.sigkill") != null) {
        outputs.upToDateWhen { false }
        outputs.cacheIf { false }
    }
    // The default Gradle test-worker heap (512 MB) is well under the §5/§7.2 GB-scale memory
    // corridors the perf tier measures against (SORT-PERF, PERF-2): without headroom, a
    // multi-million-entry perf test hits a harness OOM before the pipeline itself gets anywhere
    // near the budget being asserted. Opt-in only, alongside the perf tag.
    if (project.hasProperty("perf") || project.hasProperty("onlyPerf")) maxHeapSize = "2g"
    // Testcontainers reaches the Docker daemon from the test JVM. Default to the
    // local unix socket when the environment doesn't already set DOCKER_HOST
    // (CI provides its own); silence the JNA native-access warning.
    environment("DOCKER_HOST", System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock")
    // docker-java (bundled by Testcontainers) defaults to API 1.32, which newer
    // daemons reject ("client version too old; minimum 1.40"). docker-java reads
    // the version from the `api.version` JVM property, so pin a modern API there.
    systemProperty("api.version", System.getProperty("api.version") ?: "1.43")
    // Forward the opt-in real-kill-9 IT gate (HardCrashSigkillResumeProcessIT,
    // @EnabledIfSystemProperty) from the `./gradlew -Dswath.it.sigkill=true` invocation into the
    // forked test JVM -- a Gradle-CLI `-D` only sets the DAEMON's system property by default, not
    // the forked Test JVM's. Absent (the common case), nothing is forwarded and the IT stays
    // disabled, exactly as before.
    System.getProperty("swath.it.sigkill")?.let { systemProperty("swath.it.sigkill", it) }
    // Same forwarding gap for the CLI help-golden regeneration switch (HelpUsageGoldenTest,
    // `./gradlew test -Dswath.goldens.update=true`) — otherwise it silently no-ops.
    System.getProperty("swath.goldens.update")?.let { systemProperty("swath.goldens.update", it) }
    // The parallel-merge measurement harness is an explicitly selected JUnit test, but Gradle
    // does not otherwise pass daemon -D properties into the forked test JVM. Forward only its
    // opt-in/corpus controls and the sort knobs it deliberately sweeps; without this seam the
    // documented -Dswath.bench=on invocation silently skips the harness.
    if (System.getProperty("swath.bench") == "on") {
        System.getProperties().stringPropertyNames()
            .filter { it == "swath.bench" || it.startsWith("swath.bench.") || it.startsWith("swath.sort.") }
            .forEach { systemProperty(it, System.getProperty(it)) }
    }
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
