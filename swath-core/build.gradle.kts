// The embeddable listing library: engine, runtime, output (incl.
// output/parquet), sort, checkpoint, filter, pipeline, observability, error,
// concurrent, and the store ABSTRACTION (store/* except store/s3, which lives in
// swath-s3). NO aws sdk, NO picocli — see the drift guards in CI / this repo's DoD.
// engine<->runtime is a harmless intra-module cycle (both live here).
plugins {
    id("swath.java-conventions")
    `java-library`
    `java-test-fixtures`
    alias(libs.plugins.jmh)
}

// Hadoop ships as a cluster runtime, so it drags its whole service stack: an embedded Jetty web
// server and Jersey REST layer for the NameNode/DataNode UIs, Guice servlet wiring, JSch for the
// SFTP FileSystem, Kerby for Kerberos, and netty-all for HDFS transport. swath uses exactly one
// corner of Hadoop — `Configuration` plus the local-filesystem codec path that parquet-hadoop
// needs — and reaches none of it. Keeping it out of the shaded jar drops dead weight, keeps
// aged Jetty/Jersey CVEs out of the published image, and removes `javax.servlet.jsp:jsp-api`,
// which resolves with no declared license at all and would otherwise need a blanket exemption in
// the dependency-license gate.
//
// netty is deliberately excluded only by the `netty-all` uber-artifact: the AWS SDK requests the
// individual netty modules for its async client and must keep them.
fun ModuleDependency.excludeHadoopServiceStack() {
    exclude(group = "org.eclipse.jetty")
    exclude(group = "com.sun.jersey")
    exclude(group = "com.github.pjfanning", module = "jersey-json")
    exclude(group = "com.google.inject.extensions", module = "guice-servlet")
    exclude(group = "javax.servlet", module = "javax.servlet-api")
    exclude(group = "javax.servlet.jsp", module = "jsp-api")
    exclude(group = "com.jcraft", module = "jsch")
    exclude(group = "org.apache.kerby")
    exclude(group = "io.netty", module = "netty-all")
}

dependencies {
    // swath-core's public API surface (e.g. RunContext, PageFetcher) exposes model
    // types, so this is `api`, not `implementation`.
    api(project(":swath-model"))

    implementation(libs.slf4j.api)
    // `api`, not `implementation`: core's public surface exposes Micrometer types
    // (RunContext, RunMetrics, JsonRunSummaryWriter) — a published-swath-core consumer
    // needs them on its compile classpath.
    api(libs.micrometer.core)
    // OTLP export is an internal implementation detail of MeterRegistries.fromEnv() (opt-in via
    // SWATH_OTLP_ENDPOINT); consumers don't need OtlpMeterRegistry on their compile classpath.
    implementation(libs.micrometer.registry.otlp)
    // --rate-limit-api: Bucket4j's blocking acquire respects Thread.interrupt(),
    // so a paced worker is woken on --max-duration/SIGTERM cancel.
    implementation(libs.bucket4j.core)

    // Parquet output (canonical schema §4, ZSTD via zstd-jni). hadoop-common is
    // dragged in by parquet-hadoop for Configuration/codecs; exclude its log4j
    // bindings so logback stays the only SLF4J binding.
    // Deliberately `implementation`, NOT `api`, even though some public output.parquet types
    // (ParquetSchema/PartWriter/ParquetWriterPool) expose parquet `MessageType` — making it `api`
    // would put parquet on swath-replay's COMPILE classpath (it depends on core via
    // `implementation`), breaking the compile-classpath-purity guard, i.e.
    // verifyNoParquetOrHadoopOnCompileClasspath (docs/internals/build-and-modules.md, Dependency
    // rules). A published-swath-core consumer that uses the parquet-output classes adds parquet
    // itself for now; the clean fix is extracting a separate swath-parquet module (see
    // docs/internals/build-and-modules.md).
    implementation(libs.parquet.hadoop)
    implementation(libs.zstd.jni)
    // PageCodec's LZ4 path uses io.airlift.compress.lz4 (pure-Java LZ4) directly —
    // promoted from a transitive dep of parquet-hadoop/hadoop-mapreduce-client-core to a direct
    // one so a future upstream bump can't silently drop/rev it out from under this reference.
    implementation(libs.aircompressor)
    // Reading Parquet back (ParquetEntryReader / the sort merge phase, and the planned derive API) needs
    // parquet-hadoop's read path, which resolves org.apache.hadoop.mapreduce FileInputFormat at
    // runtime. The sort read path is main-scope (all parquet-touching code lives in swath-core,
    // per the compile-classpath-purity guard above), so this is a runtime dependency, not test-only.
    implementation(libs.hadoop.mapreduce.client.core) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        excludeHadoopServiceStack()
    }

    // Checkpoint store: SQLite via the xerial JDBC driver. WAL single-writer
    // — all checkpoint writes funnel through one thread (algorithms.md §4.1).
    implementation(libs.sqlite.jdbc)
    implementation(libs.hadoop.common) {
        exclude(group = "org.slf4j", module = "slf4j-reload4j")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
        exclude(group = "ch.qos.reload4j", module = "reload4j")
        exclude(group = "org.apache.hadoop", module = "hadoop-auth")
        excludeHadoopServiceStack()
    }
    runtimeOnly(libs.logback.classic)

    // testkit (MockPageFetcher, Keyspaces, EngineHarness, ParquetReads, ...) is exposed
    // via testFixtures below; these are its own direct compile-time needs.
    testFixturesImplementation(libs.assertj)
    testFixturesImplementation(libs.micrometer.core)
    // `testFixturesApi`: ParquetReads (a testkit fixture) exposes parquet `MessageType`/`Group`
    // on its public surface, so fixture consumers need them on their compile classpath.
    testFixturesApi(libs.parquet.hadoop)

    // 2 engine property tests (R2InterpolatePropertyTest, StealMathTest) import
    // io.varve.swath.testkit.ScalarSafety, which lives in swath-model's testFixtures.
    testImplementation(testFixtures(project(":swath-model")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    // logback is the runtime SLF4J binding; observability/runtime tests capture the
    // run-summary log line via a logback ListAppender, so it must be on the test
    // compile classpath too.
    testImplementation(libs.logback.classic)
    testImplementation(libs.jqwik)
    testImplementation(libs.awaitility)
    // The export-path test needs the real OTLP wire types to read the exported counter value.
    testImplementation(libs.opentelemetry.proto)
    testRuntimeOnly(libs.junit.platform.launcher)
}

jmh {
    // JMH runtime version — same value as libs.versions.jmh.
    jmhVersion = libs.versions.jmh.get()
    // Modest iteration counts for a micro suite; scale up ad-hoc with -Pjmh.wi / -Pjmh.i.
    warmupIterations = 2
    iterations = 3
    fork = 1
    failOnError = true
    // Benchmarks do NOT run during `./gradlew build` — invoke explicitly: ./gradlew :swath-core:jmh
}
