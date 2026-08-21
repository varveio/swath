// The policy simulator's ground-truth store (see README.md): a modelled listing store that
// answers the SAME `ListingStore` range seam the replay server's stores answer, so swath's real
// policies can be driven against a fixture with no HTTP, no S3, and no wall-clock.
//
// NOT a shipped artifact. The uber-jar is `:swath-cli:shadowJar` over swath-cli's OWN
// runtimeClasspath, and the Docker image copies exactly that jar
// (`/src/swath-cli/build/libs/swath.jar`), so a module ships iff swath-cli depends on it —
// nothing here does, and nothing here may. This is a `java-library`, not an `application`:
// it produces no start scripts, no dist, and no image layer.
import java.time.Duration

plugins {
    id("swath.java-conventions")
    `java-library`
}

dependencies {
    // The store seam itself (`ListingStore`, `Projection`, `ByteKey`, `ListedObject`) plus the
    // one place S3 ListObjectsV2 pagination lives (`ListObjectsV2Pager`) and the metrics sink
    // both of those take. The simulator reuses that seam wholesale rather than restating
    // pagination/truncation semantics behind a second implementation of the same protocol.
    //
    // `api`, not `implementation`: this module's Java-visible surface EXPOSES those types --
    // ArenaListingStore implements ListingStore and returns ListedObject, SimStoreFactory.Result
    // carries a ListingStore and a ReplayMetrics -- so per the api-vs-implementation rule in
    // docs/internals/build-and-modules.md a consumer needs them on its compile classpath.
    api(project(":swath-replay"))
    // ReplayMetrics/SimStoreMetrics are Micrometer-backed and this module registers its own
    // meters on the shared registry, so Micrometer is a direct compile-time need, not a
    // transitive convenience.
    implementation(libs.micrometer.core)
    implementation(libs.slf4j.api)
    // The streaming tier decodes a sorted fixture's row groups through swath-core's
    // io.varve.swath.sort.SortedRowGroupReader. `implementation`, not `api`: that reader traffics only
    // in byte[]/long/String, so no io.varve.swath.sim type exposes it and, exactly as for
    // swath-replay, swath-core's own parquet-hadoop/hadoop deps stay off this module's compile
    // classpath -- no io.varve.swath.sim source may import an org.apache.parquet type.
    implementation(project(":swath-core"))

    // Fixture authoring in tests: swath-core's Parquet part writer + canonical schema, driven
    // through the replay module's shared testkit builders (ObjectEntries/ParquetFixtures).
    testImplementation(testFixtures(project(":swath-replay")))
    // SortConfigs (manySmallRowGroups(), etc.), the production CaptureSorter's test-side config
    // presets — the windowed tier's differential fixtures need a real sorted, multi-row-group
    // output, built the same way SortedParquetStoreTest builds its own.
    testImplementation(testFixtures(project(":swath-core")))
    testImplementation(project(":swath-model"))
    // The corpus sweep reads each staged capture's own run record (its `summary.json`) for the one
    // input it cannot invent -- the concurrency that capture ran at. Declared here rather than leaned
    // on transitively: jackson-databind is `implementation`-scoped inside swath-core, so it reaches
    // this module's runtime classpath but not its compile one, and a test that imports a type needs
    // it named.
    testImplementation(libs.jackson.databind)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}

tasks.test {
    // Forward the store bench's fixture-path switches (StoreThroughputBenchTest, @Tag("perf")) from
    // the `./gradlew -Dswath.sim.bench.fixture=... -Dswath.sim.bench.giant-fixture=...` invocation
    // into the forked test JVM -- the same forwarding gap swath.java-conventions already documents
    // for swath.it.sigkill/swath.goldens.update: a Gradle-CLI `-D` only sets the DAEMON's system
    // property by default, not the forked Test JVM's. Absent (the common case), nothing is
    // forwarded and the bench test's own assumeTrue skips it, exactly as before.
    System.getProperty("swath.sim.bench.fixture")?.let { systemProperty("swath.sim.bench.fixture", it) }
    System.getProperty("swath.sim.bench.giant-fixture")?.let { systemProperty("swath.sim.bench.giant-fixture", it) }
    // Same forwarding, same reason, for the real-listing run (RealListingRunTest, @Tag("perf")):
    // `-Dswath.sim.listing.fixture=<local sorted capture>`. The repo never names a fixture -- the
    // path is the operator's, supplied per invocation, and the test skips itself without one.
    System.getProperty("swath.sim.listing.fixture")?.let { systemProperty("swath.sim.listing.fixture", it) }
    System.getProperty("swath.sim.listing.workers")?.let { systemProperty("swath.sim.listing.workers", it) }
    System.getProperty("swath.sim.listing.trace-seed")?.let { systemProperty("swath.sim.listing.trace-seed", it) }
    System.getProperty("swath.sim.listing.arm")?.let { systemProperty("swath.sim.listing.arm", it) }
    System.getProperty("swath.sim.listing.seed")?.let { systemProperty("swath.sim.listing.seed", it) }
    // Same forwarding, same reason, for the corpus sweep (CorpusSweepRunTest, @Tag("perf")): a root
    // directory of staged captures, the TSV it writes its per-leg rows to, and the ceiling above
    // which a staged capture is passed over rather than swept. The first two are the operator's
    // paths, supplied per invocation, and the sweep skips itself without the first.
    System.getProperty("swath.sim.listing.corpus")?.let { systemProperty("swath.sim.listing.corpus", it) }
    System.getProperty("swath.sim.listing.results")?.let { systemProperty("swath.sim.listing.results", it) }
    System.getProperty("swath.sim.listing.corpus-max-keys")
        ?.let { systemProperty("swath.sim.listing.corpus-max-keys", it) }
    // Same forwarding, same reason, for the per-decision gate dump (`SimGateDump`): the TSV a single
    // run writes its owner-split and victim-scan gate inputs to, for diffing against a replay trace.
    // Absent (the common case), no run dumps anything.
    System.getProperty("swath.sim.gate-dump")?.let { systemProperty("swath.sim.gate-dump", it) }
    // A real listing runs to tens of millions of keys, and the perf tier's 2 GB is sized for the
    // synthetic benches; raise the forked JVM's heap for one invocation with
    // `-PsimTestHeap=6g` rather than lifting it for every module's perf run.
    (project.findProperty("simTestHeap") as String?)?.let { maxHeapSize = it }
    // A corpus sweep is one task that runs for as long as the corpus is large, and the conventions'
    // ten-minute cap is sized for the per-commit suite; without this the sweep is killed mid-corpus
    // and its results file simply stops. Same shape and same reason as the heap knob above: minutes,
    // for one invocation (`-PsimTestTimeout=180`), rather than lifting the cap for every module.
    (project.findProperty("simTestTimeout") as String?)?.let { timeout.set(Duration.ofMinutes(it.toLong())) }
}
