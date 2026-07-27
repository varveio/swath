// The policy simulator's ground-truth store (see README.md): a modelled listing store that
// answers the SAME `ListingStore` range seam the replay server's stores answer, so swath's real
// policies can be driven against a fixture with no HTTP, no S3, and no wall-clock.
//
// NOT a shipped artifact. The uber-jar is `:swath-cli:shadowJar` over swath-cli's OWN
// runtimeClasspath, and the Docker image copies exactly that jar
// (`/src/swath-cli/build/libs/swath.jar`), so a module ships iff swath-cli depends on it —
// nothing here does, and nothing here may. This is a `java-library`, not an `application`:
// it produces no start scripts, no dist, and no image layer.
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
    api(project(":swath-replay-server"))
    // ReplayMetrics/SimStoreMetrics are Micrometer-backed and this module registers its own
    // meters on the shared registry, so Micrometer is a direct compile-time need, not a
    // transitive convenience.
    implementation(libs.micrometer.core)
    implementation(libs.slf4j.api)

    // Fixture authoring in tests: swath-core's Parquet part writer + canonical schema, driven
    // through the replay module's shared testkit builders (ObjectEntries/ParquetFixtures).
    testImplementation(testFixtures(project(":swath-replay-server")))
    testImplementation(project(":swath-core"))
    // SortConfigs (manySmallRowGroups(), etc.), the production CaptureSorter's test-side config
    // presets — the windowed tier's differential fixtures need a real sorted, multi-row-group
    // output, built the same way SortedParquetStoreTest builds its own.
    testImplementation(testFixtures(project(":swath-core")))
    testImplementation(project(":swath-model"))
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.logback.classic)
}
