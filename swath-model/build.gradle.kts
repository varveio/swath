// The listing entity/value-type module — a true leaf: imports no other
// internal swath package. testFixtures exposes ScalarSafety, a dependency-free predicate
// mirroring ByteMidpoint.isSafe's excluded-scalar set, shared by this module's own
// ByteMidpoint* tests and by swath-core's engine property tests — and CodePointKeys, the
// shared jqwik key generators those same ByteMidpoint* tests draw from (hence the testFixtures
// jqwik dependency below).
plugins {
    id("swath.java-conventions")
    `java-library`
    `java-test-fixtures`
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.jqwik)
    testRuntimeOnly(libs.junit.platform.launcher)

    testFixturesApi(libs.jqwik)
}
