// The S3 store backend: store/s3/* (S3PageFetcher, S3ClientFactory, ...)
// + the AWS SDK. Future swath-gcs/swath-azure sit beside this as siblings.
plugins {
    id("swath.java-conventions")
    `java-library`
    `java-test-fixtures`
}

dependencies {
    // S3PageFetcher's public surface exposes core types (RunContext, PageFetcher, ...),
    // so this is `api`, not `implementation`.
    api(project(":swath-core"))

    // `api`, not `implementation`: swath-cli's ListCommand wires S3Client/credentials
    // providers/Region directly, and swath-cli only depends on swath-s3 via
    // `implementation` — so the AWS SDK types must ride swath-s3's own `api` surface to
    // reach swath-cli's compile classpath.
    api(platform(libs.awssdk.bom))
    api(libs.awssdk.s3)
    // `implementation`, not `api`: S3ClientFactory's public signature is `SdkHttpClient` (from
    // awssdk.s3, above); `ApacheHttpClient` is only the factory-body implementation, not exposed.
    // Runtime consumers still get it via runtimeElements.
    implementation(libs.awssdk.apache.client)
    // Bundle aws-sdk `sts` so the default credential chain's reflectively-loaded
    // WebIdentityTokenFileCredentialsProvider resolves AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE
    // (OIDC / GKE/EKS workload identity). runtimeOnly: no compile-time reference (loaded reflectively).
    runtimeOnly(libs.awssdk.sts)
    // micrometer-core is `implementation`-scoped in swath-core, so it doesn't cross the
    // module boundary on its own; S3PageFetcher directly references Timer/SimpleMeterRegistry.
    implementation(libs.micrometer.core)

    // LocalStackSupport (testFixtures) — the shared LocalStack S3 helpers for the
    // integration tier, used by this module's own ITs and by swath-cli's ITs.
    // `testFixturesApi` for the types LocalStackSupport exposes on its public surface
    // (`LocalStackContainer`, `S3Client`); bom platforms stay implementation.
    testFixturesImplementation(platform(libs.awssdk.bom))
    testFixturesApi(libs.awssdk.s3)
    testFixturesImplementation(platform(libs.testcontainers.bom))
    testFixturesApi(libs.testcontainers.localstack)

    // store/s3 tests (+ WorkStealingScanMetricsTest, relocated here since it drives
    // S3PageFetcher) need testkit (Keyspaces, ParquetReads, MockPageFetcher).
    testImplementation(testFixtures(project(":swath-core")))
    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.localstack)
    // (micrometer-core is now `implementation` above, on this module's own main; several
    // s3 tests reference Meter/MeterRegistry directly, and testImplementation extends
    // implementation for the same project, so no separate test-scope declaration is needed.)
    // logback is the runtime SLF4J binding; S3FaultLogAttributionTest captures the retryable
    // fault lines via a logback ListAppender (same idiom as swath-core's observability tests),
    // so it must be on the test compile classpath here too.
    testImplementation(libs.logback.classic)
    testRuntimeOnly(libs.junit.platform.launcher)
}
