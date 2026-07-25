/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.registry.otlp.AggregationTemporality;
import io.micrometer.registry.otlp.HistogramFlavor;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.varve.swath.error.InvalidArgsException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@code OTEL_RESOURCE_ATTRIBUTES}/{@code OTEL_SERVICE_NAME} merge precedence and the pinned
 * CUMULATIVE aggregation temporality.
 */
final class MeterRegistriesTest {

    @Test
    void parseOtelResourceAttributesParsesTrimsAndPercentDecodes() {
        Map<String, String> attributes =
                MeterRegistries.parseOtelResourceAttributes("a=1, b=2 ,k=hello%20world");

        assertThat(attributes).containsExactly(
                Map.entry("a", "1"),
                Map.entry("b", "2"),
                Map.entry("k", "hello world"));
    }

    @Test
    void parseOtelResourceAttributesKeepsLiteralPlusUnlikeFormDecoding() {
        // OTEL_RESOURCE_ATTRIBUTES values are pure percent-decoded octets (W3C baggage), NOT
        // form-encoded -- a literal '+' (e.g. a runner slug like "run+7") must survive unchanged,
        // not become a space the way URLDecoder alone would treat it. %XX decoding (including
        // %2B, an explicitly-encoded '+') must still work normally.
        Map<String, String> attributes = MeterRegistries.parseOtelResourceAttributes(
                "k=a+b,literal_percent_2b=c%2Bd,space=hello%20world");

        assertThat(attributes).containsExactly(
                Map.entry("k", "a+b"),
                Map.entry("literal_percent_2b", "c+d"),
                Map.entry("space", "hello world"));
    }

    @Test
    void parseOtelResourceAttributesIgnoresMalformedPairs() {
        Map<String, String> attributes = MeterRegistries.parseOtelResourceAttributes("x,ok=1,=novalue,");

        assertThat(attributes).containsExactly(Map.entry("ok", "1"));
    }

    @Test
    void parseOtelResourceAttributesOnNullOrBlankYieldsEmptyMap() {
        assertThat(MeterRegistries.parseOtelResourceAttributes(null)).isEmpty();
        assertThat(MeterRegistries.parseOtelResourceAttributes("")).isEmpty();
        assertThat(MeterRegistries.parseOtelResourceAttributes("   ")).isEmpty();
    }

    @Test
    void mergedResourceAttributesIncludesOtelResourceAttributesEnvValue() {
        Map<String, String> merged = MeterRegistries.mergedResourceAttributes(
                "campaign_id=800,tier=spot", null, Map.of());

        assertThat(merged).containsEntry("campaign_id", "800");
        assertThat(merged).containsEntry("tier", "spot");
    }

    @Test
    void mergedResourceAttributesPerRunExtraOverridesSameKeyEnvValue() {
        Map<String, String> merged = MeterRegistries.mergedResourceAttributes(
                "bucket=env-bucket", null, Map.of("bucket", "run-bucket"));

        assertThat(merged).containsEntry("bucket", "run-bucket");
    }

    @Test
    void mergedResourceAttributesServiceNameDefaultsToSwathAndIsOverriddenByEnv() {
        Map<String, String> defaulted = MeterRegistries.mergedResourceAttributes(null, null, Map.of());
        assertThat(defaulted).containsEntry("service.name", "swath");

        Map<String, String> overridden = MeterRegistries.mergedResourceAttributes(null, "my-service", Map.of());
        assertThat(overridden).containsEntry("service.name", "my-service");
    }

    @Test
    void mergedResourceAttributesSkipsNullOrBlankExtraValues() {
        Map<String, String> merged = MeterRegistries.mergedResourceAttributes(
                "prefix=env-prefix", null, Map.of("prefix", " "));

        // Blank extra is skipped, so the env value survives.
        assertThat(merged).containsEntry("prefix", "env-prefix");
    }

    @Test
    void otlpConfigPinsCumulativeAggregationTemporality() {
        OtlpConfig config = MeterRegistries.buildOtlpConfig(
                "http://localhost:4318/v1/metrics", Duration.ofSeconds(5), Map.of());

        assertThat(config.aggregationTemporality()).isEqualTo(AggregationTemporality.CUMULATIVE);
    }

    @Test
    void otlpConfigPinsExplicitBucketHistogramFlavourEvenWhenTheAmbientConfigSelectsExponential() {
        // Micrometer 1.17's DEFAULT histogramFlavor() consults get("otlp.histogramFlavor"),
        // which is backed by the OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION env var.
        // Subclass swath's config so that source resolves to exponential — exactly what an ambient
        // env var would do — and assert the pin still wins. Deleting the histogramFlavor() override
        // makes histogramFlavor() fall back to the default, which then reads this get() and returns
        // BASE2_EXPONENTIAL_BUCKET_HISTOGRAM, so this test fences the pin.
        OtlpConfig ambientSelectsExponential = new MeterRegistries.SwathOtlpConfig(
                "http://localhost:4318/v1/metrics", Duration.ofSeconds(5), Map.of()) {
            @Override
            public String get(String key) {
                return key.endsWith("histogramFlavor") ? "base2_exponential_bucket_histogram" : null;
            }
        };

        assertThat(ambientSelectsExponential.histogramFlavor())
                .isEqualTo(HistogramFlavor.EXPLICIT_BUCKET_HISTOGRAM);
    }

    @Test
    void fromEnvWithNoEndpointReturnsSimpleMeterRegistryByDefault() {
        MeterRegistry registry = MeterRegistries.fromEnv(Map.of());

        assertThat(registry).isInstanceOf(SimpleMeterRegistry.class);
    }

    // ---- fromEnv/pure-env-inference must never hard-crash --------------------------------------
    //
    // fromEnv(Map) itself can't safely mutate SWATH_OTLP_ENDPOINT (no reflection-into-process-env
    // hack in this codebase), so these exercise MeterRegistries#resolve -- the exact core fromEnv
    // delegates to, with the two env values taken as explicit parameters instead of read via
    // System.getenv -- pinning the lenient-vs-fail-fast dichotomy.

    @Test
    void resolvePureEnvInferenceFallsBackLenientlyOnAMalformedEndpoint() throws Exception {
        // Neither --metrics-export nor --metrics-endpoint was passed (fromEnv's own shape, and
        // ListCommand's default dispatch for any caller who never touches the new flags): a
        // malformed SWATH_OTLP_ENDPOINT must degrade to no-export, not throw -- the regression the
        // AssertionError would previously have surfaced as an uncatchable Error.
        MeterRegistry registry = MeterRegistries.resolve(null, null, null, "http://[bad", null, Map.of());

        assertThat(registry).isInstanceOf(SimpleMeterRegistry.class);
    }

    @Test
    void resolveExplicitMetricsExportOtlpFailsFastOnTheSameMalformedEndpointValue() {
        // The dichotomy: the IDENTICAL malformed value fails fast once the caller explicitly opts
        // in via --metrics-export=otlp -- a deliberate CLI action earns exit 2, not a silent
        // fallback.
        assertThatThrownBy(() -> MeterRegistries.resolve("otlp", null, null, "http://[bad", null, Map.of()))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("invalid --metrics-endpoint");
    }

    @Test
    void resolveExplicitMetricsEndpointFlagFailsFastEvenWithMetricsExportUnset() {
        // An explicit --metrics-endpoint (even with --metrics-export left unset) is still a
        // deliberate CLI action, not pure env inference -- a typo'd flag value must fail fast too,
        // not be silently swallowed the way a stale env var is.
        assertThatThrownBy(() -> MeterRegistries.resolve(null, "http://[bad", null, null, null, Map.of()))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("invalid --metrics-endpoint");
    }

    @Test
    void resolvePureEnvInferenceWithAWellFormedEndpointStillBuildsAnOtlpRegistry() throws Exception {
        // The lenient fallback is scoped to a MALFORMED value only -- a well-formed env-resolved
        // endpoint with no explicit flags still enables real OTLP export (fromEnv's existing,
        // unchanged happy path).
        MeterRegistry registry = MeterRegistries.resolve(
                null, null, null, "http://localhost:4318/v1/metrics", null, Map.of());
        try {
            assertThat(registry).isInstanceOf(OtlpMeterRegistry.class);
        } finally {
            registry.close();
        }
    }

    // ---- --metrics-export/--metrics-endpoint/--metrics-interval CLI resolution -----------------

    @Test
    void parseExportModeUnsetIsNull() throws Exception {
        assertThat(MeterRegistries.parseExportMode(null)).isNull();
    }

    @Test
    void parseExportModeAcceptsNoneAndOtlpCaseInsensitively() throws Exception {
        assertThat(MeterRegistries.parseExportMode("none")).isFalse();
        assertThat(MeterRegistries.parseExportMode("NONE")).isFalse();
        assertThat(MeterRegistries.parseExportMode("otlp")).isTrue();
        assertThat(MeterRegistries.parseExportMode(" Otlp ")).isTrue();
    }

    @Test
    void parseExportModeRejectsAnUnknownValue() {
        assertThatThrownBy(() -> MeterRegistries.parseExportMode("bogus"))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--metrics-export");
    }

    @Test
    void firstNonBlankPrefersTheFlagOverTheEnvValue() {
        assertThat(MeterRegistries.firstNonBlank("http://flag", "http://env")).isEqualTo("http://flag");
        assertThat(MeterRegistries.firstNonBlank(null, "http://env")).isEqualTo("http://env");
        assertThat(MeterRegistries.firstNonBlank("  ", "http://env")).isEqualTo("http://env");
        assertThat(MeterRegistries.firstNonBlank(null, null)).isNull();
        assertThat(MeterRegistries.firstNonBlank(null, "  ")).isNull();
    }

    @Test
    void fromCliConfigExplicitNoneReturnsSimpleMeterRegistryEvenWithAnEndpointFlag() throws Exception {
        // An explicit --metrics-export=none is an override, not merely "unset": it must win even
        // if the caller also passed --metrics-endpoint (a plausible fat-fingered combination).
        MeterRegistry registry = MeterRegistries.fromCliConfig(
                "none", "http://localhost:4318/v1/metrics", null, Map.of());

        assertThat(registry).isInstanceOf(SimpleMeterRegistry.class);
    }

    @Test
    void fromCliConfigExplicitOtlpWithNoEndpointFailsFast() {
        assertThatThrownBy(() -> MeterRegistries.fromCliConfig("otlp", null, null, Map.of()))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("--metrics-export=otlp")
                .hasMessageContaining("--metrics-endpoint");
    }

    @Test
    void fromCliConfigExplicitOtlpWithAMalformedEndpointFailsFast() {
        assertThatThrownBy(() -> MeterRegistries.fromCliConfig("otlp", "http://[bad", null, Map.of()))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("invalid --metrics-endpoint");
    }

    @Test
    void credentialBearingMetricsEndpointsFailWithoutEchoingSecrets() {
        for (String unsafe : List.of(
                "https://user:metrics-secret@example.test/v1/metrics",
                "https://example.test/v1/metrics?access_token=metrics-secret")) {
            assertThatThrownBy(() -> MeterRegistries.fromCliConfig("otlp", unsafe, null, Map.of()))
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("invalid --metrics-endpoint")
                    .hasMessageNotContaining("metrics-secret")
                    .satisfies(error -> assertThat(error.getCause()).isNull());
        }
    }

    @Test
    void explicitNoneDoesNotBypassCredentialSafetyForAnExplicitEndpoint() {
        for (String unsafe : List.of(
                "https://user:metrics-secret@example.test",
                "https://example.test/v1/metrics?token=metrics-secret",
                "https:user:metrics-secret@example.test")) {
            assertThatThrownBy(() -> MeterRegistries.fromCliConfig("none", unsafe, null, Map.of()))
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("invalid --metrics-endpoint")
                    .hasMessageNotContaining("metrics-secret")
                    .satisfies(error -> assertThat(error.getCause()).isNull());
        }
    }

    @Test
    void explicitNoneWithBlankEndpointDoesNotValidateEnvironmentFallback() throws Exception {
        MeterRegistry registry = MeterRegistries.resolve("none", "  ", null,
                "https://user:environment-secret@example.test", null, Map.of());

        assertThat(registry).isInstanceOf(SimpleMeterRegistry.class);
    }

    @Test
    void fromCliConfigExplicitOtlpWithAFlagEndpointBuildsAnOtlpRegistry() throws Exception {
        MeterRegistry registry = MeterRegistries.fromCliConfig(
                "otlp", "http://localhost:4318/v1/metrics", Duration.ofSeconds(2), Map.of());
        try {
            assertThat(registry).isInstanceOf(OtlpMeterRegistry.class);
        } finally {
            registry.close();
        }
    }

    @Test
    void fromCliConfigUnsetExportModeWithNoEndpointAnywhereReturnsSimpleMeterRegistry() throws Exception {
        // The "never touched these flags" case must reduce to fromEnv's own behavior: this process
        // has no SWATH_OTLP_ENDPOINT set, so unset --metrics-export/--metrics-endpoint stays off.
        MeterRegistry registry = MeterRegistries.fromCliConfig(null, null, null, Map.of());

        assertThat(registry).isInstanceOf(SimpleMeterRegistry.class);
    }
}
