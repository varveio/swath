/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.registry.otlp.AggregationTemporality;
import io.micrometer.registry.otlp.HistogramFlavor;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.varve.swath.error.InvalidArgsException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses the {@link MeterRegistry} for a production run: the default in-memory
 * {@link SimpleMeterRegistry} (no export, today's behaviour), or — opt-in via
 * {@code SWATH_OTLP_ENDPOINT} — an {@link OtlpMeterRegistry} that pushes swath's
 * existing meters (unchanged) to an OTLP/HTTP collector.
 */
public final class MeterRegistries {

    private static final Logger log = LoggerFactory.getLogger(MeterRegistries.class);

    static final String ENDPOINT_ENV = "SWATH_OTLP_ENDPOINT";
    static final String INTERVAL_ENV = "SWATH_OTLP_INTERVAL";
    static final String OTEL_RESOURCE_ATTRIBUTES_ENV = "OTEL_RESOURCE_ATTRIBUTES";
    static final String OTEL_SERVICE_NAME_ENV = "OTEL_SERVICE_NAME";
    private static final Duration DEFAULT_STEP = Duration.ofSeconds(5);

    private MeterRegistries() {
    }

    /** The registry for the production CLI entry point ({@code RunContext.create()}). */
    public static MeterRegistry fromEnv() {
        return fromEnv(Map.of());
    }

    /**
     * Same as {@link #fromEnv()}, plus per-run OTLP resource attributes (e.g. bucket/prefix)
     * merged on top of the OTel-standard {@code OTEL_RESOURCE_ATTRIBUTES}/{@code
     * OTEL_SERVICE_NAME} env (see {@link #mergedResourceAttributes(String, String, Map)}).
     * Entries with a null or blank value are skipped so an unknown/empty field doesn't emit an
     * empty label. Ignored entirely on the {@link SimpleMeterRegistry} (no-export) path.
     */
    public static MeterRegistry fromEnv(Map<String, String> extraResourceAttributes) {
        try {
            return resolve(null, null, null, System.getenv(ENDPOINT_ENV), System.getenv(INTERVAL_ENV),
                    extraResourceAttributes);
        } catch (InvalidArgsException e) {
            // Defense in depth, not the primary guard: with metricsExport/metricsEndpoint both
            // unset, resolve()'s own "pure env inference" branch already falls back leniently on a
            // malformed SWATH_OTLP_ENDPOINT (see below) rather than throwing, so this should be
            // unreachable today. But an env-only caller (this method, and ListCommand's default
            // dispatch when the caller never touches --metrics-export/--metrics-endpoint at all)
            // must NEVER hard-crash a run just because metrics export couldn't be configured, so
            // this stays a lenient WARN+fallback.
            log.warn("OTLP metrics export disabled: {} (falling back to no-export)", e.getMessage());
            return new SimpleMeterRegistry();
        }
    }

    /**
     * CLI-level resolution ({@code swath list --metrics-export/--metrics-endpoint/
     * --metrics-interval}): an explicit flag always overrides the matching {@code SWATH_OTLP_*}
     * env var; omitting a flag falls back to that env var (so a caller that never touches these
     * flags gets exactly today's {@link #fromEnv(Map)} behavior — {@code metricsExport == null}
     * infers OTLP purely from whether an endpoint (flag or env) resolves, unchanged).
     *
     * @param metricsExport   the parsed {@code --metrics-export} value ({@code none}/{@code otlp}),
     *                        or {@code null} if the flag was not passed (defer to env-inferred on/off)
     * @param metricsEndpoint the {@code --metrics-endpoint} value, or {@code null}/blank if not passed
     * @param metricsInterval the {@code --metrics-interval} value, or {@code null} if not passed
     * @throws InvalidArgsException on an unrecognized {@code --metrics-export} value, on {@code
     *                              --metrics-export=otlp} with no endpoint resolved from either the
     *                              flag or {@code SWATH_OTLP_ENDPOINT} (fail-fast, exit 2),
     *                              or on a syntactically invalid endpoint URL FROM AN EXPLICIT
     *                              {@code --metrics-export}/{@code --metrics-endpoint} flag (pure
     *                              env inference — neither flag passed — degrades leniently instead,
     *                              see {@link #resolve} — this is what keeps {@link #fromEnv(Map)}
     *                              and a caller who never touches these flags at all non-throwing)
     */
    public static MeterRegistry fromCliConfig(String metricsExport, String metricsEndpoint,
            Duration metricsInterval, Map<String, String> extraResourceAttributes) throws InvalidArgsException {
        return resolve(metricsExport, metricsEndpoint, metricsInterval,
                System.getenv(ENDPOINT_ENV), System.getenv(INTERVAL_ENV), extraResourceAttributes);
    }

    /**
     * The shared resolution core behind both {@link #fromEnv(Map)} and {@link #fromCliConfig}, with
     * the two {@code SWATH_OTLP_*} env values taken as explicit parameters (rather than read via
     * {@code System.getenv} internally) — a test seam so the lenient-vs-fail-fast
     * dichotomy below is directly unit-testable without mutating process environment.
     *
     * <p><b>Lenient vs. fail-fast.</b> A malformed endpoint URL is
     * fail-fast (exit 2) ONLY when the caller explicitly touched {@code --metrics-export} or
     * {@code --metrics-endpoint} — a deliberate CLI action earns a clean, immediate usage error.
     * "Pure env inference" ({@code metricsExport == null && metricsEndpoint == null}, i.e. neither
     * flag was passed at all — {@link #fromEnv(Map)}'s own shape, and {@code ListCommand}'s default
     * dispatch for any caller who never touches the new flags) instead degrades LENIENTLY to the
     * no-export {@link SimpleMeterRegistry} with a WARN log: this is the path every existing runner
     * with a {@code SWATH_OTLP_ENDPOINT} env var already relies on never crashing a listing just
     * because that value happens to be syntactically malformed.
     */
    static MeterRegistry resolve(String metricsExport, String metricsEndpoint, Duration metricsInterval,
            String envEndpoint, String envIntervalRaw, Map<String, String> extraResourceAttributes)
            throws InvalidArgsException {
        Boolean explicitOtlp = parseExportMode(metricsExport);
        String resolvedEndpoint = firstNonBlank(metricsEndpoint, envEndpoint);
        boolean pureEnvInference = metricsExport == null && metricsEndpoint == null;

        boolean wantOtlp = explicitOtlp != null ? explicitOtlp : (resolvedEndpoint != null);
        if (wantOtlp && resolvedEndpoint == null) {
            throw new InvalidArgsException("--metrics-export=otlp needs an endpoint: pass "
                    + "--metrics-endpoint or set " + ENDPOINT_ENV);
        }
        // A non-blank explicitly supplied endpoint is validated even when export=none: accepting a
        // credential-bearing CLI value on a nominally-disabled path would make the safety rule
        // dependent on another flag and invite a later diagnostic/persistence regression. With
        // export=none, an absent/blank flag does not select (or validate) the env fallback.
        String explicitEndpoint = firstNonBlank(metricsEndpoint, null);
        String endpointToValidate = explicitEndpoint != null
                ? explicitEndpoint
                : (wantOtlp ? resolvedEndpoint : null);
        if (endpointToValidate != null) {
            try {
                SafeInput.endpoint("--metrics-endpoint", endpointToValidate);
            } catch (InvalidArgsException e) {
                if (pureEnvInference) {
                    log.warn("OTLP metrics export disabled: invalid {} (falling back to no-export; "
                            + "pass --metrics-export=otlp explicitly to fail fast on this instead)",
                            ENDPOINT_ENV);
                    return new SimpleMeterRegistry();
                }
                throw new InvalidArgsException("invalid --metrics-endpoint (or " + ENDPOINT_ENV
                        + "): endpoint URI rejected by the credential-safety policy");
            }
        }
        if (!wantOtlp) {
            return new SimpleMeterRegistry();
        }

        Duration step = metricsInterval != null ? metricsInterval : parseStep(envIntervalRaw);
        Map<String, String> resourceAttributes = mergedResourceAttributes(
                System.getenv(OTEL_RESOURCE_ATTRIBUTES_ENV), System.getenv(OTEL_SERVICE_NAME_ENV),
                extraResourceAttributes);
        return buildOtlpRegistry(resolvedEndpoint, step, resourceAttributes);
    }

    /** {@code metricsExport}, validated: {@code null} unset, {@code TRUE} otlp, {@code FALSE} none. */
    static Boolean parseExportMode(String metricsExport) throws InvalidArgsException {
        if (metricsExport == null) {
            return null;
        }
        return switch (metricsExport.trim().toLowerCase(Locale.ROOT)) {
            case "none" -> Boolean.FALSE;
            case "otlp" -> Boolean.TRUE;
            default -> throw new InvalidArgsException(
                    "--metrics-export must be one of: none, otlp (got '" + metricsExport + "')");
        };
    }

    /** Precedence helper: the flag value wins over the env value; both blank/null ⇒ null. */
    static String firstNonBlank(String flagValue, String envValue) {
        if (flagValue != null && !flagValue.isBlank()) {
            return flagValue;
        }
        return envValue == null || envValue.isBlank() ? null : envValue;
    }

    private static OtlpMeterRegistry buildOtlpRegistry(String endpoint, Duration step,
            Map<String, String> resourceAttributes) {
        OtlpConfig config = buildOtlpConfig(endpoint, step, resourceAttributes);
        OtlpMeterRegistry registry = OtlpMeterRegistry.builder(config).build();
        Runtime.getRuntime().addShutdownHook(new Thread(registry::close, "swath-otlp-flush"));
        log.info("OTLP metrics export enabled: endpoint={} step={}",
                SafeInput.logText(endpoint), step);
        return registry;
    }

    /**
     * Builds the {@link OtlpConfig} for {@link #fromEnv(Map)}, factored out so the CUMULATIVE
     * temporality pin and other overrides are reachable for a direct unit test without an actual
     * OTLP endpoint or a live {@link OtlpMeterRegistry}.
     */
    static OtlpConfig buildOtlpConfig(String endpoint, Duration step, Map<String, String> resourceAttributes) {
        return new SwathOtlpConfig(endpoint, step, resourceAttributes);
    }

    /**
     * swath's {@link OtlpConfig}: the endpoint/step/resource-attribute wiring plus the CUMULATIVE
     * temporality and explicit-bucket histogram pins. A named, non-final, package-private class (not
     * an anonymous instance) so a test can subclass it to override {@link #get} — the config source
     * Micrometer's DEFAULT {@code histogramFlavor()} consults — and thereby prove the pin, not the
     * ambient default, is what {@link #histogramFlavor()} returns.
     */
    static class SwathOtlpConfig implements OtlpConfig {

        private final String endpoint;
        private final Duration step;
        private final Map<String, String> resourceAttributes;

        SwathOtlpConfig(String endpoint, Duration step, Map<String, String> resourceAttributes) {
            this.endpoint = endpoint;
            this.step = step;
            this.resourceAttributes = resourceAttributes;
        }

        @Override
        public String get(String key) {
            return null;
        }

        @Override
        public String url() {
            return endpoint;
        }

        @Override
        public Duration step() {
            return step;
        }

        @Override
        public Map<String, String> resourceAttributes() {
            return resourceAttributes;
        }

        // Cumulative maps cleanly to Cloud Monitoring / Managed-Prometheus; do not switch to
        // delta for short-lived processes.
        @Override
        public AggregationTemporality aggregationTemporality() {
            return AggregationTemporality.CUMULATIVE;
        }

        // Pin explicit-bucket HISTOGRAM. Micrometer 1.17's DEFAULT histogramFlavor() reads
        // the ambient OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION env var. This changes
        // NOTHING on today's wire: swath configures only client-side publishPercentiles — no
        // percentilesHistogram/SLOs/MeterFilter — so no current series ever reaches the
        // flavour-sensitive histogram branch, and today's payload carries SUMMARY/GAUGE datapoints
        // under any env value. The pin is forward-looking: it guarantees that any FUTURE
        // histogram-publishing series gets a wire shape decided by swath, not by ambient environment.
        @Override
        public HistogramFlavor histogramFlavor() {
            return HistogramFlavor.EXPLICIT_BUCKET_HISTOGRAM;
        }
    }

    /**
     * Builds the OTLP resource-attribute map with this precedence (lowest to highest, later
     * wins): (1) {@code otelResourceAttributesEnv} (the raw {@code OTEL_RESOURCE_ATTRIBUTES} env
     * value, see {@link #parseOtelResourceAttributes}), (2) {@code service.name} from {@code
     * otelServiceNameEnv} (the raw {@code OTEL_SERVICE_NAME} env value), defaulting to {@code
     * swath} if not already set by (1), then (3) {@code extraResourceAttributes} (swath's own
     * per-run attrs, e.g. bucket/prefix), which always win. Entries in (3) with a null or blank
     * value are skipped. Takes the raw env strings (rather than reading {@code System.getenv}
     * itself) so the merge is testable without mutating process environment.
     */
    static Map<String, String> mergedResourceAttributes(String otelResourceAttributesEnv,
            String otelServiceNameEnv, Map<String, String> extraResourceAttributes) {
        Map<String, String> resourceAttributes = new LinkedHashMap<>();
        resourceAttributes.putAll(parseOtelResourceAttributes(otelResourceAttributesEnv));
        resourceAttributes.putIfAbsent("service.name",
                otelServiceNameEnv == null || otelServiceNameEnv.isBlank() ? "swath" : otelServiceNameEnv);
        extraResourceAttributes.forEach((key, value) -> {
            if (value != null && !value.isBlank()) {
                resourceAttributes.put(key, value);
            }
        });
        return resourceAttributes;
    }

    /**
     * Parses the OpenTelemetry-spec {@code OTEL_RESOURCE_ATTRIBUTES} format: comma-separated
     * {@code key=value} pairs, each key/value whitespace-trimmed, values percent-decoded.
     * Malformed pairs (no {@code =}) are ignored. Returns an ordered map; {@code null}/blank
     * input yields an empty map.
     */
    static Map<String, String> parseOtelResourceAttributes(String env) {
        Map<String, String> attributes = new LinkedHashMap<>();
        if (env == null || env.isBlank()) {
            return attributes;
        }
        for (String pair : env.split(",")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String key = pair.substring(0, eq).trim();
            String value = pair.substring(eq + 1).trim();
            if (key.isEmpty()) {
                continue;
            }
            attributes.put(key, decodePercent(value));
        }
        return attributes;
    }

    /**
     * {@code OTEL_RESOURCE_ATTRIBUTES} values are pure
     * percent-decoded octets (W3C baggage), NOT form-encoded — but plain {@link URLDecoder} also
     * turns a literal {@code +} into a space (form semantics), silently corrupting a value like
     * {@code slug=run+7} into {@code run 7}. Escape any literal {@code +} to its percent-encoding
     * ({@code %2B}) first, so {@link URLDecoder} only ever expands {@code %XX} triples and a
     * literal {@code +} survives unchanged.
     */
    private static String decodePercent(String value) {
        try {
            return URLDecoder.decode(value.replace("+", "%2B"), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return value;
        }
    }

    private static Duration parseStep(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT_STEP;
        }
        try {
            return Duration.parse(raw);
        } catch (DateTimeParseException e) {
            try {
                return Duration.ofSeconds(Long.parseLong(raw.trim()));
            } catch (NumberFormatException ignored) {
                log.warn("{}={} is not a valid duration (ISO-8601 or plain seconds); using default {}",
                        INTERVAL_ENV, raw, DEFAULT_STEP);
                return DEFAULT_STEP;
            }
        }
    }
}
