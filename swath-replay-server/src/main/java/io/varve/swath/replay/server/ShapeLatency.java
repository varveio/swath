/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import io.varve.swath.replay.protocol.S3ListRequest;
import io.varve.swath.replay.protocol.S3ListResult;
import io.varve.swath.replay.protocol.S3ResultEntry;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * A per-request latency injector keyed on the <b>shape</b> of the request, so a replay fixture can be
 * driven to a real bucket's measured latency <em>profile</em> rather than a single uniform delay.
 *
 * <p>This is the piece a TCP-level proxy (toxiproxy) cannot provide: link latency is uniform across
 * every request, but a dense-tail collapse is driven by a profile in which the shapes are
 * wildly unequal. The built-in reference profile uses {@code worker_page} at ~223 ms,
 * {@code pivot_probe} at ~121 ms, and {@code structure_probe} (the {@code delimiter=/} split
 * probe) at <b>~6.9 s</b> against a 3 s probe budget. Injecting that 6.9 s only on the structure-probe
 * shape reproduces the exact mechanism — a probe that cannot finish inside its budget, so the range
 * never splits — deterministically and on demand, with no live bucket.
 *
 * <p><b>Fanout-proportional structure probes.</b> A structure probe's delay may carry a per-entry
 * term ({@code base + perCommonPrefix × CommonPrefixes returned}), which is why this hook sees the
 * {@link S3ListResult} and not just the request: S3's own cost for a {@code delimiter=/} listing
 * grows with the sub-directories it rolls up and <em>stops early at {@code max-keys}</em>, so a
 * capped probe is proportionally cheaper. A flat per-shape delay cannot express that — it charges a
 * {@code max-keys=32} probe and a {@code max-keys=1000} probe identically, hiding exactly the lever
 * an engine-side fanout cap pulls. The per-entry term restores it: fewer prefixes returned, less
 * injected latency. The store's own scan cost cannot stand in for this either — the sorted store's
 * native skip-scan is O(entries emitted) and already respects {@code max-keys}, so it runs in
 * microseconds regardless of what production S3 actually charges for the same shape — so faithful
 * cost modelling belongs here, in injection, on top of a fast baseline.
 *
 * <p>Request classification mirrors {@code ReplayLatencyAdapter} (the test-side bridge): a request
 * carrying a {@code delimiter} is a structure probe (checked first, so a {@code delimiter}'d
 * {@code max-keys<=1} still classes structural); a bare {@code maxKeys <= 1} request is a pivot
 * probe; anything else is an ordinary worker page.
 *
 * <p>Jitter, when set, is a deterministic fraction of the computed delay in {@code [-jitter,
 * +jitter]}, keyed off the request bytes (never {@code Math.random}, which would make a run
 * irreproducible). Two runs against the same fixture with the same profile see byte-identical
 * delays.
 *
 * <p><b>Compressed time.</b> A profile can be divided by a scale factor at parse time, so a long
 * profile can be walked end to end in a fraction of the wall clock it describes. The scale divides
 * only what this injector adds; the server's own per-request cost is untouched, so a scaled run
 * over-weights that fixed cost by the same factor and its absolute wall clock is not a prediction
 * of the unscaled one.
 */
public final class ShapeLatency implements BiFunction<S3ListRequest, S3ListResult, Duration> {

    /** No compression: the profile's delays are injected exactly as written. */
    public static final double UNSCALED = 1.0;

    /** The three request shapes a work-stealing scan issues, in classification order. */
    public enum Shape {
        STRUCTURE_PROBE,
        PIVOT_PROBE,
        WORKER_PAGE
    }

    /**
     * One shape's delay: a flat base plus an optional per-CommonPrefix term (structure probes only —
     * the parser rejects it elsewhere, since only a delimiter'd response has a fanout to count).
     */
    public record Delay(Duration base, Duration perCommonPrefix) {
        public static Delay flat(Duration base) {
            return new Delay(base, Duration.ZERO);
        }
    }

    /**
     * The reserved {@code prod-commoncrawl} profile (see class javadoc). Its structure-probe delay
     * uses a 6,865 ms reference at 121 CommonPrefixes, decomposed as one worker-page's worth
     * of base cost plus ~55 ms per rolled-up prefix: {@code 223 + 55 × 121 ≈ 6,878 ms}. The
     * decomposition is the modelling choice: the reference does not separate a flat
     * floor from the per-prefix slope, so the base is pinned to the worker-page value and
     * the slope absorbs the rest. It reproduces both the absolute cost at full fanout and the
     * near-linear max-keys curve measured on the replay server's own seek loop.
     */
    public static final Map<Shape, Delay> PROD_COMMONCRAWL = Map.of(
            Shape.WORKER_PAGE, Delay.flat(Duration.ofMillis(223)),
            Shape.PIVOT_PROBE, Delay.flat(Duration.ofMillis(121)),
            Shape.STRUCTURE_PROBE, new Delay(Duration.ofMillis(223), Duration.ofMillis(55)));

    private final Delay workerPage;
    private final Delay pivotProbe;
    private final Delay structureProbe;
    private final double jitter;

    public ShapeLatency(Map<Shape, Delay> delays, double jitter) {
        // The negated in-range form so NaN (for which every comparison is false) is rejected too —
        // a NaN jitter would otherwise scale every delay to zero and silently disable injection.
        if (!(jitter >= 0.0 && jitter < 1.0)) {
            throw new IllegalArgumentException("jitter must be in [0, 1), got " + jitter);
        }
        this.workerPage = delays.getOrDefault(Shape.WORKER_PAGE, Delay.flat(Duration.ZERO));
        this.pivotProbe = delays.getOrDefault(Shape.PIVOT_PROBE, Delay.flat(Duration.ZERO));
        this.structureProbe = delays.getOrDefault(Shape.STRUCTURE_PROBE, Delay.flat(Duration.ZERO));
        this.jitter = jitter;
    }

    static Shape classify(S3ListRequest req) {
        if (req.delimiter() != null && req.delimiter().length > 0) {
            return Shape.STRUCTURE_PROBE;
        }
        if (req.maxKeys() <= 1) {
            return Shape.PIVOT_PROBE;
        }
        return Shape.WORKER_PAGE;
    }

    @Override
    public Duration apply(S3ListRequest req, S3ListResult result) {
        Delay delay = switch (classify(req)) {
            case STRUCTURE_PROBE -> structureProbe;
            case PIVOT_PROBE -> pivotProbe;
            case WORKER_PAGE -> workerPage;
        };
        Duration base = delay.base();
        if (!delay.perCommonPrefix().isZero() && result != null) {
            base = base.plus(delay.perCommonPrefix().multipliedBy(commonPrefixCount(result)));
        }
        if (base.isZero() || jitter == 0.0) {
            return base;
        }
        // Deterministic jitter in [-jitter, +jitter], keyed off the request so a given request always
        // sees the same delay (reproducible, unlike Math.random). mix64 avalanches requestHash before
        // the low bits are dropped below: successive continuation tokens within one walk are base64
        // encodings of nearby boundary keys, so their plain polynomial hashCodes can land close
        // together (even one apart) — and >>> 11 would otherwise discard exactly that difference,
        // silently re-flattening jitter back into a per-walk constant.
        long h = mix64(requestHash(req));
        double signed = ((h >>> 11) * 0x1.0p-53) * 2.0 - 1.0;   // a stable double in [-1, 1)
        long scaledNanos = (long) (base.toNanos() * (1.0 + jitter * signed));
        return Duration.ofNanos(Math.max(0, scaledNanos));
    }

    private static int commonPrefixCount(S3ListResult result) {
        int n = 0;
        for (S3ResultEntry entry : result.entries()) {
            if (entry instanceof S3ResultEntry.CommonPrefixResult) {
                n++;
            }
        }
        return n;
    }

    /** SplitMix64's finalizer: avalanches every input bit across all 64 output bits. */
    private static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    private static long requestHash(S3ListRequest req) {
        long h = 1125899906842597L;
        h = 31 * h + Arrays.hashCode(req.prefix());
        h = 31 * h + Arrays.hashCode(req.delimiter());
        h = 31 * h + Arrays.hashCode(req.startAfter());
        // The continuation token is the ONLY field distinguishing one continuation page of a walk
        // from the next (prefix/delimiter/max-keys are constant and startAfter is null past page
        // one) — omit it and every page of a walk draws identical jitter, systematically biasing a
        // latency experiment instead of jittering it.
        h = 31 * h + (req.continuationToken() == null ? 0 : req.continuationToken().hashCode());
        h = 31 * h + req.maxKeys();
        return h;
    }

    /**
     * Parses a profile spec: either the reserved name {@code prod-commoncrawl}, or a comma-separated
     * list of {@code shape=delay} pairs where shape is {@code worker_page} / {@code pivot_probe} /
     * {@code structure_probe} and delay is a duration ({@code 6865ms}, {@code 6.9s}, or a bare
     * millisecond count like {@code 250}), optionally — for {@code structure_probe} only — followed
     * by a per-CommonPrefix term: {@code structure_probe=223ms+55ms/cp}. An unlisted shape gets no
     * delay. Returns {@code null} for a null/blank spec so the caller can install a no-op.
     *
     * <p>{@code scale} divides every parsed delay ({@link #UNSCALED} leaves the profile exactly as
     * written). A scale other than {@link #UNSCALED} against a spec that injects nothing is refused
     * rather than ignored: it scales nothing, and an operator who passed it meant to compress a
     * profile that is not there.
     */
    public static ShapeLatency parse(String spec, double jitter, double scale) {
        // The negated in-range form, so NaN is rejected too (see the constructor's jitter check).
        if (!(scale > 0.0 && scale < Double.POSITIVE_INFINITY)) {
            throw new IllegalArgumentException("latency scale must be finite and > 0, got " + scale);
        }
        if (spec == null || spec.isBlank()) {
            if (scale != UNSCALED) {
                throw new IllegalArgumentException("a latency scale requires a latency profile to scale");
            }
            return null;
        }
        if (spec.trim().toLowerCase(Locale.ROOT).equals("prod-commoncrawl")) {
            return new ShapeLatency(scaled(PROD_COMMONCRAWL, scale), jitter);
        }
        Map<Shape, Delay> delays = new LinkedHashMap<>();
        for (String pair : spec.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("latency spec entry must be shape=delay, got: " + trimmed);
            }
            Shape shape = parseShape(trimmed.substring(0, eq).trim());
            delays.put(shape, parseDelay(shape, trimmed.substring(eq + 1).trim()));
        }
        return new ShapeLatency(scaled(delays, scale), jitter);
    }

    /**
     * Divides every delay in a profile by {@code scale}. {@link #UNSCALED} returns the profile
     * itself, so the default path cannot perturb a single nanosecond of the wire behavior.
     */
    private static Map<Shape, Delay> scaled(Map<Shape, Delay> delays, double scale) {
        if (scale == UNSCALED) {
            return delays;
        }
        Map<Shape, Delay> compressed = new LinkedHashMap<>();
        delays.forEach((shape, delay) -> compressed.put(shape,
                new Delay(divide(delay.base(), scale), divide(delay.perCommonPrefix(), scale))));
        return compressed;
    }

    private static Duration divide(Duration delay, double scale) {
        return Duration.ofNanos((long) (delay.toNanos() / scale));
    }

    private static Shape parseShape(String s) {
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "worker_page", "page" -> Shape.WORKER_PAGE;
            case "pivot_probe", "pivot" -> Shape.PIVOT_PROBE;
            case "structure_probe", "structure" -> Shape.STRUCTURE_PROBE;
            default -> throw new IllegalArgumentException(
                    "unknown latency shape '" + s + "' (worker_page|pivot_probe|structure_probe)");
        };
    }

    private static Delay parseDelay(Shape shape, String s) {
        int plus = s.indexOf('+');
        if (plus < 0) {
            return Delay.flat(parseDuration(s));
        }
        String perCp = s.substring(plus + 1).trim();
        if (!perCp.toLowerCase(Locale.ROOT).endsWith("/cp")) {
            throw new IllegalArgumentException(
                    "per-entry latency term must end in /cp (e.g. 55ms/cp), got: " + perCp);
        }
        if (shape != Shape.STRUCTURE_PROBE) {
            throw new IllegalArgumentException(
                    "a /cp term only applies to structure_probe (only a delimiter'd response has a fanout)");
        }
        return new Delay(parseDuration(s.substring(0, plus).trim()),
                parseDuration(perCp.substring(0, perCp.length() - 3).trim()));
    }

    private static Duration parseDuration(String s) {
        String v = s.toLowerCase(Locale.ROOT);
        Duration parsed;
        try {
            if (v.endsWith("ms")) {
                parsed = Duration.ofNanos((long) (Double.parseDouble(v.substring(0, v.length() - 2)) * 1_000_000));
            } else if (v.endsWith("s")) {
                parsed = Duration.ofNanos((long) (Double.parseDouble(v.substring(0, v.length() - 1)) * 1_000_000_000));
            } else {
                parsed = Duration.ofMillis(Long.parseLong(v));   // bare number is milliseconds
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad latency duration '" + s + "' (e.g. 6865ms, 6.9s, 250)", e);
        }
        // A negative delay would be silently treated as no sleep by the handler — an experiment
        // that thinks it is injecting but is not. Refuse it here, where the operator can see why.
        if (parsed.isNegative()) {
            throw new IllegalArgumentException("latency duration must be >= 0, got '" + s + "'");
        }
        return parsed;
    }
}
