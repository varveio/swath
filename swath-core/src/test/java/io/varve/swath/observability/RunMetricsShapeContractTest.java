/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The {@code shape} feature-vector — the {@link ShapeAccumulator} seam (extracted from
 * {@code RunMetrics}; split from {@code RunMetricsContractTest}): alphabet-cardinality
 * union, split-pivot divergence-depth histogram, delimiter fan-out, and the api-latency-percentile
 * fields that ride alongside the same {@code shape} block, plus the pivot-byte-region /
 * child-mass classification signals that fold into it. Moved verbatim, no assertion changes.
 */
final class RunMetricsShapeContractTest {

    @Test
    void shapeIsOmittedUntilTheEngineFetchesAListingPage() {
        // The `shape` feature-vector is only meaningful once a listing pass actually ran, so
        // it must be null (⇒ omitted by the writer, see JsonRunSummaryWriterTest) on a metrics object
        // that never fetched a page — exactly what the pre-run early-exit summaries
        // (seed_failure / resume_refused / no-op completed) carry. An all-zero `shape` there would
        // contradict the contract: the block is omitted whenever no shape was computed.
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        // Even non-listing activity (an API call, an emitted count) does NOT fabricate a shape:
        // only a RangeScanner page fetch (recordListingPageShape) does.
        metrics.recordApiCall();
        metrics.recordEntriesEmitted(3);

        assertThat(metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 0L, 0L).shape())
                .as("no shape before any listing page was fetched").isNull();

        // A single listing page ⇒ a real run ⇒ the shape block is now present.
        metrics.recordListingPageShape(1000, false, 1000);
        assertThat(metrics.summary(Duration.ofMillis(500), "WORK_STEALING", 0L, 0L).shape())
                .as("shape present once the engine fetched a listing page").isNotNull();
    }

    @Test
    void shapeAlphabetCardinalityUnionsObservedScalarsPerPosition() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built
        // Two completed nodes fold masks in; the aggregate is a UNION per relative position.
        // 'a'/'b'/'c'/'x'/'y' all live in word index 1 (scalar >>> 6 == 1).
        metrics.recordAlphabetObservation(maskWords(word('a', 'b'), word('x')));
        metrics.recordAlphabetObservation(maskWords(word('a', 'c'), word('x', 'y')));

        RunSummary.ShapeSummary shape = metrics.summary(
                Duration.ofMillis(500), "WORK_STEALING", 1L, 0L).shape();

        // pos 0 saw {a,b,c} -> 3; pos 1 saw {x,y} -> 2 (union across both nodes).
        assertThat(shape.alphabetCardinality()[0]).isEqualTo(3);
        assertThat(shape.alphabetCardinality()[1]).isEqualTo(2);
        assertThat(shape.alphabetPositionsObserved()).isEqualTo(2);
    }

    @Test
    void shapeDivergenceDepthHistogramBucketsPivotLcpDepth() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built
        // pivot vs reference: diverge at byte index 2, 2, and 1 respectively.
        metrics.recordPivotByteRegion(bytes("abd"), bytes("abc"));
        metrics.recordPivotByteRegion(bytes("abe"), bytes("abc"));
        metrics.recordPivotByteRegion(bytes("axc"), bytes("abc"));

        RunSummary.ShapeSummary shape = metrics.summary(
                Duration.ofMillis(500), "WORK_STEALING", 1L, 0L).shape();

        assertThat(shape.divergenceDepthHistogram()[2]).isEqualTo(2L);
        assertThat(shape.divergenceDepthHistogram()[1]).isEqualTo(1L);
        assertThat(shape.divergenceDepthHistogram()).hasSize(16);
    }

    @Test
    void shapeDelimiterFanoutTracksMaxTotalAndProbes() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built
        metrics.recordDelimiterFanout(5);
        metrics.recordDelimiterFanout(12);
        metrics.recordDelimiterFanout(3);

        RunSummary.ShapeSummary shape = metrics.summary(
                Duration.ofMillis(500), "WORK_STEALING", 1L, 0L).shape();

        assertThat(shape.delimiterFanoutMax()).isEqualTo(12L);
        assertThat(shape.delimiterFanoutTotal()).isEqualTo(20L);
        assertThat(shape.delimiterProbes()).isEqualTo(3L);
    }

    @Test
    void shapeMassSkewGiniIsHigherForABimodalChildMassDistribution() {
        // Bimodal (many empty + a few huge) — the zero-transfer-split fingerprint — is more unequal
        // than an all-same-bucket distribution, so its Gini is strictly higher (and in [0,1]).
        RunMetrics skewed = new RunMetrics(new SimpleMeterRegistry());
        skewed.setStrategy("WORK_STEALING");
        skewed.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built
        for (int i = 0; i < 20; i++) {
            skewed.recordChildMass(0);          // empty
        }
        skewed.recordChildMass(500_000);        // large
        skewed.recordChildMass(500_000);

        RunMetrics uniform = new RunMetrics(new SimpleMeterRegistry());
        uniform.setStrategy("WORK_STEALING");
        uniform.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built
        for (int i = 0; i < 20; i++) {
            uniform.recordChildMass(5_000);     // all "small"
        }

        double skewedGini = skewed.summary(Duration.ofMillis(1), "WORK_STEALING", 1L, 0L).shape().massSkewGini();
        double uniformGini = uniform.summary(Duration.ofMillis(1), "WORK_STEALING", 1L, 0L).shape().massSkewGini();

        assertThat(skewedGini).isBetween(0.0, 1.0).isGreaterThan(uniformGini);
        assertThat(uniformGini).isEqualTo(0.0);   // one bucket only -> perfectly equal
    }

    @Test
    void shapeApiLatencyPercentilesAreNonNullWhenLatenciesExist() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built
        for (int i = 0; i < 50; i++) {
            metrics.recordS3Latency(metrics.startS3PageTimer());
        }

        RunSummary.ShapeSummary shape = metrics.summary(
                Duration.ofMillis(500), "WORK_STEALING", 1L, 0L).shape();

        // publishPercentiles(0.5, 0.90, 0.99) is enabled on swath.api.latency, so p50/p99 are real, not null.
        assertThat(shape.apiLatencyP50Ms()).isNotNull();
        assertThat(shape.apiLatencyP99Ms()).isNotNull();
    }

    @Test
    void shapeApiLatencyPercentilesAreNullWhenNoCallTimed() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.setStrategy("WORK_STEALING");
        metrics.recordListingPageShape(1, false, 1000);   // a listing ran ⇒ shape is built ...
        // ... but no S3 call was TIMED, so the latency percentiles inside that shape stay null.

        RunSummary.ShapeSummary shape = metrics.summary(
                Duration.ofMillis(500), "WORK_STEALING", 1L, 0L).shape();

        assertThat(shape.apiLatencyP50Ms()).isNull();
        assertThat(shape.apiLatencyP99Ms()).isNull();
    }

    @Test
    void pivotByteRegionClassifiesTheDivergenceByteIntoHexRegions() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        byte[] cursor = "2022/03/05/".getBytes(StandardCharsets.UTF_8);

        // Divergence byte is the first byte where pivot != cursor. Here cursor is a prefix of each
        // pivot, so the divergence byte is the appended byte.
        metrics.recordPivotByteRegion(concat(cursor, (byte) '7'), cursor);   // 0x37 hex_digit
        metrics.recordPivotByteRegion(concat(cursor, (byte) 'c'), cursor);   // 0x63 hex_alpha
        metrics.recordPivotByteRegion(concat(cursor, (byte) 'Q'), cursor);   // 0x51 dead_zone
        metrics.recordPivotByteRegion(concat(cursor, (byte) 0x20), cursor);  // 0x20 (MIN_SAFE sliver) dead? no -> other
        metrics.recordPivotByteRegion(concat(cursor, (byte) 'z'), cursor);   // 0x7A other (past hex letters)

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.get("PIVOT_BYTE.hex_digit")).isEqualTo(1L);
        assertThat(reasons.get("PIVOT_BYTE.hex_alpha")).isEqualTo(1L);
        assertThat(reasons.get("PIVOT_BYTE.dead_zone")).isEqualTo(1L);
        assertThat(reasons.get("PIVOT_BYTE.other")).isEqualTo(2L);   // 0x20 and 0x7A
    }

    @Test
    void pivotByteRegionUsesFirstDivergingByteNotAnAppendedSuffix() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // cursor and pivot diverge at index 2: cursor[2]='a' (0x61), pivot[2]='b' (0x62 hex_alpha).
        byte[] cursor = "abadef".getBytes(StandardCharsets.UTF_8);
        byte[] pivot = "abbxxx".getBytes(StandardCharsets.UTF_8);
        metrics.recordPivotByteRegion(pivot, cursor);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.get("PIVOT_BYTE.hex_alpha")).isEqualTo(1L);
        assertThat(reasons).doesNotContainKey("PIVOT_BYTE.hex_digit");
    }

    @Test
    void pivotByteRegionIsNullAndEmptySafeAndSkipsAPrefixPivot() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordPivotByteRegion(null, "x".getBytes(StandardCharsets.UTF_8));
        metrics.recordPivotByteRegion(new byte[0], "x".getBytes(StandardCharsets.UTF_8));
        // pivot is a strict prefix of the reference: no divergence byte, nothing recorded.
        metrics.recordPivotByteRegion("ab".getBytes(StandardCharsets.UTF_8), "abc".getBytes(StandardCharsets.UTF_8));
        // null reference is treated as bottom (empty): the whole pivot diverges at index 0.
        metrics.recordPivotByteRegion("5".getBytes(StandardCharsets.UTF_8), null);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.keySet()).containsExactly("PIVOT_BYTE.hex_digit");
        assertThat(reasons.get("PIVOT_BYTE.hex_digit")).isEqualTo(1L);
    }

    @Test
    void childMassBucketsEmittedKeysAtTheContractBoundaries() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        metrics.recordChildMass(0L);        // empty
        metrics.recordChildMass(1L);        // tiny
        metrics.recordChildMass(100L);      // tiny (upper bound)
        metrics.recordChildMass(101L);      // small
        metrics.recordChildMass(10_000L);   // small (upper bound)
        metrics.recordChildMass(10_001L);   // large

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.get("CHILD_MASS.empty")).isEqualTo(1L);
        assertThat(reasons.get("CHILD_MASS.tiny")).isEqualTo(2L);
        assertThat(reasons.get("CHILD_MASS.small")).isEqualTo(2L);
        assertThat(reasons.get("CHILD_MASS.large")).isEqualTo(1L);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A single printable-ASCII scalar's presence bit within its 128-bit position mask's word 1 (0x40..0x7F). */
    private static long word(char... scalars) {
        long w = 0L;
        for (char c : scalars) {
            w |= 1L << (c & 63);
        }
        return w;
    }

    /** Flatten per-position word-1 masks into the {@code maskWords()} layout ({@code out[2*pos+1]}). */
    private static long[] maskWords(long... word1PerPosition) {
        long[] out = new long[ShapeAccumulator.ALPHABET_POSITIONS * 2];
        for (int pos = 0; pos < word1PerPosition.length && pos < ShapeAccumulator.ALPHABET_POSITIONS; pos++) {
            out[2 * pos + 1] = word1PerPosition[pos];
        }
        return out;
    }

    private static byte[] concat(byte[] a, byte b) {
        byte[] out = Arrays.copyOf(a, a.length + 1);
        out[a.length] = b;
        return out;
    }
}
