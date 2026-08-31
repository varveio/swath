/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * {@link SortConfig} knob parsing, defaults, and the heap-adaptive segment gate. Exercised
 * through {@link SortConfig#fromProperties} with an in-memory lookup — never mutates the real,
 * process-global {@link System} properties, so this test is order-independent and parallel-safe.
 */
class SortConfigTest {

    private static SortConfig fromProperties(Map<String, String> overrides) {
        return SortConfig.fromProperties(key -> overrides.get(key.substring("swath.sort.".length())));
    }

    @Test
    void exposesNoPublicGenericConfigurationConstructor() {
        assertThat(SortConfig.class.getConstructors()).isEmpty();
    }

    @Test
    void defaultsMatchTheContractTable() {
        SortConfig config = fromProperties(Map.of());
        assertThat(config.heapFraction()).isEqualTo(0.08);
        assertThat(config.buffers()).isEqualTo(2);
        // A high fan-in default keeps a billion-scale run single-pass, runtime-clamped by the fd
        // limit / page size in SortTransform.
        assertThat(config.fanIn()).isEqualTo(10000);
        // Rolls the sorted output into ~1 GiB parts by default.
        assertThat(config.finalFileBytes()).isEqualTo(1L << 30);
        assertThat(config.segmentEntries()).isEqualTo(Long.MAX_VALUE);   // bytes gate governs by default
        assertThat(config.finalRowGroupBytes()).isEqualTo(8L * 1024 * 1024);
        // The page-run merge-memory denominator — ≈ one packed page (~64 KiB estimate).
        assertThat(config.mergePerStreamBytes()).isEqualTo(64L * 1024);
        // mergeBudgetBytes defaults to the SAME heap-adaptive shape as segmentBytes (floor 64 MB).
        assertThat(config.mergeBudgetBytes()).isEqualTo(config.segmentBytes());
        assertThat(config.minParallelStagedBytes()).isEqualTo(256L * 1024 * 1024);
        assertThat(config.stagingRetention()).isEqualTo(StagingRetention.DELETE_AFTER_PUBLISH);
        assertThat(config.mergeBoundaryPolicy()).isEqualTo(MergeBoundaryPolicy.DISTINCT);
    }

    @Test
    void mergeParallelismDefaultsToHalfTheCoresCappedAtEightAndIsReadable() {
        // On by default and core-derived: half the cores, capped at 8, floored at 1. The cap is where
        // parallel efficiency stops paying for the heap and read amplification (51 % at R=8 against
        // 34 % at R=16); the halving is the ramp for smaller machines. ParallelRangeMerge clamps this
        // further per run, so it is a ceiling rather than a promise.
        int expected = Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors() / 2));
        assertThat(SortConfig.DEFAULT.mergeParallelism()).isEqualTo(expected);
        assertThat(fromProperties(Map.of()).mergeParallelism()).isEqualTo(expected);
        assertThat(expected).as("never zero, however few cores the host reports").isGreaterThanOrEqualTo(1);
        assertThat(expected).as("never past the efficiency cap, however many").isLessThanOrEqualTo(8);
        // Still explicitly settable, including back down to a serial merge.
        assertThat(fromProperties(Map.of("merge-parallelism", "4")).mergeParallelism()).isEqualTo(4);
        assertThat(fromProperties(Map.of("merge-parallelism", "1")).mergeParallelism()).isEqualTo(1);
    }

    @Test
    void rejectsMergeParallelismBelowOne() {
        assertThatThrownBy(() -> minimalConfigWithMergeParallelism(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("merge-parallelism must be between 1 and 16, got 0");
        assertThatThrownBy(() -> fromProperties(Map.of("merge-parallelism", "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge-parallelism");
    }

    @Test
    void rejectsMergeParallelismAboveTheSupportedCoreCeiling() {
        assertThat(minimalConfigWithMergeParallelism(16).mergeParallelism()).isEqualTo(16);
        assertThatThrownBy(() -> minimalConfigWithMergeParallelism(17))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("merge-parallelism must be between 1 and 16, got 17");
        assertThatThrownBy(() -> fromProperties(Map.of("merge-parallelism", "17")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge-parallelism")
                .hasMessageContaining("16");
    }

    @Test
    void segmentBytesIsHeapAdaptiveWithA64MbFloor() {
        long expected = Math.max(SortConfig.SEGMENT_BYTES_FLOOR,
                (long) (0.08 * Runtime.getRuntime().maxMemory()));
        assertThat(fromProperties(Map.of()).segmentBytes()).isEqualTo(expected);
        // The floor holds for a tiny heap fraction.
        assertThat(SortConfig.adaptiveSegmentBytes(1e-9)).isEqualTo(SortConfig.SEGMENT_BYTES_FLOOR);
    }

    @Test
    void propertyOverridesTheAdaptiveDefault() {
        SortConfig config = fromProperties(Map.of("segment-bytes", "123456789"));
        assertThat(config.segmentBytes()).isEqualTo(123_456_789L);
    }

    @Test
    void groupedCopiesPreserveTheFormerRecordValueSemantics() {
        SortConfig config = SortConfig.DEFAULT
                .withSegmentBytes(1)
                .withSegmentEntries(2)
                .withHeapFraction(0.25)
                .withBuffers(3)
                .withFanIn(4)
                .withFinalFileBytes(5)
                .withFinalRowGroupBytes(6)
                .withFinalPageRows(7)
                .withMergeBudgetBytes(8)
                .withMergeParallelism(9)
                .withMergePerStreamBytes(10)
                .withSegmentCodec(PageCodec.NONE)
                .withMinParallelStagedBytes(11)
                .withStagingRetention(StagingRetention.RETAIN_ORIGINALS)
                .withMergeBoundaryPolicy(MergeBoundaryPolicy.ROWS)
                .withFinalization(SortFinalization.PIPELINE);

        SortConfig equivalent = config.withFanIn(config.fanIn());
        assertThat(config).isEqualTo(equivalent).hasSameHashCodeAs(equivalent);
        assertThat(config).hasToString("SortConfig[segmentBytes=1, segmentEntries=2, heapFraction=0.25, "
                + "buffers=3, fanIn=4, finalFileBytes=5, finalRowGroupBytes=6, finalPageRows=7, "
                + "mergeBudgetBytes=8, mergeParallelism=9, mergePerStreamBytes=10, segmentCodec=NONE, "
                + "minParallelStagedBytes=11, stagingRetention=RETAIN_ORIGINALS, mergeBoundaryPolicy=ROWS, "
                + "finalization=PIPELINE]");
    }

    @Test
    void keepStagingDefaultsOffAndParsesTheJvmProperty() {
        assertThat(fromProperties(Map.of("keep-staging", "on")).stagingRetention())
                .isEqualTo(StagingRetention.RETAIN_ORIGINALS);
        assertThat(fromProperties(Map.of("keep-staging", "off")).stagingRetention())
                .isEqualTo(StagingRetention.DELETE_AFTER_PUBLISH);
        assertThatThrownBy(() -> fromProperties(Map.of("keep-staging", "yes")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swath.sort.keep-staging")
                .hasMessageContaining("on or off");
    }

    @Test
    void mergeBoundaryPolicyDefaultsDistinctAndParsesTheJvmProperty() {
        assertThat(fromProperties(Map.of()).mergeBoundaryPolicy())
                .isEqualTo(MergeBoundaryPolicy.DISTINCT);
        assertThat(fromProperties(Map.of("merge-boundary-policy", "rows")).mergeBoundaryPolicy())
                .isEqualTo(MergeBoundaryPolicy.ROWS);
        assertThat(fromProperties(Map.of("merge-boundary-policy", "DISTINCT")).mergeBoundaryPolicy())
                .isEqualTo(MergeBoundaryPolicy.DISTINCT);
        assertThatThrownBy(() -> fromProperties(Map.of("merge-boundary-policy", "keys")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("swath.sort.merge-boundary-policy")
                .hasMessageContaining("distinct or rows");
    }

    @Test
    void everyKnobIsReadable() {
        SortConfig config = fromProperties(Map.of(
                "heap-fraction", "0.5",
                "segment-entries", "1000",
                "buffers", "4",
                "fan-in", "8",
                "final-file-bytes", "999",
                "final-row-group-bytes", "111",
                "merge-budget-bytes", "333",
                "merge-boundary-policy", "rows",
                "merge-per-stream-bytes", "444",
                "min-parallel-staged-bytes", "555"));
        assertThat(config.heapFraction()).isEqualTo(0.5);
        assertThat(config.segmentEntries()).isEqualTo(1000L);
        assertThat(config.buffers()).isEqualTo(4);
        assertThat(config.fanIn()).isEqualTo(8);
        assertThat(config.finalFileBytes()).isEqualTo(999L);
        assertThat(config.finalRowGroupBytes()).isEqualTo(111L);
        assertThat(config.mergeBudgetBytes()).isEqualTo(333L);
        assertThat(config.mergeBoundaryPolicy()).isEqualTo(MergeBoundaryPolicy.ROWS);
        assertThat(config.mergePerStreamBytes()).isEqualTo(444L);
        assertThat(config.minParallelStagedBytes()).isEqualTo(555L);
    }

    @Test
    void fromSystemPropertiesDelegatesToTheRealSystemProperties() {
        // A thin smoke test of the production entry point itself (no mutation): it must at least
        // return the same defaults as the injectable path when nothing sort-related is set.
        assertThat(SortConfig.fromSystemProperties().fanIn()).isEqualTo(fromProperties(Map.of()).fanIn());
    }

    @Test
    void rejectsInvalidKnobs() {
        assertThatThrownBy(() -> SortConfig.DEFAULT.withSegmentBytes(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("segment-bytes must be > 0, got 0");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withFanIn(1)) // fan-in < 2
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fan-in must be >= 2, got 1");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withHeapFraction(0.0)) // heap-fraction 0
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("heap-fraction must be > 0, got 0.0");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withMergeBudgetBytes(0))
                // merge-budget-bytes 0
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("merge-budget-bytes must be > 0, got 0");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withMinParallelStagedBytes(-1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("min-parallel-staged-bytes must be >= 0, got -1");
        assertThatThrownBy(() -> fromProperties(Map.of("min-parallel-staged-bytes", "-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("min-parallel-staged-bytes");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withMergeBoundaryPolicy(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("merge-boundary-policy must not be null");
    }

    // ------------------------------------------------------------------
    // buffers >= 2: SortLane bounds live sealed buffers to buffers() (fill + buffers()-1
    // off-thread); buffers=1 either deadlocks (0 off-thread slots) or, if floored, silently
    // allows 2 live buffers while claiming a cap of 1.
    // ------------------------------------------------------------------

    @Test
    void rejectsBuffersBelowTwo() {
        assertThatThrownBy(() -> SortConfig.DEFAULT.withBuffers(1))
                .as("buffers=1 must be rejected: 0 off-thread slots would deadlock every seal")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("buffers must be >= 2, got 1");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withBuffers(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("buffers must be >= 2, got 0");
        assertThatThrownBy(() -> SortConfig.DEFAULT.withBuffers(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("buffers must be >= 2, got -1");
    }

    @Test
    void acceptsBuffersAtOrAboveTwo() {
        assertThat(SortConfig.DEFAULT.withBuffers(2).buffers()).isEqualTo(2);
        assertThat(SortConfig.DEFAULT.withBuffers(3).buffers()).isEqualTo(3);
    }

    @Test
    void buffersSyspropOfOneIsRejectedThroughFromProperties() {
        // The swath.sort.buffers=1 sysprop path (not just the direct constructor call):
        // fromProperties eagerly constructs a SortConfig, so an invalid override fails fast here
        // rather than surfacing later inside SortLane.
        assertThatThrownBy(() -> fromProperties(Map.of("buffers", "1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("buffers");
    }

    // ------------------------------------------------------------------
    // effectiveFanIn() — the memory-budget bound (I11).
    // ------------------------------------------------------------------

    @Test
    void effectiveFanInIsUnboundedByBudgetWhenBudgetIsGenerous() {
        SortConfig config = fanInConfig(Long.MAX_VALUE, SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES);
        assertThat(config.effectiveFanIn()).isEqualTo(512);   // budget never binds ⇒ raw fan-in
    }

    @Test
    void effectiveFanInIsCappedByTheMergeBudget() {
        // 640 KiB budget / 64 KiB per stream gives 10 streams per pass.
        // 640 KiB budget / 64 KiB per stream ⇒ 10 streams/pass, well under the raw fan-in of 512.
        SortConfig config = fanInConfig(10L * (64L << 10), SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES);
        assertThat(config.effectiveFanIn()).isEqualTo(10);
    }

    @Test
    void effectiveFanInUsesMergePerStreamBytesAsTheDenominator() {
        SortConfig config = fanInConfig(8L << 20, 128L << 10);   // 8 MB / 128 KiB = 64
        assertThat(config.effectiveFanIn()).isEqualTo(64);
    }

    @Test
    void effectiveFanInIsFlooredAtTwoEvenUnderAnExtremelyTightBudget() {
        SortConfig config = fanInConfig(1L, SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES); // 1 byte budget
        assertThat(config.effectiveFanIn()).isEqualTo(2);
    }

    @Test
    void effectiveFanInAtTheDashXmx2gRunbookDefaultsMatchesTheDocumentedWorkedNumber() {
        // heap-fraction 0.08 gives ≈160 MB budget at -Xmx2g, and the 64 KiB per-stream estimate ⇒
        // effective fan-in ≈ 2560 (well under the raw fan-in default of 10000).
        long twoGb = 2L * 1024 * 1024 * 1024;
        long budget = Math.max(SortConfig.SEGMENT_BYTES_FLOOR, (long) (0.08 * twoGb));
        SortConfig config = SortConfig.DEFAULT.withSegmentBytes(budget).withMergeBudgetBytes(budget);
        assertThat(config.effectiveFanIn()).isEqualTo((int) (budget / SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES));
        assertThat(config.effectiveFanIn()).isLessThan(10000);   // strictly tighter than the raw knob
    }

    // ------------------------------------------------------------------
    // segmentCodec: compress-at-pack codec knob.
    // ------------------------------------------------------------------

    @Test
    void segmentCodecDefaultsToZstd1AndParsesCaseInsensitively() {
        assertThat(fromProperties(Map.of()).segmentCodec()).isEqualTo(PageCodec.ZSTD1);
        assertThat(fromProperties(Map.of("segment-codec", "none")).segmentCodec()).isEqualTo(PageCodec.NONE);
        assertThat(fromProperties(Map.of("segment-codec", "LZ4")).segmentCodec()).isEqualTo(PageCodec.LZ4);
        assertThat(fromProperties(Map.of("segment-codec", "zstd1")).segmentCodec()).isEqualTo(PageCodec.ZSTD1);
    }

    @Test
    void rejectsUnknownSegmentCodec() {
        assertThatThrownBy(() -> fromProperties(Map.of("segment-codec", "bogus")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment-codec");
    }

    @Test
    void rejectsNullSegmentCodec() {
        assertThatThrownBy(() -> SortConfig.DEFAULT.withSegmentCodec(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("segment-codec must not be null");
    }

    @Test
    void rejectsMergePerStreamBytesAtOrBelowZero() {
        assertThatThrownBy(() -> fanInConfig(8L << 20, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("merge-per-stream-bytes must be > 0, got 0");
        assertThatThrownBy(() -> fromProperties(Map.of("merge-per-stream-bytes", "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge-per-stream-bytes");
    }

    // ------------------------------------------------------------------
    // These test-local factories derive fixtures from the documented defaults, overriding only the
    // knobs each case needs.
    // ------------------------------------------------------------------

    /** A valid config overriding only {@code mergeParallelism} (the field under test). */
    private static SortConfig minimalConfigWithMergeParallelism(int mergeParallelism) {
        return SortConfig.DEFAULT.withMergeParallelism(mergeParallelism);
    }

    /** The {@code effectiveFanIn} test fixture with explicit budget and per-stream price. */
    private static SortConfig fanInConfig(long mergeBudgetBytes, long mergePerStreamBytes) {
        return SortConfig.DEFAULT
                .withFanIn(512)
                .withMergeBudgetBytes(mergeBudgetBytes)
                .withMergePerStreamBytes(mergePerStreamBytes);
    }
}
