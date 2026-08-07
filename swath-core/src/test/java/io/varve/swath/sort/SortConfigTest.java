/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static io.varve.swath.sort.SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES;
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
        // segmentRowGroupBytes now only sizes columnar-Parquet staging (fixtures and the
        // off-by-default parallel-merge path); it no longer drives effectiveFanIn.
        assertThat(config.segmentRowGroupBytes()).isEqualTo(1L * 1024 * 1024);
        // The page-run merge-memory denominator — ≈ one packed page (~64 KiB estimate).
        assertThat(config.mergePerStreamBytes()).isEqualTo(64L * 1024);
        // mergeBudgetBytes defaults to the SAME heap-adaptive shape as segmentBytes (floor 64 MB).
        assertThat(config.mergeBudgetBytes()).isEqualTo(config.segmentBytes());
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
                .hasMessageContaining("merge-parallelism");
        assertThatThrownBy(() -> fromProperties(Map.of("merge-parallelism", "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge-parallelism");
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
    void everyKnobIsReadable() {
        SortConfig config = fromProperties(Map.of(
                "heap-fraction", "0.5",
                "segment-entries", "1000",
                "buffers", "4",
                "fan-in", "8",
                "final-file-bytes", "999",
                "final-row-group-bytes", "111",
                "segment-row-group-bytes", "222",
                "merge-budget-bytes", "333",
                "merge-per-stream-bytes", "444"));
        assertThat(config.heapFraction()).isEqualTo(0.5);
        assertThat(config.segmentEntries()).isEqualTo(1000L);
        assertThat(config.buffers()).isEqualTo(4);
        assertThat(config.fanIn()).isEqualTo(8);
        assertThat(config.finalFileBytes()).isEqualTo(999L);
        assertThat(config.finalRowGroupBytes()).isEqualTo(111L);
        assertThat(config.segmentRowGroupBytes()).isEqualTo(222L);
        assertThat(config.mergeBudgetBytes()).isEqualTo(333L);
        assertThat(config.mergePerStreamBytes()).isEqualTo(444L);
    }

    @Test
    void fromSystemPropertiesDelegatesToTheRealSystemProperties() {
        // A thin smoke test of the production entry point itself (no mutation): it must at least
        // return the same defaults as the injectable path when nothing sort-related is set.
        assertThat(SortConfig.fromSystemProperties().fanIn()).isEqualTo(fromProperties(Map.of()).fanIn());
    }

    @Test
    void rejectsInvalidKnobs() {
        assertThatThrownBy(() -> new SortConfig(0, 1, 0.08, 2, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, 2, 1, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))   // fan-in < 2
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.0, 2, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))  // heap-fraction 0
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, 2, 512, 1, 1, 0, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))   // segment-row-group-bytes 0
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, 2, 512, 1, 1, 1, 0, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))   // merge-budget-bytes 0
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------
    // buffers >= 2: SortLane bounds live sealed buffers to buffers() (fill + buffers()-1
    // off-thread); buffers=1 either deadlocks (0 off-thread slots) or, if floored, silently
    // allows 2 live buffers while claiming a cap of 1.
    // ------------------------------------------------------------------

    @Test
    void rejectsBuffersBelowTwo() {
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, 1, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))
                .as("buffers=1 must be rejected: 0 off-thread slots would deadlock every seal")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("buffers");
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, 0, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("buffers");
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, -1, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("buffers");
    }

    @Test
    void acceptsBuffersAtOrAboveTwo() {
        assertThat(new SortConfig(1, 1, 0.08, 2, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L).buffers()).isEqualTo(2);
        assertThat(new SortConfig(1, 1, 0.08, 3, 512, 1, 1, 1, 1, 1, DEFAULT_MERGE_PER_STREAM_BYTES, PageCodec.LZ4, 0L).buffers()).isEqualTo(3);
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
        SortConfig config = new SortConfig(64L << 20, Long.MAX_VALUE, 0.08, 2, 512,
                Long.MAX_VALUE, 8L << 20, 1L << 20, Long.MAX_VALUE, SortConfig.DEFAULT_MERGE_PARALLELISM,
                SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES, SortConfig.DEFAULT_SEGMENT_CODEC, 0L);
        assertThat(config.effectiveFanIn()).isEqualTo(512);   // budget never binds ⇒ raw fan-in
    }

    @Test
    void effectiveFanInIsCappedByTheMergeBudget() {
        // The denominator is merge-per-stream-bytes (default 64 KiB), not segment-row-group-bytes.
        // 640 KiB budget / 64 KiB per stream ⇒ 10 streams/pass, well under the raw fan-in of 512.
        SortConfig config = new SortConfig(64L << 20, Long.MAX_VALUE, 0.08, 2, 512,
                Long.MAX_VALUE, 8L << 20, 1L << 20, 10L * (64L << 10), SortConfig.DEFAULT_MERGE_PARALLELISM,
                SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES, SortConfig.DEFAULT_SEGMENT_CODEC, 0L);
        assertThat(config.effectiveFanIn()).isEqualTo(10);
    }

    @Test
    void effectiveFanInUsesMergePerStreamBytesNotSegmentRowGroupBytesAsTheDenominator() {
        // The denominator is merge-per-stream-bytes; segment-row-group-bytes (a columnar-Parquet
        // concept) must not affect the merge-memory bound. Same budget, two very different
        // segment-row-group values ⇒ identical effectiveFanIn.
        SortConfig smallRowGroup = fanInBudgetConfig(1L << 20, 128L << 10);   // 8 MB / 128 KiB = 64
        SortConfig hugeRowGroup = fanInBudgetConfig(512L << 20, 128L << 10);
        assertThat(smallRowGroup.effectiveFanIn()).isEqualTo(64);
        assertThat(hugeRowGroup.effectiveFanIn()).isEqualTo(64);   // segment-row-group-bytes is irrelevant to the bound
    }

    @Test
    void effectiveFanInIsFlooredAtTwoEvenUnderAnExtremelyTightBudget() {
        SortConfig config = new SortConfig(64L << 20, Long.MAX_VALUE, 0.08, 2, 512,
                Long.MAX_VALUE, 8L << 20, 1L << 20, 1L, SortConfig.DEFAULT_MERGE_PARALLELISM,
                SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES, SortConfig.DEFAULT_SEGMENT_CODEC, 0L);   // 1 byte budget
        assertThat(config.effectiveFanIn()).isEqualTo(2);
    }

    @Test
    void effectiveFanInAtTheDashXmx2gRunbookDefaultsMatchesTheDocumentedWorkedNumber() {
        // heap-fraction 0.08 gives ≈160 MB budget at -Xmx2g, and the 64 KiB per-stream estimate ⇒
        // effective fan-in ≈ 2560 (well under the raw fan-in default of 10000).
        long twoGb = 2L * 1024 * 1024 * 1024;
        long budget = Math.max(SortConfig.SEGMENT_BYTES_FLOOR, (long) (0.08 * twoGb));
        SortConfig config = new SortConfig(budget, Long.MAX_VALUE, 0.08, 2, 10000,
                Long.MAX_VALUE, 8L << 20, 1L << 20, budget, SortConfig.DEFAULT_MERGE_PARALLELISM,
                SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES, SortConfig.DEFAULT_SEGMENT_CODEC, 0L);
        assertThat(config.effectiveFanIn()).isEqualTo((int) (budget / SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES));
        assertThat(config.effectiveFanIn()).isLessThan(10000);   // strictly tighter than the raw knob
    }

    // ------------------------------------------------------------------
    // segmentCodec: compress-at-pack codec knob.
    // ------------------------------------------------------------------

    @Test
    void segmentCodecDefaultsToLz4AndParsesCaseInsensitively() {
        assertThat(fromProperties(Map.of()).segmentCodec()).isEqualTo(PageCodec.LZ4);
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
        assertThatThrownBy(() -> new SortConfig(1, 1, 0.08, 2, 512, 1, 1, 1, 1, 1, 1, null, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment-codec");
    }

    @Test
    void rejectsMergePerStreamBytesAtOrBelowZero() {
        assertThatThrownBy(() -> fanInBudgetConfig(1L << 20, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge-per-stream-bytes");
        assertThatThrownBy(() -> fromProperties(Map.of("merge-per-stream-bytes", "0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("merge-per-stream-bytes");
    }

    // ------------------------------------------------------------------
    // These test-local factories build fixtures from a fixed base via the canonical 12-arg
    // constructor, each overriding just the one field the case needs.
    // ------------------------------------------------------------------

    /** All-1s minimal config, overriding only {@code mergeParallelism} (the field under test). */
    private static SortConfig minimalConfigWithMergeParallelism(int mergeParallelism) {
        return new SortConfig(1, 1, 0.08, 2, 512, 1, 1, 1, 1, mergeParallelism,
                SortConfig.DEFAULT_MERGE_PER_STREAM_BYTES, SortConfig.DEFAULT_SEGMENT_CODEC, 0L);
    }

    /** The {@code effectiveFanIn} test fixture, overriding only {@code segmentRowGroupBytes} and
     *  {@code mergePerStreamBytes} (the fields under test). */
    private static SortConfig fanInBudgetConfig(long segmentRowGroupBytes, long mergePerStreamBytes) {
        return new SortConfig(64L << 20, Long.MAX_VALUE, 0.08, 2, 512, Long.MAX_VALUE, 8L << 20,
                segmentRowGroupBytes, 8L << 20, SortConfig.DEFAULT_MERGE_PARALLELISM, mergePerStreamBytes,
                SortConfig.DEFAULT_SEGMENT_CODEC, 0L);
    }
}
