/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.fixture;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.sort.SortMetrics;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Micrometer-backed counters/timers for the {@code io.varve.swath.replay.fixture} package: the
 * {@code sort-fixture} legacy-transform path and the sorted-serving
 * index derive/sanity-check path. Deliberately a <b>sibling</b> to {@code
 * io.varve.swath.replay.server.ReplayMetrics}, not a dependency on it — {@code .server} already
 * depends on {@code .fixture} (the CLI wires {@link SortFixtureCommand} in), so this package must
 * not depend back on {@code .server} or the two packages would form a cycle.
 */
public final class FixtureMetrics implements SortMetrics {

    private final MeterRegistry registry;
    private final Timer sortFixtureBuildLatency;
    private final DistributionSummary sortFixtureOutputBytes;
    private final Timer indexLoadLatency;
    private final DistributionSummary indexEntries;
    private final Counter sortProgress;
    private final Counter sortBoundaryEmbeddedEntries;
    private final Counter sortBoundaryEmbeddedBytes;
    private final Counter sortBoundaryScanBytes;
    private final Counter sortRangeIndexBytes;
    private final Counter sortOverlapClusters;
    private final ConcurrentMap<String, Counter> sortStealReasonCounters = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong sortOverlapPagesPeak =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong sortOverlapRowsPeak =
            new java.util.concurrent.atomic.AtomicLong();

    public FixtureMetrics() {
        this(new SimpleMeterRegistry());
    }

    public FixtureMetrics(MeterRegistry registry) {
        this.registry = registry;
        sortFixtureBuildLatency = Timer.builder("swath.replay.sortfixture.build.latency").register(registry);
        sortFixtureOutputBytes = DistributionSummary.builder("swath.replay.sortfixture.output.bytes")
                .register(registry);
        // The "source" tag anticipates a future "footer" value once an embedded routing
        // blob lets the server skip the derive pass; v1 only ever records "derived".
        indexLoadLatency = Timer.builder("swath.replay.index.load.latency").tag("source", "derived")
                .register(registry);
        indexEntries = DistributionSummary.builder("swath.replay.index.entries").register(registry);
        sortProgress = Counter.builder("swath.replay.sort.progress").register(registry);
        sortBoundaryEmbeddedEntries = Counter.builder("swath.replay.sort.merge.boundaries.embedded.entries")
                .register(registry);
        sortBoundaryEmbeddedBytes = Counter.builder("swath.replay.sort.merge.boundaries.embedded.bytes")
                .baseUnit("bytes").register(registry);
        sortBoundaryScanBytes = Counter.builder("swath.replay.sort.merge.boundaries.scan.bytes")
                .baseUnit("bytes").register(registry);
        sortRangeIndexBytes = Counter.builder("swath.replay.sort.merge.range.index.bytes")
                .baseUnit("bytes").register(registry);
        sortOverlapClusters = Counter.builder("swath.replay.sort.merge.overlap.clusters").register(registry);
        registry.gauge("swath.replay.sort.merge.overlap.pages.peak", sortOverlapPagesPeak,
                java.util.concurrent.atomic.AtomicLong::get);
        registry.gauge("swath.replay.sort.merge.overlap.rows.peak", sortOverlapRowsPeak,
                java.util.concurrent.atomic.AtomicLong::get);
    }

    public MeterRegistry registry() {
        return registry;
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    /** Records one {@code sort-fixture} run: build wall time + the resulting output file size(s). */
    public void recordSortFixtureBuild(Timer.Sample sample, long outputBytes) {
        sample.stop(sortFixtureBuildLatency);
        sortFixtureOutputBytes.record(outputBytes);
    }

    /** Records one {@link SortedFixtures#loadIndex} call: derive wall time + entries produced. */
    public void recordIndexLoad(Timer.Sample sample, int entries) {
        sample.stop(indexLoadLatency);
        indexEntries.record(entries);
    }

    /**
     * {@code swath.replay.serving.fallback\{reason\}}: auto-serving-mode declined sorted
     * serving. Reasons in play: {@code no_stamp} (no recognized {@link
     * io.varve.swath.sort.SortStamp}), {@code unsupported_mode} (a stamped
     * {@code versions} file, §0.6), {@code unknown_format_version} (a
     * resolved file's stamp carries a {@code format_version} this reader doesn't recognize),
     * {@code incomplete_multifile} (the resolved file set's {@code file_index}/{@code file_final}
     * stamps don't prove multi-file completeness, e.g. a crash left a stamped prefix of a multi-file
     * publish on disk), {@code mixed_row_types} (a row group's {@code row_type} footer stats
     * do not prove every row is {@code OBJECT}), {@code sanity_failed}
     * (this package's own {@link SortedFixtures#loadIndex} ascending check). The meter is defined
     * here, so every reason shares one counter family.
     */
    public void recordFallback(String reason) {
        registry.counter("swath.replay.serving.fallback", "reason", reason).increment();
    }

    /**
     * Registry-backed {@link SortMetrics} adapter for {@code sort-fixture}. Mirrors
     * {@code io.varve.swath.observability.RunMetrics#recordStealReason}'s tagging scheme
     * ({@code swath.replay.sort.steal_reason\{outcome,reason\}}) without depending on that class.
     */
    @Override
    public void recordStealReason(String outcome, String reason) {
        sortStealReasonCounters.computeIfAbsent(outcome + "." + reason,
                ignored -> Counter.builder("swath.replay.sort.steal_reason")
                        .tag("outcome", outcome)
                        .tag("reason", reason)
                        .register(registry))
                .increment();
    }

    /** Records one fixture-sort progress tick. */
    @Override
    public void markProgress() {
        sortProgress.increment();
    }

    /** Records fixture-sort boundary-selection metadata and fallback scan volume. */
    @Override
    public void recordBoundaryIo(long embeddedEntries, long embeddedBytes, long scanBytes) {
        sortBoundaryEmbeddedEntries.increment(embeddedEntries);
        sortBoundaryEmbeddedBytes.increment(embeddedBytes);
        sortBoundaryScanBytes.increment(scanBytes);
    }

    @Override
    public void recordPageAwareOverlapCluster() {
        sortOverlapClusters.increment();
    }

    @Override
    public void recordPageAwareOverlapState(long activePages, long retainedRows) {
        sortOverlapPagesPeak.accumulateAndGet(activePages, Math::max);
        sortOverlapRowsPeak.accumulateAndGet(retainedRows, Math::max);
    }

    @Override
    public void recordRangeIndexBytes(long bytes) {
        sortRangeIndexBytes.increment(bytes);
    }

    /**
     * How many {@code SORT.segment_flushed} engagements this instance has recorded — {@code
     * sort-fixture}'s summary line reads this back since {@code io.varve.swath.sort.SortTransformResult}
     * doesn't carry a segment count itself (only the final published file(s) and merge-pass counts).
     * {@code 0} if {@link #recordStealReason} was never wired (e.g. {@code SortMetrics.NO_OP} still in
     * use) or no segment was ever flushed.
     */
    public long segmentsFlushed() {
        Counter counter = sortStealReasonCounters.get("SORT.segment_flushed");
        return counter == null ? 0 : (long) counter.count();
    }
}
