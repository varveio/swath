/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.engine.Thief;
import io.varve.swath.engine.WorkerState;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * The {@code slow_ranges[]} terminal-summary dump. Drives the SAME deterministic
 * {@code Thief}/{@code WorkerState} fixture {@code ThiefTest#exhaustedFrontierAtCeilingIsGenuinelyUnsplittable}
 * uses — an open frontier whose cursor has consumed real span but sits exactly at the prefix
 * ceiling, a genuine dead end with no room left to split (the "serial tail" shape: the one range
 * still standing, draining to completion by itself) — then registers the live-worklist snapshot
 * source the same way {@code WorkStealingScan} does in production, and asserts the dumped range
 * carries its own per-range {@code steal_reason} tally. No live/racing engine needed: {@code
 * WorkerState}'s tallies are plain per-object counters and {@code RunMetrics.summary()}'s {@code
 * slow_ranges[]} just reads whatever {@code RangeSnapshot} source is registered.
 */
final class SlowRangeDumpTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Minimal no-op {@link CheckpointStore} — this fixture's steal never reaches {@code splitNode}. */
    private static final class NoopStore implements CheckpointStore {
        @Override public RunMeta openRun(RunKey k, boolean resume, boolean restart) { throw new UnsupportedOperationException(); }
        @Override public long insertNode(NodeSpec spec) { throw new UnsupportedOperationException(); }
        @Override public List<Node> loadResumable(long runId, boolean fileSink) { throw new UnsupportedOperationException(); }
        @Override public long splitNode(SplitSpec s) { throw new UnsupportedOperationException(); }
        @Override public void commitPage(PageCommit c) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<Void> commitPageAsync(PageCommit c) { throw new UnsupportedOperationException(); }
        @Override public void partFinalized(PartFinalize f) { throw new UnsupportedOperationException(); }
        @Override public List<PartRef> finalizedParts(long runId) { throw new UnsupportedOperationException(); }
        @Override public void markOutputComplete(long runId) { throw new UnsupportedOperationException(); }
        @Override public void markRunFinished(long runId, RunStatus status) { throw new UnsupportedOperationException(); }
        @Override public void close() { }
    }

    @Test
    void slowRangesDumpsTheDenseSerialTailRangeWithItsStealReasonTallies()
            throws SwathException, InterruptedException {
        // prefixCeil("hot/") == "hot0"; the cursor sits exactly there — consumed real span (cursor >
        // lo) but no forward room below the ceiling, so extrapolate's null pivot is the GENUINE
        // terminal case (ThiefTest's exhaustedFrontierAtCeilingIsGenuinelyUnsplittable).
        byte[] prefix = b("hot/");
        WorkerState denseRange = WorkerStates.of(1, b("hot/00"), b("hot0"), null);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(new NoopStore(), MockPageFetcher.builder().keys(List.of(b("m1"))).build(),
                7L, prefix, ListingMode.OBJECTS, (childId, lo, hi) -> { }, metrics);

        assertThat(thief.steal(List.of(denseRange))).isEqualTo(Thief.Outcome.UNSPLITTABLE);
        assertThat(denseRange.noPivotTally()).as("the dense range's own no-pivot tally fired").isEqualTo(1L);
        assertThat(denseRange.cursorPassedPivotTally()).isZero();
        assertThat(denseRange.demandGatedTally()).isZero();

        // Register the live-worklist snapshot source exactly the way WorkStealingScan does in
        // production (see WorkStealingScan#snapshotLiveRanges) — a single remaining range.
        metrics.registerRangeSnapshotSource(() -> List.of(new RunMetrics.RangeSnapshot(
                denseRange.lo(), denseRange.hi(), denseRange.cursor(), 42.0, 3.5,
                denseRange.cursorPassedPivotTally(), denseRange.noPivotTally(),
                denseRange.structureSuppressedTally(), denseRange.demandGatedTally())));

        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.slowRanges()).hasSize(1);
        RunSummary.SlowRange dumped = summary.slowRanges().get(0);
        assertThat(dumped.lo()).isEqualTo("hot/00");
        assertThat(dumped.hi()).as("open frontier renders as the shared <none> display sentinel")
                .isEqualTo("<none>");
        assertThat(dumped.cursor()).isEqualTo("hot0");
        assertThat(dumped.estRemaining()).isEqualTo(42.0);
        assertThat(dumped.drainRate()).isEqualTo(3.5);
        assertThat(dumped.noPivot()).isEqualTo(1L);
        assertThat(dumped.cursorPassedPivot()).isZero();
        assertThat(dumped.structureSuppressed()).isZero();
        assertThat(dumped.demandGated()).isZero();
    }

    @Test
    void slowRangesIsEmptyWhenNoSourceIsRegistered() throws SwathException, InterruptedException {
        // The default (most unit tests): no WorkStealingScan ever constructed against this
        // RunMetrics, so no source is registered — slow_ranges[] must render empty, never throw.
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RunSummary summary = metrics.summary(Duration.ofSeconds(1), "WORK_STEALING", 0L, 0L);
        assertThat(summary.slowRanges()).isEmpty();
    }
}
