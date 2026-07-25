/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SegmentSink;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortLane;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortStamp;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cascade-scale coverage for sort-phase resume.
 * {@link SortMergeReentryContractTest} already proves the merge-pending
 * redo contract at small scale (single-pass, hand-sized fixtures, a hand-built segment list in the
 * cascade case); this closes the gap at cascade scale — hundreds of real staging segments, a real
 * {@link SqliteCheckpointStore} tracking them via {@code partFinalized}/{@code finalizedParts} (not
 * a hand-built {@code List<Path>}), and a genuine multi-pass cascade forced the SAME way
 * {@code SortPerfTest#mergePhaseStress} forces it (tiny {@code segment-entries} + tiny
 * {@code merge-budget-bytes} &rArr; a small {@link SortConfig#effectiveFanIn()}).
 *
 * <p><b>How the mid-cascade interruption is simulated</b> (a real {@code kill -9} isn't available
 * in-process, per the adversarial-test idiom every other SORT-RESUME test in this suite uses): the listing
 * side is driven to completion through the real checkpoint store — a real {@link SortLane} finalizes
 * every segment via {@code partFinalized} (in seal order), the root node is committed
 * {@code COMPLETED}, and {@link io.varve.swath.checkpoint.CheckpointStore#markOutputComplete} latches
 * {@code durable_cursor}, so {@code loadResumable} reports nothing left to list — exactly the
 * merge-pending resume state {@link ListCommand#runSortedParquet}'s dispatch reads (nodes empty, no
 * manifest). A stale partial {@code merge-N.parquet} — the debris a crash mid-cascade leaves behind,
 * per {@code KWayMerge}'s deletion-policy javadoc — is then planted directly in staging. From there,
 * {@link ListRunner#runSortMergeOnly} is the real production re-entry: it takes no {@link
 * io.varve.swath.output.PageFetcher} at all, so "zero new LIST fetches" holds by construction, not
 * merely by assertion; {@code SORT.merge_redone} is asserted anyway as the production observability
 * signal an operator/dashboard would actually see fire.
 *
 * <p>Contract points asserted:
 * <ul>
 *   <li>the cascade genuinely engaged multi-pass at this scale ({@code swath.sort.merge.passes > 1},
 *       {@code SORT.merge_pass_cascaded});</li>
 *   <li>zero new LIST fetches on resume ({@code SORT.merge_redone} fires exactly once; structurally,
 *       {@code runSortMergeOnly} never touches a fetcher);</li>
 *   <li>every durable sealed segment is reused and the final output is complete, globally sorted, and
 *       duplicate-free against the keyspace ground truth (no gap, no overlap);</li>
 *   <li>the stale planted {@code merge-N.parquet} cascade intermediates are discarded, never read —
 *       the cascade restarts clean at pass 0, not mid-cascade (never a partial resume of
 *       the merge cascade itself, only of the listing that feeds it);</li>
 *   <li>publish completes (manifest present, {@code SortPhase.PUBLISHED}) and staging — including the
 *       stale planted debris — is fully cleaned up.</li>
 * </ul>
 */
final class SortResumeCascadeScaleTest {

    private static final String ARGS_HASH = "sort-resume-cascade-scale-hash";
    // Mirrors SortPerfTest#mergePhaseStress's knobs (proven to force a genuine multi-pass cascade):
    // segment-entries == the page size, so every admitted page seals its own segment deterministically.
    private static final int OBJECTS = 60_000;
    private static final int PAGE_SIZE = 100;

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, 32, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** Tiny segment-entries ⇒ hundreds of segments, and an explicitly pinned fan-in (= 8: the
     * budget-bounded effectiveFanIn denominator changed, so pin fan-in directly instead of relying on
     * merge-budget/row-group arithmetic), forcing a genuine multi-pass cascade at scale. */
    private static SortConfig cascadeAtScale() {
        return SortConfigs.base()
                .withSegmentBytes(Long.MAX_VALUE)
                .withSegmentEntries(PAGE_SIZE)
                .withFanIn(8)
                .withMergeBudgetBytes(8L << 20);
    }

    @Test
    @Timeout(180)
    void cascadeAtScaleMergePendingResume_zeroListFetches_allSegmentsReused_staleIntermediateDiscarded(
            @TempDir Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        Path db = tmp.resolve("c.sqlite");
        List<byte[]> keyspace = Keyspaces.exactly(OBJECTS);

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));

            // Build durable staging segments through the REAL checkpoint store (partFinalized/
            // finalizedParts), at cascade scale — hundreds of segments, not a hand-built list.
            SegmentSink sink = result -> store.partFinalized(new PartFinalize(run.id(), 0,
                    result.path().getFileName().toString(), ListRunner.SORT_SEGMENT_FORMAT,
                    result.rows(), result.bytes(), result.perNodeMaxKeys().entrySet().stream()
                    .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList()));
            SortLane lane = new SortLane(cascadeAtScale(), new ListEntryComparator(), DuplicateHook.NO_OP,
                    SortMetrics.NO_OP, SortLaneMeters.NO_OP, stagingDir, "seg-" + run.id() + "-x", sink);
            for (List<ListEntry> page : pages(keyspace, PAGE_SIZE)) {
                lane.admit(nodeId, page);
            }
            lane.close();

            // Drive the listing side to the real "nothing left to list" state through the checkpoint
            // store's own APIs (not a shortcut): commit the root node COMPLETED, then latch
            // durable_cursor — exactly what a clean pool.close() does at the end of a real listing.
            store.commitPage(new PageCommit(nodeId, null, true));
            store.markOutputComplete(run.id());

            List<PartRef> segRows = store.finalizedParts(run.id());
            assertThat(segRows).as("cascade scale: hundreds of durable sealed segments")
                    .hasSizeGreaterThan(100);
            assertThat(store.loadResumable(run.id(), true))
                    .as("listing complete: no resumable nodes remain (the merge-pending resume state)")
                    .isEmpty();
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).as("nothing published yet").isFalse();

            // Simulate the mid-cascade crash debris (we cannot literally kill -9 in-process, per the
            // idiom every SORT-RESUME test in this suite uses): a stale partial merge-N.parquet, as a
            // crashed prior cascade attempt would leave behind (KWayMerge's deletion-policy javadoc).
            Files.writeString(stagingDir.resolve("merge-0.parquet"),
                    "stale cascade intermediate from a crashed prior attempt");
            Files.writeString(stagingDir.resolve("merge-3.parquet"), "another stale cascade intermediate");

            // ---- resume: the real merge-pending dispatch. runSortMergeOnly takes no PageFetcher at
            //      all, so "zero new LIST fetches" holds structurally, not merely by assertion. ----
            new ListRunner().runSortMergeOnly(ctx, outputDir, stagingDir, store, run.id(),
                    cascadeAtScale(), SortMode.OBJECTS, spec());

            // The cascade genuinely engaged multi-pass at this scale (not a lucky single pass).
            assertThat(counter(ctx, "swath.sort.merge.passes"))
                    .as("cascade genuinely multi-pass at this scale").isGreaterThan(1.0);
            assertThat(counter(ctx, "swath.steal_reason", "merge_pass_cascaded"))
                    .as("SORT.merge_pass_cascaded engaged").isGreaterThan(0.0);

            // Zero new LIST fetches on resume — the production observability signal fired.
            assertThat(counter(ctx, "swath.steal_reason", "merge_redone"))
                    .as("SORT.merge_redone fired exactly once (zero new LIST fetches)").isEqualTo(1.0);

            // Every sealed segment reused; final output complete, globally sorted, duplicate-free.
            Path finalFile = DatasetLayout.of(outputDir).dataFile("part-00001.parquet");
            List<String> outKeys = ParquetReads.keys(finalFile);
            List<String> expected = sortedStrings(keyspace);
            assertThat(outKeys).as("no duplicate rows").doesNotHaveDuplicates();
            assertThat(outKeys)
                    .as("complete + globally sorted, byte-exact vs the keyspace ground truth (no gap/overlap)")
                    .containsExactlyElementsOf(expected);
            assertThat(SortStamp.read(finalFile)).isPresent();

            // Publish completed; staging (including the stale planted debris) fully cleaned up.
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);
            assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).isTrue();
            assertThat(Files.exists(stagingDir))
                    .as("staging dir (incl. the stale planted merge-*.parquet) removed after republish")
                    .isFalse();
        }
    }

    private static List<List<ListEntry>> pages(List<byte[]> keyspace, int pageSize) {
        List<byte[]> sorted = new ArrayList<>(keyspace);
        sorted.sort(Arrays::compareUnsigned);
        List<List<ListEntry>> pages = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i += pageSize) {
            List<ListEntry> page = new ArrayList<>();
            for (int j = i; j < Math.min(i + pageSize, sorted.size()); j++) {
                page.add(new ObjectEntry(KeyBytes.of(sorted.get(j)), 1L, 0L, null, null, null,
                        false, null, null, null, null));
            }
            pages.add(page);
        }
        return pages;
    }

    private static List<String> sortedStrings(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).sorted().toList();
    }

    private static double counter(RunContext ctx, String name) {
        Counter c = ctx.metrics().registry().find(name).counter();
        return c == null ? 0.0 : c.count();
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }
}
