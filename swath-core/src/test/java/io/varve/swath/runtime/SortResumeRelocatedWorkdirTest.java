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
import io.varve.swath.testkit.CrashResumeOracle;
import io.varve.swath.testkit.Keyspaces;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * <b>Relocated / round-tripped workdir resume.</b>
 *
 * <p>The "download a merge-pending workdir to a fresh VM and resume it" case: this
 * drives a {@code --sort} run to the merge-pending state, then physically <b>copies the entire
 * workdir</b> (checkpoint SQLite + staging directory) to a <i>different absolute path</i> and resumes
 * {@link ListRunner#runSortMergeOnly} from the new location. The final output must be identical to a
 * clean un-killed baseline via the {@link CrashResumeOracle}. This proves resume is
 * path-independent: nothing in the checkpoint or the staging segments hard-codes the original
 * directory. {@code args_hash} and {@code run_id} are asserted to still match after relocation (the
 * checkpoint's identity survives the copy), and {@code openRun(resume=true)} on the relocated DB
 * validates the {@code args_hash} itself.
 *
 * <p>A crash in the sort merge does NOT discard the entire listing: the
 * relocated resume re-uses the durable staging segments and re-runs <i>only</i> the merge with
 * <b>zero new LIST fetches</b> — never a full re-list on retry. {@link ListRunner#runSortMergeOnly}
 * takes no {@code PageFetcher} at all, so "zero LIST fetches" holds by construction, not merely by
 * assertion; the production observability signal {@code SORT.merge_redone} is asserted to fire
 * exactly once as the evidence an operator would actually see. (The same contract is guarded
 * deterministically in {@code SortMergeReentryContractTest} and {@code SortResumeCascadeScaleTest}; this
 * adds the relocated-workdir dimension.)
 *
 * <p><b>Scope note:</b> this calls {@link ListRunner#runSortMergeOnly} directly, bypassing the CLI's
 * {@code --resume} dispatch router (mode/format/args_hash-mismatch refusal, etc.) — that router is
 * exercised end-to-end (real subprocess, real {@code --resume} flag) by the
 * {@code HardCrashSigkillResumeMatrixIT} crash-matrix, not here.
 */
final class SortResumeRelocatedWorkdirTest {

    private static final String ARGS_HASH = "sort-relocated-workdir-hash";
    private static final int OBJECTS = 12_000;
    private static final int PAGE_SIZE = 100;
    private final ListEntryComparator cmp = new ListEntryComparator();

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, 32, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** Tiny segment-entries ⇒ many segments, and a pinned fan-in (= 8: pin directly rather than
     * relying on the changed merge-budget/row-group denominator): a real multi-pass cascade, so the
     * merge that the relocated resume re-runs is non-trivial. */
    private static SortConfig cascade() {
        return SortConfigs.base()
                .withSegmentBytes(Long.MAX_VALUE)
                .withSegmentEntries(PAGE_SIZE)
                .withFanIn(8)
                .withMergeBudgetBytes(8L << 20);
    }

    @Test
    @Timeout(180)
    void relocatedWorkdirMergePendingResume_matchesCleanBaseline_zeroListFetches(@TempDir Path tmp)
            throws Exception {
        List<byte[]> keyspace = Keyspaces.exactly(OBJECTS);

        // ---- (1) clean un-killed baseline: build + merge in place, to completion. ----
        Path baseRoot = Files.createDirectories(tmp.resolve("baseline/out"));
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(tmp.resolve("baseline.sqlite"),
                RunContext.create().metrics())) {
            long runId = buildMergePending(store, baseRoot, keyspace);
            new ListRunner().runSortMergeOnly(RunContext.create(), baseRoot,
                    baseRoot.resolve("_staging"), store, runId, cascade(), SortMode.OBJECTS, spec());
        }

        // ---- (2) relocation SOURCE: build to the merge-pending state, then CLOSE the store so the
        //          SQLite WAL is checkpointed and the on-disk DB is a clean, copyable snapshot. ----
        Path srcRoot = Files.createDirectories(tmp.resolve("workdir-a/out"));
        Path srcDb = tmp.resolve("workdir-a/c.sqlite");
        RunMeta original;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(srcDb, RunContext.create().metrics())) {
            long runId = buildMergePending(store, srcRoot, keyspace);
            original = store.openRun(sortKey(), true, false);   // snapshot id/args_hash pre-relocation
            assertThat(original.id()).isEqualTo(runId);
        }

        // ---- (3) relocate the ENTIRE workdir (checkpoint + staging) to a DIFFERENT absolute path. ----
        Path dstRoot = Files.createDirectories(tmp.resolve("relocated-elsewhere/out"));
        Path dstDb = tmp.resolve("relocated-elsewhere/c.sqlite");
        copyTree(srcRoot, dstRoot);
        copyDb(srcDb, dstDb);

        // ---- (4) resume runSortMergeOnly from the NEW location. args_hash/run_id must still match. ----
        RunContext resumeCtx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dstDb, resumeCtx.metrics())) {
            RunMeta relocated = store.openRun(sortKey(), true, false);   // validates args_hash on the copy
            assertThat(relocated.resumed()).as("the relocated DB resumes an existing run").isTrue();
            assertThat(relocated.id()).as("run_id survives relocation").isEqualTo(original.id());
            assertThat(relocated.argsHash()).as("args_hash survives relocation").isEqualTo(original.argsHash());

            List<PartRef> segs = store.finalizedParts(relocated.id());
            assertThat(segs).as("the durable staging segments came across in the copy").isNotEmpty();

            new ListRunner().runSortMergeOnly(resumeCtx, dstRoot, dstRoot.resolve("_staging"),
                    store, relocated.id(), cascade(), SortMode.OBJECTS, spec());

            // Evidence: zero new LIST fetches. runSortMergeOnly takes no PageFetcher (structural),
            // and the production redo signal fired exactly once.
            assertThat(counter(resumeCtx, "swath.steal_reason", "merge_redone"))
                    .as("SORT.merge_redone fired once on the relocated resume (ZERO new LIST fetches — FALSIFIED)")
                    .isEqualTo(1.0);
            assertThat(store.sortPhase(relocated.id())).isEqualTo(SortPhase.PUBLISHED);
        }

        // ---- (5) identical-to-clean: same set + exactly-once + global sorted order vs the baseline. ----
        CrashResumeOracle.assertResumeMatchesBaseline(baseRoot, dstRoot, true);
        assertThat(Files.exists(dstRoot.resolve("_staging")))
                .as("staging cleaned up after the relocated republish").isFalse();
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    /** Build the merge-pending state: durable staging segments tracked in a real store + listing done. */
    private long buildMergePending(SqliteCheckpointStore store, Path outputDir, List<byte[]> keyspace)
            throws Exception {
        Path stagingDir = Files.createDirectories(outputDir.resolve("_staging"));
        RunMeta run = store.openRun(sortKey(), false, false);
        long nodeId = store.insertNode(NodeSpec.rootRange(run.id()));

        SegmentSink sink = result -> store.partFinalized(new PartFinalize(run.id(), 0,
                result.path().getFileName().toString(), ListRunner.SORT_SEGMENT_FORMAT,
                result.rows(), result.bytes(), result.perNodeMaxKeys().entrySet().stream()
                .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList()));
        SortLane lane = new SortLane(cascade(), cmp, DuplicateHook.NO_OP, SortMetrics.NO_OP,
                SortLaneMeters.NO_OP, stagingDir, "seg-" + run.id() + "-x", sink);
        for (List<ListEntry> page : pages(keyspace, PAGE_SIZE)) {
            lane.admit(nodeId, page);
        }
        lane.close();

        // Drive the listing side to the real "nothing left to list" merge-pending state.
        store.commitPage(new PageCommit(nodeId, null, true));
        store.markOutputComplete(run.id());
        assertThat(store.loadResumable(run.id(), true))
                .as("listing complete: the merge-pending resume state").isEmpty();
        assertThat(Files.exists(DatasetLayout.of(outputDir).manifest())).as("nothing published yet").isFalse();
        return run.id();
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

    private static void copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> walk = Files.walk(from)) {
            for (Path src : (Iterable<Path>) walk::iterator) {
                Path dst = to.resolve(from.relativize(src).toString());
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dst);
                } else {
                    Files.createDirectories(dst.getParent());
                    Files.copy(src, dst);
                }
            }
        }
    }

    /** Copy the SQLite DB plus any residual WAL/SHM sidecars (belt-and-suspenders after a clean close). */
    private static void copyDb(Path from, Path to) throws IOException {
        Files.createDirectories(to.getParent());
        Files.copy(from, to);
        for (String sidecar : new String[] {"-wal", "-shm"}) {
            Path s = from.resolveSibling(from.getFileName() + sidecar);
            if (Files.exists(s)) {
                Files.copy(s, to.resolveSibling(to.getFileName() + sidecar));
            }
        }
    }

    private static double counter(RunContext ctx, String name, String reason) {
        Counter c = ctx.metrics().registry().find(name).tag("outcome", "SORT").tag("reason", reason).counter();
        return c == null ? 0.0 : c.count();
    }
}
