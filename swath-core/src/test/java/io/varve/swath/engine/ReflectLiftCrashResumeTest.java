/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.ForwardingCheckpointStore;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RecordingSplitStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * RES-3 family: {@link F4OwnerSplitCrashResumeTest} covers a crash mid owner-split for an ORDINARY
 * (interpolated/reflect-clamped) pivot; this test reuses the exact same harness/idiom for a pivot that
 * was specifically REFLECT-LIFTED ({@code OWNER_SPLIT.pivot_reflect_lifted}) — the new boundary
 * geometry the lift introduces. Crashes right after a lifted split's durable commit, before the child
 * is ever drained ({@code AFTER_COMMIT_BEFORE_CHILD_LISTING}, the same sub-step already proved safe
 * for an ordinary pivot), and asserts:
 * <ul>
 *   <li>the durable range set tiles {@code (⊥, ⊤]} with no gap/no overlap AT THE LIFTED PIVOT
 *       BOUNDARY specifically;</li>
 *   <li>the resumed run is BYTE-EXACT against a clean run (I4/I6, exactly once);</li>
 *   <li>the resumed run's {@code OWNER_SPLIT_CHILD.{confetti,substantial}} counters do not
 *       double-count the crashed run's split child (NB: this crash seam throws from {@code
 *       splitNode} BEFORE the tag-add even runs, so the child was never tagged) — {@code ownerSplitTaggedChildren}/{@code
 *       WorkerState#hasSplit} are process-local (never persisted, see their javadoc), so the
 *       reopened child completes UNTAGGED in the resumed process and contributes zero
 *       classifications of its own; the resumed run's total classified children equals exactly its
 *       OWN {@code self_published} count, never more.</li>
 * </ul>
 *
 * <p>A single worker over a tight bounded range (no thief exists) guarantees every split here is an
 * owner self-split, exactly like the dedicated crash/resume fixture.
 */
final class ReflectLiftCrashResumeTest {

    private static final int OBJECTS = 40_000;
    private static final int MAX_KEYS = 100;
    private static final String ARGS_HASH = "reflect-lift-crash-hash";

    /** The exact shape/bound proven (deterministically, single worker) to produce a reflect-lift. */
    private static final byte[] LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HI = "d/05".getBytes(StandardCharsets.UTF_8);

    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static NodeSpec boundedSeed(long runId) {
        return new NodeSpec(runId, null, NodeKind.RANGE, LO, HI, LO, null);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(
                new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    @Test
    @Timeout(120)
    void crashAfterALiftedSplitCommitsBeforeChildListing_ownerAndChildResumeByteExactNoDoubleCount(
            @TempDir Path tmp) throws Exception {
        Result r = runLiftedSplitCrashThenResume(tmp);

        // This shape (T=1, dense/uniform, deterministic) recursively self-splits several times
        // BEFORE its first reflect-lift fires (verified: 11 ordinary owner-splits precede it on this
        // fixture) -- the crash store lets every prior split commit normally and crashes only on the
        // FIRST one it identifies as lifted, so `preResumeSplits()` holds that whole prior cascade
        // plus the crash-triggering lift as its LAST entry (RecordingSplitStore appends in commit
        // order, and the wrapper's delegate call — which durably commits — always runs before it
        // decides whether to throw).
        assertThat(r.preResumeSplits()).as("at least the crash-triggering split committed").isNotEmpty();
        RangePartition.Split liftedSplit = r.preResumeSplits().getLast();

        // The crash-triggering split's pivot is strictly inside its victim's (lo, oldHi] — the SAME
        // strict semantics RangePartition.replay enforces (item 3) — cross-referenced against the
        // resumed owner seed's own reloaded rangeStart (its lo never changes across splits).
        Node liftedOwner = r.preResumeSeeds().stream()
                .filter(n -> n.id() == liftedSplit.victimId())
                .findFirst()
                .orElseThrow(() -> new AssertionError("the lifted split's victim must still be live post-crash"));
        assertThat(Arrays.compareUnsigned(liftedOwner.rangeStart(), liftedSplit.pivot()))
                .as("lifted pivot strictly above the victim's lo").isLessThan(0);
        assertThat(Arrays.compareUnsigned(liftedSplit.pivot(), liftedSplit.oldHi()))
                .as("lifted pivot strictly below the victim's oldHi (never == hi — item 3 strict semantics)")
                .isLessThan(0);

        // Both durable nodes at the LIFTED boundary reload correctly: owner narrowed to (lo, m], child
        // (m, oldHi] with its cursor still on its lower bound (never drained pre-crash).
        assertThat(liftedOwner.rangeEnd())
                .as("owner resumes narrowed to the committed LIFTED pivot m")
                .isEqualTo(liftedSplit.pivot());
        assertThat(r.preResumeSeeds()).anySatisfy(child -> {
            assertThat(child.id()).isEqualTo(liftedSplit.childId());
            assertThat(Arrays.equals(child.rangeStart(), liftedSplit.pivot()))
                    .as("child starts at the committed LIFTED pivot m").isTrue();
            assertThat(Arrays.equals(child.rangeEnd(), liftedSplit.oldHi()))
                    .as("child owns the lifted split's oldHi").isTrue();
            assertThat(Arrays.equals(child.cursor(), liftedSplit.pivot()))
                    .as("child has not listed past its durable lower cursor before resume (never drained)").isTrue();
        });

        // The FULL durable range set — every split in the cascade, including the lift — tiles
        // (⊥, ⊤] with no gap / no overlap anywhere, in particular at the lifted pivot boundary.
        // RangePartition.replay/assertTilesFromSplits hardcode an UNBOUNDED root (⊥, ⊤]; this seed is
        // a BOUNDED (LO, HI] range (like the dedicated crash/resume fixture), so replay it locally the same way (same
        // strict pivot < oldHi semantics, item 3) seeded at (LO, HI], plus the two outer sentinels.
        assertBoundedCascadeTiles(LO, HI, r.seedId(), r.preResumeSplits());

        assertExactlyOnce(r.resumedUnion(), r.cleanUnion());
        assertExactlyOnce(r.cleanUnion(), expectedKeys(r.keyspace()));

        // Resume semantics (process-local tagging): the reopened child completes
        // UNTAGGED in the resumed process (ownerSplitTaggedChildren/hasSplit are never persisted), so
        // it contributes ZERO classifications of its own -- the resumed run's total classified
        // children must equal exactly its OWN self_published count, never more (no stray
        // classification of the crashed run's child; NB this seam crashes in splitNode before the
        // tag-add even runs, so that child was never actually tagged pre-crash either).
        Map<String, Long> resumed = r.resumedStealReasons();
        long resumedSelfPublished = resumed.getOrDefault("OWNER_SPLIT.self_published", 0L);
        long resumedClassified = resumed.getOrDefault("OWNER_SPLIT_CHILD.confetti", 0L)
                + resumed.getOrDefault("OWNER_SPLIT_CHILD.substantial", 0L);
        assertThat(resumedClassified)
                .as("resumed classified children (%d) must equal the resumed run's own self_published "
                        + "count (%d) -- no stray classification of the crashed run's (untagged) child",
                        resumedClassified, resumedSelfPublished)
                .isEqualTo(resumedSelfPublished);
    }

    // -------------------------------------------------------------------------

    private record Result(List<String> resumedUnion, List<String> cleanUnion, List<byte[]> keyspace,
                          List<Node> preResumeSeeds, List<RangePartition.Split> preResumeSplits, long seedId,
                          Map<String, Long> resumedStealReasons) {
    }

    private static Result runLiftedSplitCrashThenResume(Path tmp) throws Exception {
        List<byte[]> keyspace = denseFlat(OBJECTS);
        Path resumedDir = tmp.resolve("resumed");
        Path cleanDir = tmp.resolve("clean");
        Path db = tmp.resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);
        Files.createDirectories(db.getParent());

        List<Node> preResumeSeeds;
        List<RangePartition.Split> preResumeSplits;
        List<String> resumedUnion;
        long seedId;
        Map<String, Long> resumedStealReasons;

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RecordingSplitStore recorder = new RecordingSplitStore(sqlite);
            RunContext crashCtx = RunContext.create();
            LiftedSplitCrashStore crashingStore = new LiftedSplitCrashStore(recorder, crashCtx);

            RunMeta run = recorder.openRun(runKey(), false, false);
            seedId = recorder.insertNode(boundedSeed(run.id()));
            List<Node> initialSeeds = recorder.loadResumable(run.id(), true);
            assertThat(initialSeeds).as("one bounded owner range seeds the crash run").hasSize(1);

            // ONE worker: no thief exists, so every split that commits is an owner self-split; this
            // exact shape/bound/maxKeys deterministically produces a reflect-lift with a lone worker
            // (verified: 8 lifts across 20 owner-splits on this fixture before any crash is injected).
            MockPageFetcher crashingFetcher = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .build();
            assertThatThrownBy(() -> new ListRunner().runToParquetWorkStealing(
                    crashCtx, crashingFetcher, resumedDir, spec(), crashingStore, run.id(),
                    1, initialSeeds, List.of()))
                    .isInstanceOf(CheckpointException.class)
                    .hasMessageContaining("reflect-lift crash");
            assertThat(crashingStore.fired()).as("the lifted-split crash seam fired").isTrue();

            RunMeta resumed = recorder.openRun(runKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<PartInfo> existing = reconcileParquetResume(recorder, resumed.id(), resumedDir);
            preResumeSeeds = recorder.loadResumable(resumed.id(), true);
            preResumeSplits = recorder.splits();

            MockPageFetcher cleanFetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            RunContext resumeCtx = RunContext.create();
            new ListRunner().runToParquetWorkStealing(
                    resumeCtx, cleanFetcher, resumedDir, spec(), recorder, resumed.id(),
                    1, preResumeSeeds, existing);
            assertThat(recorder.loadResumable(resumed.id(), true))
                    .as("resumed run is now output-complete").isEmpty();
            resumedUnion = allPartKeys(resumedDir);
            resumedStealReasons = resumeCtx.metrics().diagnostics(Duration.ZERO).stealReasons();
        }

        List<String> cleanUnion = runCleanBoundedWorkStealing(
                tmp.resolve("clean.sqlite"), cleanDir, keyspace);
        return new Result(resumedUnion, cleanUnion, keyspace, preResumeSeeds, preResumeSplits, seedId,
                resumedStealReasons);
    }

    private static List<String> runCleanBoundedWorkStealing(Path db, Path dir, List<byte[]> keyspace)
            throws Exception {
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RunMeta run = sqlite.openRun(runKey(), false, false);
            sqlite.insertNode(boundedSeed(run.id()));
            List<Node> seeds = sqlite.loadResumable(run.id(), true);
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec(), sqlite, run.id(), 4, seeds, List.of());
            assertThat(sqlite.loadResumable(run.id(), true)).as("clean run is output-complete").isEmpty();
        }
        return allPartKeys(dir);
    }

    /**
     * {@link RangePartition#replay}/{@code assertTilesFromSplits} hardcode an UNBOUNDED root {@code
     * (⊥, ⊤]}; this fixture's seed is a BOUNDED {@code (lo, hi]} range instead (mirroring the dedicated
     * crash/resume fixture), so replay the same way LOCALLY — same strict {@code pivot < oldHi} semantics (item
     * 3) — seeded at {@code (lo, hi]}, then feed the result plus the two outer sentinel intervals
     * ({@code (⊥, lo]}, {@code (hi, ⊤]}) into the shared {@link RangePartition#assertTiles}.
     */
    private static void assertBoundedCascadeTiles(byte[] lo, byte[] hi, long rootId,
                                                  List<RangePartition.Split> splits) {
        Map<Long, RangePartition.Interval> live = new HashMap<>();
        live.put(rootId, new RangePartition.Interval(lo, hi));
        for (RangePartition.Split s : splits) {
            RangePartition.Interval v = live.get(s.victimId());
            assertThat(v).as("split victim %d must be a live range", s.victimId()).isNotNull();
            assertThat(Arrays.equals(v.hi(), s.oldHi()))
                    .as("split oldHi must equal the victim's current hi").isTrue();
            if (v.lo() != null) {
                assertThat(Arrays.compareUnsigned(v.lo(), s.pivot())).as("lo < pivot").isLessThan(0);
            }
            if (s.oldHi() != null) {
                assertThat(Arrays.compareUnsigned(s.pivot(), s.oldHi()))
                        .as("pivot < oldHi (strict — item 3 semantics)").isLessThan(0);
            }
            live.put(s.victimId(), new RangePartition.Interval(v.lo(), s.pivot()));
            live.put(s.childId(), new RangePartition.Interval(s.pivot(), s.oldHi()));
        }
        List<RangePartition.Interval> intervals = new ArrayList<>(live.values());
        intervals.add(new RangePartition.Interval(null, lo));
        intervals.add(new RangePartition.Interval(hi, null));
        RangePartition.assertTiles(intervals);
    }

    /**
     * Test-only crash seam: crashes at the {@code splitNode} commit of the FIRST split whose pivot
     * was REFLECT-LIFTED (detected by snapshotting {@code OWNER_SPLIT.pivot_reflect_lifted} off the
     * SAME {@link RunContext#metrics()} the engine records into — {@code maybeOwnerSelfSplit} records
     * that mark synchronously, in the same call stack, strictly before invoking {@code
     * store.splitNode(...)}; a single worker makes this ordering race-free). Throws AFTER the delegate
     * commits (the (b)-to-(c) sub-step already proved safe for an ordinary pivot) so the child is
     * never drained pre-crash.
     */
    private static final class LiftedSplitCrashStore extends ForwardingCheckpointStore {
        private final RunContext ctx;
        private final AtomicBoolean fired = new AtomicBoolean();
        private final AtomicLong lastLiftCount = new AtomicLong();

        LiftedSplitCrashStore(CheckpointStore delegate, RunContext ctx) {
            super(delegate);
            this.ctx = ctx;
        }

        @Override
        public long splitNode(SplitSpec s) throws CheckpointException {
            long liftCountNow = ctx.metrics().diagnostics(Duration.ZERO).stealReasons()
                    .getOrDefault("OWNER_SPLIT.pivot_reflect_lifted", 0L);
            boolean thisSplitIsLifted = liftCountNow > lastLiftCount.get();
            lastLiftCount.set(liftCountNow);
            long childId = delegate.splitNode(s);
            if (thisSplitIsLifted
                    && childId != CheckpointStore.SPLIT_ABORTED
                    && fired.compareAndSet(false, true)) {
                throw new CheckpointException("reflect-lift crash after lifted splitTxn commit before child listing");
            }
            return childId;
        }

        boolean fired() {
            return fired.get();
        }
    }

    private static List<PartInfo> reconcileParquetResume(RecordingSplitStore store, long runId, Path dir)
            throws Exception {
        List<PartRef> finalized = store.finalizedParts(runId);
        Set<String> finalizedNames = finalized.stream().map(PartRef::path).collect(Collectors.toSet());
        ParquetResume.discardNonFinalized(dir, finalizedNames);
        return finalized.stream()
                .map(p -> new PartInfo(p.path(), p.writerId(), p.rows(), p.bytes(), ""))
                .toList();
    }

    private static List<String> allPartKeys(Path dir) throws IOException {
        List<String> keys = new ArrayList<>();
        for (Path part : DatasetLayout.of(dir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }

    private static List<String> expectedKeys(List<byte[]> keyspace) {
        return keyspace.stream().map(k -> new String(k, StandardCharsets.UTF_8)).toList();
    }

    /** Exactly-once acceptance: no duplicate rows AND the same key set (count == count DISTINCT). */
    private static void assertExactlyOnce(List<String> actual, List<String> expected) {
        assertThat(actual).as("no duplicate rows (count == count DISTINCT)").doesNotHaveDuplicates();
        assertThat(expected).as("expected side has no duplicate rows").doesNotHaveDuplicates();
        assertThat(actual.stream().sorted().toList())
                .as("same key set under the unsorted Parquet part contract")
                .containsExactlyElementsOf(expected.stream().sorted().toList());
    }
}
