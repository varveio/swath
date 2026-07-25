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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The explicit crash-resume test for owner-self-split:
 * <b>crash DURING an owner-split sub-step, resume byte-exact.</b>
 *
 * <p>The owner self-split transaction ({@link OwnerSelfSplit#maybeOwnerSelfSplit}) is three steps:
 * <ol>
 *   <li>(a) {@code ws.narrowHi(m)} — narrow the owner's upper bound to the pivot <i>in memory only</i>;</li>
 *   <li>(b) {@code store.splitNode(...)} — the durable CAS-guarded split (the child row + the owner's
 *       {@code range_end := m}, committed atomically);</li>
 *   <li>(c) {@code enqueueChild(...)} + {@code ws.markStolen()} — hand the child to the ready queue.</li>
 * </ol>
 * The single-writer-FIFO argument for safety is: a split can only commit <i>after</i> its own preceding
 * page cursor-commit is already durable, and it commits atomically, so at every crash boundary the durable
 * range set still tiles. This test makes that argument concrete by deterministically crashing at each
 * boundary and proving the resumed Parquet union equals a clean run <b>exactly once</b> (I4/I6):
 * <ul>
 *   <li><b>BEFORE_COMMIT</b> — crash between (a) and (b): the in-memory narrow happened but the durable
 *       split did NOT commit, so on resume the owner still owns the FULL {@code (LO, HI]} (the child never
 *       existed durably — <b>no gap</b>).</li>
 *   <li><b>AFTER_COMMIT</b> — crash after (b) but before the child is ever drained: on resume the owner
 *       {@code (LO, m]} and the child {@code (m, HI]} both reload from their durable cursors and tile with
 *       <b>no double-count at m</b>.</li>
 * </ul>
 *
 * <p>The crashing split is <b>guaranteed to be an owner self-split</b> because the crash run uses a
 * <b>single worker</b> over a tight bounded range {@code (LO, HI]} (no other worker exists to steal, and
 * {@code maybeOwnerSelfSplit} only fires when {@code hi != null}). This is exactly the fixture
 * {@code OwnerSelfSplitContractTest#ownerSelfSplitAbortPathIsByteExact} uses to force the owner path. The
 * {@code SPLIT_ABORTED} branch of the same transaction is already covered by that test; this test
 * adds the crash-mid-split cases it does not have. It touches no engine/product code.
 */
final class F4OwnerSplitCrashResumeTest {

    private static final int OBJECTS = 8_000;
    private static final int MAX_KEYS = 32;
    private static final String ARGS_HASH = "f4-owner-split-hash";

    /** A tight bound snug around the dense flat cluster (every key strictly inside {@code (LO, HI]}). */
    private static final byte[] LO = "d/00".getBytes(StandardCharsets.UTF_8);
    private static final byte[] HI = "d/05".getBytes(StandardCharsets.UTF_8);

    private enum OwnerSplitCrashPoint {
        /** (a)→(b): {@code narrowHi(m)} ran in memory, the durable {@code splitNode} did NOT commit. */
        AFTER_HI_NARROW_BEFORE_COMMIT,
        /** (b)→(c): the durable {@code splitNode} committed, the child was NOT yet drained. */
        AFTER_COMMIT_BEFORE_CHILD_LISTING
    }

    /** A dense flat directory {@code d/000000..} of {@code n} uniform keys, all inside {@code (LO, HI]}. */
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
        // A bounded range whose cursor begins at its lower bound (exactly like a split child / SeedStep tile):
        // hi != null is what makes the lone owner eligible to self-split (the open frontier never does).
        return new NodeSpec(runId, null, NodeKind.RANGE, LO, HI, LO, null);
    }

    private static ListRunner.ParquetSpec spec() {
        // Small target bytes so parts rotate/finalize across the run, exercising the resume discard/carry path.
        return new ListRunner.ParquetSpec(
                new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    @Test
    @Timeout(120)
    void crashAfterHiNarrowBeforeOwnerSplitCommit_ownerResumesFullRange(@TempDir Path tmp) throws Exception {
        Result r = runOwnerSplitCrashThenResume(tmp, OwnerSplitCrashPoint.AFTER_HI_NARROW_BEFORE_COMMIT);

        // (a) happened, (b) did not: no durable split, so the owner still owns the whole (LO, HI].
        assertThat(r.preResumeSplits())
                .as("splitTxn never committed, so the durable range set is still the single owner range")
                .isEmpty();
        assertThat(r.preResumeSeeds()).singleElement().satisfies(owner -> {
            assertThat(owner.id()).isEqualTo(r.seedId());
            assertThat(Arrays.equals(owner.rangeStart(), LO)).as("owner keeps its lower bound").isTrue();
            assertThat(Arrays.equals(owner.rangeEnd(), HI))
                    .as("owner still owns the FULL upper bound HI — the child never existed durably (no gap)")
                    .isTrue();
        });

        assertExactlyOnce(r.resumedUnion(), r.cleanUnion());
        assertExactlyOnce(r.cleanUnion(), expectedKeys(r.keyspace()));
    }

    @Test
    @Timeout(120)
    void crashAfterOwnerSplitCommitBeforeChildListing_ownerAndChildResume(@TempDir Path tmp) throws Exception {
        Result r = runOwnerSplitCrashThenResume(tmp, OwnerSplitCrashPoint.AFTER_COMMIT_BEFORE_CHILD_LISTING);

        // (b) committed exactly once (a lone-worker split can only be an owner self-split).
        assertThat(r.preResumeSplits())
                .as("the crash fires only after the owner's splitTxn has committed").hasSize(1);
        RangePartition.Split split = r.preResumeSplits().getFirst();
        assertThat(split.victimId()).as("the owner self-split its own node").isEqualTo(r.seedId());
        assertThat(Arrays.equals(split.oldHi(), HI)).as("the split narrowed the owner's old upper bound HI").isTrue();
        assertThat(KeyBytesBetween(LO, split.pivot(), HI))
                .as("the owner pivot m is strictly inside (LO, HI]").isTrue();

        // Both durable nodes reload: owner (LO, m] and child (m, HI], cursors on their bounds.
        assertThat(r.preResumeSeeds()).hasSize(2);
        assertThat(r.preResumeSeeds()).anySatisfy(owner -> {
            assertThat(owner.id()).isEqualTo(split.victimId());
            assertThat(Arrays.equals(owner.rangeStart(), LO)).isTrue();
            assertThat(Arrays.equals(owner.rangeEnd(), split.pivot()))
                    .as("owner resumes narrowed to the committed pivot m").isTrue();
        });
        assertThat(r.preResumeSeeds()).anySatisfy(child -> {
            assertThat(child.id()).isEqualTo(split.childId());
            assertThat(Arrays.equals(child.rangeStart(), split.pivot()))
                    .as("child starts at the committed pivot m").isTrue();
            assertThat(Arrays.equals(child.rangeEnd(), HI)).as("child owns the owner's old upper bound HI").isTrue();
            assertThat(Arrays.equals(child.cursor(), split.pivot()))
                    .as("child has not listed past its durable lower cursor before resume (never drained)").isTrue();
        });

        // The durable range set tiles (⊥, ⊤] with no gap / no overlap at the m boundary.
        RangePartition.assertTiles(List.of(
                new RangePartition.Interval(null, LO),
                new RangePartition.Interval(LO, split.pivot()),
                new RangePartition.Interval(split.pivot(), HI),
                new RangePartition.Interval(HI, null)));

        assertExactlyOnce(r.resumedUnion(), r.cleanUnion());
        assertExactlyOnce(r.cleanUnion(), expectedKeys(r.keyspace()));
    }

    // -------------------------------------------------------------------------

    private record Result(List<String> resumedUnion, List<String> cleanUnion, List<byte[]> keyspace,
                          List<Node> preResumeSeeds, List<RangePartition.Split> preResumeSplits, long seedId) {
    }

    private static Result runOwnerSplitCrashThenResume(Path tmp, OwnerSplitCrashPoint crashPoint) throws Exception {
        List<byte[]> keyspace = denseFlat(OBJECTS);
        String tag = crashPoint.name().toLowerCase();
        Path resumedDir = tmp.resolve(tag).resolve("resumed");
        Path cleanDir = tmp.resolve(tag).resolve("clean");
        Path db = tmp.resolve(tag).resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);
        Files.createDirectories(db.getParent());

        List<Node> preResumeSeeds;
        List<RangePartition.Split> preResumeSplits;
        List<String> resumedUnion;
        long seedId;

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RecordingSplitStore recorder = new RecordingSplitStore(sqlite);
            OwnerSplitCrashStore crashingStore = new OwnerSplitCrashStore(recorder, crashPoint);

            RunMeta run = recorder.openRun(runKey(), false, false);
            seedId = recorder.insertNode(boundedSeed(run.id()));
            List<Node> initialSeeds = recorder.loadResumable(run.id(), true);
            assertThat(initialSeeds).as("one bounded owner range seeds the crash run").hasSize(1);

            // ONE worker ⇒ no thief exists, so the only split that can commit is the owner self-split.
            MockPageFetcher crashingFetcher = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(MAX_KEYS)
                    .pageDelay(Duration.ofMillis(2))
                    .build();
            assertThatThrownBy(() -> new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), crashingFetcher, resumedDir, spec(), crashingStore, run.id(),
                    1, initialSeeds, List.of()))
                    .isInstanceOf(CheckpointException.class)
                    .hasMessageContaining("owner-split crash");
            assertThat(crashingStore.fired()).as("the owner-split crash seam fired").isTrue();

            RunMeta resumed = recorder.openRun(runKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<PartInfo> existing = reconcileParquetResume(recorder, resumed.id(), resumedDir);
            preResumeSeeds = recorder.loadResumable(resumed.id(), true);
            preResumeSplits = recorder.splits();

            MockPageFetcher cleanFetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), cleanFetcher, resumedDir, spec(), recorder, resumed.id(),
                    1, preResumeSeeds, existing);
            assertThat(recorder.loadResumable(resumed.id(), true))
                    .as("resumed run is now output-complete").isEmpty();
            resumedUnion = allPartKeys(resumedDir);
        }

        List<String> cleanUnion = runCleanBoundedWorkStealing(
                tmp.resolve(tag).resolve("clean.sqlite"), cleanDir, keyspace);
        return new Result(resumedUnion, cleanUnion, keyspace, preResumeSeeds, preResumeSplits, seedId);
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
     * STRICT {@code lo < m < hi} — {@code m == hi} would make the child's
     * {@code (m, hi]} range structurally empty (a zero-width interval), which {@link
     * RangePartition#replay}'s own {@code pivot < oldHi} check already treats as a hard failure
     * elsewhere. Do not relax this to {@code m <= hi}: a structurally-empty-child split could then
     * pass here even though it can never actually happen (the CAS guard's {@code cursor < pivot}
     * clause makes {@code m == hi} unreachable by construction), silently agreeing with a
     * regression that made it reachable.
     */
    private static boolean KeyBytesBetween(byte[] lo, byte[] m, byte[] hi) {
        return Arrays.compareUnsigned(lo, m) < 0 && Arrays.compareUnsigned(m, hi) < 0;
    }

    /**
     * Test-only owner-split crash seam. The owner-split transaction calls {@code splitNode} after
     * {@code ws.narrowHi(m)} and before {@code enqueueChild(...)}, so throwing before or after the
     * delegate maps directly onto the (a)→(b) and (b)→(c) sub-steps without touching production code.
     */
    private static final class OwnerSplitCrashStore extends ForwardingCheckpointStore {
        private final OwnerSplitCrashPoint crashPoint;
        private final AtomicBoolean fired = new AtomicBoolean();

        OwnerSplitCrashStore(CheckpointStore delegate, OwnerSplitCrashPoint crashPoint) {
            super(delegate);
            this.crashPoint = crashPoint;
        }

        @Override
        public long splitNode(SplitSpec s) throws CheckpointException {
            if (crashPoint == OwnerSplitCrashPoint.AFTER_HI_NARROW_BEFORE_COMMIT
                    && fired.compareAndSet(false, true)) {
                throw new CheckpointException("owner-split crash after hi narrow before splitTxn commit");
            }
            long childId = delegate.splitNode(s);
            if (childId != CheckpointStore.SPLIT_ABORTED
                    && crashPoint == OwnerSplitCrashPoint.AFTER_COMMIT_BEFORE_CHILD_LISTING
                    && fired.compareAndSet(false, true)) {
                throw new CheckpointException("owner-split crash after splitTxn commit before child listing");
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
