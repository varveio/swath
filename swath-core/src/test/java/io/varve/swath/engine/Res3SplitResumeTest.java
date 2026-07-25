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
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SplitSpec;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.ListingException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.ForwardingCheckpointStore;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RecordingSplitStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * RES-3 — kill during a split-heavy work-stealing run, then resume the Parquet sink
 * through the production resume spine. The guard is intentionally end-to-end: the
 * first run uses {@link ListRunner#runToParquetWorkStealing}, reaches at least one
 * durable split and one finalized Parquet part ({@code durable_cursor} advanced
 * past a node's split pivot / range start),
 * then a deterministic {@link MockPageFetcher.PageInterceptor} throws. Resume uses
 * the same {@link RunKey}, {@code output_format=parquet}, {@code openRun(..., true)},
 * {@link SqliteCheckpointStore#loadResumable(long, boolean)} with {@code fileSink=true},
 * carried-over finalized parts, and {@link ParquetResume#discardNonFinalized}.
 *
 * <p>The assertion is the RES-3/I6 acceptance check: the byte-sorted union of every
 * finalized pre-crash part plus every resumed part is exactly equal to a clean
 * non-resumed Parquet work-stealing run over the same keyspace, with no duplicates
 * and no gaps.
 */
final class Res3SplitResumeTest {

    private static final int OBJECTS = 2_048;
    private static final int WORKERS = 4;
    private static final int MAX_KEYS = 32;
    private static final int KILL_AFTER_CALLS = 16;
    private static final Duration FINALIZE_WAIT = Duration.ofSeconds(3);
    private static final String ARGS_HASH = "res3-parquet-hash";

    private enum SplitCrashPoint {
        AFTER_HI_NARROW_BEFORE_COMMIT,
        AFTER_COMMIT_BEFORE_CHILD_LISTING
    }

    private record CrashResumeResult(
            List<String> resumedUnion,
            List<String> cleanUnion,
            List<byte[]> keyspace,
            List<Node> preResumeSeeds,
            List<RangePartition.Split> preResumeSplits,
            long rootId) {
    }

    private static RunKey runKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(
                new byte[0], 256, MAX_KEYS, FilterChain.EMPTY, 2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    @Test
    @Timeout(60)
    void res3_crashAfterHiNarrowBeforeSplitCommit_resumesVictimFullRange(@TempDir Path tmp)
            throws Exception {
        CrashResumeResult r = runSplitCrashThenResume(tmp, SplitCrashPoint.AFTER_HI_NARROW_BEFORE_COMMIT);

        assertThat(r.preResumeSplits())
                .as("splitTxn never committed, so the durable range set is still the root range")
                .isEmpty();
        assertThat(r.preResumeSeeds()).singleElement().satisfies(root -> {
            assertThat(root.id()).isEqualTo(r.rootId());
            assertThat(root.rangeStart()).as("root still starts at bottom").isNull();
            assertThat(root.rangeEnd()).as("root still owns the open frontier").isNull();
        });

        assertExactlyEqualUnderSortContract(r.resumedUnion(), r.cleanUnion());
        assertExactlyEqualUnderSortContract(r.cleanUnion(), expectedKeys(r.keyspace()));
    }

    @Test
    @Timeout(60)
    void res3_crashAfterSplitCommitBeforeChildListing_resumesVictimAndChild(@TempDir Path tmp)
            throws Exception {
        CrashResumeResult r = runSplitCrashThenResume(tmp, SplitCrashPoint.AFTER_COMMIT_BEFORE_CHILD_LISTING);

        assertThat(r.preResumeSplits())
                .as("the crash fires only after the first splitTxn has committed")
                .hasSize(1);
        RangePartition.assertTilesFromSplits(r.rootId(), r.preResumeSplits());
        RangePartition.Split split = r.preResumeSplits().getFirst();
        assertThat(r.preResumeSeeds()).hasSize(2);
        assertThat(r.preResumeSeeds()).anySatisfy(victim -> {
            assertThat(victim.id()).isEqualTo(split.victimId());
            assertThat(victim.rangeStart()).isNull();
            assertThat(Arrays.equals(victim.rangeEnd(), split.pivot()))
                    .as("victim resumes narrowed to the committed pivot").isTrue();
        });
        assertThat(r.preResumeSeeds()).anySatisfy(child -> {
            assertThat(child.id()).isEqualTo(split.childId());
            assertThat(Arrays.equals(child.rangeStart(), split.pivot()))
                    .as("child starts at the committed pivot").isTrue();
            assertThat(Arrays.equals(child.rangeEnd(), split.oldHi()))
                    .as("child owns the victim's old upper bound").isTrue();
            assertThat(Arrays.equals(child.cursor(), split.pivot()))
                    .as("child has not listed past its durable lower cursor before resume").isTrue();
        });

        assertExactlyEqualUnderSortContract(r.resumedUnion(), r.cleanUnion());
        assertExactlyEqualUnderSortContract(r.cleanUnion(), expectedKeys(r.keyspace()));
    }

    @Test
    @Timeout(60)
    void res3_midSplitParquetResume_unionEqualsCleanRun(@TempDir Path tmp) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(OBJECTS);
        Path resumedDir = tmp.resolve("resumed");
        Path cleanDir = tmp.resolve("clean");
        Path db = tmp.resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);

        AtomicReference<RecordingSplitStore> runningStore = new AtomicReference<>();
        AtomicLong runningRunId = new AtomicLong(-1L);
        AtomicBoolean killed = new AtomicBoolean();

        MockPageFetcher crashing = MockPageFetcher.builder()
                .keys(keyspace)
                .maxKeysCap(MAX_KEYS)
                .interceptor((req, idx, page) -> {
                    RecordingSplitStore store = runningStore.get();
                    long runId = runningRunId.get();
                    boolean bulkWorkerPage = req.maxKeys() > 1;
                    if (bulkWorkerPage && idx >= KILL_AFTER_CALLS && store != null && runId > 0
                            && !store.splits().isEmpty()
                            && awaitFinalizedPart(store, runId, FINALIZE_WAIT)
                            && killed.compareAndSet(false, true)) {
                        throw new ListingException("injected RES-3 stop after durable split + finalized part");
                    }
                    return page;
                })
                .build();

        List<String> resumedUnion;
        List<String> cleanUnion;
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            runningStore.set(store);

            RunMeta run = store.openRun(runKey(), false, false);
            runningRunId.set(run.id());
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> initialSeeds = store.loadResumable(run.id(), true);
            assertThat(initialSeeds).hasSize(1);

            assertThatThrownBy(() -> new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), crashing, resumedDir, spec(), store, run.id(),
                    WORKERS, initialSeeds, List.of()))
                    .isInstanceOf(ListingException.class);

            assertThat(killed).as("the deterministic crash seam fired after a split and finalized part").isTrue();
            assertThat(store.splits()).as("the pre-crash run committed at least one split").isNotEmpty();
            List<PartRef> preCrashFinalized = store.finalizedParts(run.id());
            assertThat(preCrashFinalized).as("at least one Parquet part finalized before the crash").isNotEmpty();
            assertThat(preCrashFinalized.stream().mapToLong(PartRef::rows).sum())
                    .as("finalized pre-crash parts contain rows").isPositive();
            assertThat(nodesAdvancedPastRangeStart(db, run.id()))
                    .as("partFinalized advanced durable_cursor past a node's range_start before resume")
                    .isPositive();

            RunMeta resumed = store.openRun(runKey(), true, false);
            assertThat(resumed.resumed()).as("same RunKey resumed through openRun(..., true)").isTrue();
            assertThat(resumed.storedOutputFormat()).isEqualTo("parquet");

            List<PartInfo> existing = reconcileParquetResume(store, resumed.id(), resumedDir);
            assertThat(existing).as("finalized pre-crash parts are carried into the resumed manifest")
                    .hasSize(preCrashFinalized.size());
            List<Node> resumedSeeds = store.loadResumable(resumed.id(), true);
            assertThat(resumedSeeds).as("file-sink resume reopens the not-yet-durable tail").isNotEmpty();

            MockPageFetcher cleanFetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), cleanFetcher, resumedDir, spec(), store, resumed.id(),
                    WORKERS, resumedSeeds, existing);

            assertThat(store.loadResumable(resumed.id(), true))
                    .as("resumed run is now output-complete").isEmpty();
            resumedUnion = allPartKeys(resumedDir);
        }

        cleanUnion = runCleanParquetWorkStealing(tmp.resolve("clean.sqlite"), cleanDir, keyspace);

        assertExactlyEqualUnderSortContract(resumedUnion, cleanUnion);
        assertExactlyEqualUnderSortContract(cleanUnion, expectedKeys(keyspace));
    }

    private static CrashResumeResult runSplitCrashThenResume(Path tmp, SplitCrashPoint crashPoint)
            throws Exception {
        int seamMaxKeys = 8;
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(2_048);
        Path resumedDir = tmp.resolve(crashPoint.name().toLowerCase()).resolve("resumed");
        Path cleanDir = tmp.resolve(crashPoint.name().toLowerCase()).resolve("clean");
        Path db = tmp.resolve(crashPoint.name().toLowerCase()).resolve("c.sqlite");
        Files.createDirectories(resumedDir);
        Files.createDirectories(cleanDir);
        Files.createDirectories(db.getParent());

        List<Node> preResumeSeeds;
        List<RangePartition.Split> preResumeSplits;
        List<String> resumedUnion;
        long rootId;

        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RecordingSplitStore recorder = new RecordingSplitStore(sqlite);
            CrashingSplitStore crashingStore = new CrashingSplitStore(recorder, crashPoint);

            RunMeta run = recorder.openRun(runKey(), false, false);
            rootId = recorder.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> initialSeeds = recorder.loadResumable(run.id(), true);
            assertThat(initialSeeds).hasSize(1);

            MockPageFetcher crashingFetcher = MockPageFetcher.builder()
                    .keys(keyspace)
                    .maxKeysCap(seamMaxKeys)
                    .pageDelay(Duration.ofMillis(5))
                    .build();
            assertThatThrownBy(() -> new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), crashingFetcher, resumedDir, spec(), crashingStore, run.id(),
                    2, initialSeeds, List.of()))
                    .isInstanceOf(CheckpointException.class)
                    .hasMessageContaining("injected RES-3 split crash");
            assertThat(crashingStore.fired()).as("the split crash seam fired").isTrue();

            RunMeta resumed = recorder.openRun(runKey(), true, false);
            assertThat(resumed.resumed()).isTrue();
            List<PartInfo> existing = reconcileParquetResume(recorder, resumed.id(), resumedDir);
            preResumeSeeds = recorder.loadResumable(resumed.id(), true);
            preResumeSplits = recorder.splits();

            MockPageFetcher cleanFetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(seamMaxKeys).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), cleanFetcher, resumedDir, spec(), recorder, resumed.id(),
                    2, preResumeSeeds, existing);
            assertThat(recorder.loadResumable(resumed.id(), true))
                    .as("resumed run is now output-complete").isEmpty();
            resumedUnion = allPartKeys(resumedDir);
        }

        List<String> cleanUnion = runCleanParquetWorkStealing(
                tmp.resolve(crashPoint.name().toLowerCase()).resolve("clean.sqlite"), cleanDir, keyspace);
        return new CrashResumeResult(resumedUnion, cleanUnion, keyspace,
                preResumeSeeds, preResumeSplits, rootId);
    }

    private static List<String> runCleanParquetWorkStealing(Path db, Path dir, List<byte[]> keyspace)
            throws Exception {
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(db)) {
            RunMeta run = sqlite.openRun(runKey(), false, false);
            sqlite.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = sqlite.loadResumable(run.id(), true);
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToParquetWorkStealing(
                    RunContext.create(), fetcher, dir, spec(), sqlite, run.id(), WORKERS, seeds, List.of());
            assertThat(sqlite.loadResumable(run.id(), true))
                    .as("clean run is output-complete").isEmpty();
        }
        return allPartKeys(dir);
    }

    /**
     * Test-only RES-3 split seam. The thief calls {@code splitNode} only after
     * {@code victim.narrowHi(m)} and before {@code childSink.accept(...)} can enqueue
     * or list the child, so throwing before or after the delegate maps directly to
     * the two contract sub-steps without touching production code.
     */
    private static final class CrashingSplitStore extends ForwardingCheckpointStore {
        private final SplitCrashPoint crashPoint;
        private final AtomicBoolean fired = new AtomicBoolean();

        CrashingSplitStore(CheckpointStore delegate, SplitCrashPoint crashPoint) {
            super(delegate);
            this.crashPoint = crashPoint;
        }

        @Override
        public long splitNode(SplitSpec s) throws CheckpointException {
            if (crashPoint == SplitCrashPoint.AFTER_HI_NARROW_BEFORE_COMMIT
                    && fired.compareAndSet(false, true)) {
                throw new CheckpointException("injected RES-3 split crash after hi narrow before splitTxn commit");
            }

            long childId = delegate.splitNode(s);
            if (childId != CheckpointStore.SPLIT_ABORTED
                    && crashPoint == SplitCrashPoint.AFTER_COMMIT_BEFORE_CHILD_LISTING
                    && fired.compareAndSet(false, true)) {
                throw new CheckpointException("injected RES-3 split crash after splitTxn commit before child listing");
            }
            return childId;
        }

        boolean fired() {
            return fired.get();
        }
    }

    private static boolean awaitFinalizedPart(RecordingSplitStore store, long runId, Duration timeout)
            throws ListingException, InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            do {
                if (!store.finalizedParts(runId).isEmpty()) {
                    return true;
                }
                Thread.sleep(10);
            } while (System.nanoTime() < deadline);
            return !store.finalizedParts(runId).isEmpty();
        } catch (CheckpointException e) {
            throw new ListingException("failed while polling for finalized parts", e);
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
        return keyspace.stream()
                .map(k -> new String(k, StandardCharsets.UTF_8))
                .toList();
    }

    private static void assertExactlyEqualUnderSortContract(List<String> actual, List<String> expected) {
        assertThat(actual).as("no duplicate rows").doesNotHaveDuplicates();
        assertThat(expected).as("expected side has no duplicate rows").doesNotHaveDuplicates();
        assertThat(actual.stream().sorted().toList())
                .as("same key set under the unsorted Parquet part contract")
                .containsExactlyElementsOf(expected.stream().sorted().toList());
    }

    private static int nodesAdvancedPastRangeStart(Path db, long runId) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement(
                     "SELECT range_start, durable_cursor FROM listing_node "
                             + "WHERE run_id=? AND durable_cursor IS NOT NULL")) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    byte[] rangeStart = rs.getBytes("range_start");
                    byte[] durableCursor = rs.getBytes("durable_cursor");
                    byte[] lo = rangeStart == null ? new byte[0] : rangeStart;
                    if (durableCursor != null && Arrays.compareUnsigned(durableCursor, lo) > 0) {
                        count++;
                    }
                }
                return count;
            }
        }
    }
}
