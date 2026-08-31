/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import static io.varve.swath.checkpoint.CheckpointStoreTestSupport.b;
import static io.varve.swath.checkpoint.CheckpointStoreTestSupport.key;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.sort.PageRunFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the checkpoint spine (algorithms.md §4): commit-before-emit
 * cursor advance (I1/I5), the CAS-guarded split (I4), and the {@code durable_cursor}
 * two-cursor model (I6). These exercise the store contract directly; the end-to-end
 * RES-1/2/4 fault-injection tests drive it through the runner.
 */
final class SqliteCheckpointStoreTest {

    @Test
    void finalizedPartFormatMetadataRoundTripsWithoutClassifyingOrdinaryParquet(@TempDir Path dir)
            throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("format-metadata"), false, false);
            PageRunFormat pageRun = PageRunFormat.currentListing();
            store.partFinalized(new PartFinalize(run.id(), 0, "seg-0.pageseg", pageRun,
                    3, 100, List.of()));
            store.partFinalized(new PartFinalize(run.id(), 1, "data/part-0.parquet", "parquet",
                    5, 200, List.of()));

            assertThat(store.finalizedParts(run.id())).satisfiesExactly(
                    staged -> {
                        assertThat(staged.formatVersion()).isEqualTo(pageRun.formatVersion());
                        assertThat(staged.extensionType()).isEqualTo(pageRun.extensionType());
                    },
                    output -> {
                        assertThat(output.formatVersion()).isNull();
                        assertThat(output.extensionType()).isNull();
                    });
        }
    }

    @Test
    void ordinaryOutputCannotCarryPageRunFormatMetadata() {
        PageRunFormat pageRun = PageRunFormat.currentListing();

        assertThatThrownBy(() -> new PartFinalize(1, 0, "part-0.parquet", "parquet",
                pageRun.formatVersion(), pageRun.extensionType(), 1, 1, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only page-run staging parts");
    }

    @Test
    void newPageRunFinalizationRequiresExplicitSupportedMetadata() {
        assertThatThrownBy(() -> new PartFinalize(1, 0, "seg-0.pageseg", PageRunFormat.NAME,
                1, 1, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete supported format metadata")
                .hasMessageContaining("LEGACY_UNRECORDED");
        assertThatThrownBy(() -> new PartFinalize(1, 0, "seg-0.pageseg", PageRunFormat.NAME,
                PageRunFormat.CURRENT_FORMAT_VERSION, PageRunFormat.ABSENT_EXTENSION + 1,
                1, 1, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete supported format metadata")
                .hasMessageContaining("UNKNOWN_EXTENSION_TYPE");
    }

    @Test
    void pageRunCompatibilityClassifiesSupportedIncompleteAndFutureMetadata() {
        assertThat(PageRunFormat.compatibility(PageRunFormat.CURRENT_FORMAT_VERSION,
                PageRunFormat.ABSENT_EXTENSION)).isEqualTo(PageRunFormat.Compatibility.SUPPORTED);
        assertThat(PageRunFormat.compatibility(null, null))
                .isEqualTo(PageRunFormat.Compatibility.LEGACY_UNRECORDED);
        assertThat(PageRunFormat.compatibility(PageRunFormat.CURRENT_FORMAT_VERSION, null))
                .isEqualTo(PageRunFormat.Compatibility.INCOMPLETE);
        assertThat(PageRunFormat.compatibility(null, PageRunFormat.ABSENT_EXTENSION))
                .isEqualTo(PageRunFormat.Compatibility.INCOMPLETE);
        assertThat(PageRunFormat.compatibility(PageRunFormat.CURRENT_FORMAT_VERSION + 1,
                PageRunFormat.ABSENT_EXTENSION))
                .isEqualTo(PageRunFormat.Compatibility.UNKNOWN_FORMAT_VERSION);
    }

    @Test
    void preColumnPartRowsMigrateAsUnrecordedLegacyMetadata(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key("legacy-format-metadata"), false, false);
            runId = run.id();
            store.partFinalized(new PartFinalize(runId, 0, "seg-legacy.pageseg",
                    PageRunFormat.currentListing(), 2, 128, List.of()));
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             var st = c.createStatement()) {
            st.execute("ALTER TABLE part_file DROP COLUMN extension_type");
            st.execute("ALTER TABLE part_file DROP COLUMN format_version");
        }

        try (SqliteCheckpointStore migrated = SqliteCheckpointStore.open(db)) {
            PartRef legacy = migrated.finalizedParts(runId).getFirst();
            assertThat(legacy.format()).isEqualTo(PageRunFormat.NAME);
            assertThat(legacy.formatVersion()).isNull();
            assertThat(legacy.extensionType()).isNull();
        }
    }

    /**
     * Raw {@code byte[]} literal — for the I10 unsigned-BLOB byte-order tests. These keys
     * carry bytes &gt; 0x7F that are unreachable through {@link CheckpointStoreTestSupport#b(String)}
     * (UTF-8 would multi-byte-encode them), and we need to prove SQLite orders them by
     * <b>unsigned</b> memcmp, not signed-byte or UTF-16.
     */
    private static byte[] bytes(int... v) {
        byte[] out = new byte[v.length];
        for (int i = 0; i < v.length; i++) {
            out[i] = (byte) v[i];
        }
        return out;
    }

    private SqliteCheckpointStore open(Path dir) throws Exception {
        return SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"));
    }

    private static RunKey keyAt(String prefix, String argsHash) {
        return new RunKey("s3", null, "bucket", b(prefix), argsHash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet");
    }

    /**
     * Reads {@code [cursor, durable_cursor]} of a node directly from the DB file — an
     * output-complete COMPLETED node is excluded from {@code loadResumable}, so this is
     * the only way to assert the latched columns byte-exact.
     */
    private static byte[][] readCursors(Path dir, long nodeId) throws Exception {
        String url = "jdbc:sqlite:" + dir.resolve("ckpt.sqlite").toAbsolutePath();
        try (Connection c = DriverManager.getConnection(url);
             PreparedStatement ps = c.prepareStatement(
                     "SELECT cursor, durable_cursor FROM listing_node WHERE id=?")) {
            ps.setLong(1, nodeId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new byte[][]{rs.getBytes(1), rs.getBytes(2)};
            }
        }
    }

    private static void ignoreChecksAndUpdate(Path dir, String sql, Object value) throws Exception {
        String url = "jdbc:sqlite:" + dir.resolve("ckpt.sqlite").toAbsolutePath();
        try (Connection c = DriverManager.getConnection(url);
             PreparedStatement pragma = c.prepareStatement("PRAGMA ignore_check_constraints=ON");
             PreparedStatement ps = c.prepareStatement(sql)) {
            pragma.execute();
            ps.setObject(1, value);
            ps.executeUpdate();
        }
    }

    /**
     * {@code openEphemeral} backs the {@code --checkpoint none} decoupling — the exact
     * same {@link CheckpointStore} contract (open a run, insert/commit/split/close) must work
     * against the in-process {@code :memory:} database, with nothing surviving {@link
     * SqliteCheckpointStore#close()} (no file is ever created).
     */
    @Test
    void openEphemeral_supportsFullRunLifecycle_andPersistsNothingToDisk(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = SqliteCheckpointStore.openEphemeral(null)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k1"), false));
            List<Node> resumable = store.loadResumable(run.id(), false);
            assertThat(resumable).hasSize(1);
            assertThat(resumable.getFirst().cursor()).isEqualTo(b("k1"));
            store.markRunFinished(run.id(), RunStatus.COMPLETED);
        }
        try (var files = Files.list(dir)) {
            assertThat(files).isEmpty();   // the ephemeral store never touched this directory
        }
    }

    @Test
    void commitPageAsync_futureCompletesOnlyAfterDurableCommit_I1(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));

            CompletableFuture<Void> f =
                    store.commitPageAsync(new PageCommit(node, b("k1"), false));
            f.get(5, TimeUnit.SECONDS);   // resolves only after the txn commits
            assertThat(f).isDone();

            // Read the cursor back off the DB file: the future signalled durability, so it is there.
            assertThat(readCursors(dir, node)[0]).isEqualTo(b("k1"));
        }
    }

    @Test
    void commitPagePreservesCursorOnResume_I5(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));

            // Two pages committed, not yet completed → IN_PROGRESS at cursor "k2".
            store.commitPage(new PageCommit(node, b("k1"), false));
            store.commitPage(new PageCommit(node, b("k2"), false));

            // Resume reverts IN_PROGRESS → PENDING but keeps the cursor (I5).
            List<Node> resumable = store.loadResumable(run.id(), false);
            assertThat(resumable).hasSize(1);
            assertThat(resumable.getFirst().status()).isEqualTo(NodeStatus.PENDING);
            assertThat(resumable.getFirst().cursor()).isEqualTo(b("k2"));
        }
    }

    @Test
    void completedNodeIsNotResumable_text(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k9"), true));   // completed
            assertThat(store.loadResumable(run.id(), false)).isEmpty();
        }
    }

    @Test
    void emptyBatchLeavesCursorUnchanged(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k1"), false));
            store.commitPage(new PageCommit(node, null, true));   // empty terminal page → cursor unchanged
            // Completed: not resumable, but the cursor stayed at k1 (no advance from the empty page).
            List<Node> fileResumable = store.loadResumable(run.id(), false);
            assertThat(fileResumable).isEmpty();
        }
    }

    /**
     * {@code countNodes} counts nodes in ANY state, so the CLI re-seed gate can tell a
     * never-seeded run (zero nodes — a STUCK seed committed none, I2) from one with a live/completed
     * worklist. Zero before any seed; the full count after; still counts a COMPLETED node (a genuinely
     * finished run is NOT zero-node, so it is never mistaken for never-seeded).
     */
    @Test
    void countNodes_reflectsTotalNodesInAnyState(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("count"), false, false);
            assertThat(store.countNodes(run.id())).as("a never-seeded run has zero nodes").isZero();

            store.insertNodes(List.of(NodeSpec.rootRange(run.id()), NodeSpec.rootRange(run.id())));
            assertThat(store.countNodes(run.id())).as("counts every seeded node").isEqualTo(2);

            List<Node> nodes = store.loadResumable(run.id(), false);
            store.commitPage(new PageCommit(nodes.get(0).id(), b("z"), true));   // drive one to COMPLETED
            assertThat(store.countNodes(run.id()))
                    .as("a COMPLETED node still counts — a finished run is not zero-node")
                    .isEqualTo(2);
        }
    }

    @Test
    void splitHappyPath_narrowsVictim_insertsChild_I4(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));   // (⊥, frontier], cursor=⊥
            store.commitPage(new PageCommit(victim, b("c"), false));        // cursor=c < pivot m

            long child = store.splitNode(new SplitSpec(run.id(), victim, b("m"), null));
            assertThat(child).isNotEqualTo(CheckpointStore.SPLIT_ABORTED);

            List<Node> nodes = store.loadResumable(run.id(), false);
            Node v = nodes.stream().filter(n -> n.id() == victim).findFirst().orElseThrow();
            Node c = nodes.stream().filter(n -> n.id() == child).findFirst().orElseThrow();
            assertThat(v.rangeEnd()).isEqualTo(b("m"));      // victim narrowed to (⊥, m]
            assertThat(v.cursor()).isEqualTo(b("c"));        // cursor never advanced by a split
            assertThat(c.rangeStart()).isEqualTo(b("m"));    // child (m, frontier]
            assertThat(c.rangeEnd()).isNull();
            assertThat(c.cursor()).isEqualTo(b("m"));
        }
    }

    @Test
    void splitChildDurableCursorStartsAtPivot_forFileSinkResume_I6(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(keyAt("", "split-child-durable"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, b("c"), false));

            long child = store.splitNode(new SplitSpec(run.id(), victim, b("m"), null));
            assertThat(child).isNotEqualTo(CheckpointStore.SPLIT_ABORTED);
            byte[][] childCursors = readCursors(dir, child);
            assertThat(childCursors[0]).as("child cursor starts at the pivot").isEqualTo(b("m"));
            assertThat(childCursors[1]).as("child durable floor starts at the pivot").isEqualTo(b("m"));

            List<Node> nodes = store.loadResumable(run.id(), true);
            Node c = nodes.stream().filter(n -> n.id() == child).findFirst().orElseThrow();
            assertThat(c.cursor()).as("file-sink resume keeps the child's pivot floor").isEqualTo(b("m"));
            assertThat(c.durableCursor()).isEqualTo(b("m"));
        }
    }

    @Test
    void splitOnFreshNode_cursorNull_isAllowed(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));   // cursor = NULL (⊥)
            long child = store.splitNode(new SplitSpec(run.id(), victim, b("m"), null));
            assertThat(child).isNotEqualTo(CheckpointStore.SPLIT_ABORTED);
        }
    }

    @Test
    void splitAborts_whenCursorPassedPivot(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, b("z"), false));        // cursor=z > pivot m
            long child = store.splitNode(new SplitSpec(run.id(), victim, b("m"), null));
            assertThat(child).isEqualTo(CheckpointStore.SPLIT_ABORTED);
            assertThat(store.loadResumable(run.id(), false)).hasSize(1);    // no child inserted
        }
    }

    @Test
    void splitAborts_whenBoundMoved_staleOldHi(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));   // range_end = NULL (frontier)
            // A first split narrowed range_end to "t"; a second thief's snapshot still thinks oldHi=NULL.
            store.splitNode(new SplitSpec(run.id(), victim, b("t"), null));
            long second = store.splitNode(new SplitSpec(run.id(), victim, b("m"), null));   // oldHi NULL is stale
            assertThat(second).isEqualTo(CheckpointStore.SPLIT_ABORTED);
        }
    }

    @Test
    void splitAborts_whenVictimCompleted(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, b("c"), true));         // COMPLETED (cursor c < m)
            long child = store.splitNode(new SplitSpec(run.id(), victim, b("m"), null));
            assertThat(child).isEqualTo(CheckpointStore.SPLIT_ABORTED);     // status<>'COMPLETED' clause
        }
    }

    @Test
    void durableCursorModel_reopensCompletedButNotDurable_I6(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k5"), true));          // COMPLETED, cursor=k5
            // Only pages up to k3 landed in a finalized part.
            store.partFinalized(new PartFinalize(run.id(), 0, "part-0.parquet", "parquet", 3, 100,
                    List.of(new PartFinalize.DurableAdvance(node, b("k3")))));

            // File sink: COMPLETED-but-not-durable (k3 < k5) → reopen, cursor reset to durable k3 (§4.5).
            List<Node> resumable = store.loadResumable(run.id(), true);
            assertThat(resumable).hasSize(1);
            assertThat(resumable.getFirst().status()).isEqualTo(NodeStatus.PENDING);
            assertThat(resumable.getFirst().cursor()).isEqualTo(b("k3"));
            assertThat(resumable.getFirst().durableCursor()).isEqualTo(b("k3"));
        }
    }

    @Test
    void durableCursorModel_outputCompleteNodeNotReopened(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k5"), true));          // COMPLETED, cursor=k5
            store.partFinalized(new PartFinalize(run.id(), 0, "part-0.parquet", "parquet", 5, 100,
                    List.of(new PartFinalize.DurableAdvance(node, b("k5")))));   // durable == cursor
            // Output-complete (durable == cursor) → not reopened even for a file sink.
            assertThat(store.loadResumable(run.id(), true)).isEmpty();
        }
    }

    @Test
    void resumeRefusedOnArgsHashMismatch(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            store.openRun(key("h1"), false, false);
        }
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            assertThatThrownBy(() -> store.openRun(key("h2"), true, false))
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("--restart");
        }
    }

    @Test
    void resumeRefusedWhenNothingToResume(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            assertThatThrownBy(() -> store.openRun(key("h1"), true, false))
                    .isInstanceOf(InvalidArgsException.class)
                    .hasMessageContaining("nothing to resume");
        }
    }

    @Test
    void resumeMatchesByArgsHash(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        long runId;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            runId = store.openRun(key("h1"), false, false).id();
        }
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta resumed = store.openRun(key("h1"), true, false);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.id()).isEqualTo(runId);
        }
    }

    @Test
    void malformedRunModeFromCheckpointThrowsCheckpointException(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            store.openRun(key("h1"), false, false);
        }
        ignoreChecksAndUpdate(dir, "UPDATE run_meta SET mode=?", "BROKEN");

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            assertThatThrownBy(() -> store.openRun(key("h1"), true, false))
                    .isInstanceOf(CheckpointException.class)
                    .satisfies(e -> assertThat(((CheckpointException) e).exitCode()).isEqualTo(1));
        }
    }

    @Test
    void malformedNodeEnumFromCheckpointThrowsCheckpointException(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
        }
        ignoreChecksAndUpdate(dir, "UPDATE listing_node SET kind=?", "BROKEN");

        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), true, false);
            assertThatThrownBy(() -> store.loadResumable(run.id(), false))
                    .isInstanceOf(CheckpointException.class)
                    .satisfies(e -> assertThat(((CheckpointException) e).exitCode()).isEqualTo(1));
        }
    }

    @Test
    void uncheckedWriterTaskFailureFailsFutureAndSubsequentEnqueueFast(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);

            assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                    assertThatThrownBy(() -> store.insertNode(
                            new NodeSpec(run.id(), null, null, null, null, null, null)))
                            .isInstanceOf(CheckpointException.class));

            assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                    assertThatThrownBy(() -> store.insertNode(NodeSpec.rootRange(run.id())))
                            .isInstanceOf(CheckpointException.class)
                            .hasMessageContaining("failed"));
        }
    }

    @Test
    void enqueueBlockedOnFullQueueReleasedWhenWriterFails_doesNotDeadlock(@TempDir Path dir) throws Exception {
        SqliteCheckpointStore store = SqliteCheckpointStore.openForTesting(dir.resolve("ckpt.sqlite"), 1);

        CountDownLatch failingTaskStarted = new CountDownLatch(1);
        CountDownLatch failNow = new CountDownLatch(1);
        CompletableFuture<Object> failing = store.enqueueForTesting(c -> {
            failingTaskStarted.countDown();
            try {
                if (!failNow.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("test did not release failing writer task");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("writer boom");
        });
        assertThat(failingTaskStarted.await(2, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Object> queuedBehindFailure = store.enqueueForTesting(c -> null);
        AtomicReference<CompletableFuture<Object>> lateFuture = new AtomicReference<>();
        AtomicReference<Throwable> enqueueError = new AtomicReference<>();
        CountDownLatch lateEnqueueReturned = new CountDownLatch(1);
        Thread lateEnqueuer = Thread.ofPlatform().daemon().name("swath-test-late-enqueuer").start(() -> {
            try {
                lateFuture.set(store.enqueueForTesting(c -> null));
            } catch (Throwable t) {
                enqueueError.set(t);
            } finally {
                lateEnqueueReturned.countDown();
            }
        });

        await().atMost(Duration.ofSeconds(2)).until(() ->
                lateFuture.get() == null && (lateEnqueuer.getState() == Thread.State.WAITING
                        || lateEnqueuer.getState() == Thread.State.TIMED_WAITING));

        failNow.countDown();

        await().atMost(Duration.ofSeconds(5)).until(() -> lateEnqueueReturned.getCount() == 0);
        assertThat(enqueueError.get()).isNull();
        assertThat(lateFuture.get()).isNotNull();
        await().atMost(Duration.ofSeconds(5)).until(() -> lateFuture.get().isDone());

        assertThatThrownBy(failing::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(CheckpointException.class);
        assertThatThrownBy(queuedBehindFailure::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(CheckpointException.class);
        assertThatThrownBy(() -> lateFuture.get().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(CheckpointException.class);
        assertThatThrownBy(() -> store.insertNode(NodeSpec.rootRange(1L)))
                .isInstanceOf(CheckpointException.class)
                .hasMessageContaining("failed");

        store.close();
    }

    @Test
    void restartDiscardsPriorRun(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k1"), false));
        }
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta restarted = store.openRun(key("h1"), false, true);
            assertThat(restarted.resumed()).isFalse();
            assertThat(store.loadResumable(restarted.id(), false)).isEmpty();   // prior nodes gone
        }
    }

    // ---- the I6 output-complete latch (markOutputComplete) ----------

    @Test
    void markOutputComplete_latchesCleanFilteredTail_I6(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            // BEFORE — a sibling run that never latches: its filtered-tail node is
            // COMPLETED at k5 but durable_cursor is NULL (every emitted page filtered out
            // of every part → no partFinalized), so a file resume reopens it.
            // (We assert BEFORE on a separate run because loadResumable(.,true) *itself*
            // reopens-and-rewrites the node — checking it on the to-be-latched node would
            // mutate cursor:=durable_cursor=NULL and defeat the latch we want to test.)
            RunMeta noLatch = store.openRun(keyAt("before/", "h"), false, false);
            long n1 = store.insertNode(NodeSpec.rootRange(noLatch.id()));
            store.commitPage(new PageCommit(n1, b("k5"), true));            // COMPLETED, cursor=k5, durable NULL
            List<Node> reopened = store.loadResumable(noLatch.id(), true);
            assertThat(reopened).hasSize(1);                                // not output-complete → reopens

            // AFTER — markOutputComplete on a clean Parquet close latches durable_cursor=cursor
            // for the COMPLETED node, so the same filtered-tail node is now output-complete.
            RunMeta latched = store.openRun(keyAt("after/", "h"), false, false);
            long n2 = store.insertNode(NodeSpec.rootRange(latched.id()));
            store.commitPage(new PageCommit(n2, b("k5"), true));            // COMPLETED, cursor=k5, durable NULL
            store.markOutputComplete(latched.id());
            assertThat(store.loadResumable(latched.id(), true)).isEmpty();  // output-complete → not reopened

            byte[][] cd = readCursors(dir, n2);                            // [cursor, durable_cursor]
            assertThat(cd[0]).isEqualTo(b("k5"));                          // cursor unchanged by the latch
            assertThat(cd[1]).isEqualTo(b("k5"));                          // durable_cursor == cursor (byte-exact)
        }
    }

    @Test
    void markOutputComplete_allEmptyResultStaysOutputComplete_I6(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, null, true));            // empty terminal page → COMPLETED, cursor NULL
            // NULL durable_cursor IS NULL cursor ⇒ already output-complete, before any latch.
            assertThat(store.loadResumable(run.id(), true)).isEmpty();
            store.markOutputComplete(run.id());                            // durable_cursor:=cursor (NULL→NULL): no-op
            assertThat(store.loadResumable(run.id(), true)).isEmpty();     // still output-complete

            byte[][] cd = readCursors(dir, node);
            assertThat(cd[0]).isNull();                                    // cursor NULL
            assertThat(cd[1]).isNull();                                    // durable_cursor NULL
        }
    }

    // ---- I10 unsigned-BLOB byte order + deterministic split-ABORT losers ----

    @Test
    void splitCas_unsignedByteOrder_allowsAcross0x7Fto0x80_I10(@TempDir Path dir) throws Exception {
        // cursor={0x7F} < pivot={0x80}: unsigned 127<128 ⇒ split ALLOWED. Under signed-byte
        // ordering pivot=0x80=-128 < cursor=0x7F=127, so `cursor < pivot` would be FALSE and the
        // split would WRONGLY abort. (UTF-16 would also order 0x007F<0x0080, so this case alone
        // discriminates *signed*; the multi-byte sibling below discriminates UTF-16 too.)
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, bytes(0x7F), false));   // cursor=0x7F < pivot 0x80
            long child = store.splitNode(new SplitSpec(run.id(), victim, bytes(0x80), null));
            assertThat(child).isNotEqualTo(CheckpointStore.SPLIT_ABORTED);

            List<Node> nodes = store.loadResumable(run.id(), false);
            Node v = nodes.stream().filter(n -> n.id() == victim).findFirst().orElseThrow();
            Node c = nodes.stream().filter(n -> n.id() == child).findFirst().orElseThrow();
            assertThat(v.rangeEnd()).isEqualTo(bytes(0x80));               // victim narrowed to (⊥, 0x80] byte-exact
            assertThat(c.rangeStart()).isEqualTo(bytes(0x80));            // child (0x80, frontier]
            assertThat(c.cursor()).isEqualTo(bytes(0x80));
        }
    }

    @Test
    void splitCas_unsignedByteOrder_lastByteDiffers_I10(@TempDir Path dir) throws Exception {
        // Keys differ only in the last byte: cursor={0x41,0x7F} < pivot={0x41,0x80}. memcmp
        // walks byte-by-byte (first byte ties) then compares 0x7F<0x80 unsigned ⇒ ALLOWED.
        // Signed-byte would read 0x80 as -128<0x7F ⇒ abort; UTF-16 would re-pack the bytes into
        // code units and reorder ⇒ also wrong. Only unsigned memcmp gets this right.
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, bytes(0x41, 0x7F), false));
            long child = store.splitNode(new SplitSpec(run.id(), victim, bytes(0x41, 0x80), null));
            assertThat(child).isNotEqualTo(CheckpointStore.SPLIT_ABORTED);

            List<Node> nodes = store.loadResumable(run.id(), false);
            Node v = nodes.stream().filter(n -> n.id() == victim).findFirst().orElseThrow();
            assertThat(v.rangeEnd()).isEqualTo(bytes(0x41, 0x80));
        }
    }

    @Test
    void splitCas_unsignedByteOrder_abortsWhen0xFFcursorBeyondPivot_I10(@TempDir Path dir) throws Exception {
        // cursor={0xFF} > pivot={0x80}: unsigned 255>128 ⇒ `cursor < pivot` FALSE ⇒ ABORTED
        // (the victim already listed past the pivot). Confirms the abort direction holds for
        // top-of-range 0xFF bytes.
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, bytes(0xFF), false));
            long child = store.splitNode(new SplitSpec(run.id(), victim, bytes(0x80), null));
            assertThat(child).isEqualTo(CheckpointStore.SPLIT_ABORTED);
            assertThat(store.loadResumable(run.id(), false)).hasSize(1);   // no child inserted
        }
    }

    @Test
    void durableCursorAdvance_unsignedMonotonic_ignoresLowerHighByte_I10(@TempDir Path dir) throws Exception {
        // partFinalized advances durable_cursor only when `durable_cursor IS NULL OR durable_cursor < ?`.
        // Advance to {0x80} sticks; a later advance to the unsigned-LOWER {0x7F} is IGNORED
        // (0x80=128 < 0x7F=127 is FALSE). Under signed-byte 0x80=-128 < 0x7F=127 would be TRUE and
        // would WRONGLY roll the durable cursor backwards.
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, bytes(0xFF), false));     // IN_PROGRESS; cursor ahead of durable
            store.partFinalized(new PartFinalize(run.id(), 0, "part-0.parquet", "parquet", 1, 100,
                    List.of(new PartFinalize.DurableAdvance(node, bytes(0x80)))));
            store.partFinalized(new PartFinalize(run.id(), 1, "part-1.parquet", "parquet", 1, 100,
                    List.of(new PartFinalize.DurableAdvance(node, bytes(0x7F)))));   // lower unsigned ⇒ ignored

            Node n = store.loadResumable(run.id(), false).stream()
                    .filter(x -> x.id() == node).findFirst().orElseThrow();
            assertThat(n.durableCursor()).isEqualTo(bytes(0x80));          // stuck at the higher key, byte-exact
        }
    }

    // CONC-3: deterministic two-loser split aborts at the store, with adversarial high bytes.
    // Each loser hits a *distinct* guard. NOTE: the engine-level "thief restores victim.hi=H and
    // retries" is WorkStealingScan (RES-3/PROP-1); here we assert only the store's
    // SPLIT_ABORTED return.

    @Test
    void splitAborts_highBytes_cursorPastPivot_CONC3(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, bytes(0xC0), false));   // cursor=0xC0 > pivot 0x90 (stale snapshot)
            long child = store.splitNode(new SplitSpec(run.id(), victim, bytes(0x90), null));
            assertThat(child).isEqualTo(CheckpointStore.SPLIT_ABORTED);     // `cursor < pivot` guard fails
            assertThat(store.loadResumable(run.id(), false)).hasSize(1);
        }
    }

    @Test
    void splitAborts_highBytes_staleOldHi_CONC3(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));   // range_end = NULL (frontier)
            store.splitNode(new SplitSpec(run.id(), victim, bytes(0xE0), null));  // first split narrows range_end→0xE0
            // Second thief's snapshot still thinks oldHi=NULL ⇒ `range_end IS :oldHi` fails.
            long second = store.splitNode(new SplitSpec(run.id(), victim, bytes(0x90), null));
            assertThat(second).isEqualTo(CheckpointStore.SPLIT_ABORTED);
        }
    }

    @Test
    void splitAborts_highBytes_victimCompleted_CONC3(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h1"), false, false);
            long victim = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(victim, bytes(0x90), true));    // COMPLETED (cursor 0x90 < pivot 0xE0)
            long child = store.splitNode(new SplitSpec(run.id(), victim, bytes(0xE0), null));
            assertThat(child).isEqualTo(CheckpointStore.SPLIT_ABORTED);     // `status <> COMPLETED` guard fails
        }
    }

    // ---- insertNodes: atomic batch seed insert (I2 blocker fix) ----------------

    @Test
    void insertNodes_commitsAllSpecsInOneBatch(@TempDir Path dir) throws Exception {
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(key("h-ins-batch"), false, false);
            List<NodeSpec> specs = List.of(
                    new NodeSpec(run.id(), null, NodeKind.RANGE, null,    b("p1/"), null,    null),
                    new NodeSpec(run.id(), null, NodeKind.RANGE, b("p1/"), b("p2/"), b("p1/"), null),
                    new NodeSpec(run.id(), null, NodeKind.RANGE, b("p2/"), null,    b("p2/"), null)
            );
            List<Long> ids = store.insertNodes(specs);
            assertThat(ids).hasSize(3);
            // All three specs are durable and resumable immediately.
            assertThat(store.loadResumable(run.id(), false)).hasSize(3);
        }
    }

    @Test
    void insertNodes_atomicRollback_leavesZeroNodesOnFailure(@TempDir Path dir) throws Exception {
        // Prove the "all-or-nothing" guarantee: a failure INSIDE the single writer-thread
        // task causes the whole SQLite transaction to roll back, leaving zero nodes durable.
        RunKey rk = key("h-ins-rollback");
        long runId;
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta run = store.openRun(rk, false, false);
            runId = run.id();
            // Submit ONE task that inserts a listing_node row, then throws to trigger rollback.
            // The writer catches the RuntimeException, calls conn.rollback(), then permanently
            // fails the store.
            CompletableFuture<Object> f = store.enqueueForTesting(c -> {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO listing_node (run_id, kind, status, generation, "
                                + "pages_emitted, api_calls, unsplittable, updated_at) "
                                + "VALUES (?, 'RANGE', 'PENDING', 0, 0, 0, 0, ?)")) {
                    ps.setLong(1, runId);
                    ps.setLong(2, System.currentTimeMillis());
                    ps.executeUpdate();   // row written in the txn — NOT yet committed
                } catch (SQLException e) {
                    throw new RuntimeException("insert failed", e);
                }
                throw new RuntimeException("simulated crash mid-insert");  // triggers rollback
            });
            // The future must fail (the writer rolled back and set the store to a terminal-failure state).
            assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            // Store is now permanently failed; don't try to call it again — reopen instead.
        }
        // Reopen the same DB file. The run_meta row (committed in its own earlier task) IS durable,
        // but the listing_node row (written in the failing task) must have been rolled back.
        try (SqliteCheckpointStore store2 = open(dir)) {
            RunMeta resumed = store2.openRun(rk, true, false);   // resume the same run
            assertThat(store2.loadResumable(resumed.id(), false))
                    .as("atomic rollback: 0 nodes durably committed despite partial write in task")
                    .isEmpty();
        }
    }

    // ---- run_meta run-context (no_sign_request/profile/region/fetch_owner/raw_output/output_path) ----

    /**
     * A fresh run persists the full run-context; a later {@code resume=true} openRun (a fresh
     * process, same identity) reads it back byte-exact. Guards the RES-context restore path that
     * {@link io.varve.swath.cli.ListCommand} relies on to reconstruct auth/output on a bare
     * {@code --resume} — dropping/swapping any single field here would silently regress it.
     */
    @Test
    void openRun_resumed_restoresFullRunContext_RES85(@TempDir Path dir) throws Exception {
        RunKey fresh = new RunKey("s3", null, "bucket", b("p/"), "h-ctx",
                "auto", ListingMode.OBJECTS, "", "jsonl",
                new SoftRestoreContext(true, "my-profile", "us-west-2", true, true, "/tmp/out.jsonl", true, null, null),
                false);
        long runId;
        try (SqliteCheckpointStore store = open(dir)) {
            runId = store.openRun(fresh, false, false).id();
        }
        // Reopen (a fresh process, as a real resume would) and resume by identity only — the
        // resuming key's own context fields are irrelevant to openRun; what matters is what
        // comes BACK on the RunMeta.
        try (SqliteCheckpointStore store2 = open(dir)) {
            RunMeta resumed = store2.openRun(key("h-ctx"), true, false);
            assertThat(resumed.id()).isEqualTo(runId);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.context().noSignRequest()).isTrue();
            assertThat(resumed.context().profile()).isEqualTo("my-profile");
            assertThat(resumed.context().region()).isEqualTo("us-west-2");
            assertThat(resumed.context().fetchOwner()).isTrue();
            assertThat(resumed.context().rawOutput()).isTrue();
            assertThat(resumed.context().outputPath()).isEqualTo("/tmp/out.jsonl");
            // --request-payer requester round-trips exactly like the other context fields.
            assertThat(resumed.context().requestPayer()).isTrue();
        }
    }

    /**
     * The resolved output destination kind and the {@code --output-type} override persist on
     * a fresh run and read back byte-exact on resume — for each destination kind, and with an
     * override present. This is the persistence foundation consumers rely on to stop re-inferring
     * the kind from the path; here it is round-trip only (nothing yet changes a decision on it).
     */
    @Test
    void openRun_persistsResolvedOutputConfig(@TempDir Path dir) throws Exception {
        assertOutputConfigRoundTrips(dir.resolve("stdout"), "STDOUT", null, null);
        assertOutputConfigRoundTrips(dir.resolve("file"), "FILE", "file", "/tmp/out.jsonl");
        assertOutputConfigRoundTrips(dir.resolve("dir"), "DIRECTORY", "dir", "/tmp/dataset");
    }

    private void assertOutputConfigRoundTrips(Path dir, String destinationKind, String outputType,
                                              String outputPath) throws Exception {
        Files.createDirectories(dir);
        RunKey fresh = new RunKey("s3", null, "bucket", b("p/"), "h-out",
                "auto", ListingMode.OBJECTS, "", "jsonl",
                new SoftRestoreContext(false, null, null, false, false, outputPath, false,
                        destinationKind, outputType),
                false);
        try (SqliteCheckpointStore store = open(dir)) {
            store.openRun(fresh, false, false);
        }
        // Reopen (a fresh process, as a real resume would) and resume by identity only.
        try (SqliteCheckpointStore store2 = open(dir)) {
            RunMeta resumed = store2.openRun(key("h-out"), true, false);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.context().destinationKind()).isEqualTo(destinationKind);
            assertThat(resumed.context().outputType()).isEqualTo(outputType);
        }
    }

    /**
     * A checkpoint DB without the run-context columns ({@code no_sign_request}/{@code profile}/
     * {@code region}/{@code fetch_owner}/{@code raw_output}/{@code output_path}) must still open
     * and resume — the migration backfills the missing columns with their defaults (false/null),
     * not a crash or a refused resume.
     */
    @Test
    void oldCheckpointWithoutContextColumns_stillResumes_backCompat(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath())) {
            CheckpointStoreTestSupport.writePreContextRunMeta(c);
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO run_meta (store_scheme, endpoint, bucket, prefix, args_hash, strategy, "
                            + "filter_spec, output_format, mode, started_at, status) "
                            + "VALUES ('s3', NULL, 'bucket', ?, 'h-legacy', 'WORK_STEALING', '', 'jsonl', "
                            + "'OBJECTS', ?, 'RUNNING')")) {
                ps.setBytes(1, b("p/"));
                ps.setLong(2, System.currentTimeMillis());
                ps.executeUpdate();
            }
        }
        // Opening via the store migrates (idempotent ALTER TABLE ADD COLUMN) and resumes cleanly.
        try (SqliteCheckpointStore store = open(dir)) {
            RunMeta resumed = store.openRun(key("h-legacy"), true, false);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.context().noSignRequest()).isFalse();
            assertThat(resumed.context().profile()).isNull();
            assertThat(resumed.context().region()).isNull();
            assertThat(resumed.context().fetchOwner()).isFalse();
            assertThat(resumed.context().rawOutput()).isFalse();
            assertThat(resumed.context().outputPath()).isNull();
            // A checkpoint DB without the request_payer column also backfills it to off.
            assertThat(resumed.context().requestPayer()).isFalse();
        }
    }
}
