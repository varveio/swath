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
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.awaitility.Awaitility.await;

import io.varve.swath.error.CheckpointException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Characterization of {@link SqliteCheckpointStore}'s <b>writer engine</b> — the queue, the
 * batching writer loop, the failure latch and the close handshake — as distinct from the DAO
 * statements around it. Every assertion here was written <i>after</i> observing what today's code
 * does, so this class is a behavioural fence around a future {@code CheckpointWriteQueue}
 * extraction: the engine may be moved, but any change to what a caller can observe turns this red.
 *
 * <p>Everything is asserted through the store's own surface — public API, the package-private
 * {@code enqueueForTesting}/{@code openForTesting} seams, and an <b>independent JDBC connection</b>
 * to the same file. The independent reader is what makes durability observable: in WAL mode it sees
 * exactly the last committed snapshot, so "visible to that connection" <i>is</i> "durably
 * committed", which is the only honest way to assert I1 commit-before-emit from outside.
 *
 * <p><b>Scope discipline.</b> Where the engine makes no guarantee, this class says so rather than
 * inventing one — see {@link #resolvedBatchFutureImpliesTheWholeBatchIsAlreadyDurable} (only
 * STORE-DRIVEN successful resolution implies durability; exceptional resolution implies nothing in
 * either direction) and {@link #submitAfterCloseHasLatchedIsRejected_raceWindowUnpinned}. It also
 * does not claim to be a crash test: the true kill-9 guard is {@code HardCrashSigkillResume*IT} in
 * swath-cli, and {@link #abandonedStoreWithoutCloseReopensAtLastCommittedAdvance} is honestly
 * scoped to abandonment, not process death.
 *
 * <p><b>Two behaviours are NOT YET PINNED but ARE pinnable</b> — the commit-vs-future-resolution
 * ordering, and the close/submit overlap. A deterministic technique for each is written out on the
 * test that would carry it, as a follow-up for CK3's own test work. They are gaps, not limits.
 *
 * <p>Complements, and deliberately does not duplicate, {@link SqliteCheckpointStoreTest} (DAO/CAS/
 * cursor semantics, single-task rollback, full-queue release) and
 * {@link SqliteCheckpointStoreMetricsTest} (meter identity).
 */
final class CheckpointWriteQueueCharacterizationTest {

    /** The observed batch ceiling: consecutive queued tasks coalesce up to this many per txn. */
    private static final int OBSERVED_MAX_BATCH = 256;

    // ---- scaffolding -----------------------------------------------------------------

    /**
     * A writer task that parks on the writer thread until the test releases it. Parking a task
     * <i>inside</i> {@code runBatch} is the whole lever of this class: it freezes the engine at a
     * known point (queue drained, transaction open, nothing committed) so the test can enqueue more
     * work, or inspect the durable file, with no timing assumption at all.
     */
    private static final class Gate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        Object park(Connection ignored) {
            entered.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("gate never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return null;
        }

        void awaitEntered() throws InterruptedException {
            assertThat(entered.await(30, TimeUnit.SECONDS)).as("writer reached the gate").isTrue();
        }

        void release() {
            release.countDown();
        }
    }

    /** Freeze the writer inside an open transaction, and return the gate holding it there. */
    private static Gate parkWriter(SqliteCheckpointStore store) throws Exception {
        Gate gate = new Gate();
        store.enqueueForTesting(gate::park);
        gate.awaitEntered();
        return gate;
    }

    /**
     * Opens a store with a DAEMON writer thread ({@code openForTesting}), so a failed assertion can
     * never strand a non-daemon writer parked in {@code queue.take()} and hang the test JVM. Used
     * for every store this class GATES or deliberately abandons. The daemon flag is belt-and-braces,
     * not the cleanup strategy: a daemon writer still holds its JDBC connection (and, through
     * {@code this::writerLoop}, the whole {@code CheckpointWriteQueue}) reachable for the life of
     * the worker JVM, so every
     * store opened here is also closed — in a {@code finally} where a gate could otherwise leave the
     * writer parked, by try-with-resources otherwise.
     *
     * <p>Two stores deliberately use the production {@link SqliteCheckpointStore#open(Path)} with
     * its non-daemon writer: the {@code seed} store and the reopened store in
     * {@link #abandonedStoreWithoutCloseReopensAtLastCommittedAdvance}. Neither is ever gated, and
     * both are closed by try-with-resources.
     */
    private static SqliteCheckpointStore openStore(Path db) throws CheckpointException {
        return SqliteCheckpointStore.openForTesting(db, 4096);
    }

    private static Connection reader(Path db) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
    }

    /** A scratch table committed in its own transaction, so later batches only add to it. */
    private static void createProbeTable(SqliteCheckpointStore store) throws Exception {
        store.enqueueForTesting(c -> {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE probe (i INTEGER)");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        }).get(30, TimeUnit.SECONDS);
    }

    private static void insertProbe(Connection c, int i) {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO probe (i) VALUES (?)")) {
            ps.setInt(1, i);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static int probeCount(Connection c) {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM probe")) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Integer> probeRowsInInsertOrder(Connection c) throws SQLException {
        List<Integer> out = new ArrayList<>();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT i FROM probe ORDER BY rowid")) {
            while (rs.next()) {
                out.add(rs.getInt(1));
            }
        }
        return out;
    }

    private static byte[] durableCursor(Path db, long nodeId) throws SQLException {
        try (Connection c = reader(db);
             PreparedStatement ps = c.prepareStatement("SELECT cursor FROM listing_node WHERE id=?")) {
            ps.setLong(1, nodeId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBytes(1);
            }
        }
    }

    private static Throwable failureCauseOf(CompletableFuture<Object> f) {
        try {
            f.get(30, TimeUnit.SECONDS);
            throw new AssertionError("future completed normally, expected failure");
        } catch (ExecutionException e) {
            return e.getCause();
        } catch (Exception e) {
            throw new AssertionError("future did not fail within the timeout", e);
        }
    }

    private static void awaitParked(Thread t) {
        await().atMost(Duration.ofSeconds(10)).until(() ->
                t.getState() == Thread.State.WAITING || t.getState() == Thread.State.TIMED_WAITING);
    }

    // ---- batching: one transaction per batch, MAX_BATCH tasks wide --------------------

    /**
     * Consecutive queued tasks coalesce into ONE transaction, capped at
     * {@value #OBSERVED_MAX_BATCH}. Asserted through the durable consequence rather than an
     * internal counter: each task records how many rows an independent connection can see at the
     * moment it runs, and that number only moves at a batch boundary. 300 tasks queued behind a
     * parked writer therefore split 256 + 44, and the first 256 all observe zero committed rows —
     * i.e. the engine committed once for the batch, not once per task.
     */
    @Test
    void batchCoalescesUpToMaxBatchAndCommitsOncePerBatch(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try (Connection rd = reader(db)) {
            createProbeTable(store);
            gate = parkWriter(store);

            int tasks = OBSERVED_MAX_BATCH + 44;
            int[] committedRowsSeenByTask = new int[tasks];
            List<CompletableFuture<Object>> futures = new ArrayList<>(tasks);
            for (int i = 0; i < tasks; i++) {
                final int index = i;
                futures.add(store.enqueueForTesting(c -> {
                    committedRowsSeenByTask[index] = probeCount(rd);
                    insertProbe(c, index);
                    return null;
                }));
            }
            gate.release();
            gate = null;
            for (CompletableFuture<Object> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            for (int i = 0; i < OBSERVED_MAX_BATCH; i++) {
                assertThat(committedRowsSeenByTask[i])
                        .as("task %d is inside the first batch: nothing of it is committed yet", i)
                        .isZero();
            }
            for (int i = OBSERVED_MAX_BATCH; i < tasks; i++) {
                assertThat(committedRowsSeenByTask[i])
                        .as("task %d is in the second batch: exactly the first batch is committed", i)
                        .isEqualTo(OBSERVED_MAX_BATCH);
            }
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    /**
     * Arrival order is application order, and a batch's whole effect appears in one step: while the
     * writer is stopped mid-batch, nothing of the open transaction is visible to another connection
     * and no future has resolved. Note this pins only the state BEFORE the commit — the commit and
     * the future resolutions are NOT simultaneous (the commit strictly precedes them), so no
     * simultaneity is claimed here; see
     * {@link #resolvedBatchFutureImpliesTheWholeBatchIsAlreadyDurable}.
     */
    @Test
    void batchIsAppliedInSubmissionOrderAndIsInvisibleUntilItsCommit(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        Gate midBatch = null;
        try (Connection rd = reader(db)) {
            createProbeTable(store);
            gate = parkWriter(store);

            midBatch = new Gate();
            final Gate mid = midBatch;
            CompletableFuture<Object> first = store.enqueueForTesting(c -> {
                insertProbe(c, 1);
                return mid.park(c);
            });
            CompletableFuture<Object> second = store.enqueueForTesting(c -> {
                insertProbe(c, 2);
                return null;
            });
            CompletableFuture<Object> third = store.enqueueForTesting(c -> {
                insertProbe(c, 3);
                return null;
            });

            gate.release();
            gate = null;
            midBatch.awaitEntered();
            assertThat(probeCount(rd)).as("mid-batch: the open transaction is invisible").isZero();
            assertThat(first.isDone()).isFalse();
            assertThat(second.isDone()).isFalse();
            assertThat(third.isDone()).isFalse();

            midBatch.release();
            midBatch = null;
            first.get(30, TimeUnit.SECONDS);
            second.get(30, TimeUnit.SECONDS);
            third.get(30, TimeUnit.SECONDS);
            assertThat(probeRowsInInsertOrder(rd))
                    .as("the whole batch lands at once, in submission order")
                    .containsExactly(1, 2, 3);
        } finally {
            if (gate != null) {
                gate.release();
            }
            if (midBatch != null) {
                midBatch.release();
            }
            store.close();
        }
    }

    /**
     * The I1-load-bearing half of the batch's commit/resolve ordering: <b>whenever the store itself
     * resolves a batch's future successfully, that batch's data is ALREADY durable.</b> Asserted
     * from a dependent on the first task's future, which therefore runs at or after that future's
     * completion; from there an independent connection must already see the whole batch.
     *
     * <p><b>Strength of this guard.</b> The assertion is a true property of the engine, but it is
     * not a deterministic fence: under a resolve-before-commit mutation the observing thread can
     * wake only after the writer has already reached the (mutated) commit, so the JDBC read can
     * still pass despite premature resolution. Measured empirically at 5/5 kills, NOT guaranteed.
     *
     * <p><b>Scope of the claim.</b> It covers only STORE-DRIVEN (natural) completion. A future
     * handed back to a caller is caller-mutable — completing it externally resolves it with no
     * durability implication whatsoever.
     *
     * <p><b>Exceptional resolution carries no durability guarantee in EITHER direction.</b> It does
     * not imply rollback: {@code runBatch} commits at {@code CheckpointWriteQueue:151} and then
     * runs the metrics calls inside the SAME {@code try}, so a Throwable thrown after the commit
     * enters the catch, {@code conn.rollback()} cannot undo the already-committed transaction, and
     * {@code failStore} completes those futures exceptionally over a fully durable batch. Caller
     * {@code cancel}/{@code completeExceptionally} is a second counterexample.
     *
     * <p><b>Not yet pinned (deterministic technique known) — follow-up for CK3.</b> The commit at
     * {@code CheckpointWriteQueue:151} strictly precedes the future-resolution loop at
     * {@code :158-161}, so a write is
     * genuinely visible while later futures in the same batch are unresolved. That ordering IS
     * pinnable, by either of two seams: (i) a SYNCHRONOUS (non-async) dependent can park the sole
     * completing writer inside the loop, provided no thread is waiting on the source future — note
     * {@code CompletableFuture.timedGet} runs {@code postComplete} on the WAITING thread and
     * OpenJDK lets threads compete to pop dependents, which is what made the earlier attempt racy;
     * or (ii) {@code RunMetrics} takes an injected {@code MeterRegistry}, so a custom Micrometer
     * {@code Clock} can block inside {@code Timer.Sample.stop()}, which {@code runBatch} calls
     * after the commit and before completing futures. Neither is implemented here.
     */
    @Test
    void resolvedBatchFutureImpliesTheWholeBatchIsAlreadyDurable(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try (Connection rd = reader(db)) {
            createProbeTable(store);
            gate = parkWriter(store);

            int tasks = 8;
            List<CompletableFuture<Object>> futures = new ArrayList<>(tasks);
            for (int i = 0; i < tasks; i++) {
                final int index = i;
                futures.add(store.enqueueForTesting(c -> {
                    insertProbe(c, index);
                    return null;
                }));
            }

            AtomicInteger visibleRowsWhenFirstFutureResolved = new AtomicInteger(-1);
            futures.get(0).thenRun(() ->
                    visibleRowsWhenFirstFutureResolved.set(probeCount(rd)));

            gate.release();
            gate = null;
            for (CompletableFuture<Object> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            assertThat(visibleRowsWhenFirstFutureResolved.get())
                    .as("a resolved batch future implies the ENTIRE batch is already durably committed")
                    .isEqualTo(tasks);
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    // ---- failure semantics ------------------------------------------------------------

    /**
     * The highest-value pin. When one task in a batch throws, today's engine:
     * <ul>
     *   <li>ABANDONS the rest of the batch — tasks after the thrower never run at all;</li>
     *   <li>rolls the transaction back, so work done by tasks BEFORE the thrower is discarded;</li>
     *   <li>fails the store permanently, completing every task of the doomed batch <i>and</i>
     *       everything still queued behind it with the SAME {@link CheckpointException}
     *       instance — an unchecked cause is wrapped as {@code "checkpoint writer failed"};</li>
     *   <li>rejects every later call with {@code "checkpoint store has failed"}, caused by that
     *       same latched exception.</li>
     * </ul>
     */
    @Test
    void taskFailureAbandonsRestOfBatch_rollsBack_andPermanentlyFailsStore(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        Gate midBatch = null;
        try (Connection rd = reader(db)) {
            createProbeTable(store);
            gate = parkWriter(store);

            midBatch = new Gate();
            final Gate mid = midBatch;
            AtomicBoolean afterThrowerRan = new AtomicBoolean(false);
            CompletableFuture<Object> beforeThrower = store.enqueueForTesting(c -> {
                insertProbe(c, 1);
                return mid.park(c);
            });
            CompletableFuture<Object> thrower = store.enqueueForTesting(c -> {
                throw new IllegalStateException("boom-mid-batch");
            });
            CompletableFuture<Object> afterThrower = store.enqueueForTesting(c -> {
                afterThrowerRan.set(true);
                insertProbe(c, 3);
                return null;
            });

            gate.release();
            gate = null;
            midBatch.awaitEntered();
            // Queued behind the doomed batch — the writer is already past its drain.
            CompletableFuture<Object> queuedBehind = store.enqueueForTesting(c -> {
                insertProbe(c, 4);
                return null;
            });
            midBatch.release();
            midBatch = null;

            Throwable latched = failureCauseOf(thrower);
            assertThat(latched)
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint writer failed")
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(failureCauseOf(beforeThrower)).as("batch-mate before the thrower").isSameAs(latched);
            assertThat(failureCauseOf(afterThrower)).as("batch-mate after the thrower").isSameAs(latched);
            assertThat(failureCauseOf(queuedBehind)).as("task queued behind the doomed batch").isSameAs(latched);

            assertThat(afterThrowerRan).as("tasks after the thrower are never executed").isFalse();
            assertThat(probeCount(rd)).as("the whole batch rolled back — nothing durable").isZero();

            assertThatThrownBy(() -> store.countNodes(1L))
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store has failed")
                    .cause().isSameAs(latched);
            assertThatThrownBy(() -> store.enqueueForTesting(c -> null))
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store has failed")
                    .cause().isSameAs(latched);
        } finally {
            if (gate != null) {
                gate.release();
            }
            if (midBatch != null) {
                midBatch.release();
            }
            // A failed store is already `closed`; close() must still return and stay idempotent.
            // No assertion is made on how long the writer's own teardown takes.
            store.close();
            store.close();
        }
    }

    /**
     * A {@link CheckpointException} raised inside a task reaches the caller as the <b>very same
     * object</b> — the engine neither re-wraps nor rebuilds it — and that same instance is what the
     * store latches as the cause of every later rejection. Asserting object identity (not merely
     * message and cause) is what makes this catch a message-and-cause-preserving re-wrap, which is
     * otherwise indistinguishable from a rethrow.
     */
    @Test
    void checkpointExceptionFromTaskSurfacesAsTheSameInstanceAndIsLatched(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        try (SqliteCheckpointStore seed = openStore(db)) {
            RunMeta run = seed.openRun(key("h-verbatim"), false, false);
            seed.insertNode(NodeSpec.rootRange(run.id()));
        }
        try (Connection c = reader(db); Statement st = c.createStatement()) {
            st.execute("PRAGMA ignore_check_constraints=ON");
            st.execute("UPDATE listing_node SET kind='BROKEN'");
        }

        SqliteCheckpointStore store = openStore(db);
        try {
            RunMeta run = store.openRun(key("h-verbatim"), true, false);
            CheckpointException fromTask = catchThrowableOfType(CheckpointException.class,
                    () -> store.loadResumable(run.id(), false));
            assertThat(fromTask)
                    .isNotNull()
                    .hasMessage("checkpoint row has invalid listing_node.kind: BROKEN")
                    .hasCauseInstanceOf(IllegalArgumentException.class);
            // The latched failure IS that object: proves await() rethrew it rather than rebuilding it.
            assertThatThrownBy(() -> store.countNodes(run.id()))
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store has failed")
                    .cause().isSameAs(fromTask);
        } finally {
            store.close();
        }
    }

    // ---- lifecycle / close handshake ---------------------------------------------------

    /**
     * {@code close()} DRAINS: the stop sentinel goes to the tail of the queue, so everything
     * already enqueued still runs and commits, in order, before the writer exits. {@code close()}
     * blocks until that drain finishes, and is idempotent.
     */
    @Test
    void closeDrainsPendingWorkAndIsIdempotent(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try {
            createProbeTable(store);
            gate = parkWriter(store);

            List<CompletableFuture<Object>> pending = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final int index = i;
                pending.add(store.enqueueForTesting(c -> {
                    insertProbe(c, index);
                    return null;
                }));
            }

            AtomicReference<Throwable> closeError = new AtomicReference<>();
            CountDownLatch closeReturned = new CountDownLatch(1);
            Thread closer = Thread.ofPlatform().daemon().name("swath-test-closer").start(() -> {
                try {
                    store.close();
                } catch (Throwable t) {
                    closeError.set(t);
                } finally {
                    closeReturned.countDown();
                }
            });
            awaitParked(closer);
            assertThat(closeReturned.getCount()).as("close() blocks until the writer drains").isEqualTo(1);

            gate.release();
            gate = null;
            assertThat(closeReturned.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(closeError.get()).isNull();

            for (CompletableFuture<Object> f : pending) {
                assertThat(f.isDone()).isTrue();
                assertThat(f.isCompletedExceptionally())
                        .as("work enqueued before close() still commits").isFalse();
            }
            try (Connection rd = reader(db)) {
                assertThat(probeRowsInInsertOrder(rd))
                        .as("close() drains rather than discards, preserving order")
                        .containsExactly(0, 1, 2, 3, 4);
            }
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();   // idempotent
        }
    }

    /**
     * Once {@code close()} has latched {@code closed}, a submit is rejected by {@code enqueue}'s
     * lifecycle pre-check with {@code "checkpoint store is closed"} — deterministic, because the
     * test only submits after observing the closer parked in {@code writer.join()}.
     *
     * <p><b>Deliberately NOT pinned:</b> the genuinely racy overlap, where a submit passes that
     * pre-check BEFORE close latches and only then enqueues. Such a task can land either side of
     * the stop sentinel, and the writer may take and commit it before {@code enqueue}'s post-put
     * re-check reaches {@code queue.remove} — so its future may complete normally OR exceptionally
     * depending on scheduling. Asserting either outcome would manufacture a guarantee the engine
     * does not make.
     *
     * <p><b>Not yet pinned (deterministic recipe known) — follow-up for CK3.</b> The overlap CAN be
     * forced: open at capacity 1; park the writer on gate 1; fill the queue with a gate-2 task;
     * start a submitter and observe it blocked in {@code queue.put}, i.e. already PAST its
     * lifecycle pre-check; start {@code close()} and observe it blocked after latching
     * {@code closed}; then release gate 1 so the target task enqueues only after close latched, at
     * which point its post-put re-check deterministically completes it exceptionally, with gate 2
     * controlling cleanup. Not implemented here — this test pins only the deterministic pre-check
     * rejection.
     */
    @Test
    void submitAfterCloseHasLatchedIsRejected_raceWindowUnpinned(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try {
            createProbeTable(store);
            gate = parkWriter(store);

            CountDownLatch closeReturned = new CountDownLatch(1);
            Thread closer = Thread.ofPlatform().daemon().name("swath-test-closer").start(() -> {
                try {
                    store.close();
                } catch (Throwable ignored) {
                    // surfaced by the assertions below
                } finally {
                    closeReturned.countDown();
                }
            });
            awaitParked(closer);   // closed is latched; the closer is blocked joining the writer

            assertThatThrownBy(() -> store.enqueueForTesting(c -> {
                insertProbe(c, 777);
                return null;
            }))
                    .as("a submit issued after close() latched is rejected by the pre-check")
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store is closed");

            gate.release();
            gate = null;
            assertThat(closeReturned.await(30, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> store.countNodes(1L))
                    .as("and likewise after close() has returned")
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store is closed");
            try (Connection rd = reader(db)) {
                assertThat(probeCount(rd)).as("the rejected task never ran").isZero();
            }
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    // ---- interrupt semantics ------------------------------------------------------------

    /**
     * An interrupted wait for a commit throws {@link CheckpointException} and RESTORES the
     * interrupt flag — the caller's cancellation is neither swallowed nor escalated. The two
     * blocking points carry deliberately different messages, so this pins which one was hit.
     */
    @Test
    void interruptedAwaitOfCommitThrowsAndRestoresInterruptFlag(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try {
            RunMeta run = store.openRun(key("h-await-int"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            gate = parkWriter(store);

            AtomicReference<Throwable> thrown = new AtomicReference<>();
            AtomicBoolean flagRestored = new AtomicBoolean(false);
            CountDownLatch done = new CountDownLatch(1);
            Thread waiter = Thread.ofPlatform().daemon().name("swath-test-commit-waiter").start(() -> {
                try {
                    store.commitPage(new PageCommit(node, b("k1"), false));
                } catch (Throwable t) {
                    thrown.set(t);
                    flagRestored.set(Thread.currentThread().isInterrupted());
                } finally {
                    done.countDown();
                }
            });
            awaitParked(waiter);
            waiter.interrupt();
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();

            assertThat(thrown.get())
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("interrupted awaiting checkpoint commit")
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(flagRestored).as("interrupt flag is restored before rethrowing").isTrue();
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    /**
     * The other blocking point: an enqueue parked on a FULL queue. Same contract — distinct
     * message, interrupt flag restored — and the task is never queued.
     */
    @Test
    void interruptedEnqueueOnFullQueueThrowsAndRestoresInterruptFlag(@TempDir Path dir) throws Exception {
        SqliteCheckpointStore store = SqliteCheckpointStore.openForTesting(dir.resolve("ckpt.sqlite"), 1);
        Gate gate = null;
        try {
            gate = parkWriter(store);
            store.enqueueForTesting(c -> null);   // fills the capacity-1 queue

            AtomicReference<Throwable> thrown = new AtomicReference<>();
            AtomicBoolean flagRestored = new AtomicBoolean(false);
            CountDownLatch done = new CountDownLatch(1);
            Thread blocked = Thread.ofPlatform().daemon().name("swath-test-enqueuer").start(() -> {
                try {
                    store.enqueueForTesting(c -> null);
                } catch (Throwable t) {
                    thrown.set(t);
                    flagRestored.set(Thread.currentThread().isInterrupted());
                } finally {
                    done.countDown();
                }
            });
            awaitParked(blocked);
            blocked.interrupt();
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();

            assertThat(thrown.get())
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("interrupted enqueuing checkpoint write")
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(flagRestored).as("interrupt flag is restored before rethrowing").isTrue();
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    /**
     * Guards the writer-interrupt latch. Interrupting the writer thread itself makes
     * {@code writerLoop} FAIL THE STORE via {@code failStore} before it returns, rather than
     * exiting silently: {@code failure} is latched to a {@link CheckpointException} carrying
     * "checkpoint writer interrupted", {@code closed} is set, and — the load-bearing half —
     * {@code failStore} drains the queue, completing every task already queued behind the interrupt
     * <i>exceptionally</i> with that same latched cause. No work is left stranded: later submits are
     * rejected at {@code enqueue}'s pre-check, and {@code close()} returns cleanly with the store in
     * its failed state, nothing incomplete.
     *
     * <p><b>Reaching the {@code queue.take()} interrupt deterministically.</b> The bug lives on the
     * writer's IDLE wait, not on {@code runBatch}: a task interrupted mid-batch takes the already
     * latched {@code runBatch} path instead. So the writer is held inside a task that busy-spins
     * (ignoring interrupts) until released; while it is held there — already PAST this batch's queue
     * drain — the test (a) queues work behind it and (b) sets its interrupt flag. The flag is then
     * observed only at the writer's NEXT {@code queue.take()}: {@code ArrayBlockingQueue.take}
     * acquires the lock interruptibly, so it throws {@code InterruptedException} the instant the
     * writer loops back — even with the queued work present, which {@code failStore} then drains.
     */
    @Test
    void writerThreadInterruptFailsTheStoreAndDrainsPendingWork(@TempDir Path dir) throws Exception {
        SqliteCheckpointStore store = SqliteCheckpointStore.openForTesting(dir.resolve("ckpt.sqlite"), 64);
        AtomicBoolean release = new AtomicBoolean(false);
        try {
            AtomicReference<Thread> writerThread = new AtomicReference<>();
            CountDownLatch spinning = new CountDownLatch(1);

            // Hold the writer in a task that ignores interrupts, so the flag survives to the next
            // queue.take() — the writerLoop idle wait — rather than tripping runBatch.
            CompletableFuture<Object> held = store.enqueueForTesting(c -> {
                writerThread.set(Thread.currentThread());
                spinning.countDown();
                while (!release.get()) {
                    Thread.onSpinWait();
                }
                return null;
            });
            assertThat(spinning.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(writerThread.get().getName()).isEqualTo("swath-checkpoint-writer");

            // Queued behind the held batch: these stay in the queue (already drained past).
            CompletableFuture<Object> pendingA = store.enqueueForTesting(c -> null);
            CompletableFuture<Object> pendingB = store.enqueueForTesting(c -> null);

            writerThread.get().interrupt();   // flag set while the writer is uninterruptibly busy
            release.set(true);                // let the held task commit; the loop then hits take()
            held.get(30, TimeUnit.SECONDS);   // held task itself commits normally

            writerThread.get().join(30_000);
            assertThat(writerThread.get().isAlive())
                    .as("the writer loop exits once the interrupt trips its next take()")
                    .isFalse();

            // failStore latched the interrupt as the store's failure and drained the queue.
            Throwable latched = failureCauseOf(pendingA);
            assertThat(latched)
                    .as("queued work is failed with the latched interrupt cause")
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint writer interrupted")
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(failureCauseOf(pendingB))
                    .as("every task queued behind the interrupt shares that one latched cause")
                    .isSameAs(latched);

            // Later submits are rejected at the pre-check, caused by the same latched exception.
            assertThatThrownBy(() -> store.enqueueForTesting(c -> null))
                    .as("the store is permanently failed, so it no longer accepts work")
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store has failed")
                    .cause().isSameAs(latched);

            // close() does not strand: it returns cleanly with the store still failed.
            store.close();
            assertThatThrownBy(() -> store.countNodes(1L))
                    .as("close() left the store failed, nothing incomplete")
                    .isInstanceOf(CheckpointException.class)
                    .hasMessage("checkpoint store has failed")
                    .cause().isSameAs(latched);
        } finally {
            release.set(true); // always free the spinning writer, or close()'s join hangs on failure
            store.close();
        }
    }

    // ---- I1 commit-before-emit ------------------------------------------------------------

    /**
     * I1's load-bearing direction: {@code commitPage} DOES NOT RETURN until its transaction has
     * committed, so a caller that emits on return can never emit an uncommitted page. Proven by
     * holding the writer: the calling thread stays blocked and the durable cursor stays put, and
     * only once the writer is released does the call return with the advance already durable.
     *
     * <p>The blocked-while-parked half is what makes this a real guard — without it a
     * fire-and-forget {@code commitPage} would still pass, because the writer would commit before
     * any assertion on the file could run. The non-return window only opens once the committer is
     * observed PARKED, which establishes it truly reached the blocking wait inside
     * {@code commitPage}, so the negative assertion cannot pass vacuously.
     */
    @Test
    void commitPageDoesNotReturnUntilItsTransactionIsCommitted_I1(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try {
            RunMeta run = store.openRun(key("h-i1-sync"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));
            store.commitPage(new PageCommit(node, b("k-first"), false));
            assertThat(durableCursor(db, node))
                    .as("commitPage returned: the advance is ALREADY durable")
                    .isEqualTo(b("k-first"));

            gate = parkWriter(store);
            AtomicReference<Throwable> error = new AtomicReference<>();
            CountDownLatch aboutToCommit = new CountDownLatch(1);
            CountDownLatch returned = new CountDownLatch(1);
            Thread committer = Thread.ofPlatform().daemon().name("swath-test-committer").start(() -> {
                try {
                    aboutToCommit.countDown();
                    store.commitPage(new PageCommit(node, b("k-second"), false));
                } catch (Throwable t) {
                    error.set(t);
                } finally {
                    returned.countDown();
                }
            });
            assertThat(aboutToCommit.await(30, TimeUnit.SECONDS)).isTrue();
            // The latch alone only proves the thread started — it could be descheduled before
            // entering commitPage, making the window below vacuous. Waiting until it is parked
            // establishes it actually reached the blocking get() INSIDE commitPage.
            awaitParked(committer);
            assertThat(returned.await(1, TimeUnit.SECONDS))
                    .as("commitPage must NOT return while its transaction is un-committed")
                    .isFalse();
            assertThat(durableCursor(db, node))
                    .as("the writer is parked on an earlier task, so this one has not run at all")
                    .isEqualTo(b("k-first"));

            gate.release();
            gate = null;
            assertThat(returned.await(30, TimeUnit.SECONDS)).isTrue();
            assertThat(error.get()).isNull();
            assertThat(durableCursor(db, node))
                    .as("commitPage returned only once the advance was durable")
                    .isEqualTo(b("k-second"));
            committer.join(5_000);
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    /**
     * The async counterpart: {@code commitPageAsync}'s future resolving <b>successfully</b> implies
     * the advance is durable, which is what lets a worker emit outside its lock and still honour I1.
     *
     * <p>The claim covers only completion DRIVEN BY THE STORE. {@code commitPageAsync} hands the
     * caller a live {@link CompletableFuture}: calling {@code async.complete(null)} on it while the
     * writer is gated resolves it successfully with nothing durable, so caller-driven completion
     * carries no guarantee at all.
     *
     * <p>Neither other direction is claimed, because both are false. <b>Exceptional</b> resolution
     * does NOT imply rollback — {@code runBatch}'s metrics calls run after {@code conn.commit()}
     * inside the same {@code try}, so a Throwable there fails the futures over an already-durable
     * batch (and a caller may cancel besides). And an <b>unresolved</b> future does not imply "not
     * durable", because the commit precedes the future-completion loop (see
     * {@link #resolvedBatchFutureImpliesTheWholeBatchIsAlreadyDurable}). So the only "nothing is
     * durable" claim made here is the narrow one this fixture actually establishes: the writer has
     * not yet BEGUN the task, because it is parked on an earlier one.
     */
    @Test
    void asyncCommitIsDurableOnceTheStoreResolvesItsFutureSuccessfully_I1(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        SqliteCheckpointStore store = openStore(db);
        Gate gate = null;
        try {
            RunMeta run = store.openRun(key("h-i1-async"), false, false);
            long node = store.insertNode(NodeSpec.rootRange(run.id()));

            gate = parkWriter(store);
            CompletableFuture<Void> async = store.commitPageAsync(new PageCommit(node, b("k-async"), false));
            assertThat(async.isDone()).isFalse();
            assertThat(durableCursor(db, node))
                    .as("the writer is parked on an earlier task, so this one has not run at all")
                    .isNull();

            gate.release();
            gate = null;
            async.get(30, TimeUnit.SECONDS);
            assertThat(durableCursor(db, node))
                    .as("the future resolved, so the advance is durable and the caller may emit")
                    .isEqualTo(b("k-async"));
        } finally {
            if (gate != null) {
                gate.release();
            }
            store.close();
        }
    }

    /**
     * Recovery after the store is ABANDONED — never closed, writer never asked to drain, JDBC
     * connection never released — reopens onto exactly the last committed advance.
     *
     * <p><b>Honest scope:</b> this is abandonment inside one JVM, NOT process death. It does not
     * exercise WAL recovery after an untidy exit, fsync loss, or a torn write. The repo's real
     * kill-9 guard is {@code HardCrashSigkillResume{Process,Matrix}IT} in swath-cli (opt-in via
     * {@code -Dswath.it.sigkill=true}); this characterizes only the store-level property beneath
     * it — that durability survives the owning store object simply going away.
     */
    @Test
    void abandonedStoreWithoutCloseReopensAtLastCommittedAdvance(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("ckpt.sqlite");
        // Daemon writer: the store stays un-closed across the reopen below, so it must not be able
        // to hang the JVM meanwhile. It is left with its connection open and its writer parked in
        // queue.take() — which is the abandonment this test is about.
        SqliteCheckpointStore abandoned = SqliteCheckpointStore.openForTesting(db, 4096);
        try {
            RunMeta run = abandoned.openRun(key("h-abandon"), false, false);
            long node = abandoned.insertNode(NodeSpec.rootRange(run.id()));
            abandoned.commitPage(new PageCommit(node, b("k-last"), false));

            try (SqliteCheckpointStore reopened = SqliteCheckpointStore.open(db)) {
                List<Node> resumable = reopened.loadResumable(run.id(), false);
                assertThat(resumable).hasSize(1);
                assertThat(resumable.get(0).cursor())
                        .as("resume picks up at exactly the last committed advance")
                        .isEqualTo(b("k-last"));
            }
        } finally {
            // The abandonment only needs to hold across the reopen above; releasing the writer
            // thread and the JDBC connection afterwards does not weaken what was proved, and
            // leaving them alive would leak both for the life of the worker JVM.
            abandoned.close();
        }
    }
}
