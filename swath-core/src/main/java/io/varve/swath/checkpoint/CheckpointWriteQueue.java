/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.error.CheckpointException;
import io.varve.swath.observability.RunMetrics;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The checkpoint <b>writer engine</b>: the single-writer task queue, the batching writer thread,
 * the failure/closed latch machinery and the close handshake — extracted from
 * {@link SqliteCheckpointStore} so the store is left as schema + DAO + delegation. It owns the sole
 * JDBC connection once the store hands it over and knows nothing about checkpoint SQL: a caller
 * enqueues a {@link SqlOp} ("run this op on the writer connection inside the batch transaction")
 * and the engine batches, commits, and resolves the op's future.
 *
 * <p><b>Single-writer protocol</b> (algorithms.md §4.1): every operation is a task on one dedicated
 * writer thread holding the only JDBC connection — SQLite WAL is single-writer, and routing all
 * access through one thread keeps a run's commits strictly serialized (so a split can be a
 * standalone, CAS-guarded transaction). The writer drains its queue in <b>arrival order</b>,
 * batching consecutive tasks into one transaction (flush on queue-empty or at {@link #MAX_BATCH});
 * batching never reorders a node's commits. A {@link #submit} caller blocks until its transaction
 * is durably committed (I1 commit-before-emit); {@link #enqueue} is the non-blocking primitive
 * whose future resolves after that same durable commit.
 */
final class CheckpointWriteQueue {

    private static final Logger log = LoggerFactory.getLogger(CheckpointWriteQueue.class);

    /** Consecutive queued tasks coalesce into one transaction, capped at this many. */
    static final int MAX_BATCH = 256;
    /** Default bound on the writer queue when a caller does not specify one. */
    static final int DEFAULT_QUEUE_CAPACITY = 4096;

    /** A unit of work for the writer thread; its future resolves after the txn commits. */
    private static final class Task {
        final SqlOp op;
        final CompletableFuture<Object> future = new CompletableFuture<>();
        // Enqueue timestamp for swath.checkpoint.queue.wait — set once, read once
        // per task at batch-commit time (runBatch), no hot-path cost beyond one nanoTime() call.
        final long enqueuedAtNanos = System.nanoTime();
        Object result;

        Task(SqlOp op) {
            this.op = op;
        }
    }

    /** The SQL-execution callback the store hands the engine; the engine treats it opaquely. */
    @FunctionalInterface
    interface SqlOp {
        Object run(Connection c) throws SQLException, CheckpointException;
    }

    /** Shutdown sentinel (its op is never executed). */
    private static final Task STOP = new Task(c -> null);

    private final Connection conn;
    private final BlockingQueue<Task> queue;
    private final Thread writer;
    private final Object lifecycleLock = new Object();
    // Optional (null-safe) run metrics — commit latency, queue depth/wait. (Resume-engagement
    // counters stay with the store's DAO side.) Null in every test that opens the store without a
    // run (the overwhelming majority; only ListCommand's production path wires a live RunMetrics).
    private final RunMetrics metrics;
    private volatile boolean closed = false;
    private volatile CheckpointException failure;

    CheckpointWriteQueue(Connection conn, int queueCapacity, boolean daemonWriter, RunMetrics metrics) {
        this.conn = conn;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.metrics = metrics;
        if (metrics != null) {
            metrics.registerCheckpointQueueDepthGauge(queue::size);
        }
        // The thread name is operator-facing identity (thread dumps, and the writer-interrupt
        // test asserts it verbatim): it stays byte-identical to its pre-extraction value.
        Thread.Builder.OfPlatform writerBuilder = Thread.ofPlatform().name("swath-checkpoint-writer");
        if (daemonWriter) {
            writerBuilder = writerBuilder.daemon();
        }
        this.writer = writerBuilder.unstarted(this::writerLoop);
        this.writer.start();
    }

    // ---- writer thread --------------------------------------------------------

    private void writerLoop() {
        List<Task> batch = new ArrayList<>(MAX_BATCH);
        try {
            while (true) {
                Task first;
                try {
                    first = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failStore(new CheckpointException("checkpoint writer interrupted", e), List.of());
                    return;
                }
                batch.clear();
                boolean stop = false;
                if (first == STOP) {
                    stop = true;
                } else {
                    batch.add(first);
                }
                // Drain whatever is immediately available (arrival order preserved).
                Task t;
                while (batch.size() < MAX_BATCH && (t = queue.poll()) != null) {
                    if (t == STOP) {
                        stop = true;
                        break;
                    }
                    batch.add(t);
                }
                if (!batch.isEmpty()) {
                    runBatch(batch);
                    if (failure != null) {
                        return;
                    }
                }
                if (stop) {
                    return;
                }
            }
        } catch (Throwable e) {
            failStore(e, batch);
        } finally {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // closing the connection on shutdown — nothing actionable
            }
        }
    }

    private void runBatch(List<Task> batch) {
        // Commit latency covers op-execution + conn.commit() (the I1 WAL-fsync
        // critical path) for the whole batch; queue wait is measured per-task from enqueue to
        // here (batch drain), both timed only on the successful path (a rollback's failure is
        // the writer-thread's terminal state, not a representative commit-latency sample).
        long batchStartedNanos = metrics == null ? 0L : System.nanoTime();
        try {
            for (Task t : batch) {
                t.result = t.op.run(conn);
            }
            conn.commit();
        } catch (Throwable e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
                // rollback best-effort; the original failure is what we surface
            }
            failStore(e, batch);
            return;
        }

        long committedAtNanos = metrics == null ? 0L : System.nanoTime();
        // Once commit succeeds, report success before doing any fallible post-commit work. A
        // metrics failure cannot roll the transaction back and must not make already-durable work
        // appear failed to callers.
        try {
            for (Task t : batch) {
                t.future.complete(t.result);
            }
        } catch (Throwable completionFailure) {
            // CompletableFuture normally absorbs dependent failures, but never let an abnormal
            // completion implementation terminate the only writer without latching the store.
            for (Task t : batch) {
                try {
                    t.future.complete(t.result);
                } catch (Throwable retryFailure) {
                    if (retryFailure != completionFailure) {
                        completionFailure.addSuppressed(retryFailure);
                    }
                }
            }
            failStore(completionFailure, List.of());
            return;
        }
        if (metrics != null) {
            try {
                metrics.recordCheckpointCommit(
                        committedAtNanos - batchStartedNanos, batch.size());
                for (Task t : batch) {
                    metrics.recordCheckpointQueueWait(batchStartedNanos - t.enqueuedAtNanos);
                }
            } catch (Throwable e) {
                log.warn("checkpoint metrics recording failed after a successful commit", e);
            }
        }
    }

    private void failStore(Throwable cause, List<Task> batch) {
        CheckpointException ce = cause instanceof CheckpointException checkpoint
                ? checkpoint
                : new CheckpointException("checkpoint writer failed", cause);
        synchronized (lifecycleLock) {
            if (failure == null) {
                failure = ce;
            } else {
                ce = failure;
            }
            closed = true;
            for (Task t : batch) {
                t.future.completeExceptionally(ce);
            }
            Task pending;
            while ((pending = queue.poll()) != null) {
                if (pending != STOP) {
                    pending.future.completeExceptionally(ce);
                }
            }
        }
    }

    /**
     * Enqueue {@code op} on the writer thread and return its completion future
     * <b>without blocking</b> — the future resolves after the txn durably commits.
     * The non-blocking primitive under both {@link #submit} and {@code commitPageAsync}.
     */
    CompletableFuture<Object> enqueue(SqlOp op) throws CheckpointException {
        Task task = new Task(op);
        synchronized (lifecycleLock) {
            if (failure != null) {
                throw new CheckpointException("checkpoint store has failed", failure);
            }
            if (closed) {
                throw new CheckpointException("checkpoint store is closed");
            }
        }
        try {
            queue.put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CheckpointException("interrupted enqueuing checkpoint write", e);
        }
        synchronized (lifecycleLock) {
            if (failure != null) {
                queue.remove(task);
                task.future.completeExceptionally(failure);
            } else if (closed) {
                queue.remove(task);
                task.future.completeExceptionally(new CheckpointException("checkpoint store is closed"));
            }
        }
        return task.future;
    }

    @SuppressWarnings("unchecked")
    <T> T submit(SqlOp op) throws CheckpointException {
        return (T) await(enqueue(op));
    }

    /** Block on a writer-thread future, translating its failures to {@link CheckpointException}. */
    private static Object await(CompletableFuture<Object> future) throws CheckpointException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CheckpointException("interrupted awaiting checkpoint commit", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof CheckpointException ce) {
                throw ce;
            }
            throw new CheckpointException("checkpoint commit failed", e.getCause());
        }
    }

    /**
     * Signal the writer to drain and stop, then block until it exits. The stop sentinel goes to the
     * tail of the queue, so everything already enqueued still runs and commits, in order, before the
     * writer closes the connection. Idempotent, and safe on an already-failed store.
     */
    void close() throws CheckpointException {
        boolean signalStop = false;
        synchronized (lifecycleLock) {
            if (!closed) {
                closed = true;
                signalStop = true;
            }
        }
        try {
            if (signalStop) {
                queue.put(STOP);
            }
            writer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CheckpointException("interrupted closing checkpoint store", e);
        }
    }
}
