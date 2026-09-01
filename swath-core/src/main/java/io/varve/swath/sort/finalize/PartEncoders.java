/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageBlockCursor;
import io.varve.swath.sort.spill.PageRef;
import io.varve.swath.sort.spill.PageRunSegmentIo;
import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/**
 * Work-sharing complete-plan encoders. Every worker takes the next merge-order plan from one bounded
 * queue, reads referenced pages positionally through shared segment channels, and owns at most one
 * writer. Plan ordinals—not worker identity—restore publication order. A shared queue avoids both a
 * slow striped lane stalling the router and idle workers sitting beside queued work; its capacity is
 * still exactly {@code encoderCount * QUEUE_DEPTH} plans.
 */
final class PartEncoders implements AutoCloseable {
    static final int QUEUE_DEPTH = 2;
    static final long WRITER_HEAP_ESTIMATE_BYTES = 8L << 20;
    private static final long FAILURE_CHECK_MILLIS = 100;

    /**
     * A footer-closed temporary and its close-gated metadata owner. The writer remains attached so
     * publication can verify raw bounds without reopening or becoming a second footer owner.
     */
    record CompletedPart(int ordinal, Path path, SortedFileWriter writer) {
    }

    private final List<Lane> lanes;
    private final ArrayBlockingQueue<Item> queue;
    private final List<Thread> threads = new ArrayList<>();
    private final ConcurrentLinkedQueue<CompletedPart> completed = new ConcurrentLinkedQueue<>();
    private final FinalizationFailure failure;
    private final PartSizer sizer;
    private final SortMetrics metrics;
    private final AtomicBoolean aborting = new AtomicBoolean();
    private final AtomicInteger partsOpen = new AtomicInteger();
    private final LongConsumer progressCallback;
    private final CountDownLatch firstCompletion = new CountDownLatch(1);

    /**
     * Start exactly {@code count} consumers over one shared bounded queue. Each lane receives an
     * independent output-sequence factory because Parquet writer state is not shareable; segment
     * channels are shared because all encoder reads are positional and therefore commute.
     */
    PartEncoders(int count, List<PageRunSegmentIo> segments, long clusterBudgetBytes,
            Path stagingDir, SortedFileWriterFactory factory, Comparator<ListEntry> comparator,
            DuplicateHook hook, EqualKeyPolicy equalKeyPolicy, SortMetrics metrics,
            FinalizationFailure failure, PartSizer sizer, LongConsumer progressCallback) {
        if (count < 1 || clusterBudgetBytes < 1) {
            throw new IllegalArgumentException("pipeline encoder settings must be positive");
        }
        this.failure = failure;
        this.sizer = sizer;
        this.metrics = metrics;
        this.progressCallback = progressCallback;
        metrics.bindPipelinePartsOpen(partsOpen);
        queue = new ArrayBlockingQueue<>(Math.multiplyExact(count, QUEUE_DEPTH));
        lanes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Lane lane = new Lane(segments, clusterBudgetBytes, stagingDir,
                    factory.forOutputSequence(), comparator, hook, equalKeyPolicy);
            lanes.add(lane);
            threads.add(Thread.ofVirtual().name("sort-pipeline-encoder-" + i).start(lane::run));
        }
    }

    /**
     * Submit one complete plan or surface a peer failure while back-pressured. Timed offers are
     * required: a blocking {@code put} could strand the router forever after all consumers fail.
     * The queue is shared so plan availability, rather than an ordinal-to-lane stripe, assigns work.
     */
    void submit(PartPlan plan) {
        Item.Plan item = new Item.Plan(plan);
        if (queue.offer(item)) {
            return;
        }
        metrics.recordStealReason("SORT", "pipeline_plan_queue_saturated");
        long started = System.nanoTime();
        try {
            while (!queue.offer(item, FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                failure.check();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MergeCancellation.Cancelled();
        } finally {
            long waited = System.nanoTime() - started;
            metrics.recordPipelineRouterWait(waited);
            metrics.recordPipelinePlanQueueWait(waited);
        }
    }

    /**
     * Stop after all submitted plans, then return the dense merge-order completion sequence.
     * Completion order is intentionally ignored because a later, smaller plan may close first;
     * ordinals are the only publication-order authority.
     */
    List<CompletedPart> finish(int expectedParts) {
        for (int lane = 0; lane < lanes.size(); lane++) {
            putStop();
        }
        joinAll();
        failure.check();
        List<CompletedPart> result = completed.stream()
                .sorted(Comparator.comparingInt(CompletedPart::ordinal)).toList();
        if (result.size() != expectedParts) {
            throw new IllegalStateException("pipeline completed part count mismatch: expected="
                    + expectedParts + " actual=" + result.size());
        }
        for (int i = 0; i < result.size(); i++) {
            if (result.get(i).ordinal() != i) {
                throw new IllegalStateException("pipeline part ordinals are not dense at " + i);
            }
        }
        return result;
    }

    /**
     * Wait for the first durable part so calibrated routing cannot run a whole queue-depth ahead.
     * The wait observes the failure relay at a fixed cadence, preventing a failed warm-up encoder
     * from leaving the single router asleep on a latch that can no longer count down.
     */
    void awaitFirstCompletion() {
        try {
            while (!firstCompletion.await(FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                failure.check();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MergeCancellation.Cancelled();
        }
    }

    /**
     * Enqueue one terminal token per consumer after all plans. Tokens share normal queue ordering,
     * which guarantees no lane exits ahead of accepted work; timed admission still detects failure.
     */
    private void putStop() {
        try {
            while (!queue.offer(Item.Stop.INSTANCE, FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                failure.check();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MergeCancellation.Cancelled();
        }
    }

    /**
     * Abort every lane and wait for writer discard. This is an abort operation, not graceful finish:
     * callers that want publication must use {@link #finish(int)} before dropping this owner.
     */
    @Override
    public void close() {
        if (aborting.compareAndSet(false, true)) {
            for (Thread thread : threads) {
                thread.interrupt();
            }
        }
        joinAll();
    }

    /**
     * Join uninterruptibly enough to reclaim every writer, then restore interruption. Losing the
     * interrupt would violate caller cancellation; returning with a live lane would let cleanup race
     * an open file descriptor and temporary writer.
     */
    private void joinAll() {
        boolean interrupted = false;
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                    failure.record(new MergeCancellation.Cancelled());
                    if (aborting.compareAndSet(false, true)) {
                        for (Thread peer : threads) {
                            peer.interrupt();
                        }
                    }
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private final class Lane {
        private final List<PageRunSegmentIo> segments;
        private final Path stagingDir;
        private final SortedFileWriterFactory factory;
        private final Comparator<ListEntry> comparator;
        private final AdjacentEntryGuard entryGuard;
        private final DecodedPageBudget decodedBudget;
        private SortedFileWriter writer;
        private Path path;
        /** This lane's sub-rung remainder; single-thread ownership avoids shared progress state. */
        private long progressRows;

        /**
         * Give one worker its own guard, decoded budget, and output factory. None of those objects is
         * shared between virtual threads, so their hot row path needs no synchronization.
         */
        Lane(List<PageRunSegmentIo> segments, long clusterBudgetBytes, Path stagingDir,
                SortedFileWriterFactory factory, Comparator<ListEntry> comparator,
                DuplicateHook hook, EqualKeyPolicy equalKeyPolicy) {
            this.segments = segments;
            this.stagingDir = stagingDir;
            this.factory = factory;
            this.comparator = comparator;
            decodedBudget = new DecodedPageBudget(clusterBudgetBytes, metrics);
            entryGuard = new AdjacentEntryGuard(
                    comparator, hook, equalKeyPolicy, metrics, "pipeline");
        }

        /**
         * Consume until a terminal token or first failure, always discarding a partially open part.
         * Expected asynchronous-close noise during coordinated abort is suppressed; without that
         * distinction it could replace the original typed failure in the shared relay.
         */
        void run() {
            try {
                while (true) {
                    Item item = nextItem();
                    if (item instanceof Item.Stop) {
                        return;
                    }
                    execute(((Item.Plan) item).value());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!aborting.get()) {
                    failure.record(new MergeCancellation.Cancelled());
                }
            } catch (AsynchronousCloseException e) {
                if (!aborting.get()) {
                    failure.record(e);
                }
            } catch (Throwable e) {
                failure.record(e);
            } finally {
                discardOpen();
            }
        }

        /**
         * Wait for work while polling peer health. A plain queue take has no wakeup when another lane
         * fails before the producer can enqueue stop tokens, which creates a shutdown deadlock.
         */
        private Item nextItem() throws InterruptedException {
            Item item;
            while ((item = queue.poll(FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) == null) {
                failure.check();
            }
            return item;
        }

        /**
         * Open, execute, and durably close one complete plan without retaining decoded pages between
         * plans. The file index comes from the router ordinal, not lane identity, so arbitrary work
         * sharing cannot perturb footer stamps or final lexical order.
         */
        private void execute(PartPlan plan) throws IOException {
            failure.check();
            if (writer != null) {
                throw new IllegalStateException("encoder received overlapping plans");
            }
            path = stagingDir.resolve(StagingNames.pipelineTmp(plan.ordinal()));
            writer = factory.create(path, plan.ordinal() + 1);
            partsOpen.incrementAndGet();
            for (PartPlan.Item item : plan.items()) {
                MergeCancellation.check();
                switch (item) {
                    case PartPlan.Page page -> writePage(read(page.ref()));
                    case PartPlan.Cluster cluster -> writeCluster(cluster.refs());
                }
            }
            metrics.recordPipelineDecodedPagePeak(decodedBudget.peakResidentBytes());
            if (plan.mergeEnd()) {
                writer.markFinal();
            }
            writer.close();
            long bytes = Files.size(path);
            completed.add(new CompletedPart(plan.ordinal(), path, writer));
            sizer.completed(bytes, plan.logicalBytes());
            if (plan.ordinal() == 0) {
                firstCompletion.countDown();
            }
            writer = null;
            path = null;
            entryGuard.reset();
            partsOpen.decrementAndGet();
            flushProgress();
        }

        /**
         * Positionally read and CRC-verify exactly the frame named by {@code ref}. Shared channels
         * avoid K descriptors per encoder; positional reads avoid a shared mutable file position.
         */
        private PageBlock read(PageRef ref) throws IOException {
            long started = System.nanoTime();
            try {
                PageBlock page = segments.get(ref.segmentId()).readPage(ref);
                metrics.recordPipelineEncoderPageReads(1);
                return page;
            } finally {
                metrics.recordPipelineEncoderReadWait(System.nanoTime() - started);
            }
        }

        /**
         * Reserve a whole page in retained-byte units before cursor construction can decompress it.
         * Release in {@code finally} so a row decode or writer fault cannot leak the lane's budget.
         */
        private void writePage(PageBlock page) throws IOException {
            long reserved = decodedBudget.reserve(page);
            try {
                PageBlockCursor cursor = page.cursor();
                while (cursor.hasNext()) {
                    write(cursor.next());
                }
            } finally {
                decodedBudget.release(reserved);
            }
        }

        /**
         * Incrementally admit a cluster so a long transitive chain need not be resident at once.
         * A future page enters only when its minimum can compete with the next active row; admitting
         * the full component eagerly would make valid overlap consume O(component pages) heap.
         */
        private void writeCluster(List<PageRef> refs) throws IOException {
            PageRowMerger merger = new PageRowMerger(comparator);
            int next = 0;
            long reserved = 0;
            try {
                PageBlock first = read(refs.get(next));
                reserved = decodedBudget.reserve(first);
                merger.add(refs.get(next).segmentId(), first, reserved);
                reserved = 0;
                next++;
                while (merger.hasNext()) {
                    while (next < refs.size()
                            && KeyBytes.compareUnsigned(
                                    refs.get(next).minKey(), merger.nextKey()) <= 0) {
                        PageRef ref = refs.get(next++);
                        PageBlock page = read(ref);
                        long pageBytes = decodedBudget.reserve(page);
                        try {
                            merger.add(ref.segmentId(), page, pageBytes);
                            pageBytes = 0;
                        } finally {
                            decodedBudget.release(pageBytes);
                        }
                    }
                    write(merger.next());
                    decodedBudget.release(merger.releasedBytes());
                }
                if (next != refs.size()) {
                    // The router closes a transitive range component before dispatch. Keep this
                    // check because silently dropping a future ref would defeat cardinality only
                    // after an expensive full encode and could publish misordered output if counts
                    // happened to match after separate corruption.
                    throw new IllegalStateException("cluster refs were not fully consumed");
                }
            } finally {
                decodedBudget.release(reserved);
                decodedBudget.release(merger.releaseAllBytes());
            }
        }

        /**
         * Enforce local adjacency before writing and report progress at the shared merge rung. The
         * greater-than-or-equal check remains correct if a future bulk path increments by more than
         * one row, instead of silently skipping the heartbeat forever.
         */
        private void write(ListEntry entry) throws IOException {
            entryGuard.accept(entry);
            writer.write(entry);
            if (++progressRows >= KWayMerge.PROGRESS_BATCH_ROWS) {
                progressCallback.accept(progressRows);
                progressRows = 0;
            }
        }

        /** Report the final sub-rung remainder only after its part is durably closed. */
        private void flushProgress() {
            if (progressRows > 0) {
                progressCallback.accept(progressRows);
                progressRows = 0;
            }
        }

        /**
         * Reclaim a partially written file without publishing close-gated metadata. Discard failure
         * joins the relay as a secondary fault; it must not make the lane appear successfully closed.
         */
        private void discardOpen() {
            if (writer == null) {
                return;
            }
            try {
                writer.discard();
            } catch (IOException e) {
                failure.record(e);
            } finally {
                writer = null;
                path = null;
                partsOpen.decrementAndGet();
            }
        }
    }

    /** Queue protocol kept disjoint from data so a valid empty plan cannot be confused with stop. */
    private sealed interface Item {
        record Plan(PartPlan value) implements Item {
        }

        enum Stop implements Item {
            INSTANCE
        }
    }
}
