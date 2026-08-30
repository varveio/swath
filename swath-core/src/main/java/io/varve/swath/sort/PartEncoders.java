/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/**
 * Bounded final-part encoders. Ordinal {@code k} stays on lane {@code k mod N}, so every batch of a
 * part reaches one writer in sequence while independent parts can close out of order.
 */
final class PartEncoders implements AutoCloseable {
    static final int QUEUE_DEPTH = 4;
    /** Planning estimate for one open parquet writer's buffered heap. */
    static final long WRITER_HEAP_ESTIMATE_BYTES = 8L << 20;
    private static final long FAILURE_CHECK_MILLIS = 100;

    /** One durably closed part, retaining only the state the ordered assembler consumes. */
    record CompletedPart(int ordinal, Path path, SortedFileWriter writer) {
    }

    private final List<Lane> lanes;
    private final List<Thread> threads = new ArrayList<>();
    private final ConcurrentLinkedQueue<CompletedPart> completed = new ConcurrentLinkedQueue<>();
    private final PipelineFailure failure;
    private final PipelinePartSizer sizer;
    private final SortMetrics metrics;
    private final AtomicBoolean aborting = new AtomicBoolean();
    private final AtomicInteger partsOpen = new AtomicInteger();
    private final LongConsumer progressCallback;

    PartEncoders(int count, Path stagingDir, SortedFileWriterFactory factory,
            Comparator<ListEntry> comparator, DuplicateHook hook, EqualKeyPolicy equalKeyPolicy,
            SortMetrics metrics, PipelineFailure failure, PipelinePartSizer sizer,
            LongConsumer progressCallback) {
        if (count < 1) {
            throw new IllegalArgumentException("pipeline encoder count must be positive");
        }
        this.failure = failure;
        this.sizer = sizer;
        this.metrics = metrics;
        this.progressCallback = progressCallback;
        metrics.bindPipelinePartsOpen(partsOpen);
        this.lanes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Lane lane = new Lane(stagingDir, factory.forOutputSequence(), comparator, hook,
                    equalKeyPolicy);
            lanes.add(lane);
            threads.add(Thread.ofVirtual().name("sort-pipeline-encoder-" + i).start(lane::run));
        }
    }

    void submit(PipelineBatch batch) {
        ArrayBlockingQueue<Item> queue = lanes.get(batch.partOrdinal() % lanes.size()).queue;
        if (queue.offer(new Item.Batch(batch))) {
            return;
        }
        long started = System.nanoTime();
        try {
            while (!queue.offer(new Item.Batch(batch), FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                failure.check();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MergeCancellation.Cancelled();
        } finally {
            long waited = System.nanoTime() - started;
            metrics.recordPipelineRouterWait(waited);
            metrics.recordPipelineEncoderQueueFull(waited);
        }
    }

    List<CompletedPart> finish(int expectedParts) {
        for (Lane lane : lanes) {
            putStop(lane.queue);
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

    private void putStop(ArrayBlockingQueue<Item> queue) {
        try {
            while (!queue.offer(Item.Stop.INSTANCE, FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                failure.check();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MergeCancellation.Cancelled();
        }
    }

    @Override
    public void close() {
        if (aborting.compareAndSet(false, true)) {
            for (Thread thread : threads) {
                thread.interrupt();
            }
        }
        joinAll();
    }

    private void joinAll() {
        boolean interrupted = false;
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private final class Lane {
        private final ArrayBlockingQueue<Item> queue = new ArrayBlockingQueue<>(QUEUE_DEPTH);
        private final Path stagingDir;
        private final SortedFileWriterFactory factory;
        private final AdjacentEntryGuard entryGuard;
        private SortedFileWriter writer;
        private Path path;
        private int ordinal = -1;
        private long logicalBytes;
        private long lastSequence = -1;

        Lane(Path stagingDir, SortedFileWriterFactory factory, Comparator<ListEntry> comparator,
                DuplicateHook hook, EqualKeyPolicy equalKeyPolicy) {
            this.stagingDir = stagingDir;
            this.factory = factory;
            this.entryGuard = new AdjacentEntryGuard(
                    comparator, hook, equalKeyPolicy, metrics, "pipeline");
        }

        void run() {
            try {
                while (true) {
                    Item item = queue.take();
                    if (item instanceof Item.Stop) {
                        if (writer != null) {
                            throw new IllegalStateException("encoder stopped with an open part");
                        }
                        return;
                    }
                    encode(((Item.Batch) item).batch());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!aborting.get()) {
                    failure.record(new MergeCancellation.Cancelled());
                }
            } catch (Throwable e) {
                failure.record(e);
            } finally {
                discardOpen();
            }
        }

        private void encode(PipelineBatch batch) throws IOException {
            failure.check();
            if (batch.sequence() <= lastSequence) {
                throw new IllegalStateException("pipeline batch sequence regressed");
            }
            lastSequence = batch.sequence();
            if (batch.partFirst()) {
                if (writer != null) {
                    throw new IllegalStateException("pipeline encoder opened overlapping parts");
                }
                ordinal = batch.partOrdinal();
                path = stagingDir.resolve(StagingNames.pipelineTmp(ordinal));
                writer = factory.create(path, ordinal + 1);
                partsOpen.incrementAndGet();
            } else if (writer == null || ordinal != batch.partOrdinal()) {
                throw new IllegalStateException("pipeline batch arrived without its open part");
            }
            logicalBytes = Math.addExact(logicalBytes, batch.payload().logicalBytes());
            switch (batch.payload()) {
                case PipelineBatch.WholePage whole -> writePage(whole.page());
                case PipelineBatch.Rows rows -> {
                    for (ListEntry entry : rows.entries()) {
                        write(entry);
                    }
                }
                case PipelineBatch.Empty ignored -> { }
            }
            if (batch.payload().rowCount() > 0) {
                progressCallback.accept(batch.payload().rowCount());
            }
            if (batch.partLast()) {
                if (batch.mergeEnd()) {
                    writer.markFinal();
                }
                writer.close();
                long bytes = Files.size(path);
                completed.add(new CompletedPart(ordinal, path, writer));
                sizer.completed(bytes, logicalBytes);
                writer = null;
                path = null;
                // The router closes parts only across a strict raw-key boundary, so no adjacent
                // comparator-equal pair can straddle this part-local guard reset.
                entryGuard.reset();
                logicalBytes = 0;
                partsOpen.decrementAndGet();
            }
        }

        private void writePage(PageBlock page) throws IOException {
            PageBlockCursor cursor = page.cursor();
            while (cursor.hasNext()) {
                write(cursor.next());
            }
        }

        private void write(ListEntry entry) throws IOException {
            entryGuard.accept(entry);
            writer.write(entry);
        }

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
                partsOpen.decrementAndGet();
            }
        }
    }

    private sealed interface Item {
        record Batch(PipelineBatch batch) implements Item {
        }

        enum Stop implements Item {
            INSTANCE
        }
    }
}
