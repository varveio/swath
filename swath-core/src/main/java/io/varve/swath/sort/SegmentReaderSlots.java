/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sequential reader slots over page-run segments. Each slot is bounded independently, while one
 * shared semaphore keeps decompression and decoded pages waiting to enter a full slot at or below
 * the configured active-decode count.
 */
final class SegmentReaderSlots implements AutoCloseable {
    private static final int MAX_DEPTH = 4;
    private static final long FAILURE_CHECK_MILLIS = 100;

    private final List<ArrayBlockingQueue<Item>> slots;
    private final List<Thread> readers = new ArrayList<>();
    private final PipelineFailure failure;
    private final SortMetrics metrics;
    private final AtomicBoolean closing = new AtomicBoolean();

    SegmentReaderSlots(PageRunCatalog catalog, long mergeBudgetBytes, SortMetrics metrics,
            PipelineFailure failure) {
        int segmentCount = catalog.descriptors().size();
        int maxRawPayload = catalog.maxRawPayloadLength() > 0
                ? catalog.maxRawPayloadLength() : PageBlock.MAX_RAW_PAYLOAD_BYTES;
        int depth = slotDepth(segmentCount, mergeBudgetBytes, maxRawPayload);
        int decoders = Math.max(1, Math.min(segmentCount,
                Runtime.getRuntime().availableProcessors()));
        Semaphore decodePermits = new Semaphore(decoders);
        this.failure = failure;
        this.metrics = metrics;
        this.slots = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            slots.add(new ArrayBlockingQueue<>(depth));
        }
        for (int i = 0; i < segmentCount; i++) {
            int slot = i;
            PageRunSegmentDescriptor descriptor = catalog.descriptors().get(i);
            Thread reader = Thread.ofVirtual().name("sort-pipeline-reader-" + i).start(
                    () -> read(slot, descriptor, decodePermits));
            readers.add(reader);
        }
    }

    static int slotDepth(int segments, long mergeBudgetBytes, int maxRawPayload) {
        if (segments <= 0 || mergeBudgetBytes <= 0 || maxRawPayload <= 0) {
            return 1;
        }
        long denominator;
        try {
            denominator = Math.multiplyExact((long) segments, maxRawPayload);
        } catch (ArithmeticException overflow) {
            denominator = Long.MAX_VALUE;
        }
        long affordable = denominator == 0 ? 1 : mergeBudgetBytes / denominator;
        return (int) Math.max(1L, Math.min(MAX_DEPTH, affordable));
    }

    PageBlock next(int slot) {
        ArrayBlockingQueue<Item> queue = slots.get(slot);
        Item item = queue.poll();
        if (item == null) {
            long started = System.nanoTime();
            try {
                while (item == null) {
                    failure.check();
                    item = queue.poll(FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MergeCancellation.Cancelled();
            } finally {
                metrics.recordPipelineReaderWait(System.nanoTime() - started);
            }
        }
        return switch (item) {
            case Item.Page page -> page.block();
            case Item.End ignored -> null;
        };
    }

    private void read(int slot, PageRunSegmentDescriptor descriptor, Semaphore decodePermits) {
        long seenEntries = 0;
        int decodedLimit = descriptor.hasDecodedPageMaximum()
                ? descriptor.maxRawPayloadLength() : PageBlock.MAX_RAW_PAYLOAD_BYTES;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(
                descriptor.path(), metrics, decodedLimit)) {
            for (long page = 0; page < descriptor.trailer().totalRecords(); page++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                PageRunSegmentIo.Page encoded = io.nextPage();
                decodePermits.acquire();
                try {
                    PageBlock block = encoded.decode(descriptor.path());
                    block.prepareDecoded();
                    slots.get(slot).put(new Item.Page(block));
                    seenEntries = Math.addExact(seenEntries, block.count());
                } finally {
                    decodePermits.release();
                }
            }
            io.checkComplete(seenEntries);
            slots.get(slot).put(Item.End.INSTANCE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closing.get()) {
                failure.record(new MergeCancellation.Cancelled());
            }
        } catch (Throwable e) {
            failure.record(e);
        }
    }

    @Override
    public void close() {
        closing.set(true);
        for (Thread reader : readers) {
            reader.interrupt();
        }
        boolean interrupted = false;
        for (Thread reader : readers) {
            while (reader.isAlive()) {
                try {
                    reader.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private sealed interface Item {
        record Page(PageBlock block) implements Item {
        }

        enum End implements Item {
            INSTANCE
        }
    }
}
