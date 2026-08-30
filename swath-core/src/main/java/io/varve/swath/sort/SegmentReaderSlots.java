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
 * shared semaphore limits active decompression only; decoded pages release that permit before they
 * wait for bounded slot capacity.
 */
final class SegmentReaderSlots implements AutoCloseable {
    private static final int MAX_DEPTH = 4;
    private static final long FAILURE_CHECK_MILLIS = 100;

    private final List<ArrayBlockingQueue<Item>> slots;
    private final List<Thread> readers = new ArrayList<>();
    private final PipelineFailure failure;
    private final SortMetrics metrics;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final int legacyDecodedLimit;

    SegmentReaderSlots(PageRunCatalog catalog, long mergeBudgetBytes, SortMetrics metrics,
            PipelineFailure failure) {
        int segmentCount = catalog.descriptors().size();
        this.legacyDecodedLimit = decodedPageLimit(mergeBudgetBytes);
        int maxRawPayload = catalog.maxRawPayloadLength() > 0
                ? catalog.maxRawPayloadLength() : legacyDecodedLimit;
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
                long waited = System.nanoTime() - started;
                metrics.recordPipelineReaderWait(waited);
                metrics.recordPipelineRouterWait(waited);
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
                ? descriptor.maxRawPayloadLength() : legacyDecodedLimit;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(
                descriptor.path(), metrics, decodedLimit)) {
            for (long page = 0; page < descriptor.trailer().totalRecords(); page++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                PageRunSegmentIo.Page encoded = io.nextPage();
                decodePermits.acquire();
                PageBlock block;
                try {
                    block = encoded.decode(descriptor.path());
                    block.prepareDecoded();
                } finally {
                    decodePermits.release();
                }
                // Slot back-pressure must not retain a scarce decode permit: with K > decoders and
                // depth one, permit holders can otherwise fill their slots while the requested
                // segment waits forever to begin decoding.
                slots.get(slot).put(new Item.Page(block));
                seenEntries = Math.addExact(seenEntries, block.count());
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

    private static int decodedPageLimit(long mergeBudgetBytes) {
        long bounded = Math.min(PageBlock.MAX_RAW_PAYLOAD_BYTES, mergeBudgetBytes);
        return (int) Math.max(1L, Math.min(Integer.MAX_VALUE, bounded));
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
