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
 * One sequential reader per page-run segment, with a bounded decoded-page handoff to the ordered
 * router. Per-slot permits are acquired before frame read/decode, so a full queue cannot retain an
 * extra producer-local page; reader residency is at most queue depth plus one router frontier head
 * per segment. A separate shared semaphore limits active decompression without coupling progress
 * across slots.
 */
final class SegmentReaderSlots implements AutoCloseable {
    private static final long FAILURE_CHECK_MILLIS = 100;

    private final List<ArrayBlockingQueue<Item>> slots;
    private final List<Semaphore> slotPermits;
    private final List<Thread> readers = new ArrayList<>();
    private final PipelineFailure failure;
    private final SortMetrics metrics;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final int legacyDecodedLimit;

    SegmentReaderSlots(PageRunCatalog catalog, Settings settings, SortMetrics metrics,
            PipelineFailure failure) {
        int segmentCount = catalog.descriptors().size();
        this.legacyDecodedLimit = settings.legacyDecodedLimit();
        Semaphore decodePermits = new Semaphore(settings.decoderParallelism());
        this.failure = failure;
        this.metrics = metrics;
        this.slots = new ArrayList<>(segmentCount);
        this.slotPermits = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            slots.add(new ArrayBlockingQueue<>(settings.slotDepth()));
            slotPermits.add(new Semaphore(settings.slotDepth()));
        }
        for (int i = 0; i < segmentCount; i++) {
            int slot = i;
            PageRunSegmentDescriptor descriptor = catalog.descriptors().get(i);
            Thread reader = Thread.ofVirtual().name("sort-pipeline-reader-" + i).start(
                    () -> read(slot, descriptor, decodePermits, settings.hook()));
            readers.add(reader);
        }
    }

    /** Bind the planned depth/legacy limit and cap concurrent decoders at available processors. */
    static Settings planned(MergePlanner.PipelinePlan plan, int segments) {
        int decoders = Math.max(1, Math.min(segments,
                Runtime.getRuntime().availableProcessors()));
        return new Settings(plan.slotDepth(), decoders, plan.legacyDecodedLimit(), Hook.NO_OP);
    }

    /**
     * Take the next page from one segment, surfacing peer failure while idle. Releasing capacity at
     * poll time lets that segment prepare its successor while the router owns the frontier page.
     */
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
        if (item instanceof Item.Page) {
            slotPermits.get(slot).release();
        }
        return switch (item) {
            case Item.Page page -> page.block();
            case Item.End ignored -> null;
        };
    }

    /** Read, eagerly decode, and enqueue one segment without ever holding an unpriced extra page. */
    private void read(int slot, PageRunSegmentDescriptor descriptor, Semaphore decodePermits,
            Hook hook) {
        long seenEntries = 0;
        int decodedLimit = descriptor.hasDecodedPageMaximum()
                ? descriptor.maxRawPayloadLength() : legacyDecodedLimit;
        try (PageRunSegmentIo io = PageRunSegmentIo.open(
                descriptor.path(), metrics, decodedLimit)) {
            for (long page = 0; page < descriptor.trailer().totalRecords(); page++) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                Semaphore capacity = slotPermits.get(slot);
                hook.beforeCapacityAcquire(slot, page);
                capacity.acquire();
                boolean enqueued = false;
                try {
                    PageRunSegmentIo.Page encoded = io.nextPage();
                    hook.beforeDecode(slot, page);
                    decodePermits.acquire();
                    PageBlock block;
                    try {
                        block = encoded.decode(descriptor.path());
                        block.prepareDecoded();
                    } finally {
                        decodePermits.release();
                    }
                    hook.beforeEnqueue(slot, page);
                    if (!slots.get(slot).offer(new Item.Page(block))) {
                        throw new IllegalStateException("reader slot permit disagrees with queue capacity");
                    }
                    enqueued = true;
                    seenEntries = Math.addExact(seenEntries, block.count());
                } finally {
                    if (!enqueued) {
                        capacity.release();
                    }
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

    record Settings(int slotDepth, int decoderParallelism, int legacyDecodedLimit, Hook hook) {
        Settings {
            if (slotDepth < 1 || decoderParallelism < 1 || legacyDecodedLimit < 1) {
                throw new IllegalArgumentException("reader slot settings must be positive");
            }
        }
    }

    interface Hook {
        Hook NO_OP = new Hook() { };

        default void beforeDecode(int slot, long page) throws InterruptedException {
        }

        default void beforeCapacityAcquire(int slot, long page) throws InterruptedException {
        }

        default void beforeEnqueue(int slot, long page) throws InterruptedException {
        }
    }

    /** Interrupt and join every segment reader; no decoded page may outlive pipeline teardown. */
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
