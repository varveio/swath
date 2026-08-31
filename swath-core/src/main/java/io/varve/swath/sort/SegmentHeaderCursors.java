/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One sequential header cursor per open segment. A shared permit bounds concurrent metadata reads;
 * it is released before the bounded per-segment queue handoff, so a full slot cannot prevent an
 * unstarted cursor from exposing its first reference to the router. Per-segment queues, rather than
 * one shared cursor queue, preserve the router's ability to hold exactly one ordered frontier head
 * from every run without decoding bodies or allowing a hot segment to hide an unstarted one.
 */
final class SegmentHeaderCursors implements AutoCloseable {
    static final int QUEUE_DEPTH = 2;
    private static final long FAILURE_CHECK_MILLIS = 100;

    private final List<ArrayBlockingQueue<Item>> queues;
    private final List<Thread> cursors = new ArrayList<>();
    private final PipelineFailure failure;
    private final SortMetrics metrics;
    private final AtomicBoolean closing = new AtomicBoolean();

    /**
     * Start one sequential cursor per segment while sharing a bounded metadata-read semaphore. One
     * cursor per file preserves frame order; the semaphore prevents thousands of open segments from
     * issuing simultaneous small reads against the filesystem.
     */
    SegmentHeaderCursors(List<PageRunSegmentIo> segments, Settings settings, SortMetrics metrics,
            PipelineFailure failure) {
        this.failure = failure;
        this.metrics = metrics;
        Semaphore scanPermits = new Semaphore(settings.scanParallelism());
        queues = new ArrayList<>(segments.size());
        for (int i = 0; i < segments.size(); i++) {
            queues.add(new ArrayBlockingQueue<>(settings.queueDepth()));
        }
        for (int i = 0; i < segments.size(); i++) {
            int segment = i;
            Thread cursor = Thread.ofVirtual().name("sort-pipeline-header-" + i).start(
                    () -> scan(segment, segments.get(segment), scanPermits, settings.hook()));
            cursors.add(cursor);
        }
    }

    /**
     * Keep header-read concurrency no wider than both the catalog and available processors. Virtual
     * threads make blocked cursors cheap, but do not make unbounded physical I/O contention cheap.
     */
    static Settings planned(int segments) {
        int parallelism = Math.max(1, Math.min(segments,
                Runtime.getRuntime().availableProcessors()));
        return new Settings(QUEUE_DEPTH, parallelism, Hook.NO_OP);
    }

    /**
     * Take one reference while continuing to surface failures from any cursor or encoder lane. A
     * timed poll is required because a failed peer cannot necessarily enqueue this segment's End.
     */
    PageRef next(int segment) {
        ArrayBlockingQueue<Item> queue = queues.get(segment);
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
                metrics.recordPipelineRouterWait(System.nanoTime() - started);
            }
        }
        return switch (item) {
            case Item.Ref ref -> ref.value();
            case Item.End ignored -> null;
        };
    }

    /**
     * Produce a complete ordered reference stream and an End only after the segment proves exact
     * header-to-trailer tiling and totals. The scan permit is released before queue handoff so a full
     * slot cannot monopolize the only permit and deadlock an unstarted segment's frontier head.
     */
    private void scan(int segment, PageRunSegmentIo io, Semaphore scanPermits, Hook hook) {
        long pageNumber = 0;
        try {
            while (true) {
                hook.beforePermitAcquire(segment, pageNumber);
                scanPermits.acquire();
                PageRunSegmentIo.RoutingPage page;
                long started = System.nanoTime();
                try {
                    page = io.nextRoutingPage();
                    if (page == null) {
                        io.checkRoutingComplete();
                    }
                } finally {
                    metrics.recordPipelineHeaderScan(System.nanoTime() - started);
                    scanPermits.release();
                }
                if (page == null) {
                    put(segment, Item.End.INSTANCE);
                    return;
                }
                PageBlockCodec.RoutingHeader header = page.header();
                PageRef ref = new PageRef(segment, page.ordinal(), page.offset(), page.framedLen(),
                        header.minKey(), header.maxKey(), header.count(),
                        header.rawPayloadLength());
                hook.beforeHandoff(segment, page.ordinal());
                put(segment, new Item.Ref(ref));
                pageNumber++;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // closing is set only after the router drained this cursor to End or after another
            // failure was recorded. Suppressing this shutdown interrupt therefore cannot hide the
            // sole failure or strand a router that still needs a reference from this segment.
            if (!closing.get()) {
                failure.record(new MergeCancellation.Cancelled());
            }
        } catch (Throwable e) {
            failure.record(e);
        }
    }

    /**
     * Back-pressure one segment without hiding global failure. The router needs one head from every
     * live segment, so allowing a hot cursor to append to an unbounded collection is not equivalent.
     */
    private void put(int segment, Item item) throws InterruptedException {
        ArrayBlockingQueue<Item> queue = queues.get(segment);
        while (!queue.offer(item, FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
            failure.check();
        }
    }

    /** Immutable bounds shared with admission tests and reference-memory planning. */
    record Settings(int queueDepth, int scanParallelism, Hook hook) {
        Settings {
            if (queueDepth < 1 || scanParallelism < 1) {
                throw new IllegalArgumentException("header cursor settings must be positive");
            }
            hook = Objects.requireNonNull(hook, "hook");
        }
    }

    /** Package-test scheduling seam; production always supplies the allocation-free no-op. */
    interface Hook {
        Hook NO_OP = new Hook() { };

        default void beforePermitAcquire(int segment, long page) throws InterruptedException {
        }

        default void beforeHandoff(int segment, long page) throws InterruptedException {
        }
    }

    /**
     * Interrupt and join every cursor before shared channels are closed. Joining matters because a
     * positional metadata read racing channel close can otherwise overwrite the initiating failure.
     */
    @Override
    public void close() {
        closing.set(true);
        for (Thread cursor : cursors) {
            cursor.interrupt();
        }
        boolean interrupted = false;
        for (Thread cursor : cursors) {
            while (cursor.isAlive()) {
                try {
                    cursor.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** A distinct End token lets an empty segment terminate without inventing a nullable PageRef. */
    private sealed interface Item {
        record Ref(PageRef value) implements Item {
        }

        enum End implements Item {
            INSTANCE
        }
    }
}
