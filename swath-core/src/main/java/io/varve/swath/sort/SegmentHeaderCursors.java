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
 * One sequential header cursor per open segment. A shared permit bounds concurrent metadata reads;
 * it is released before the bounded per-segment queue handoff, so a full slot cannot prevent an
 * unstarted cursor from exposing its first reference to the router.
 */
final class SegmentHeaderCursors implements AutoCloseable {
    static final int QUEUE_DEPTH = 2;
    private static final long FAILURE_CHECK_MILLIS = 100;

    private final List<ArrayBlockingQueue<Item>> slots;
    private final List<Thread> cursors = new ArrayList<>();
    private final PipelineFailure failure;
    private final SortMetrics metrics;
    private final AtomicBoolean closing = new AtomicBoolean();

    SegmentHeaderCursors(List<PageRunSegmentIo> segments, Settings settings, SortMetrics metrics,
            PipelineFailure failure) {
        this.failure = failure;
        this.metrics = metrics;
        Semaphore scanPermits = new Semaphore(settings.scanParallelism());
        slots = new ArrayList<>(segments.size());
        for (int i = 0; i < segments.size(); i++) {
            slots.add(new ArrayBlockingQueue<>(settings.queueDepth()));
        }
        for (int i = 0; i < segments.size(); i++) {
            int segment = i;
            Thread cursor = Thread.ofVirtual().name("sort-pipeline-header-" + i).start(
                    () -> scan(segment, segments.get(segment), scanPermits, settings.hook()));
            cursors.add(cursor);
        }
    }

    static Settings planned(int segments) {
        int parallelism = Math.max(1, Math.min(segments,
                Runtime.getRuntime().availableProcessors()));
        return new Settings(QUEUE_DEPTH, parallelism, Hook.NO_OP);
    }

    /** Take one reference while continuing to surface failures from any cursor or encoder lane. */
    PageRef next(int segment) {
        ArrayBlockingQueue<Item> queue = slots.get(segment);
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

    private void scan(int segment, PageRunSegmentIo io, Semaphore scanPermits, Hook hook) {
        try {
            while (true) {
                hook.beforeScan(segment);
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
                hook.beforeEnqueue(ref);
                put(segment, new Item.Ref(ref));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!closing.get()) {
                failure.record(new MergeCancellation.Cancelled());
            }
        } catch (Throwable e) {
            failure.record(e);
        }
    }

    private void put(int segment, Item item) throws InterruptedException {
        ArrayBlockingQueue<Item> queue = slots.get(segment);
        while (!queue.offer(item, FAILURE_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
            failure.check();
        }
    }

    record Settings(int queueDepth, int scanParallelism, Hook hook) {
        Settings {
            if (queueDepth < 1 || scanParallelism < 1) {
                throw new IllegalArgumentException("header cursor settings must be positive");
            }
        }
    }

    interface Hook {
        Hook NO_OP = new Hook() { };

        default void beforeScan(int segment) throws InterruptedException {
        }

        default void beforeEnqueue(PageRef ref) throws InterruptedException {
        }
    }

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

    private sealed interface Item {
        record Ref(PageRef value) implements Item {
        }

        enum End implements Item {
            INSTANCE
        }
    }
}
