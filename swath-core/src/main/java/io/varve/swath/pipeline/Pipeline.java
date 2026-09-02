/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

import io.varve.swath.concurrent.Scope;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.SwathException;
import io.varve.swath.runtime.RunContext;
import java.util.concurrent.Callable;
import java.util.function.ToLongFunction;

/**
 * The static listing pipeline skeleton: the producing side (the listing engine,
 * which sends from every fetch worker concurrently) feeds a bounded
 * {@link Channel} drained by a single consumer (the output stage). Filter/sort
 * stages are inserted between them in later phases; the shutdown discipline
 * established here is what those stages inherit.
 *
 * <p><b>I8 shutdown ordering.</b> The consumer runs on the calling thread; when
 * it returns (end-of-stream, broken pipe, or cancellation) the channel's
 * receiver is dropped <i>before</i> the producer is joined, so a producer blocked
 * on a full queue winds down instead of deadlocking. {@link Scope#close()} then
 * does {@code shutdownNow()} before awaiting termination.
 */
public final class Pipeline<T> {

    /** The producing side (the listing engine). Emits items; the pipeline appends {@link End}. */
    @FunctionalInterface
    public interface Producer<T> {
        void produce(RunContext ctx, Channel<T> out) throws Exception;
    }

    /** The consuming side (the output stage). Drains until {@link End}/{@link Failure}. */
    @FunctionalInterface
    public interface Consumer<T> {
        void consume(RunContext ctx, Channel<T> in) throws Exception;
    }

    private final long capacity;
    private final ToLongFunction<? super T> weigher;

    /** Slot-counting pipeline (one slot per item). */
    public Pipeline(int capacity) {
        this(capacity, t -> 1L);
    }

    /**
     * Weight-budgeted pipeline: {@code capacity} is the in-flight weight budget
     * and {@code weigher} maps each item to its weight (e.g. a page's entry
     * count for the {@code --object-listing-queue-size} contract, I11).
     */
    public Pipeline(long capacity, ToLongFunction<? super T> weigher) {
        this.capacity = capacity;
        this.weigher = weigher;
    }

    public void run(RunContext ctx, Producer<T> producer, Consumer<T> consumer)
            throws SwathException, InterruptedException {
        try {
            runDag(ctx, producer, consumer);
        } catch (InterruptedException e) {
            // The sequential/seed fetch paths (ScanProducer/CheckpointedScanProducer over
            // TransientRetryFetcher) do NOT convert a cancellation-driven InterruptedException the way
            // the WorkStealingScan engine's produce() does (throwCancellationOrReceiverGone). On
            // transient-retry cap exhaustion TransientRetryFetcher trips the run's cancellation with a
            // StopReason (e.g. STUCK) and aborts via InterruptedException; without this, that bare
            // InterruptedException would escape to the CLI as an unrecognized throwable (exit 1) instead
            // of the contractual CancelledException (exit 75 / stop_reason=stuck, resumable). Mirror the
            // engine: when the run's cancellation is set, surface a CancelledException carrier — the
            // token still holds the winning stop_reason, which ListCommand#timeboxExitOrRethrow reads, so
            // the conversion never overwrites max_duration/signal/stuck. A genuine interrupt with no
            // run-cancel attributed is preserved as-is. The engine path already throws CancelledException
            // (a SwathException, not InterruptedException), so it is never caught here — no double-wrap.
            if (ctx.cancellation().isCancelled()) {
                throw new CancelledException("listing cancelled", e);
            }
            throw e;
        }
    }

    private void runDag(RunContext ctx, Producer<T> producer, Consumer<T> consumer)
            throws SwathException, InterruptedException {
        Channel<T> channel = new Channel<>(capacity, weigher);
        long runId = RunContext.runIdOrNone();
        try (Scope scope = new Scope()) {
            scope.fork(() -> runWithContext(ctx, runId, () -> {
                try {
                    producer.produce(ctx, channel);
                    channel.send(new End<>());
                } catch (Exception e) {
                    sendFailureQuietly(channel, e);
                    throw e;
                }
                return null;
            }));

            Exception consumerError = null;
            try {
                runWithContext(ctx, runId, () -> {
                    consumer.consume(ctx, channel);
                    return null;
                });
            } catch (Exception e) {
                consumerError = e;
            } finally {
                channel.dropReceiver();   // I8: drop receiver BEFORE joining the producer.
            }

            scope.joinAllOrThrow();       // join the producer (now guaranteed to unblock); surfaces its failure typed.
            if (consumerError != null) {
                rethrow(consumerError);
            }
        }                                  // close(): shutdownNow() before close().
    }

    private static <T> T runWithContext(RunContext ctx, long runId, Callable<T> task)
            throws Exception {
        if (runId >= 0) {
            return ctx.runWhereBound(runId, task);
        }
        return ctx.runWhereBound(task);
    }

    private static <T> void sendFailureQuietly(Channel<T> channel, Throwable cause) {
        try {
            channel.send(new Failure<>(cause));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void rethrow(Throwable t) throws SwathException, InterruptedException {
        switch (t) {
            case SwathException e -> throw e;
            case InterruptedException e -> throw e;
            case RuntimeException e -> throw e;
            case Error e -> throw e;
            default -> throw new RuntimeException(t);
        }
    }
}
