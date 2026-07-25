/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.pipeline;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToLongFunction;

/**
 * A bounded, single-producer channel of {@link Msg} envelopes.
 *
 * <p><b>The bound is a <i>weight</i> budget, not a slot count.</b> Each data
 * {@link Item} carries a weight (its entry count for the listing pipeline,
 * {@code 1} for the default slot model); {@link End}/{@link Failure} weigh
 * {@code 0}. {@link #send} admits the next envelope <b>while in-flight weight is
 * {@code < capacity}</b> and blocks otherwise — exactly the {@code
 * --object-listing-queue-size} contract (I11). A single item heavier than
 * {@code capacity} is still admitted when the channel is empty (in-flight
 * {@code == 0 < capacity}), so the pipeline never deadlocks on a big page; the
 * documented budget {@code cap × max_key_len × #queues} already accounts for that
 * one in-flight page of slack.
 *
 * <p>Backpressure: {@link #send} blocks while over budget. The producer is never
 * wedged there: two things wake it — {@link #dropReceiver()} (I8), which makes a
 * blocked {@code send} return {@code false}, or {@code Thread.interrupt()} from
 * {@code Scope.close() → shutdownNow()}.
 */
public final class Channel<T> {

    private final BlockingQueue<Msg<T>> queue = new LinkedBlockingQueue<>();
    private final long capacity;
    private final ToLongFunction<? super T> weigher;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private long inFlight;
    private volatile boolean receiverDropped = false;

    /** Slot-counting channel (default weight {@code 1} per item) — capacity is a slot count. */
    public Channel(int capacity) {
        this(capacity, t -> 1L);
    }

    /**
     * Weight-budgeted channel. {@code capacity} is the in-flight weight budget;
     * {@code weigher} maps each data item's value to its weight (e.g. a page's
     * entry count). {@link End}/{@link Failure} envelopes weigh {@code 0}.
     */
    public Channel(long capacity, ToLongFunction<? super T> weigher) {
        if (capacity < 1) {
            throw new IllegalArgumentException("channel capacity must be >= 1, was " + capacity);
        }
        this.capacity = capacity;
        this.weigher = weigher;
    }

    private long weightOf(Msg<T> msg) {
        if (msg instanceof Item<T> item) {
            return Math.max(0L, weigher.applyAsLong(item.value()));
        }
        return 0L;   // End / Failure carry no payload
    }

    /**
     * Offer an envelope, blocking while the in-flight weight is at/over budget.
     * Returns {@code true} once enqueued, or {@code false} if the receiver has
     * been dropped (stop producing).
     *
     * @throws InterruptedException if interrupted while blocked (shutdownNow path)
     */
    public boolean send(Msg<T> msg) throws InterruptedException {
        long weight = weightOf(msg);
        lock.lockInterruptibly();
        try {
            // Admit while in-flight < cap (the whole item, even if it overshoots).
            // A zero-weight envelope (End/Failure) adds nothing to the budget, so it
            // NEVER blocks — control signals must always get through even when the
            // data budget is full. Periodic wake re-checks receiverDropped.
            while (weight > 0 && inFlight >= capacity && !receiverDropped) {
                notFull.await(50, TimeUnit.MILLISECONDS);
            }
            if (receiverDropped) {
                return false;
            }
            inFlight += weight;
        } finally {
            lock.unlock();
        }
        queue.add(msg);   // unbounded queue; the weight gate above is the real bound
        return true;
    }

    /** Blocking receive of the next envelope; releases its weight from the budget. */
    public Msg<T> receive() throws InterruptedException {
        Msg<T> msg = queue.take();
        release(weightOf(msg));
        return msg;
    }

    /** Receive with a timeout; {@code null} on timeout (releases nothing). */
    public Msg<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
        Msg<T> msg = queue.poll(timeout, unit);
        if (msg != null) {
            release(weightOf(msg));
        }
        return msg;
    }

    private void release(long weight) {
        if (weight == 0L) {
            return;
        }
        lock.lock();
        try {
            inFlight -= weight;
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Signal that the consumer is gone. Subsequent (and currently-blocked)
     * {@link #send} calls stop and return {@code false}. Call this before joining
     * the producer (I8).
     */
    public void dropReceiver() {
        lock.lock();
        try {
            receiverDropped = true;
            notFull.signalAll();   // wake a producer blocked on the budget
        } finally {
            lock.unlock();
        }
    }

    public boolean isReceiverDropped() {
        return receiverDropped;
    }
}
