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
 * A bounded, multi-producer, single-consumer channel of {@link Msg} envelopes.
 *
 * <p>The work-stealing engine drives {@link #send} from every fetch worker concurrently (thousands
 * of virtual threads), while exactly one consumer stage drains {@link #receive}. The wakeup
 * discipline is therefore a <b>relay</b>, not a broadcast: {@link #receive} signals one parked
 * sender per released envelope, and a sender that is admitted while budget remains signals the
 * next. A broadcast ({@code signalAll}) per released page would wake every parked sender to
 * re-contend one lock, and with ~1,000 senders parked that convoy cost the consumer more time per
 * page than the sink write itself (varveio/swath#206). The 50 ms bounded {@code await} in {@link
 * #send} stays as the lost-wakeup backstop: a missed relay costs one 50 ms re-check, never a wedge.
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
 * <p>Backpressure: {@link #send} blocks while over budget. A producer is never
 * wedged there: two things wake it — {@link #dropReceiver()} (I8), which makes a
 * blocked {@code send} return {@code false}, or {@code Thread.interrupt()} from
 * {@code Scope.close() → shutdownNow()}. Both exits relay the wakeup they consumed
 * to the next parked sender before leaving.
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
            // data budget is full. Periodic wake re-checks receiverDropped and is the
            // lost-wakeup backstop for the relay signalling below.
            try {
                while (weight > 0 && inFlight >= capacity && !receiverDropped) {
                    notFull.await(50, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                notFull.signal();   // relay: leaving without admitting must not swallow a wakeup
                throw e;
            }
            if (receiverDropped) {
                notFull.signal();   // relay: the drop reaches the next parked sender at once
                return false;
            }
            inFlight += weight;
            if (inFlight < capacity) {
                notFull.signal();   // relay: budget remains, so the next parked sender may admit too
            }
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
            notFull.signal();   // one waiter; an admitted sender relays onward while budget remains
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
            notFull.signalAll();   // fires once; every parked producer must observe it
        } finally {
            lock.unlock();
        }
    }

    public boolean isReceiverDropped() {
        return receiverDropped;
    }
}
