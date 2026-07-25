/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.observability.RunMetrics;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * The quiescence ledger of a {@link WorkStealingScan}: the ready queue of claimable ranges plus the
 * live-node counter, and the <b>primitive</b> atomic transitions over them. The engine keeps every
 * scheduling <i>decision</i> — when to steal, when to become a thief, the idle backoff strategy,
 * what to do after waking; this type only holds the state and moves it between legal transitions.
 *
 * <p>{@code outstanding} counts live nodes; see algorithms.md &sect;4.4 for the full counting
 * contract (increment/decrement points, quiescence, resume). The one piece that contract doesn't
 * cover: a split child's increment (in {@link #enqueue}) happens <b>inside the victim's
 * {@link WorkerState} lock, before it is released</b>, so quiescence never observes a transient
 * {@code outstanding == 0} while a child is being handed off. Acquiring {@code gate} inside
 * {@link #enqueue} therefore nests victim&rarr;gate; idle workers always release {@code gate}
 * before stealing, so no reverse nesting exists.
 *
 * <p><b>Signal on every transition.</b> The {@code work} condition is broadcast on <b>every</b>
 * enqueue and <b>every</b> decrement, not merely on the zero-crossing: an enqueue wakes a parked
 * worker to claim the new child, and the decrement that reaches zero wakes <b>all</b> parked workers
 * to observe quiescence and terminate. All four operations that touch the ledger — {@link #enqueue},
 * {@link #decrement}, {@link #poll}, {@link #signalAll} — and the timed {@link #park} run under the
 * single {@code gate} lock, so a signal can never be lost against a concurrent transition.
 *
 * <p>Package-private engine collaborator; one instance per run.
 */
final class Worklist {

    private final ReentrantLock gate = new ReentrantLock();
    private final Condition work = gate.newCondition();
    private final Deque<Claim> ready = new ArrayDeque<>();
    private final AtomicLong outstanding = new AtomicLong();

    /** A unit of work the ready queue hands out: {@code (lo, hi]} with its resume cursor. */
    record Claim(long nodeId, byte[] lo, byte[] cursor, byte[] hi) {
    }

    /**
     * The outcome of a {@link #poll}: a claimed range ({@code claim != null}), or, when the queue is
     * empty, whether the run has reached quiescence ({@code outstanding == 0}) versus is merely idle
     * with work still outstanding.
     */
    record Poll(Claim claim, boolean quiescent) {
    }

    /**
     * Seed the ready queue with one initial-worklist claim. Called only during the single-threaded
     * pre-fork seed phase (before any worker exists), so — like the {@code outstanding} seed in
     * {@link #initOutstanding} — it runs without the gate.
     */
    void addSeed(Claim claim) {
        ready.addLast(claim);
    }

    /**
     * Set {@code outstanding} to the number of non-COMPLETED seed nodes (§4.4). Called once, in the
     * single-threaded pre-fork seed phase, after the ready queue has been seeded.
     */
    void initOutstanding(long count) {
        outstanding.set(count);
    }

    /** The current live-node count; the demand-gate supplier and quiescence reads use it live. */
    long outstanding() {
        return outstanding.get();
    }

    /**
     * Poll the next claimable range. Under the gate: dequeue the head if present; otherwise, when the
     * queue is empty and {@code outstanding == 0}, broadcast {@code work} so peer workers also observe
     * quiescence and terminate. Returns the claim, or a claimless {@link Poll} tagged quiescent (empty
     * + zero outstanding, signalled) or idle (empty, work still outstanding).
     */
    Poll poll() {
        gate.lock();
        try {
            Claim c = ready.pollFirst();
            if (c != null) {
                return new Poll(c, false);
            }
            if (outstanding.get() == 0L) {
                work.signalAll();   // quiescent: wake peers so they too terminate
                return new Poll(null, true);
            }
            return new Poll(null, false);
        } finally {
            gate.unlock();
        }
    }

    /**
     * Enqueue a split child {@code (m, H]} and bump {@code outstanding}, broadcasting {@code work} so a
     * parked worker wakes to claim it. Called from the thief's {@link Thief.ChildSink} <b>while the
     * caller still holds the victim {@link WorkerState} lock</b> — see the class javadoc for why that
     * ordering rules out a false zero. Returns the new {@code outstanding} value for the caller's trace.
     */
    long enqueue(Claim claim) {
        gate.lock();
        try {
            ready.addLast(claim);
            long value = outstanding.incrementAndGet();
            work.signalAll();
            return value;
        } finally {
            gate.unlock();
        }
    }

    /**
     * Decrement {@code outstanding} for a COMPLETED node and broadcast {@code work} so a parked worker
     * re-checks quiescence; the decrement that reaches zero wakes every parked worker to terminate.
     * Returns the remaining live-node count.
     */
    long decrement() {
        gate.lock();
        try {
            long remaining = outstanding.decrementAndGet();
            work.signalAll();
            return remaining;
        } finally {
            gate.unlock();
        }
    }

    /** Broadcast {@code work} under the gate (receiver-gone teardown and prompt steal-rebalance). */
    void signalAll() {
        gate.lock();
        try {
            work.signalAll();
        } finally {
            gate.unlock();
        }
    }

    /**
     * Park the idle worker on {@code work} until an enqueue/decrement signals or the caller-supplied
     * backstop elapses, then return so the caller re-evaluates. The recheck of the ready/outstanding
     * condition and the timed await run under the same gate hold — the lost-wakeup guard — so a
     * concurrent enqueue/decrement can never signal into the gap between the check and the await. The
     * park duration ({@code parkNanos}, the engine's idle-backoff strategy) is evaluated under the gate
     * after the recheck, exactly where the caller's policy intends it; the park's own duration is
     * metered.
     */
    void park(LongSupplier parkNanos, RunMetrics metrics) throws InterruptedException {
        gate.lock();
        try {
            if (ready.isEmpty() && outstanding.get() > 0L) {
                Timer.Sample parkSample = metrics.startIdleBackoffParkTimer();
                try {
                    work.await(parkNanos.getAsLong(), TimeUnit.NANOSECONDS);
                } finally {
                    metrics.recordIdleBackoffPark(parkSample);
                }
            }
        } finally {
            gate.unlock();
        }
    }
}
