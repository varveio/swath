/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.PageCommit;
import io.varve.swath.error.CheckpointException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * A {@link CheckpointStore} decorator that pins I1 (commit-before-emit, contracts.md §0): it
 * withholds ONE targeted page-commit's durability so the engine's page callback parks at its
 * {@code commitPageAsync(...).get()} await and cannot push the page downstream. Every other
 * commit forwards to the delegate unchanged.
 *
 * <p>The gate is on {@link #commitPageAsync}: for the first {@link PageCommit} matching
 * {@code target}, the underlying durable commit is withheld entirely and a PENDING future is
 * returned. {@link #awaitParked()} blocks until the engine worker actually begins awaiting that
 * future — a deterministic barrier proving the worker has reached the commit-await point. For a
 * correct engine the await happens strictly BEFORE the page is emitted, so nothing from the held
 * page can have reached the channel at that barrier; a reordered engine that emits before awaiting
 * durability sends the page first, which {@link #awaitParked()} makes observable to the test.
 * {@link #release()} then performs the real durable commit and completes the future, so emission
 * proceeds — durably-committed strictly before emitted, exactly as I1 requires.
 */
public final class HeldCommitCheckpointStore extends ForwardingCheckpointStore {

    /**
     * A {@link CompletableFuture} that signals when the engine worker begins awaiting it. The
     * engine's I1 await is {@code commitPageAsync(...).get()} outside the worker lock; overriding
     * {@link #get()} lets the gate observe the worker has reached the await (in a correct engine,
     * strictly before the page is emitted).
     */
    private static final class ParkingFuture extends CompletableFuture<Void> {
        private final CountDownLatch parked = new CountDownLatch(1);

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            parked.countDown();
            return super.get();
        }

        // Defense-in-depth: the engine awaits via the no-arg get(), but a refactor switching to
        // join() or a timed get() must still trip the parked barrier rather than silently hang
        // the awaitParked() side of a test.
        @Override
        public Void join() {
            parked.countDown();
            return super.join();
        }

        @Override
        public Void get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            parked.countDown();
            return super.get(timeout, unit);
        }
    }

    private final Predicate<PageCommit> target;
    private final AtomicBoolean armed = new AtomicBoolean(true);
    private final ParkingFuture held = new ParkingFuture();
    private volatile PageCommit heldCommit;

    public HeldCommitCheckpointStore(CheckpointStore delegate, Predicate<PageCommit> target) {
        super(delegate);
        this.target = target;
    }

    @Override
    public CompletableFuture<Void> commitPageAsync(PageCommit c) throws CheckpointException {
        if (target.test(c) && armed.compareAndSet(true, false)) {
            heldCommit = c;   // withhold the durable commit until release()
            return held;
        }
        return delegate.commitPageAsync(c);
    }

    /** Block until the engine worker begins awaiting the held commit ({@code future.get()}). */
    public void awaitParked() throws InterruptedException {
        if (!held.parked.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("worker never awaited the held page commit");
        }
    }

    /** True once the held commit has been released (durably committed + future completed). */
    public boolean released() {
        return held.isDone();
    }

    /** Durably forward the withheld commit, then complete its future so emission proceeds. */
    public void release() throws CheckpointException {
        PageCommit c = heldCommit;
        if (c != null) {
            delegate.commitPage(c);
        }
        held.complete(null);
    }
}
