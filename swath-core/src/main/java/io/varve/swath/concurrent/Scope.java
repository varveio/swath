/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.concurrent;

import io.varve.swath.error.SwathException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-house structured-concurrency helper over
 * {@code Executors.newVirtualThreadPerTaskExecutor()}. Replaces the still-preview
 * {@code StructuredTaskScope} so no shipped artifact needs {@code --enable-preview}.
 *
 * <p>Used by the static pipeline DAG (a small, fixed set of siblings). The
 * dynamic listing engine uses the worklist, not this.
 *
 * <p><b>I8 — {@code shutdownNow()} before {@code close()}.</b> {@link #close()}
 * interrupts in-flight tasks first, then awaits termination, so a producer
 * blocked on a full queue is woken rather than deadlocking the join.
 */
public final class Scope implements AutoCloseable {

    /** A forkable unit of work that may throw a checked exception. */
    @FunctionalInterface
    public interface Task {
        void run() throws Exception;
    }

    private final ExecutorService executor;
    private final List<Future<?>> futures = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

    /** Default: a virtual thread per task — for the many, HTTP-I/O-bound siblings. */
    public Scope() {
        this(Executors.newVirtualThreadPerTaskExecutor());
    }

    private Scope(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * A scope whose tasks run on dedicated <b>platform</b> threads — for a small,
     * fixed set of long-lived siblings that do <b>blocking file I/O</b> (the
     * Parquet writer lanes, the sort lane's segment encoder). File reads/writes/fsync
     * pin a virtual thread's carrier (PERF-2: "no hot-path pinning, parquet writer"),
     * so a handful of platform threads is the correct carrier model there; the
     * listing workers stay virtual.
     */
    public static Scope ofPlatformThreads(String namePrefix) {
        return new Scope(Executors.newThreadPerTaskExecutor(
                Thread.ofPlatform().name(namePrefix + "-", 0).factory()));
    }

    /** Fork a value-returning task. */
    public <T> Future<T> fork(Callable<T> task) {
        Future<T> future = executor.submit(() -> {
            try {
                return task.call();
            } catch (Throwable t) {
                if (firstFailure.compareAndSet(null, t)) {
                    // ShutdownOnFailure: the first failure interrupts every sibling so a
                    // producer blocked on a full queue is woken NOW — otherwise joinAllOrThrow(), which
                    // awaits futures in fork order, would block on that producer forever and never reach
                    // close()'s shutdownNow(). Idempotent: only the first failure triggers it.
                    executor.shutdownNow();
                }
                throw t;
            }
        });
        futures.add(future);
        return future;
    }

    /** Fork a void task. */
    public Future<?> fork(Task task) {
        return fork(() -> {
            task.run();
            return null;
        });
    }

    /**
     * Wait for every forked task. If any task failed, rethrow the first failure
     * (preserving {@link SwathException} / {@link InterruptedException} types).
     */
    public void joinAllOrThrow() throws SwathException, InterruptedException {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // The cause was already captured by the wrapper; recorded below.
            } catch (CancellationException e) {
                // A cancelled sibling is not itself a failure to surface.
            }
        }
        rethrowFirstFailure();
    }

    /**
     * Rethrow the first recorded task failure, if any (idempotent; safe to call without
     * joining). A sibling's failure calls {@code shutdownNow()} (see {@link #fork}), which can
     * make a <i>later</i> {@code fork()} call throw a spurious {@link
     * java.util.concurrent.RejectedExecutionException} that races the real cause into the
     * caller. This lets a caller that observes that symptom recover and surface the real,
     * typed cause instead rather than letting the untyped REE escape.
     */
    public void rethrowFirstFailure() throws SwathException, InterruptedException {
        Throwable failure = firstFailure.get();
        if (failure != null) {
            rethrow(failure);
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

    @Override
    public void close() {
        executor.shutdownNow();   // I8: interrupt blocked tasks BEFORE awaiting termination.
        executor.close();         // awaits termination (final since JDK 19).
    }
}
