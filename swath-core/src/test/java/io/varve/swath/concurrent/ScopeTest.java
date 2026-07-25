/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.varve.swath.error.ListingException;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The {@link Scope} helper: typed failure propagation and the I8
 * {@code shutdownNow()}-before-{@code close()} ordering that wakes a task
 * blocked indefinitely.
 */
class ScopeTest {

    @Test
    void joinAllOrThrowPropagatesTypedSwathException() {
        assertThatThrownBy(() -> {
            try (Scope scope = new Scope()) {
                scope.fork(() -> {
                    throw new ListingException("boom");
                });
                scope.joinAllOrThrow();
            }
        }).isInstanceOf(ListingException.class).hasMessage("boom");
    }

    @Test
    void closeInterruptsABlockedTaskWithoutHanging() {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            try (Scope scope = new Scope()) {
                scope.fork(() -> {
                    started.countDown();
                    try {
                        Thread.sleep(Long.MAX_VALUE);   // would hang forever
                    } catch (InterruptedException e) {
                        interrupted.set(true);
                        throw e;
                    }
                });
                started.await();
                // exit try-with-resources → close() → shutdownNow() before await.
            }
        });

        Assertions.assertThat(interrupted).isTrue();
    }

    @Test
    void joinAllOrThrowDoesNotHangWhenASiblingFailsWhileAnotherBlocks() {
        // The deadlock class this test guards: future #1 (a producer) blocks on a full
        // queue; future #2 (a consumer) fails. joinAllOrThrow() awaits in fork order, so without
        // ShutdownOnFailure it blocks on #1 forever and close()'s shutdownNow() is unreachable.
        AtomicBoolean producerInterrupted = new AtomicBoolean(false);
        BlockingQueue<Object> full = new ArrayBlockingQueue<>(1);
        full.add(new Object());                       // queue is full ⇒ put() blocks
        CountDownLatch producerBlocked = new CountDownLatch(1);

        assertTimeoutPreemptively(Duration.ofSeconds(5), () ->
                assertThatThrownBy(() -> {
                    try (Scope scope = new Scope()) {
                        scope.fork(() -> {
                            producerBlocked.countDown();
                            try {
                                full.put(new Object());   // blocks until interrupted
                            } catch (InterruptedException e) {
                                producerInterrupted.set(true);
                                throw e;
                            }
                        });
                        producerBlocked.await();          // the producer is blocked before the failure
                        scope.fork(() -> {
                            throw new ListingException("consumer failed");
                        });
                        scope.joinAllOrThrow();
                    }
                }).isInstanceOf(ListingException.class).hasMessage("consumer failed"));

        assertThat(producerInterrupted).as("the blocked producer was interrupted by the failure").isTrue();
    }

    // ---- Recovering the real cause behind a spurious post-shutdown REE -----

    @Test
    void rethrowFirstFailureIsANoOpWhenNothingFailed() throws Exception {
        try (Scope scope = new Scope()) {
            scope.fork(() -> null);
            scope.joinAllOrThrow();
            scope.rethrowFirstFailure();   // no recorded failure ⇒ must not throw
        }
    }

    @Test
    void rethrowFirstFailureSurfacesTheRecordedCauseWithoutJoining() throws Exception {
        // Mirrors the WorkStealingScan#produce race: a sibling's failure has already
        // called shutdownNow() (recording firstFailure) by the time a caller notices a
        // RejectedExecutionException from a later fork() and wants the REAL typed cause instead.
        CountDownLatch shutDown = new CountDownLatch(1);
        try (Scope scope = new Scope()) {
            scope.fork(() -> {
                throw new ListingException("real cause");
            });
            // Poll until the failing task's shutdownNow() has actually landed, so the next
            // fork() below is guaranteed to observe a rejected executor (deterministic, not racy).
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                while (true) {
                    try {
                        scope.fork(() -> null);
                    } catch (RejectedExecutionException rejected) {
                        shutDown.countDown();
                        break;
                    }
                }
            });
            assertThat(shutDown.getCount()).isZero();

            // The REE itself carries no typed cause — the caller must consult the scope.
            assertThatThrownBy(scope::rethrowFirstFailure)
                    .isInstanceOf(ListingException.class)
                    .hasMessage("real cause");
        }
    }
}
