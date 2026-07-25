/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * {@code RunMetrics#recordFatalErrorClass} is the shared observability spine the CRASH terminal
 * summary reads its {@code error_class} from ({@code ListRunner#terminalStatus}), so its three
 * contract properties are pinned here rather than left implicit in the caller.
 *
 * <ul>
 *   <li><b>First-writer-wins.</b> The FIRST classified cause to unwind is the one that killed the run; a
 *       failure raised later while tearing down (a best-effort cleanup, a second corrupt segment on
 *       another thread) must NOT overwrite the terminal's attribution. A last-writer-wins {@code set()}
 *       would silently mis-attribute every crash whose unwind touches a second failure.</li>
 *   <li><b>Null is ignored.</b> Callers pass the result of a cause-chain walk unconditionally
 *       ({@code ListRunner#segmentErrorClass} returns null for a generic merge failure), so a null must
 *       neither be stored nor erase an already-recorded class.</li>
 *   <li><b>Unrecorded stays null.</b> An unclassified crash keeps the {@code error_class:null}
 *       summary shape.</li>
 * </ul>
 */
final class RunMetricsFatalErrorClassTest {

    @Test
    void anUnclassifiedCrashHasNoFatalErrorClass() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        assertThat(metrics.fatalErrorClass())
                .as("nothing classified the failure — the CRASH terminal keeps error_class:null")
                .isNull();
    }

    @Test
    void theFirstClassifiedFatalWinsOverALaterTeardownFailure() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordFatalErrorClass("page_run_min_regression");   // the real cause
        metrics.recordFatalErrorClass("staging_cleanup_failed");    // raised later, while unwinding

        assertThat(metrics.fatalErrorClass())
                .as("first-writer-wins: a teardown failure must not overwrite the cause that killed the run")
                .isEqualTo("page_run_min_regression");
    }

    @Test
    void aNullClassIsIgnoredAndNeverErasesARecordedOne() {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        metrics.recordFatalErrorClass(null);   // ListRunner's cause-chain walk found nothing classified
        assertThat(metrics.fatalErrorClass()).isNull();

        metrics.recordFatalErrorClass("page_run_min_regression");
        metrics.recordFatalErrorClass(null);   // a later unclassified failure on the unwind path

        assertThat(metrics.fatalErrorClass())
                .as("a null class must neither be stored nor erase the classified cause")
                .isEqualTo("page_run_min_regression");
    }

    @Test
    void recordingIsIdempotentUnderConcurrentUnwindingThreads() throws Exception {
        // Several threads can be unwinding at once (a merge fails while the lane/cleanup also fails).
        // The CAS must leave exactly ONE stable value — never a torn/last-write-wins flip — and a
        // teardown class recorded afterwards must still not displace it.
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String cls = "class_" + i;
                futures.add(pool.submit(() -> {
                    start.await();
                    metrics.recordFatalErrorClass(cls);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        String winner = metrics.fatalErrorClass();
        assertThat(winner).as("exactly one racing writer lands a class").isNotNull().startsWith("class_");
        metrics.recordFatalErrorClass("staging_cleanup_failed");
        assertThat(metrics.fatalErrorClass())
                .as("the winner is stable — a later teardown class cannot displace it")
                .isEqualTo(winner);
    }
}
