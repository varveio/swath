/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression for the WeakReference gauge hazard: {@code Gauge.builder(name, obj, fn)} holds
 * {@code obj} via a {@link WeakReference} by default. {@code
 * RunMetrics#registerCheckpointQueueDepthGauge} and {@code RunMetrics#registerDiskFreeGauge} pass
 * an object (a bare lambda / a caller-supplied {@link Path}) that may have no other strong
 * referent — if the gauge itself doesn't pin it with {@code .strongReference(true)}, the first GC
 * collects the state object and the gauge reads {@code NaN} forever afterward.
 *
 * <p>Each test builds a canary object allocated alongside the real gauge's backing state, drops
 * every strong reference to the canary, then loops {@code System.gc()} until the canary reports
 * cleared -- proof that a GC that clears weak refs actually ran, rather than hoping one {@code
 * System.gc()} call is enough. Only then is the gauge value asserted.
 */
final class RunMetricsGaugeStrongReferenceTest {

    private static final int MAX_GC_ATTEMPTS = 50;

    /** Force a real GC and block until some canary weak-ref is observed cleared. */
    private static void gcUntilCanaryCleared(WeakReference<?> canary) throws InterruptedException {
        for (int i = 0; i < MAX_GC_ATTEMPTS && canary.get() != null; i++) {
            System.gc();
            Thread.sleep(10);
        }
        assertThat(canary.get())
                .as("a GC that clears weak references must have run within %d attempts", MAX_GC_ATTEMPTS)
                .isNull();
    }

    @Test
    void checkpointQueueDepthGaugeSurvivesGc() throws InterruptedException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        // The supplier is registered as the gauge's weakly-referenced state object; nothing else
        // in this test method keeps a strong reference to it -- exactly the SqliteCheckpointStore
        // call-site shape (queue::size, a fresh method-ref with no other strong referent).
        registerAndDropSupplier(metrics);

        // Separate, unrelated canary allocated/dropped the same way, used only to make "a GC that
        // clears weak refs ran" deterministic rather than assumed.
        WeakReference<Object> canary = new WeakReference<>(new Object());
        gcUntilCanaryCleared(canary);

        assertThat(registry.get("swath.checkpoint.queue.depth").gauge().value())
                .as("gauge must survive GC, not go NaN")
                .isEqualTo(0.0);
    }

    // Isolated in its own method so the IntSupplier is provably out of scope (no local variable
    // in the caller keeps it reachable) once this method returns. Deliberately a BOUND instance
    // method reference (AtomicInteger::get on a fresh counter), matching the real call site
    // shape (SqliteCheckpointStore's queue::size) -- a non-capturing lambda like `() -> 0` would
    // be cached as a JVM-wide singleton by the lambda metafactory and never become GC-eligible,
    // which would make this test pass even without the fix (a false negative).
    private static void registerAndDropSupplier(RunMetrics metrics) {
        AtomicInteger depth = new AtomicInteger(0);
        IntSupplier depthSupplier = depth::get;
        metrics.registerCheckpointQueueDepthGauge(depthSupplier);
    }

    @Test
    void diskFreeGaugeSurvivesGc(@TempDir Path dir) throws InterruptedException {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RunMetrics metrics = new RunMetrics(registry);

        registerAndDropScratchDir(metrics, dir);

        WeakReference<Object> canary = new WeakReference<>(new Object());
        gcUntilCanaryCleared(canary);

        assertThat(registry.get("swath.disk.free_bytes").gauge().value())
                .as("gauge must survive GC, not go NaN")
                .isNotNaN();
    }

    // Constructs a fresh Path (not the @TempDir-annotated field/parameter chain the JUnit
    // extension may keep strongly reachable) so the only strong path to it is whatever
    // RunMetrics itself retains.
    private static void registerAndDropScratchDir(RunMetrics metrics, Path dir) {
        Path scratchDir = Path.of(dir.toString());
        metrics.registerDiskFreeGauge(scratchDir);
    }
}
