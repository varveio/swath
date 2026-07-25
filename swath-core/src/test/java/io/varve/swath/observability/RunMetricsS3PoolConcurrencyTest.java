/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * CONC-tier: {@link RunMetrics#updateS3Pool} is called concurrently from many
 * {@code S3PoolMetricPublisher} callbacks (one per in-flight SDK request attempt, §3.8's field
 * javadoc). Each of the four {@code swath.s3.pool.*} fields is its own independent {@code
 * AtomicLong} (never a composite/struct written under one lock), so this guards that heavy
 * concurrent writing never produces a torn/interleaved-composite value on any single field — every
 * final gauge reading must be EXACTLY one of the values some thread actually published, never a
 * corrupted number outside that published set.
 *
 * <p>A {@link CyclicBarrier} releases every writer thread at (as close to) the same instant to
 * maximize contention on the shared {@code AtomicLong}s; each thread {@code t} hammers {@link
 * RunMetrics#updateS3Pool} with its own fixed value {@code t} on all four fields for many
 * iterations. Field-to-field consistency (e.g. {@code leased} and {@code max} ending up from the
 * SAME writer) is deliberately NOT asserted — {@code updateS3Pool}'s own javadoc documents that the
 * four fields are independent atomics with no cross-field ordering guarantee; only each field's
 * OWN final value is checked against the published set.
 */
final class RunMetricsS3PoolConcurrencyTest {

    private static final int WRITER_THREADS = 8;
    private static final int ITERATIONS_PER_THREAD = 20_000;

    @Test
    @Timeout(30)
    void concurrentUpdateS3PoolNeverProducesATornOrUnpublishedGaugeValue() throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        CyclicBarrier startingLine = new CyclicBarrier(WRITER_THREADS);
        List<Thread> writers = IntStream.range(0, WRITER_THREADS)
                .mapToObj(t -> new Thread(() -> {
                    try {
                        startingLine.await();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                        metrics.updateS3Pool(t, t, t, t);
                    }
                }, "s3-pool-writer-" + t))
                .collect(Collectors.toList());

        writers.forEach(Thread::start);
        for (Thread writer : writers) {
            writer.join();
        }

        Set<Double> published = IntStream.range(0, WRITER_THREADS).mapToObj(t -> (double) t)
                .collect(Collectors.toSet());

        assertThat(gaugeValue(metrics, "swath.s3.pool.leased"))
                .as("leased gauge is exactly one writer's published value, never torn or NaN")
                .isIn(published);
        assertThat(gaugeValue(metrics, "swath.s3.pool.idle_available"))
                .as("idle_available gauge is exactly one writer's published value, never torn or NaN")
                .isIn(published);
        assertThat(gaugeValue(metrics, "swath.s3.pool.pending_acquisition"))
                .as("pending_acquisition gauge is exactly one writer's published value, never torn or NaN")
                .isIn(published);
        assertThat(gaugeValue(metrics, "swath.s3.pool.max"))
                .as("max gauge is exactly one writer's published value, never torn or NaN")
                .isIn(published);
    }

    private static double gaugeValue(RunMetrics metrics, String name) {
        Gauge gauge = metrics.registry().find(name).gauge();
        assertThat(gauge).as("gauge %s registered", name).isNotNull();
        return gauge.value();
    }
}
