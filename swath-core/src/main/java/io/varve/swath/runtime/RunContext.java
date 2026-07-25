/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.MeterRegistries;
import io.varve.swath.observability.RunMetrics;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * The immutable per-run context: cancellation token, meter registry, and
 * metrics. Bound as a single {@link ScopedValue} at the pipeline root.
 *
 * <p>A {@code ScopedValue} does not auto-inherit into a pre-existing pool
 * thread, so each long-lived worklist worker re-binds it explicitly at the top
 * of its loop via {@link #runWhereBound}.
 */
public record RunContext(CancellationToken cancellation, MeterRegistry meterRegistry, RunMetrics metrics) {

    private static final long NONE = -1L;

    public RunContext {
        Objects.requireNonNull(cancellation, "cancellation");
        Objects.requireNonNull(meterRegistry, "meterRegistry");
        Objects.requireNonNull(metrics, "metrics");
    }

    public RunContext(CancellationToken cancellation) {
        this(cancellation, new SimpleMeterRegistry());
    }

    public RunContext(CancellationToken cancellation, MeterRegistry meterRegistry) {
        this(cancellation, meterRegistry, new RunMetrics(meterRegistry));
    }

    /** The single ScopedValue carrying the run context through the pipeline. */
    public static final ScopedValue<RunContext> CURRENT = ScopedValue.newInstance();
    public static final ScopedValue<Long> RUN_ID = ScopedValue.newInstance();
    public static final ScopedValue<Long> WORKER_ID = ScopedValue.newInstance();
    public static final ScopedValue<NodeScope> NODE = ScopedValue.newInstance();

    public record NodeScope(long nodeId, byte[] lo, byte[] hi) {
    }

    public static RunContext create() {
        return create(Map.of());
    }

    /** Same as {@link #create()}, plus per-run OTLP resource attributes (see MeterRegistries). */
    public static RunContext create(Map<String, String> resourceAttrs) {
        return new RunContext(new CancellationToken(), MeterRegistries.fromEnv(resourceAttrs));
    }

    /**
     * Same as {@link #create()}, but with an already-resolved {@link MeterRegistry} — e.g.
     * {@link MeterRegistries#fromCliConfig}, which needs the CLI's {@code --metrics-export}/{@code
     * --metrics-endpoint}/{@code --metrics-interval} inputs the env-only {@link #create(java.util.Map)}
     * cannot see — instead of resolving one from the environment internally.
     */
    public static RunContext create(MeterRegistry meterRegistry) {
        return new RunContext(new CancellationToken(), meterRegistry);
    }

    /** The bound context, or throws if no run is in scope. */
    public static RunContext current() {
        return CURRENT.get();
    }

    /** Run {@code task} with this context bound as {@link #CURRENT}. */
    public <T> T runWhereBound(Callable<T> task) throws Exception {
        return ScopedValue.where(CURRENT, this).call(task::call);
    }

    /** Run {@code task} with this context and durable run id bound. */
    public <T> T runWhereBound(long runId, Callable<T> task) throws Exception {
        metrics.setRunId(runId);
        return ScopedValue.where(CURRENT, this).where(RUN_ID, runId).call(task::call);
    }

    /** Run {@code task} with this context bound as {@link #CURRENT}. */
    public void runWhereBound(Runnable task) {
        ScopedValue.where(CURRENT, this).run(task);
    }

    public static <T> T runWorkerWhereBound(long workerId, Callable<T> task) throws Exception {
        return ScopedValue.where(WORKER_ID, workerId).call(task::call);
    }

    public static <T> T runNodeWhereBound(long nodeId, byte[] lo, byte[] hi, Callable<T> task) throws Exception {
        return ScopedValue.where(NODE, new NodeScope(nodeId, lo, hi)).call(task::call);
    }

    public static long runIdOrNone() {
        return RUN_ID.isBound() ? RUN_ID.get() : NONE;
    }

    public static long workerIdOrNone() {
        return WORKER_ID.isBound() ? WORKER_ID.get() : NONE;
    }

    public static long nodeIdOrNone() {
        return NODE.isBound() ? NODE.get().nodeId() : NONE;
    }
}
