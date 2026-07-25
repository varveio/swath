/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.observability;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.LongSupplier;

/**
 * Run trace ("flight recorder") sink seam — V1 {@code --trace <file>}
 * (docs/internals/metrics-internals.md §7). One JSONL
 * event object per line describing range lifecycle, steal attempts, and splits. {@link #NONE} is the
 * always-on default: every call site guards event-object allocation behind {@link #enabled()} first,
 * so a run without {@code --trace} pays zero cost beyond one interface dispatch + a boolean check per
 * would-be event — no {@code ObjectNode}, no string formatting, no I/O.
 *
 * <p>Threaded the same way {@link io.varve.swath.engine.EngineToggles} is: {@link
 * io.varve.swath.engine.WorkStealingScan} carries the {@code TraceSink} on its {@link
 * io.varve.swath.engine.EngineContext} (from which {@link io.varve.swath.engine.Thief} receives it),
 * where an omitted seam null-defaults to {@link #NONE}, so callers/tests that don't care about tracing
 * never see this parameter.
 *
 * <p><b>Sensitivity:</b> traces carry real key names (lo/cursor/hi/pivot), the same
 * sensitivity class as the output listing itself — unlike the summary JSON, which is keys-free apart
 * from cursor samples. See docs/internals/metrics-internals.md §7.
 */
public interface TraceSink extends AutoCloseable {

    /** The no-op default — every method is a free no-op; {@link #enabled()} is {@code false}. */
    TraceSink NONE = NoopTraceSink.INSTANCE;

    /** Opens a JSONL trace sink at {@code path} (truncates any prior trace there), real wall/nano clocks. */
    static TraceSink jsonl(Path path) throws IOException {
        return jsonl(path, System::nanoTime);
    }

    /**
     * As {@link #jsonl(Path)}, with an injectable monotonic {@code nanoClock} — mirrors {@link
     * RunMetrics#RunMetrics(io.micrometer.core.instrument.MeterRegistry, LongSupplier)}'s test seam,
     * so a test gets deterministic event timestamps for free.
     */
    static TraceSink jsonl(Path path, LongSupplier nanoClock) throws IOException {
        return JsonlTraceSink.open(path, nanoClock);
    }

    /** Whether this sink actually records events — guard event-data allocation behind this. */
    boolean enabled();

    /** A worklist range came into existence at scan start (bounds only — no worker/cursor yet). */
    void seeded(long nodeId, byte[] lo, byte[] hi);

    /** A worker claimed {@code (lo, hi]} (resuming from {@code cursor}) off the ready queue. */
    void claimed(long workerId, long nodeId, byte[] lo, byte[] cursor, byte[] hi);

    /** A page was durably committed: {@code keysEmitted} keys, the new cursor, and whether this page completed the range. */
    void pageCommitted(long workerId, long nodeId, int keysEmitted, byte[] cursor, boolean completed);

    /** The outcome of one {@link io.varve.swath.engine.Thief#steal} attempt (piggybacks {@code recordStealReason}). */
    void stealAttempt(long workerId, String outcome, String reason);

    /** A thief split {@code parentNodeId} at {@code pivot}, handing {@code (pivot, hi]} to {@code childNodeId}. */
    void split(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi);

    /** An owner-side proactive self-split — identical shape to {@link #split}, a distinct event name. */
    void ownerSplit(long workerId, long parentNodeId, long childNodeId, String mechanism, byte[] pivot, byte[] hi);

    /** {@code nodeId} finished (drained all the way to its {@code hi} bound). */
    void completed(long workerId, long nodeId);

    /** {@code nodeId}'s scan ended without completing (cancellation, error, broken pipe). */
    void failed(long workerId, long nodeId, String reason);

    /** Flushes and closes the underlying file (a no-op for {@link #NONE}). Never throws. */
    @Override
    void close();
}
