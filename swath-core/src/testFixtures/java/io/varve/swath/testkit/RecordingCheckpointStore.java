/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.error.CheckpointException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * A {@link CheckpointStore} decorator that records the durability + run-completion mutations a
 * listing run issues at the end of its lifecycle — {@link #partFinalized}, {@link #markOutputComplete},
 * {@link #setSortPhase} and {@link #markRunFinished} — in call order, forwarding every call to a real
 * delegate store. Per-page/per-split traffic ({@code commitPage}, {@code splitNode}, …) is ignored.
 *
 * <p><b>Two views, on purpose.</b> {@code partFinalized} fires many times (once per finalized part/
 * segment) whereas the tail latch/phase/finish calls fire once, so the fixture exposes the same
 * recording through two accessors:
 * <ul>
 *   <li>{@link #completionChain()} — the recording with {@code partFinalized} <b>filtered out</b>:
 *       the terminal ordered chain of once-only latch/phase/finish calls. Asserting the exact chain
 *       against this view keeps it robust to however many parts a fixture happens to produce.
 *       {@code setSortPhase} records its phase argument, which distinguishes the {@code MERGING} and
 *       {@code PUBLISHED} transitions of the sort-phase machine.</li>
 *   <li>{@link #completionMutations()} — the <b>full</b> recording including every {@code partFinalized}:
 *       used to pin the I6 precondition that the LAST {@code partFinalized} precedes
 *       {@code markOutputComplete} (durable_cursor is latched only after the final part is durable).</li>
 * </ul>
 *
 * <p>For a sink whose completion involves a step that is <em>not</em> a store call (a text run
 * publishes its finished output by an atomic rename, then records the run finished), an optional
 * {@code publishProbe} is sampled at the instant {@code markRunFinished} is recorded, so a test can
 * pin "publish happened before the run was recorded finished" without depending on wall-clock time.
 */
public final class RecordingCheckpointStore extends ForwardingCheckpointStore {

    /** Recorded label for a finalized part/segment — filtered out of {@link #completionChain()}. */
    public static final String PART_FINALIZED = "partFinalized";

    private final List<String> completionMutations = Collections.synchronizedList(new ArrayList<>());
    private final BooleanSupplier publishProbe;
    private volatile Boolean publishObservedAtRunFinished;

    public RecordingCheckpointStore(CheckpointStore delegate) {
        this(delegate, null);
    }

    public RecordingCheckpointStore(CheckpointStore delegate, BooleanSupplier publishProbe) {
        super(delegate);
        this.publishProbe = publishProbe;
    }

    @Override
    public void partFinalized(PartFinalize f) throws CheckpointException {
        completionMutations.add(PART_FINALIZED);
        delegate.partFinalized(f);
    }

    @Override
    public void markOutputComplete(long runId) throws CheckpointException {
        completionMutations.add("markOutputComplete");
        delegate.markOutputComplete(runId);
    }

    @Override
    public void setSortPhase(long runId, SortPhase phase) throws CheckpointException {
        completionMutations.add("setSortPhase:" + phase);
        delegate.setSortPhase(runId, phase);
    }

    @Override
    public void markRunFinished(long runId, RunStatus status) throws CheckpointException {
        if (publishProbe != null) {
            publishObservedAtRunFinished = publishProbe.getAsBoolean();
        }
        completionMutations.add("markRunFinished");
        delegate.markRunFinished(runId, status);
    }

    /**
     * The <b>full</b> recording in call order, including every {@link #PART_FINALIZED} — the view
     * for pinning the I6 precondition (last {@code partFinalized} before {@code markOutputComplete}).
     */
    public List<String> completionMutations() {
        synchronized (completionMutations) {
            return List.copyOf(completionMutations);
        }
    }

    /**
     * The recording with {@link #PART_FINALIZED} filtered out — the terminal ordered chain of
     * once-only latch/phase/finish calls, robust to how many parts a fixture produced.
     */
    public List<String> completionChain() {
        synchronized (completionMutations) {
            return completionMutations.stream().filter(m -> !m.equals(PART_FINALIZED)).toList();
        }
    }

    /**
     * The {@code publishProbe} value sampled when {@code markRunFinished} was recorded, or
     * {@code null} if no probe was supplied or {@code markRunFinished} was never called.
     */
    public Boolean publishObservedAtRunFinished() {
        return publishObservedAtRunFinished;
    }

    /** Drops everything recorded so far — call right before the run to exclude setup traffic. */
    public void reset() {
        completionMutations.clear();
        publishObservedAtRunFinished = null;
    }
}
