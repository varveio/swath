/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Mechanics of the published per-worker state (algorithms.md §1.2/§3): the leading
 * cursor, the volatile bound exposed to {@link RangeScanner}, and the thief's
 * snapshot / narrow / restore. The cross-cutting commit-vs-split race guards
 * (RES-3/CONC-3) live elsewhere.
 */
final class WorkerStateTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void snapshotReadsCoherentCursorAndHi() {
        WorkerState w = WorkerStates.of(7, b("a"), b("c"), b("z"));
        WorkerState.Snapshot s = w.snapshot();
        assertThat(s.cursor()).isEqualTo(b("c"));
        assertThat(s.hi()).isEqualTo(b("z"));
        assertThat(w.nodeId()).isEqualTo(7);
        assertThat(w.lo()).isEqualTo(b("a"));
    }

    @Test
    void hiSupplierTracksNarrowAndRestore() {
        WorkerState w = WorkerStates.of(1, null, null, b("z"));
        assertThat(w.hiSupplier().get()).isEqualTo(b("z"));   // open: per-key reader sees the wide bound

        w.lock().lock();
        try {
            w.narrowHi(b("m"));                                // thief lowers (lo, m]
        } finally {
            w.lock().unlock();
        }
        assertThat(w.hiSupplier().get()).isEqualTo(b("m"));   // published to the lock-free reader
        assertThat(w.snapshot().hi()).isEqualTo(b("m"));

        w.lock().lock();
        try {
            w.restoreHi(b("z"));                               // aborted split restores the just-validated bound
        } finally {
            w.lock().unlock();
        }
        assertThat(w.hiSupplier().get()).isEqualTo(b("z"));
    }

    @Test
    void cursorAdvancesAndIsTheLeadingEdge() {
        WorkerState w = WorkerStates.of(1, null, null, null);
        assertThat(w.cursor()).isNull();                       // ⊥
        assertThat(w.hiSupplier().get()).isNull();             // open frontier

        w.lock().lock();
        try {
            w.setCursor(b("k1"));                              // worker advances at commit-enqueue
        } finally {
            w.lock().unlock();
        }
        assertThat(w.cursor()).isEqualTo(b("k1"));
        assertThat(w.snapshot().cursor()).isEqualTo(b("k1"));
    }

    @Test
    void stealEligibilityGatesOnEmitProgressSinceLastSteal() {
        // Progress-gated stealing (algorithms.md §3.2): a fresh worker is NOT a steal victim until
        // it commits a non-empty page; a steal clears the flag until the next non-empty page. This
        // is what bounds re-splits to ≤1 per emitted page and closes the latency livelock.
        WorkerState w = WorkerStates.of(1, null, null, b("z"));
        assertThat(w.stealEligible()).as("fresh worker has emitted nothing yet").isFalse();

        w.lock().lock();
        try {
            w.setCursor(b("k1"));                              // commit a non-empty page → emit progress
        } finally {
            w.lock().unlock();
        }
        assertThat(w.stealEligible()).as("eligible after a non-empty page").isTrue();

        w.lock().lock();
        try {
            w.markStolen();                                    // thief carves it at hand-off
        } finally {
            w.lock().unlock();
        }
        assertThat(w.stealEligible()).as("ineligible again until it emits more").isFalse();

        w.lock().lock();
        try {
            w.setCursor(b("k2"));                              // next non-empty page re-arms eligibility
        } finally {
            w.lock().unlock();
        }
        assertThat(w.stealEligible()).as("re-eligible after emitting again").isTrue();
    }

    @Test
    void densityFractionIsPlainMidpointWithNoPageSignal() {
        // With nothing recorded yet there is no density basis, so the far-ahead fraction is the
        // plain code-point midpoint (0.5) — interpolate then degrades to byteMidpoint, unchanged.
        WorkerState w = WorkerStates.of(1, b("a"), b("c"), b("z"));
        assertThat(w.densityFraction()).isEqualTo(0.5);
    }

    @Test
    void recordPageIsANoOpForEmptyOrNullPages() {
        // An empty/degenerate page must not corrupt the digest; the fraction stays the default.
        WorkerState w = WorkerStates.of(1, b("a"), b("c"), b("z"));
        w.recordPage(null, b("m"), 3);
        w.recordPage(b("m"), null, 3);
        w.recordPage(b("m"), b("n"), 0);
        assertThat(w.densityFraction()).isEqualTo(0.5);
    }

    @Test
    void densityFractionStaysInBandAfterRecordingPages() {
        // After committing real pages the far-ahead fraction is always a valid balance knob in
        // [0.5, 0.75] — never outside (0,1), never NaN — regardless of the density signal.
        WorkerState w = WorkerStates.of(1, b("data/000"), b("data/500"), b("data/999"));
        w.lock().lock();
        try {
            w.setCursor(b("data/500"));
            w.addKeysEmitted(1000);
        } finally {
            w.lock().unlock();
        }
        w.recordPage(b("data/480"), b("data/500"), 200);
        double f = w.densityFraction();
        assertThat(f).isBetween(0.5, 0.75);
        assertThat(Double.isNaN(f)).isFalse();
    }

    @Test
    void unsplittableAndKeysEmittedArePublished() {
        WorkerState w = WorkerStates.of(1, null, null, null);
        assertThat(w.unsplittable()).isFalse();
        assertThat(w.keysEmitted()).isZero();

        w.addKeysEmitted(3);
        w.addKeysEmitted(2);
        w.setUnsplittable(true);

        assertThat(w.keysEmitted()).isEqualTo(5);
        assertThat(w.unsplittable()).isTrue();
    }
}
