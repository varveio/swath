/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Per-victim futility pacing. Guards the per-victim cooldown: consecutive futile steal
 * outcomes against ONE victim trip a per-victim cooldown that skips it as a steal target, while a
 * productive sibling stays fully stealable. Do not make the cooldown GLOBAL (shared across
 * victims) — that throttles steals from a productive sibling too; a productive carve on a victim
 * clears only its own pacing.
 *
 * <p>Ordinary unit guards of the pacing mechanics — not the PROP-1/RES-3/CONC cross-cutting interleavings.
 */
final class FutilityPacingTest {

    private static final long RUN_ID = 42L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- WorkerState pacing mechanics -----------------------------------------------------------

    @Test
    void cooldownTripsOnlyAfterTheThresholdAndIsConsumedBySkips() {
        WorkerState w = WorkerStates.of(1, b("a"), b("a5"), b("z"));
        for (int i = 0; i < WorkerState.FUTILITY_PACE_THRESHOLD - 1; i++) {
            w.recordFutileSteal();
            assertThat(w.stealPaced()).as("below the threshold the victim is not paced").isFalse();
        }
        w.recordFutileSteal();   // the threshold-th consecutive futile outcome trips the cooldown
        // A cooldown of >=1 skip is now armed: at least one stealPaced() returns true, then it expires.
        assertThat(w.stealPaced()).as("threshold reached ⇒ paced").isTrue();
        int consumed = 1;
        while (w.stealPaced()) {
            consumed++;
        }
        assertThat(consumed).as("the cooldown is a bounded number of skips, then expires")
                .isBetween(1, WorkerState.FUTILITY_PACE_MAX_COOLDOWN);
        assertThat(w.stealPaced()).as("expired cooldown ⇒ eligible again").isFalse();
    }

    @Test
    void productiveProgressResetsPacing() {
        WorkerState w = WorkerStates.of(1, b("a"), b("a5"), b("z"));
        for (int i = 0; i < WorkerState.FUTILITY_PACE_THRESHOLD; i++) {
            w.recordFutileSteal();
        }
        assertThat(w.stealPaced()).as("paced after the threshold").isTrue();

        w.markStolen();   // a productive carve — clears pacing
        assertThat(w.stealPaced()).as("productive progress clears the pacing cooldown").isFalse();
    }

    // ---- Thief selection: paced victim skipped, sibling still stolen from -----------------------

    private static final class RecordingSink implements Thief.ChildSink {
        final List<Long> children = new ArrayList<>();
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { children.add(childNodeId); }
    }

    @Test
    void pacedVictimIsSkippedWhileAProductiveSiblingIsStillStolenFrom()
            throws SwathException, InterruptedException {
        // A phantom racing drainer (paced) must NOT throttle steals from a genuinely-splittable
        // sibling. The paced victim has the LARGER est (would be argmax) but is skipped; the
        // sibling yields the child.
        List<byte[]> siblingKeys = new ArrayList<>();
        for (int i = 1; i <= 9; i++) {
            siblingKeys.add(b("s/" + i));   // s/1..s/9 — a plain uniform range the midpoint splits cleanly
        }

        WorkerState paced = WorkerStates.of(2, b("d/00"), b("d/01"), b("d/99"));
        paced.addKeysEmitted(10_000);   // large est ⇒ it WOULD be selected first, absent pacing
        for (int i = 0; i < WorkerState.FUTILITY_PACE_THRESHOLD; i++) {
            paced.recordFutileSteal();
        }

        WorkerState sibling = WorkerStates.of(3, b("s/0"), b("s/1"), b("s/9"));
        sibling.addKeysEmitted(4);
        byte[] siblingHiBefore = sibling.hi();
        byte[] pacedHiBefore = paced.hi();

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(siblingKeys).build();
        StubCheckpointStore store = StubCheckpointStore.returning(77L);
        RecordingSink sink = new RecordingSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, sink, metrics);

        assertThat(thief.steal(List.of(paced, sibling))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        // The child came from the SIBLING (node 3), not the paced victim (node 2).
        assertThat(store.lastSplit.victimId()).as("the sibling was stolen from, not the paced victim")
                .isEqualTo(3L);
        assertThat(sibling.hi()).as("sibling narrowed").isNotEqualTo(siblingHiBefore);
        assertThat(paced.hi()).as("paced victim untouched").isEqualTo(pacedHiBefore);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("STEAL.futility_paced", 0L))
                .as("the paced victim was skipped (counted)").isGreaterThanOrEqualTo(1L);
    }
}
