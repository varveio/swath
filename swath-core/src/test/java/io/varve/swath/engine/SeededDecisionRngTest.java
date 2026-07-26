/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The opt-in seeded {@link DecisionRng} live default (owner decision 2026-07-26): per-worker seed
 * derivation ({@link SeededDecisionRng#deriveWorkerSeed}), its own draw-sequence determinism, and an
 * end-to-end replay through {@link Thief}'s structure-probe suppression escape hatch (the one
 * consumer — {@code ThiefPolicy#structureProbingEnabled}, contracts.md §2.1) to prove the wiring, not
 * just the arithmetic, is deterministic.
 */
final class SeededDecisionRngTest {

    private static final long RUN_ID = 501L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Per-worker seed derivation: a pure function of (baseSeed, workerId).
    // -------------------------------------------------------------------------

    /**
     * Expected values computed independently (a standalone SplitMix64 mixing-step reference,
     * evaluated outside this codebase) rather than by re-running {@link
     * SeededDecisionRng#deriveWorkerSeed} against itself — a mutant that corrugates the formula but
     * keeps it "some function of both inputs" would evade a self-referential assertion.
     */
    @Test
    void derivedWorkerSeedMatchesAnIndependentlyComputedReference() {
        assertThat(SeededDecisionRng.deriveWorkerSeed(42L, 0L)).isEqualTo(-6387817139659442654L);
        assertThat(SeededDecisionRng.deriveWorkerSeed(42L, 1L)).isEqualTo(-4767286540954276203L);
        assertThat(SeededDecisionRng.deriveWorkerSeed(42L, 2L)).isEqualTo(2949826092126892291L);
        assertThat(SeededDecisionRng.deriveWorkerSeed(7L, 0L)).isEqualTo(1346066267577507604L);
        assertThat(SeededDecisionRng.deriveWorkerSeed(0L, 0L)).isEqualTo(0L);
        assertThat(SeededDecisionRng.deriveWorkerSeed(-1L, 5L)).isEqualTo(-5431262886246717010L);
        assertThat(SeededDecisionRng.deriveWorkerSeed(123456789L, 1000L)).isEqualTo(-2579207788442878829L);
    }

    @Test
    void derivedWorkerSeedIsDeterministicGivenTheSameInputs() {
        assertThat(SeededDecisionRng.deriveWorkerSeed(99L, 4L))
                .isEqualTo(SeededDecisionRng.deriveWorkerSeed(99L, 4L));
    }

    @Test
    void derivedWorkerSeedsAreDistinctAcrossWorkerIdsForOneBaseSeed() {
        long base = 2026L;
        Set<Long> derived = new HashSet<>();
        for (long workerId = 0; workerId < 64; workerId++) {
            derived.add(SeededDecisionRng.deriveWorkerSeed(base, workerId));
        }
        assertThat(derived).as("64 worker ids must not collide onto fewer than 64 derived seeds")
                .hasSize(64);
    }

    @Test
    void derivedWorkerSeedIsIndependentOfHowManyOtherWorkersTheRunHas() {
        // Worker 3's own derived seed must not depend on whether the run has 4 workers or 400 --
        // the derivation reads only (baseSeed, workerId), never a worker COUNT.
        long seedInASmallRun = SeededDecisionRng.deriveWorkerSeed(2026L, 3L);
        long seedInALargeRun = SeededDecisionRng.deriveWorkerSeed(2026L, 3L);
        assertThat(seedInASmallRun).isEqualTo(seedInALargeRun);
    }

    // -------------------------------------------------------------------------
    // Draw-sequence behavior: same (seed, worker) replays identically; either input changing it.
    // -------------------------------------------------------------------------

    private static List<Integer> draws(long baseSeed, long workerId, int count) throws Exception {
        return RunContext.runWorkerWhereBound(workerId, () -> {
            SeededDecisionRng rng = new SeededDecisionRng(baseSeed);
            List<Integer> out = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                out.add(rng.nextInt(64));
            }
            return out;
        });
    }

    @Test
    void sameBaseSeedAndWorkerIdReplayTheIdenticalDrawSequence() throws Exception {
        List<Integer> first = draws(42L, 3L, 50);
        List<Integer> second = draws(42L, 3L, 50);
        assertThat(first).isEqualTo(second);
    }

    @Test
    void differentWorkerIdsDrawDifferentSequencesForTheSameBaseSeed() throws Exception {
        List<Integer> worker3 = draws(42L, 3L, 50);
        List<Integer> worker4 = draws(42L, 4L, 50);
        assertThat(worker3).isNotEqualTo(worker4);
    }

    @Test
    void differentBaseSeedsDrawDifferentSequencesForTheSameWorkerId() throws Exception {
        List<Integer> seedA = draws(42L, 3L, 50);
        List<Integer> seedB = draws(7L, 3L, 50);
        assertThat(seedA).isNotEqualTo(seedB);
    }

    // -------------------------------------------------------------------------
    // End-to-end through Thief: the structure-probe suppression escape hatch
    // (ThiefPolicy#structureProbingEnabled, the one DecisionRng consumer) is the only place a live
    // decision actually branches on this draw.
    // -------------------------------------------------------------------------

    /** {@code <root>/<ddd>/obj00} for {@code dirs} zero-padded (3-digit) sibling directories --
     *  the same uncapped structure-probe shape {@code DecisionTraceGoldenTest}'s own scenarios 8/9
     *  drive {@code ThiefPolicy#structureProbingEnabled}'s escape hatch with. */
    private static List<byte[]> manyDirs(String root, int dirs) {
        List<byte[]> keys = new ArrayList<>(dirs);
        for (int d = 0; d < dirs; d++) {
            keys.add(b(root + "/%03d/obj00".formatted(d)));
        }
        return keys;
    }

    /**
     * Drives {@code attempts} FRESH one-shot {@link Thief#steal} calls, each against a brand-new
     * victim already over {@code STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD} (so every attempt reaches
     * the escape hatch's {@code rng.nextInt(64)} draw), all sharing ONE {@link SeededDecisionRng}
     * bound to one fixed worker id -- so the returned list is the ordered classification ({@code
     * "structure_probe"} when the draw hits and probing proceeds, {@code "suppressed"} when it
     * misses) of {@code attempts} consecutive draws from that one worker's stream.
     */
    private static List<String> escapeHatchStream(long baseSeed, long workerId, int attempts) throws Exception {
        return RunContext.runWorkerWhereBound(workerId, () -> {
            SeededDecisionRng rng = new SeededDecisionRng(baseSeed);
            List<String> outcomes = new ArrayList<>(attempts);
            for (int i = 0; i < attempts; i++) {
                RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
                WorkerState victim = WorkerStates.of(i, b("root/"), b("root/005/obj00"), b("root/zzz"));
                victim.addKeysEmitted(100);
                for (int z = 0; z < 8; z++) {
                    victim.recordZeroFanoutStructureProbe();
                }
                Thief thief = new Thief(StubCheckpointStore.returning(1000L + i),
                        MockPageFetcher.builder().keys(manyDirs("root", 20)).build(), RUN_ID, new byte[0],
                        ListingMode.OBJECTS, (childId, lo, hi) -> { }, metrics, EngineToggles.DEFAULT,
                        TraceSink.NONE, rng);
                thief.steal(List.of(victim));
                Map<String, Long> deltas = metrics.diagnostics(Duration.ZERO).stealReasons();
                if (deltas.getOrDefault("STRUCTURE.suppressed_zero_fanout", 0L) > 0) {
                    outcomes.add("suppressed");
                } else if (deltas.getOrDefault("PIVOT.structure_probe", 0L) > 0) {
                    outcomes.add("structure_probe");
                } else {
                    outcomes.add("other:" + deltas);
                }
            }
            return outcomes;
        });
    }

    @Test
    void sameSeedReplaysAnIdenticalDecisionStreamThroughTheEscapeHatch() throws Exception {
        List<String> first = escapeHatchStream(42L, 3L, 50);
        List<String> second = escapeHatchStream(42L, 3L, 50);
        assertThat(first).isEqualTo(second);
        assertThat(first).as("every entry must be a real classification, or this pins nothing")
                .allMatch(o -> o.equals("suppressed") || o.equals("structure_probe"));
        assertThat(first).as("the escape hatch must actually fire at least once in 50 draws at this "
                        + "seed/worker, or this only pins the (trivial) all-suppressed case")
                .contains("structure_probe");
    }

    @Test
    void differentSeedProducesADifferentDecisionStreamSomewhere() throws Exception {
        List<String> seedA = escapeHatchStream(42L, 3L, 50);
        List<String> seedB = escapeHatchStream(7L, 3L, 50);
        assertThat(seedA).isNotEqualTo(seedB);
    }
}
