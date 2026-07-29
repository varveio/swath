/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.CarveBrakeMode;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.EngineTimeBudgets;
import io.varve.swath.sim.model.LatencyModel;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * <b>Critical-2 regression:</b> {@code SimExecutor}'s owner-split mutation loop previously never
 * resolved EITHER carve-brake mutation ({@code CLAIM_CARVE_BRAKE_PROBE_SLOT}/{@code
 * CONSUME_CARVE_BRAKE_PROBE_SLOT}), so {@code carveBrakeProbeSeq} stayed at {@code 0} for an entire
 * simulated run and the probe escape was structurally dead in the simulator — any sim-side race
 * measurement of the brake would have measured a mechanism that could never recover from
 * suppression. This pins that the fixed executor actually advances and resolves the brake's own
 * probe sequence end to end, in the simulator, not just in the real engine.
 *
 * <p><b>{@code SimSeedMode.SHALLOW} plus a multi-directory corpus, not {@code NONE} plus a single
 * dense leaf.</b> Under {@code SimSeedMode.NONE} the sole seeded range always carries {@code hi ==
 * null} (a permanently open frontier — {@code seedRanges} hands it an empty cut list) — the owner-
 * split governor's OWN {@code OPEN_FRONTIER} early-out then refuses EVERY consult for the whole run,
 * so the brake (which lives entirely inside that gate chain) could never engage at all, regardless
 * of this fix. {@link io.varve.swath.sim.fixture.KeyspaceFixtures#hashFannedCorpus} under {@code
 * SHALLOW} gives the seed descent real directory structure to cut, so every range but the last one
 * gets a genuinely bounded {@code hi} the owner can carve.
 */
final class CarveBrakeProbeEscapeLivenessTest {

    private static final long LATENCY_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private static final LatencyModel CONSTANT = PolicyRunFixtures.perClass(LATENCY_NANOS, LATENCY_NANOS);
    private static final int PAGE_SIZE = 100;
    private static final int TOP_DIRS = 8;
    private static final int SUB_DIRS = 8;
    private static final int PER_DIR = 4_000;
    private static final long KEYS = (long) TOP_DIRS * SUB_DIRS * PER_DIR;
    /**
     * The carve brake's own probe cadence is a documented, inherent per-run instability (the same
     * "don't assert on one draw" discipline {@code ConfettiFeedbackWiringTest}'s carve-brake wiring
     * case applies for the real engine) — not every single attempt is guaranteed to cross the
     * modulo-16 probe boundary, so this retries a bounded number of times rather than asserting on
     * one draw. In practice this fires on the first attempt (measured directly against this fixture).
     */
    private static final int ATTEMPTS = 8;

    @Test
    void carveBrakeProbeEscapeFiresAtLeastOnceAcrossAttempts() {
        boolean probed = false;
        for (int attempt = 0; attempt < ATTEMPTS && !probed; attempt++) {
            ListingFixtureStore store =
                    new ListingFixtureStore(KeyspaceFixtures.hashFannedCorpus(TOP_DIRS, SUB_DIRS, PER_DIR));
            PolicyScenario scenario = new PolicyScenario(20260729L + attempt, 1, PAGE_SIZE, new byte[0],
                    PolicyScenario.SimSeedMode.SHALLOW, EngineToggles.DEFAULT.withCarveBrake(CarveBrakeMode.MASS_K8),
                    CONSTANT, PolicyRunFixtures.zeroedCost("carve-brake liveness, not a cost measurement"),
                    EngineTimeBudgets.engineDefaults(), PolicyScenario.FaultDisposition.RIDE_OUT, 0, false,
                    PolicyScenario.DEFAULT_MAX_EVENTS);

            PolicyRunResult result = SimExecutor.run(scenario, store, "in-memory hash-fanned corpus (carve brake)");

            assertThat(result.completed()).as("attempt %d must complete", attempt).isTrue();
            assertThat(result.keysEmitted()).as("attempt %d: exact-once emission", attempt).isEqualTo(KEYS);
            probed = result.counters().getOrDefault("OWNER_SPLIT.carve_brake_probe", 0L) > 0L;
        }
        assertThat(probed)
                .as("carve_brake_probe must fire at least once within %d attempts -- proving the "
                        + "simulator actually resolves the brake's own independent probe sequence "
                        + "(Critical-2 fix), not merely that the brake engages at all", ATTEMPTS)
                .isTrue();
    }
}
