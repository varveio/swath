/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.CarveBrakeMode;
import java.util.List;

/**
 * <b>The pre-registered protocol for the carve-brake K-mode race, written down before this round's
 * numbers exist.</b> Same discipline as {@link GeometryFloorSweepProtocol}, which this round mirrors in
 * structure rather than in subject: that round swept a continuous geometry floor, this one sweeps a
 * discrete threshold ({@code K} in {@code K * maxKeys}) that decides whether {@code
 * OwnerSplitGovernor.decide()} refuses a carve whose recent window-average realized child mass has
 * fallen too low (campaign memo §5, {@code brake-design.md}'s "Mechanism"). The arms, the seeds, the
 * roster's roles and the criteria are committed here, ahead of any per-fixture number.
 *
 * <h2>What this round is asking</h2>
 * Owner priority is locked to <b>(b) flatten the tail</b>: pull the twelve clean serial regressions
 * this brake exists to cure up toward their pre-regression duration, a modest give-back on top wins is
 * an acceptable price, and the dense/uniform shape {@code ConfettiFeedbackWiringTest}'s own javadoc
 * names this brake as the fix for ({@code #78}) must become a deterministic pass. The three {@code K}
 * arms are the family this brake ships as; this round decides which one — or none — is the shipped
 * default.
 *
 * <h2>Arms</h2>
 * {@link #ARMS}, in ladder order: {@link CarveBrakeMode#OFF} (the incumbent — the 0.2.0 shipped default
 * pair, unchanged) and the three thresholds, each installed as {@code EngineToggles.DEFAULT} with only
 * {@code carveBrake} substituted — nothing else in the default pair moves. {@code OFF} is both the
 * <b>control</b> and the <b>incumbent</b> in this round, since the brake is a single new gate added
 * after the shipped default rather than a replacement of an existing one: there is no separate
 * "shipped sensor" to compare against, the way {@code CarveAdmissionRaceProtocol}'s three arms had one.
 *
 * <h2>Seeds and yardstick</h2>
 * All four of {@code SensingRaceProtocol.SEEDS} on every roster fixture, no screening tier — this round
 * exists to resolve a verdict on a locked-in owner priority, not to find a candidate for one. The
 * yardstick is {@link CarveAdmissionRaceProtocol}'s, reused rather than restated: paired, same seed,
 * relative to {@code OFF}'s own duration at that seed, averaged over the four seeds, with that
 * protocol's {@linkplain CarveAdmissionRaceProtocol#NEUTRAL_BAND neutral band} and split rule.
 *
 * <h2>The roster</h2>
 * Staged by the operator under one local root (the {@code -Dswath.sim.listing.corpus} property {@code
 * CorpusSweepRunTest.staged} reads), named nowhere in this file. The round this brake was chartered from
 * names four regression representatives (the sharpest of the twelve clean serial regressions with a
 * captured listing available to the simulator) and two cure guards (fixtures whose win this brake must
 * not give back more than {@link #GIVE_BACK_BUDGET} of) — which directories carry which role is the
 * operator's staging record, not this file's. A fixture the operator did not stage is not raced; it is
 * left to the replay leg, which is a separate unit and reads captures this round cannot open at all
 * (the simulator races a Parquet capture, the replay leg races the shipped binary against a live or
 * replayed store).
 *
 * <h2>The #78 bench, raced apart from the roster</h2>
 * The dense/uniform shape ({@link #BENCH_WORKERS} workers, {@link #BENCH_PAGE_SIZE}-key pages —
 * {@code ConfettiFeedbackWiringTest}'s own scenario shape) over {@link #BENCH_SEEDS} (ten, not four):
 * that test's own javadoc discloses the instability is a per-run coin flip under the 0.2.0 default pair
 * (measured 4/10 passes), so four seeds cannot read a determinism claim and this round's acceptance gate
 * — {@code #78} closed — is read at ten. Generated in-memory rather than staged, because the shape is a
 * synthetic bench and not a captured bucket.
 *
 * <h2>Criteria, adapted from {@code CarveAdmissionRaceProtocol}'s F1–F4</h2>
 * <ol>
 *   <li><b>F1 — the regressions improve.</b> Each regression representative reads {@code ≥}
 *       neutral against {@code OFF} at minimum, with the owner's stated target being a reading that
 *       pulls the fixture back toward its pre-regression duration (design note: "toward {@code ≥0.95}"
 *       of the pre-regression baseline — a magnitude this file does not assert, since the baseline is
 *       the operator's staging record, not a number this protocol invents).</li>
 *   <li><b>F2 — the cures hold within the give-back budget.</b> Each cure guard reads no worse than
 *       {@code -}{@link #GIVE_BACK_BUDGET} against {@code OFF} — the "modest give-back… acceptable"
 *       locked by the owner, read as a band rather than a point.</li>
 *   <li><b>F3 — the roster neutrals stay unbroken.</b> No roster fixture that is neutral-or-better under
 *       {@code OFF} becomes a loss under the winning {@code K}.</li>
 *   <li><b>F4 — the #78 shape goes deterministic.</b> The winning {@code K}'s ten-seed pass rate on the
 *       dense/uniform bench is {@code 10/10} — {@code ConfettiFeedbackWiringTest}'s own acceptance line,
 *       raced here rather than asserted there because determinism is exactly what a four-attempt retry
 *       loop cannot certify (that engine test's own disclosure).</li>
 * </ol>
 *
 * <p><b>Nothing here asserts a magnitude.</b> As in every round of this campaign, the run test that
 * drives this protocol prints the tables these criteria are read against; a threshold enforced in code
 * would be a threshold fitted to the numbers it is judging.
 *
 * <h2>The two outcomes, both of them results</h2>
 * <b>An interior K.</b> Some arm passes F1–F4: reported with its full table, and it is a
 * <em>candidate</em> for the shipped default, not a promotion — the replay leg still owes a serial
 * confirmation on the representatives before the default flips. <b>A monotone family, or a family that
 * cannot close {@code #78}.</b> If every arm that helps the regressions also spends more than the
 * give-back budget on a cure, or if {@code K=4} reproduces the prior measurement that its threshold
 * never engages on the dense/uniform shape (14 runs, zero engagements — the finding {@link
 * #ARMS}'s ordering exists to let a reader see land or not land, arm by arm), this round says so
 * plainly rather than naming a winner it did not earn.
 */
final class CarveBrakeSweepProtocol {

    /**
     * The arms, in ladder order: the incumbent ({@link CarveBrakeMode#OFF}, both control and
     * incumbent — see the class javadoc) followed by the three thresholds in increasing {@code K}.
     */
    static final List<CarveBrakeMode> ARMS = List.of(CarveBrakeMode.OFF, CarveBrakeMode.MASS_K2,
            CarveBrakeMode.MASS_K4, CarveBrakeMode.MASS_K8);

    /** The control/incumbent's {@code --engine-toggle carve_brake} code — {@link #ARMS}' first entry. */
    static final String CONTROL_ARM = CarveBrakeMode.OFF.code();

    /**
     * The give-back a cure guard may spend against {@code OFF} and still count as held — the "modest…
     * acceptable" the owner locked at "10-15%" (design note, priority (b)), read at the wider of the two
     * so a guard is judged against the budget the owner actually granted rather than its narrower half.
     */
    static final double GIVE_BACK_BUDGET = 0.15;

    /**
     * The ten seeds the dense/uniform {@code #78} bench is read at — not the roster's four, because the
     * shape's own instability under the default pair is a per-run coin flip ({@code
     * ConfettiFeedbackWiringTest}'s own disclosure: 4/10 passes), which four seeds cannot certify either
     * way. The first four are {@code SensingRaceProtocol.SEEDS}, so a reader cross-checking one seed
     * against the roster race's own four-seed table is checking the same draw.
     */
    static final long[] BENCH_SEEDS = {SensingRaceProtocol.SEEDS[0], SensingRaceProtocol.SEEDS[1],
            SensingRaceProtocol.SEEDS[2], SensingRaceProtocol.SEEDS[3], 2L, 3L, 4L, 5L, 6L, 7L};

    /**
     * The dense/uniform bench's own fleet size — {@code ConfettiFeedbackWiringTest#runScan}'s own
     * {@code WorkStealingScan} worker count, reproduced here so the sim races the same shape at the same
     * concurrency rather than a shape that merely shares a key count.
     */
    static final int BENCH_WORKERS = 4;

    /**
     * The dense/uniform bench's own page size — {@code ConfettiFeedbackWiringTest}'s {@code MAX_KEYS},
     * the small page that lets a 20k-key flat leaf produce enough owner splits for the classification
     * gate to have something to disagree about run to run.
     */
    static final int BENCH_PAGE_SIZE = 100;

    /**
     * The dense/uniform bench's key count — {@code KeyspaceFixtures#denseFlatLeaf}, the sim's own single
     * flat leaf generator, at the same {@code n} {@code ConfettiFeedbackWiringTest#denseFlat} uses. The
     * two generators differ in their literal key prefix ({@code flat/%09d} here, {@code d/%06d} there)
     * but not in shape: neither nests a directory below the prefix, so both put every split decision on
     * interpolation over a frozen position window — the property {@code #78}'s instability turns on.
     */
    static final int BENCH_KEYS = 20_000;

    private CarveBrakeSweepProtocol() {
    }
}
