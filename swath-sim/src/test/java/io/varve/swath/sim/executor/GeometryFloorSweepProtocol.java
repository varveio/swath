/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

/**
 * <b>The pre-registered protocol for the geometry-floor sweep, written down before the floors
 * exist.</b> Same discipline as {@link CarveAdmissionRaceProtocol}, which this round continues: the
 * arms, the seeds, the roster's roles, the yardstick and — above all — the sentence that decides the
 * round are committed ahead of any variant code, because this round's whole risk is that a value
 * picked from a table of five looks like a discovery. There are no results here.
 *
 * <h2>What this round is asking</h2>
 * The band's lower half is one dial. At its settled ends the corpus says two different things: with
 * the half in force the combination gives four fixtures a regression it never cured, and with the half
 * removed those four are cured while two others give back more than the four gained — one by handing
 * back the very confetti cure that earned the combination its win there, and one by quarantining a
 * fleet from its own straggler.
 *
 * <p>The traced mechanism says why the ends disagree: the lower half cuts a <em>measured</em>
 * shortfall and an <em>inferred</em> one with the same factor, and only the inferred one is the
 * estimator's own under-statement. Removing the half discards both. <b>An interior floor discards the
 * inferred shortfall and keeps the measured one exactly when the two live at different geometries</b>
 * — which is a fact about real keyspaces, not about this code, and so is measured rather than argued.
 *
 * <h2>The floors, fixed before the numbers</h2>
 * {@link #FLOORS}: an eighth, a quarter and a half, swept between the two settled ends — the
 * symmetric band's sixteenth and the lift-only band's one. A geometric ladder because the dial is
 * multiplicative: what a floor decides is the emitted mass at which a range clears the owner's
 * admission floor, and that boundary moves as the floor's reciprocal.
 *
 * <p>Three interior values and not five: the sweep costs a full four-seed leg per arm per fixture,
 * and the two ends are already measured over the whole corpus. Any finer ladder is a follow-up to a
 * result this round produces, not a hedge against one it might not.
 *
 * <h2>Arms, seeds and the yardstick</h2>
 * Six arms — the shipped sensor as <b>control</b>, the combination as the <b>incumbent</b>, the
 * lift-only end as the <b>reference cure</b>, and the three floors — at <b>all four</b> of
 * {@code SensingRaceProtocol.SEEDS} on every roster fixture, with <b>no screening tier</b>: this
 * round exists to resolve a verdict, so every leg it reports is a four-seed leg.
 *
 * <p>The yardstick is {@link CarveAdmissionRaceProtocol}'s, unchanged and reused rather than restated:
 * paired, same seed, relative to the named baseline's own duration at that seed, averaged over the
 * four seeds, with that protocol's {@linkplain CarveAdmissionRaceProtocol#NEUTRAL_BAND neutral band}
 * and its split rule. Verdicts are read twice — against the control, and against the incumbent — for
 * the same reason that round read them twice: what a promotion turns on is the head-to-head.
 *
 * <h2>The roster, by role</h2>
 * Fourteen fixtures, staged by the operator and named nowhere here, in five roles:
 * <ol>
 *   <li><b>The two returns</b> — the fixtures the lift-only end gave back materially against the
 *       incumbent. These are what an interior floor has to win back.</li>
 *   <li><b>The four cures</b> — the fixtures the lift-only end cured, three of them regressions the
 *       incumbent itself caused. These are what an interior floor has to keep.</li>
 *   <li><b>The four give-backs</b> — the further fixtures the lift-only end handed back five to
 *       fifteen percent of.</li>
 *   <li><b>Two representative collapse cures</b> — from the eleven the whole family holds, so a floor
 *       that buys its trade with the collapse profile is visible here.</li>
 *   <li><b>Two bimodality testbeds</b> — fixtures whose per-seed readings disagree in sign, where a
 *       mean is least trustworthy and the four-seed rule earns its cost.</li>
 * </ol>
 *
 * <h2>The deciding question, in one sentence</h2>
 * <b>Does any interior floor hold the four cures — each reading {@code ≥ +15%} against the control,
 * or {@code ≥} neutral against the control on the one the incumbent itself lost — while returning
 * both of the two returns to {@code ≥} neutral against the incumbent, without turning any
 * neutral-or-better roster fixture into a loss, without costing a collapse cure, and with the bench
 * and both guards holding?</b>
 *
 * <p>Read as four criteria:
 * <ol>
 *   <li><b>F1 — the cures hold.</b> Each of the four cures reads {@code ≥ +15%} against the control,
 *       except that a cure whose incumbent reading is itself a loss need only reach {@code ≥} neutral
 *       against the control ({@code delta ≥ −}{@link CarveAdmissionRaceProtocol#NEUTRAL_BAND}).</li>
 *   <li><b>F2 — the returns come back.</b> Both returns read {@code ≥} neutral <em>against the
 *       incumbent</em>, at the same band. Against the incumbent and not the control, because what the
 *       lift-only end spent is measured from the incumbent's own reading.</li>
 *   <li><b>F3 — nothing else breaks.</b> No roster fixture that is neutral-or-better under the
 *       incumbent becomes a loss under the floor, and neither representative collapse cure moves
 *       against it — the eleven collapse cures are the family's standing property, not this round's
 *       to spend.</li>
 *   <li><b>F4 — the guards hold.</b> {@code CarveAdmissionRaceProtocol}'s C4, run for the best
 *       interior floor only: the canonical bench and both regression guards at the bench regime, and
 *       the bench again at the measured regime against the incumbent.</li>
 * </ol>
 *
 * <h2>The two outcomes, both of them results</h2>
 * <ul>
 *   <li><b>An interior optimum.</b> Some floor passes F1–F4. It is reported with its full table and
 *       its per-seed values, and it is a <em>candidate</em>: three arms were compared and the best was
 *       named, so what the roster can support is a hypothesis for corpus confirmation, never a
 *       promotion (below).</li>
 *   <li><b>A monotone response.</b> No floor passes, and the roster's two sets move together with the
 *       dial — the returns recover only as the cures decay. Then the dial is a single trade with no
 *       interior optimum, this round says so plainly, and the frame-conditioned candidate is the path,
 *       with these numbers as its baseline. <b>A monotone result is the round's finding and not its
 *       failure</b>, and it is what makes the sweep worth running before the harder change.</li>
 * </ul>
 *
 * <h2>What may not be claimed</h2>
 * <ul>
 *   <li><b>A winner on this roster is not a promotion.</b> Fourteen fixtures chosen for the roles
 *       above are the fixtures where the dial is known to act; the corpus is where a sensing change is
 *       promoted or refused, and a floor that passes here still owes the full four-arm corpus round at
 *       the same seeds, read head-to-head against the incumbent, before anything ships.</li>
 *   <li><b>Selecting the best of three floors is a selection, and is disclosed as one.</b> The
 *       reported margin of the winning floor is the margin of the maximum over three arms, which is
 *       biased upward by the selection; only the corpus round retires that bias.</li>
 *   <li><b>No cherry-picking.</b> A fixture's verdict is its four seeds, a roster subset chosen after
 *       the numbers landed is not a roster, and a floor's verdict is every criterion above rather than
 *       the one it reads best on.</li>
 *   <li><b>By-construction zeros are labelled, not counted.</b> As in the round before, both
 *       degenerate-estimate readings are zero for every rate-based arm because the emitted count
 *       <em>is</em> the estimate.</li>
 * </ul>
 */
final class GeometryFloorSweepProtocol {

    /**
     * The interior floors this round sweeps, in ladder order, between the symmetric band's sixteenth
     * and the lift-only band's one. Stated as numbers here, before any arm installs one, so the arms
     * can be held to the ladder that was registered rather than to the ladder that was run.
     */
    static final double[] FLOORS = {1.0 / 8.0, 1.0 / 4.0, 1.0 / 2.0};

    /**
     * The margin a cure has to keep against the control to count as held — the "still cured" line of
     * F1, set at the magnitude the lift-only end's own cures cleared, so a floor that merely halves a
     * cure does not pass as one.
     */
    static final double CURE_HELD_DELTA = 0.15;

    private GeometryFloorSweepProtocol() {
    }
}
