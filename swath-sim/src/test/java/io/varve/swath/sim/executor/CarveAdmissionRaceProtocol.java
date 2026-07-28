/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.sim.store.SimStoreBackend;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * <b>The pre-registered protocol for the carve-admission race, written down before the candidate
 * exists.</b> Same discipline as {@code SensingRaceProtocol} and for the same reason: the criteria,
 * the seeds, the yardstick, the roster's roles and the prediction this round is scored against are
 * committed ahead of any variant code, so what counts as a cure cannot drift toward whatever the
 * variant turns out to do. There are no results here.
 *
 * <h2>The finding this races against (traced first, not assumed)</h2>
 * The charter for this round guessed a mechanism — that a truthful estimate makes the owner's
 * far-ahead carve land on nearly-drained ranges, driving the confetti rate to one and letting the
 * feedback gate suppress owner splitting. <b>On the fixture that carries the corpus's one material
 * verdict-grade regression that guess is refused by the trace</b>: the confetti-suppression branch
 * fires zero times under every arm there, and the confetti child share is no higher under the
 * combined cure than under the shipped sensor. What the per-arm decomposition
 * ({@code RealListingRunTest}) found instead:
 *
 * <ol>
 *   <li><b>The work is identical and the packing is not.</b> Range-seconds — the integral of ranges
 *       being drained — agree to a tenth of a percent between the combined cure and the anchored one;
 *       the whole duration gap is <em>serial</em> seconds, the span with one range draining alone.</li>
 *   <li><b>The serial span is one range, and its birth instant is the run's finish line.</b> The
 *       same block of the keyspace, ~80 pages, is shed as a child by both arms and drains in the same
 *       9.4 virtual seconds in both; under the combined cure it is born ~4.8 s later, and the run is
 *       ~4.8 s longer. Nothing else differs.</li>
 *   <li><b>It is born late because the range holding it could not be divided.</b> Its parent — ~160
 *       pages of realized mass — refuses its carve at the owner's remaining-work floor on 64 of its
 *       committed pages under the combined cure, and no thief selects it either. Under the anchored
 *       cure alone a thief takes that far tail 4.8 s earlier.</li>
 *   <li><b>The arithmetic that refuses it is the geometry band's lower half.</b> The combined
 *       estimator reads {@code max(keysEmitted, page) × clamp(geometry, 1/16, 16)}. Where the anchored
 *       geometry pins at its lower bound, a range's proven mass is divided by sixteen, so a range must
 *       emit 64 pages before it clears an admission floor of four — which is exactly the count of
 *       refusals the trace recorded, page for page.</li>
 * </ol>
 *
 * <p>So the defect is in the <b>carve's admission</b> rather than in its placement, and it is the
 * same shape the sensing race's own lesson named: a gate calibrated against a sensor that
 * systematically over-read is mis-calibrated the moment the sensor tells the truth in key units.
 *
 * <h2>The candidate</h2>
 * <b>Geometry may lift a realized-mass estimate; it may not cut one.</b> The rate half's stated thesis
 * is that emitted mass is a <em>lower</em> bound on remaining mass under a heavy-tailed size law, and
 * multiplying it by a geometric factor below one asserts the opposite — that less remains than has
 * already come out — which is the inference the rate estimator exists to refuse. The candidate is
 * therefore the same estimator with the band's lower half removed, and nothing else: the exact
 * cursor-reached-the-bound test still scores a finished range zero, and the upward half of the band
 * keeps its whole role.
 *
 * <h2>Arms, seeds, and the single yardstick</h2>
 * Three arms — the shipped sensor as <b>control</b>, the combined cure as the <b>incumbent
 * candidate</b>, and the combined cure with the lift-only band — at <b>all four</b> of
 * {@code SensingRaceProtocol.SEEDS}, on every fixture of the roster, with <b>no screening tier</b>:
 * this round exists to resolve verdicts, so every leg it reports is a four-seed leg.
 *
 * <p>The yardstick is the one the review corrected the sweep to: <b>paired, same seed, relative to
 * the control's own duration at that seed</b>, averaged over the four seeds. Never a ratio of means
 * across arms, never a mean across fixtures. A fixture whose four per-seed deltas do not share a sign
 * is reported as a <b>split</b> and never as a win or a loss ({@link Verdict#split()}).
 *
 * <h2>Criteria, named before the numbers</h2>
 * <ol>
 *   <li><b>C1 — the loss it is for.</b> On the fixture carrying the material verdict-grade regression,
 *       the candidate reads <b>neutral or better against the control</b> ({@code delta ≥ −}{@link
 *       #NEUTRAL_BAND}). Turning it neutral is the whole objective; beating the control there is not
 *       required.</li>
 *   <li><b>C2 — nothing in the must-not-regress set moves against it.</b> Every roster fixture
 *       carrying that role reads <b>within the neutral band of the incumbent candidate</b>, or better.
 *       That covers, by role: the two fixtures whose bimodal regression under the anchored cure alone
 *       the combined cure <em>flips into a win</em>; the representative collapse cures; and the
 *       synthetic measured-regime fixture the charter's original evidence was taken on.</li>
 *   <li><b>C3 — no new loss anywhere.</b> No roster fixture that is neutral-or-better under the
 *       incumbent candidate becomes a loss under this one.</li>
 *   <li><b>C4 — the guards hold.</b> The canonical bench and both regression guards of the sensing
 *       race — the hash-fanned corpus and the uniform deep-nested run, on their widened occupancy and
 *       throughput assertions — pass with the candidate installed.</li>
 * </ol>
 * A candidate that fails C1 has not cured the thing it was built for; one that passes C1 and fails any
 * of C2–C4 has moved the pathology rather than removed it. <b>Either outcome is the result of this
 * round and is reported as measured.</b>
 *
 * <h2>The prediction, pre-registered so it can fail</h2>
 * The screen-grade regressions that reproduce under <em>both</em> cures are predicted <b>not to
 * move</b>: their loss is present with the anchored cure alone, where no realized-mass magnitude is
 * being scaled at all, so a change confined to how geometry scales that magnitude has no purchase on
 * them. If they move, this diagnosis is incomplete and the round says so.
 *
 * <h2>What may not be claimed</h2>
 * <ul>
 *   <li><b>By-construction zeros are labelled, not counted as cures.</b> Both degenerate-estimate
 *       readings — the estimate discarding emitted keys, and the estimate reading zero — are zero for
 *       every rate-based arm <em>by construction</em>: the emitted count is the estimate. They are
 *       reported and never quoted as evidence that this candidate improved anything.</li>
 *   <li><b>No cherry-picking.</b> A fixture's verdict is its four seeds. A subset is not a result,
 *       and neither is a roster subset chosen after the numbers landed.</li>
 *   <li><b>Regimes are stated.</b> Every roster leg runs at the measured page regime and at the fleet
 *       its own capture ran at; a serial or tail number is quoted with both.</li>
 * </ul>
 */
final class CarveAdmissionRaceProtocol {

    /**
     * Paired relative duration inside this band, either way, is neutral — the corpus sweep's own band,
     * kept identical so this round's verdicts are readable against that one's.
     */
    static final double NEUTRAL_BAND = 0.05;

    /** The tier every roster leg runs on — order-guarded, and fixed across the roster. */
    static final SimStoreBackend BACKEND = SimStoreBackend.STREAMING;

    private CarveAdmissionRaceProtocol() {
    }

    /**
     * One fixture's four-seed reading of one arm against the control, on the protocol's yardstick.
     *
     * @param delta the mean over seeds of {@code (control − arm) / control} at that seed — positive is
     *              faster than the control
     * @param perSeed the four values that mean is over, in seed order, so no reader has to take it
     * @param split whether those four disagree in sign, which makes the fixture a split rather than a
     *              win or a loss whatever the mean says
     */
    record Verdict(String fixture, String arm, double delta, List<Double> perSeed, boolean split) {

        /** Whether this reading is inside the neutral band, i.e. neither a win nor a loss. */
        boolean neutral() {
            return Math.abs(delta) <= NEUTRAL_BAND;
        }

        /** {@code win}, {@code loss}, {@code neutral} or {@code split} — the label, never a number. */
        String label() {
            if (split) {
                return "split";
            }
            return neutral() ? "neutral" : (delta > 0 ? "win" : "loss");
        }

        String row() {
            StringBuilder seeds = new StringBuilder();
            for (double value : perSeed) {
                seeds.append(String.format(Locale.ROOT, " %+7.2f%%", value * 100.0));
            }
            return String.format(Locale.ROOT, "%-34s %-22s %+7.2f%%  %-8s %s", fixture, arm,
                    delta * 100.0, label(), seeds.toString().trim());
        }
    }

    /**
     * Every arm's verdict on every fixture, read against {@code control}'s own leg at the same seed.
     * Legs with no control at their seed are skipped rather than compared against another seed's — the
     * pairing is the yardstick, so an unpaired leg is not a reading.
     */
    static List<Verdict> verdicts(List<CorpusSweep.Row> rows, SensingVariant control) {
        String controlArm = SensingRaceProtocol.label(control);
        Map<String, Map<String, Map<Long, Double>>> durations = new TreeMap<>();
        for (CorpusSweep.Row row : rows) {
            durations.computeIfAbsent(row.fixture(), fixture -> new TreeMap<>())
                    .computeIfAbsent(row.leg().variant(), arm -> new TreeMap<>())
                    .put(row.leg().seed(), (double) row.leg().result().virtualNanos());
        }
        List<Verdict> verdicts = new ArrayList<>();
        durations.forEach((fixture, byArm) -> {
            Map<Long, Double> controlLegs = byArm.getOrDefault(controlArm, Map.of());
            byArm.forEach((arm, legs) -> {
                if (arm.equals(controlArm)) {
                    return;
                }
                List<Double> perSeed = new ArrayList<>();
                legs.forEach((seed, nanos) -> {
                    Double against = controlLegs.get(seed);
                    if (against != null && against > 0) {
                        perSeed.add((against - nanos) / against);
                    }
                });
                if (perSeed.isEmpty()) {
                    return;
                }
                double mean = perSeed.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
                boolean split = perSeed.stream().anyMatch(value -> value > NEUTRAL_BAND)
                        && perSeed.stream().anyMatch(value -> value < -NEUTRAL_BAND);
                verdicts.add(new Verdict(fixture, arm, mean, List.copyOf(perSeed), split));
            });
        });
        return List.copyOf(verdicts);
    }

    /** The verdict table under one caption, in fixture-then-arm order. */
    static void printVerdicts(String caption, List<Verdict> verdicts) {
        StringBuilder out = new StringBuilder(caption).append(System.lineSeparator());
        out.append(String.format(Locale.ROOT, "%-34s %-22s %8s  %-8s %s", "fixture", "arm", "delta",
                "verdict", "per-seed")).append(System.lineSeparator());
        for (Verdict verdict : verdicts) {
            out.append(verdict.row()).append(System.lineSeparator());
        }
        System.out.print(out);
    }
}
