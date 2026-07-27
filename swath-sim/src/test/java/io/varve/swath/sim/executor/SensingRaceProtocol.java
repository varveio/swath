/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.sim.fixture.KeyspaceFixtures;
import io.varve.swath.sim.fixture.KeyspaceFixtures.SubtreeMass;
import io.varve.swath.sim.fixture.ListingFixtureStore;
import io.varve.swath.sim.model.LatencyModel;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * <b>The pre-registered protocol for the position-sensor race, written down before any variant
 * exists.</b> Everything in this file — the bench, the seeds, the criteria, the guards, the rule
 * against cherry-picking — is committed ahead of the first variant implementation, so that what
 * counts as a win cannot drift toward whatever the variants turn out to do. Read it as a claim about
 * method, not about results; there are no results yet.
 *
 * <h2>What is being raced, and against what</h2>
 * The engine steers victim choice, pivot placement, the owner-side self-split and the density
 * feedback on one quantity — a local density times a remaining span, both measured in a byte-window
 * fraction anchored at the divergence of a range's own bounds. On a deep-nested keyspace the cursor
 * moves many bytes below that anchor, so the fraction does not move, the consumed span reads zero,
 * and the estimate discards the range's emitted keys outright. Two cures are raced against that
 * estimator as swappable policy variants, plus their combination.
 *
 * <h2>Bench (designated, not chosen here)</h2>
 * {@code LEAF_CONCENTRATED} at a <b>100-key page</b>, eight workers, shallow seed, the remote latency
 * the other at-scale fixtures use — i.e. exactly {@code MassConcentrationAtScaleTest}'s concentrated
 * run. It is the designated bench because it is the only fixture here whose pathology is a property
 * of the keyspace rather than of a schedule: its serial fraction is constant to a few tenths of a
 * percent across re-seeding while the mass-matched spread control swings 37-fold, which is what makes
 * a change to sensing legible at all.
 *
 * <h2>Seeds</h2>
 * <b>{@code 20260727, 1, 424242, 987654321}</b> — the same four the bench's own constancy was
 * established over. Every variant is evaluated on all four. <b>A variant is judged on all four seeds;
 * quoting a subset is cherry-picking and is not permitted</b>, including the case where three seeds
 * agree and one does not — that case is reported as a split verdict, not as a win.
 *
 * <h2>PRIMARY criteria (seed-robust discriminators)</h2>
 * These are the three the race is decided on. They were chosen because the bench's own multi-seed
 * work showed them to be stable under re-seeding where tail magnitude is regime-scoped:
 * <ol>
 *   <li><b>{@code estZeroShare} → ~0.</b> Scored bounded victims whose estimate reads zero, so
 *       selection passes over them as having nothing left. Measured against the estimator the run
 *       actually steers on, not against the incumbent's arithmetic — the criterion is about the
 *       sensor in use.</li>
 *   <li><b>{@code estIgnoresKeys} → ~0.</b> Scored bounded victims whose estimate discards their
 *       emitted keys (the incumbent's zero-consumed-span branch). Same rule: read against the
 *       estimator in use.</li>
 *   <li><b>Revalidation loss share reduced.</b> Split proposals that died because the victim's cursor
 *       had drained past the pivot, over every proposal that reached the re-validation. The
 *       regime-robust metric of the two: it reads 0.66 at the measured page regime and 0.81 at the
 *       bench regime, where serial fraction moves by two orders of magnitude between them.</li>
 * </ol>
 * <b>Serial fraction on the bench reduced</b> is the fourth primary reading and the one the whole
 * exercise is for, but it is <b>regime-scoped</b>: it is quoted only at the page size it was taken
 * at, and never alone.
 *
 * <h2>REGRESSION GUARDS (a cure that slows well-splitting buckets loses)</h2>
 * Two healthy fixtures are carried on every leg, and a variant that damages either is rejected
 * whatever it does to the bench:
 * <ul>
 *   <li>the <b>hash-fanned</b> corpus ({@code PositionSensorAtScaleTest}'s control shape) — the
 *       well-splitting keyspace whose estimate is not degenerate; and</li>
 *   <li>the <b>uniform</b> deep-nested run ({@code PositionSensorCharacterizationTest}'s
 *       same-geometry control) — the shape that is just as blind and costs nothing, which separates
 *       geometry from mass.</li>
 * </ul>
 * Both must hold their 4/4-robust separations from the bench: healthy occupancy, a near-zero tail,
 * and no material loss of throughput. A variant that fixes the bench by making these worse has moved
 * the pathology rather than cured it.
 *
 * <h2>Regime disclosure discipline</h2>
 * The bench's tail magnitude is page-regime-dependent — 0.33 at a 100-key page against ~0.001 at the
 * measured 1,000-key page, because pages per range is the scaling variable. The race therefore runs a
 * <b>measured-regime leg</b> ({@link PolicyRunFixtures#MEASURED_TAIL_PAGE_SIZE} at
 * {@link PolicyRunFixtures#MEASURED_TAIL_LATENCY}) alongside the bench regime, and <b>every serial or
 * tail number is reported with the page size it was taken at</b>. Loss share is quoted across both.
 *
 * <h2>What may be pinned, and what may only be reported</h2>
 * Only readings that hold at <b>all four seeds</b> may become assertions. Everything else — every
 * magnitude, every ranking that one seed disagrees with — is reported as prose alongside the printed
 * table, with its four values shown. <b>Outcomes are labelled per the run's own evidence:</b> a
 * variant that eliminates the degenerate estimate but does not move the tail is reported as exactly
 * that, and that reading is a finding about what else gates the tail rather than a failure to be
 * explained away.
 *
 * <h2>The table</h2>
 * Every leg reports: serial fraction, tail fraction, {@code estZeroShare}, {@code estIgnoresKeys},
 * revalidation loss share, owner-split children, thief children, steal attempts, NO_VICTIM share,
 * mean occupancy, store calls, and virtual duration. Emitted-key totals are checked against the
 * fixture on every leg, because a variant that loses keys is not a faster variant.
 */
final class SensingRaceProtocol {

    /** The four seeds every variant is evaluated on. No subset of these is a result. */
    static final long[] SEEDS = {20260727L, 1L, 424242L, 987654321L};

    /** The bench's fleet size, and the guards' — the other at-scale fixtures' own. */
    static final int WORKERS = 8;

    /** The tail-producing page regime the bench was designated at. */
    static final int BENCH_PAGE_SIZE = 100;

    private SensingRaceProtocol() {
    }

    // ---- the three keyspaces ------------------------------------------------------------

    /** The designated bench: eight species over a Zipf rank law, each holding its mass in one leaf. */
    static Supplier<List<byte[]>> bench() {
        return () -> KeyspaceFixtures.deepNestedSharedPrefix(2, 4, 1, 400_000,
                SubtreeMass.LEAF_CONCENTRATED);
    }

    /** Regression guard: the hash-fanned corpus whose estimate is not degenerate. */
    static Supplier<List<byte[]>> hashFannedGuard() {
        return () -> KeyspaceFixtures.hashFannedCorpus(16, 16, 3_000);
    }

    /** Regression guard: the same deep geometry with its mass spread uniformly. */
    static Supplier<List<byte[]>> uniformGuard() {
        return () -> KeyspaceFixtures.deepNestedSharedPrefix(8, 8, 2, 150, SubtreeMass.UNIFORM);
    }

    // ---- one leg ------------------------------------------------------------------------

    /** One row of the race table: what a single (variant, keyspace, seed, regime) leg produced. */
    record Leg(String variant, String keyspace, long seed, int pageSize, PolicyRunResult result) {

        double serialFraction() {
            return result.timeline().serialFraction();
        }

        double tailFraction() {
            return result.timeline().tailFraction();
        }

        double estZeroShare() {
            return share(result.counter(SimExecutor.SENSOR_EST_ZERO_COUNTER),
                    result.counter(SimExecutor.SENSOR_VICTIMS_BOUNDED_COUNTER));
        }

        double estIgnoresKeysShare() {
            return share(result.counter(SimExecutor.SENSOR_EST_IGNORES_KEYS_COUNTER),
                    result.counter(SimExecutor.SENSOR_VICTIMS_BOUNDED_COUNTER));
        }

        /** Proposals that died at the re-validation, over every proposal that reached it. */
        double revalidationLossShare() {
            long lost = result.splitsLostAtRevalidation();
            return share(lost, lost + result.thiefChildren());
        }

        double noVictimShare() {
            return share(result.counter("steal.outcome.NO_VICTIM"),
                    result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER));
        }

        long stealAttempts() {
            return result.counter(SimExecutor.STEAL_ATTEMPTS_COUNTER);
        }

        String row() {
            return String.format(Locale.ROOT,
                    "%-14s %-12s %-10d %5d  %6.4f %6.4f  %6.4f %6.4f  %6.4f  %5d %5d  %5d %6.4f  "
                            + "%5.2f %7d %7.3f",
                    variant, keyspace, seed, pageSize, serialFraction(), tailFraction(), estZeroShare(),
                    estIgnoresKeysShare(), revalidationLossShare(), result.ownerSplitChildren(),
                    result.thiefChildren(), stealAttempts(), noVictimShare(),
                    result.timeline().meanOccupancy(), result.storeCalls(), result.virtualNanos() / 1e9);
        }

        private static double share(long numerator, long denominator) {
            return denominator == 0L ? 0.0 : (double) numerator / denominator;
        }
    }

    /** The table header the {@link Leg#row()} columns line up under. */
    static String header() {
        return String.format(Locale.ROOT,
                "%-14s %-12s %-10s %5s  %6s %6s  %6s %6s  %6s  %5s %5s  %5s %6s  %5s %7s %7s",
                "variant", "keyspace", "seed", "page", "serial", "tail", "estZro", "estIgn", "revLos",
                "ownCh", "thfCh", "steal", "noVic", "occ", "calls", "dur_s");
    }

    /** Prints a whole table under one caption, in the order the legs were run. */
    static void printTable(String caption, List<Leg> legs) {
        StringBuilder out = new StringBuilder();
        out.append(caption).append(System.lineSeparator());
        out.append(header()).append(System.lineSeparator());
        for (Leg leg : legs) {
            out.append(leg.row()).append(System.lineSeparator());
        }
        System.out.print(out);
    }

    /** Every value of {@code metric} across {@code legs}, for a 4-seed verdict. */
    static List<Double> across(List<Leg> legs, java.util.function.ToDoubleFunction<Leg> metric) {
        List<Double> values = new ArrayList<>(legs.size());
        for (Leg leg : legs) {
            values.add(metric.applyAsDouble(leg));
        }
        return values;
    }

    // ---- running one leg ----------------------------------------------------------------

    /**
     * Runs one leg and checks the only thing that is not up for debate: a run must emit every key in
     * its fixture. The store is built per leg because a keyspace of this size is a large share of the
     * heap, and only one is ever live.
     */
    static Leg runLeg(String variant, String keyspace, Supplier<List<byte[]>> fixture, long seed,
                      int pageSize, LatencyModel latency) {
        ListingFixtureStore store = new ListingFixtureStore(fixture.get());
        int size = store.size();
        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(WORKERS, pageSize, latency, PolicyRunFixtures.measuredCost())
                        .withSeed(seed),
                store, "in-memory " + keyspace);
        if (!result.completed()) {
            throw new AssertionError("leg " + variant + "/" + keyspace + "/" + seed + " did not complete:"
                    + System.lineSeparator() + result.describe());
        }
        if (result.keysEmitted() != size) {
            throw new AssertionError("leg " + variant + "/" + keyspace + "/" + seed + " emitted "
                    + result.keysEmitted() + " of " + size + " keys");
        }
        return new Leg(variant, keyspace, seed, pageSize, result);
    }
}
