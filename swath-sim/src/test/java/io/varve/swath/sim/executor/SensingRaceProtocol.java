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
 * <p><b>Qualification, recorded 2026-07-27 on the control legs.</b> The near-zero-tail half of this
 * criterion is <b>not applicable to the uniform guard at the {@code < 0.05} line the hash-fanned guard
 * is held to</b>: the uniform <em>control</em> — the shipped algorithm on a fixture that is healthy by
 * construction — reads a tail fraction of 0.0343–0.0686 across the four seeds, so the line would fail
 * the incumbent. That is a fact about the fixture's own geometry, not about any candidate. The uniform
 * guard is therefore held on serial fraction, occupancy and throughput, and its tail is <b>reported
 * rather than asserted</b>. Nothing else in this section is relaxed, and the hash-fanned guard carries
 * the tail criterion as written.
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

        /**
         * Owner-side carves refused by the remaining-work floor, per page committed. The mechanism the
         * estimate gates directly: a floor that reads the estimate below four pages refuses a carve
         * that costs nothing to make.
         */
        double estFloorRefusalsPerPage() {
            return share(result.counter("OWNER_SPLIT.remaining_est_floor"), result.pages());
        }

        /**
         * Of the owner-split children this run classified on completion, the share that came back
         * confetti-sized. The other mechanism the estimate gates: a carve placed by an estimate with no
         * sense of position lands on a nearly-drained range, and the feedback gate that watches for
         * exactly that then suppresses owner splitting for the rest of the run.
         */
        double confettiChildShare() {
            long confetti = result.counter("OWNER_SPLIT_CHILD.confetti");
            return share(confetti, confetti + result.counter("OWNER_SPLIT_CHILD.substantial"));
        }

        String row() {
            return String.format(Locale.ROOT,
                    "%-14s %-12s %-10d %5d  %6.4f %6.4f  %6.4f %6.4f  %6.4f  %5d %5d  %5d %6.4f  "
                            + "%5.2f %7d %7.3f  %6.3f %6.3f",
                    variant, keyspace, seed, pageSize, serialFraction(), tailFraction(), estZeroShare(),
                    estIgnoresKeysShare(), revalidationLossShare(), result.ownerSplitChildren(),
                    result.thiefChildren(), stealAttempts(), noVictimShare(),
                    result.timeline().meanOccupancy(), result.storeCalls(), result.virtualNanos() / 1e9,
                    estFloorRefusalsPerPage(), confettiChildShare());
        }

        private static double share(long numerator, long denominator) {
            return denominator == 0L ? 0.0 : (double) numerator / denominator;
        }
    }

    /** The table header the {@link Leg#row()} columns line up under. */
    static String header() {
        return String.format(Locale.ROOT,
                "%-14s %-12s %-10s %5s  %6s %6s  %6s %6s  %6s  %5s %5s  %5s %6s  %5s %7s %7s  %6s %6s",
                "variant", "keyspace", "seed", "page", "serial", "tail", "estZro", "estIgn", "revLos",
                "ownCh", "thfCh", "steal", "noVic", "occ", "calls", "dur_s", "flr/pg", "cnfti");
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

    /** The one leg a variant ran at {@code seed}, for a paired same-seed comparison. */
    static Leg at(List<Leg> legs, SensingVariant variant, long seed) {
        for (Leg leg : legs) {
            if (leg.variant().equals(label(variant)) && leg.seed() == seed) {
                return leg;
            }
        }
        throw new AssertionError("no leg for " + variant + " at seed " + seed);
    }

    // ---- running one leg ----------------------------------------------------------------

    /**
     * Runs one leg and checks the only thing that is not up for debate: a run must emit every key in
     * its fixture.
     *
     * <p>The store is passed in rather than built here so that one generated keyspace serves every
     * variant and every seed raced against it — a million-key fixture is a large share of the heap and
     * generating it once per leg would dominate the race's own cost. Sharing is safe because nothing a
     * leg writes to the store is observable in another leg's <em>result</em>: the keys are immutable, and
     * the only state a leg mutates is the fixture's own read/close tallies, which do accumulate across
     * legs and are not a column of this table — every number reported here is the run's own counter, and
     * the modelled store-call count is one of them rather than the fixture's read count.
     */
    static Leg runLeg(SensingVariant variant, String keyspace, ListingFixtureStore store, long seed,
                      int pageSize, LatencyModel latency) {
        PolicyRunResult result = SimExecutor.run(
                PolicyRunFixtures.scenario(WORKERS, pageSize, latency, PolicyRunFixtures.measuredCost())
                        .withSeed(seed),
                store, "in-memory " + keyspace, variant);
        String leg = variant + "/" + keyspace + "/" + seed + "/page " + pageSize;
        requireCompleted(result, leg);
        if (result.keysEmitted() != store.size()) {
            throw new AssertionError("leg " + leg + " emitted " + result.keysEmitted() + " of "
                    + store.size() + " keys");
        }
        return new Leg(label(variant), keyspace, seed, pageSize, result);
    }

    /**
     * The one thing every row of every table here must be true of: the leg finished. Raised as an
     * {@link AssertionError} carrying the run's own diagnosis, since a leg that stalled is unusable as
     * a measurement and the stall is the finding. Shared with the real-listing harness, which reports
     * the same columns and needs the same failure.
     */
    static void requireCompleted(PolicyRunResult result, String leg) {
        if (!result.completed()) {
            throw new AssertionError("leg " + leg + " did not complete:" + System.lineSeparator()
                    + result.describe());
        }
    }

    /**
     * The named variants against one keyspace at all four seeds, in variant-then-seed order. One
     * generated keyspace serves the whole call.
     */
    static List<Leg> raceOn(String keyspace, Supplier<List<byte[]>> fixture, int pageSize,
                            LatencyModel latency, SensingVariant... variants) {
        ListingFixtureStore store = new ListingFixtureStore(fixture.get());
        List<Leg> legs = new ArrayList<>();
        for (SensingVariant variant : variants) {
            for (long seed : SEEDS) {
                legs.add(runLeg(variant, keyspace, store, seed, pageSize, latency));
            }
        }
        return legs;
    }

    /** The four legs of one variant, out of a {@link #raceOn} result. */
    static List<Leg> of(List<Leg> legs, SensingVariant variant) {
        List<Leg> mine = new ArrayList<>();
        for (Leg leg : legs) {
            if (leg.variant().equals(label(variant))) {
                mine.add(leg);
            }
        }
        return mine;
    }

    /** The short name a variant appears under in the table. */
    static String label(SensingVariant variant) {
        return switch (variant) {
            case CURRENT -> "current";
            case RATE -> "E1-rate";
            case CURSOR_ANCHORED -> "E2-anchored";
            case RATE_CURSOR_ANCHORED -> "E1+E2";
            case RATE_ANCHORED_FLOOR_EIGHTH -> "E4b@1/8";
            case RATE_ANCHORED_FLOOR_QUARTER -> "E4b@1/4";
            case RATE_ANCHORED_FLOOR_HALF -> "E4b@1/2";
            case RATE_ANCHORED_LIFT_ONLY -> "E1+E2+E4";
        };
    }
}
