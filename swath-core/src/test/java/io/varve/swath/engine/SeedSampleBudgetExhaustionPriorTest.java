/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>Guard: exhausting the seed's second-level sample budget must not silently mean "not heavy".</b>
 *
 * <p>An ambiguous truncated-with-CommonPrefixes level is disambiguated by {@code
 * sampleProvesHeavy}, which costs up to {@code SAMPLE_WIDTH} (3) probes against a global {@code
 * SAMPLE_BUDGET} (32) — funding only about <b>ten</b> disambiguations for a whole run. Before this
 * guard, a level reached after the budget ran out fell through with {@code heavy = false} and was
 * <b>left whole</b>: the least-parallel outcome, chosen on no evidence, decided purely by where the
 * level happened to sit in the descent order.
 *
 * <p><b>Measured on {@code s3://nara-1950-census/}</b> (2026-07-25, see
 * {@code private notes 2026-07-25-listing-parallelism-collapse}): 15 truncated levels, the sample
 * ran on 11 (10 banded, 1 genuine explosion), and the four it never reached included
 * {@code 43290879-California/} (445,879 objects) and {@code 43290879-New_York/} (606,090) — the two
 * largest states in the bucket. Both were left whole because they came 12th and 13th in descent
 * order, producing a one-worker serial tail over <b>64% of the run's wall clock</b>. The sample
 * would have called them heavy: their children average 29.8 objects against {@code
 * SAMPLE_DENSE_MIN_OBJECTS} = 8. It never got to ask.
 *
 * <p>The fix carries the empirical prior from the siblings already sampled in the same descent
 * (zero extra probes — no page is fetched on that path). {@link
 * #lateHeavyLevelsAreBandedFromThePriorOnceTheSampleBudgetIsSpent()} is the guard that fails against
 * the old default; {@link #genuineExplosionKeyspaceIsStillLeftWholeByThePrior()} is the negative
 * control that pins the INT-8 shape as unchanged, so the fix cannot be "band everything".
 *
 * <p>Counters are string-bound (no symbol dependency), matching {@code SeedMassAwareDescentTest}.
 */
final class SeedSampleBudgetExhaustionPriorTest {

    private static final byte[] NO_PREFIX = new byte[0];
    private static final int WORKERS = 64;

    private static final String HEAVY_CUT_DESCENDED = "heavy_cut_descended";
    private static final String HEAVY_PRIOR_APPLIED = "heavy_prior_applied";
    private static final String HEAVY_PRIOR_BANDED = "heavy_prior_banded";
    private static final String HEAVY_PRIOR_LEFT_WHOLE = "heavy_prior_left_whole";

    /**
     * The {@code nara-1950-census} shape: MORE ambiguous truncated levels than the sample budget can
     * fund. Each {@code state<NN>/} holds {@code children} child directories — strictly more than
     * {@code PROBE_PAGE} (1000), so the level's own delimiter probe truncates and lands in the
     * ambiguous branch — and every child holds {@code perChild} objects, at or above {@code
     * SAMPLE_DENSE_MIN_OBJECTS} (8), so a sample that DOES run proves the level heavy.
     *
     * <p>Every state is identically heavy on purpose: the ONLY thing that differs between an
     * early state and a late one is its position in the descent, which is exactly the variable
     * under test.
     */
    private static List<byte[]> manyAmbiguousHeavyLevels(int states, int children, int perChild) {
        List<byte[]> keys = new ArrayList<>(states * children * perChild);
        for (int s = 0; s < states; s++) {
            for (int c = 0; c < children; c++) {
                for (int o = 0; o < perChild; o++) {
                    keys.add(("state%02d/ed%05d/img%03d".formatted(s, c, o))
                            .getBytes(StandardCharsets.UTF_8));
                }
            }
        }
        return keys;
    }

    /**
     * The INT-8 negative control at the same scale: identically MANY truncated levels, but a true
     * 1:1 explosion — one object per leaf directory. A sample that runs proves NOT-heavy, so the
     * carried prior must also come out not-heavy and every unsampled level must stay whole.
     */
    private static List<byte[]> manyAmbiguousExplosionLevels(int states, int children) {
        List<byte[]> keys = new ArrayList<>(states * children);
        for (int s = 0; s < states; s++) {
            for (int c = 0; c < children; c++) {
                keys.add(("state%02d/ed%05d/only.obj".formatted(s, c))
                        .getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    private static Map<String, Long> seedAndCollect(List<byte[]> keyspace, List<NodeSpec> out)
            throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        out.addAll(SeedSteps.of(MockPageFetcher.builder().keys(keyspace).build(),
                        NO_PREFIX, WORKERS, metrics, EngineToggles.DEFAULT)
                .seedSpecs(1L, SeedMode.SHALLOW));
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    private static long fired(Map<String, Long> reasons, String reason) {
        return reasons.getOrDefault("SEED." + reason, 0L);
    }

    /** Seed boundaries that fall strictly inside {@code state<NN>/} — i.e. the level was carved up. */
    private static long interiorCuts(List<NodeSpec> specs, int state) {
        byte[] lo = ("state%02d/".formatted(state)).getBytes(StandardCharsets.UTF_8);
        byte[] hi = ("state%02d/".formatted(state + 1)).getBytes(StandardCharsets.UTF_8);
        return specs.stream()
                .map(NodeSpec::rangeStart)
                .filter(s -> s != null
                        && Arrays.compareUnsigned(s, lo) > 0
                        && Arrays.compareUnsigned(s, hi) < 0)
                .count();
    }

    private static void assertTiles(List<NodeSpec> specs) {
        List<RangePartition.Interval> intervals = new ArrayList<>();
        for (NodeSpec s : specs) {
            intervals.add(new RangePartition.Interval(s.rangeStart(), s.rangeEnd()));
        }
        RangePartition.assertTiles(intervals);
    }

    /**
     * <b>The load-bearing guard.</b> With 14 identically-heavy ambiguous levels the sample budget
     * covers only 11; the rest must still be banded, from the prior. Ablating the prior leaves
     * three levels whole — measured as states 0, 3 and 6, which is <em>not</em> a positional
     * property (the frontier is priority-ordered), hence the total assertion over every level.
     */
    @Test
    @Timeout(120)
    void lateHeavyLevelsAreBandedFromThePriorOnceTheSampleBudgetIsSpent() throws Exception {
        int states = 14;
        List<NodeSpec> specs = new ArrayList<>();
        Map<String, Long> reasons =
                seedAndCollect(manyAmbiguousHeavyLevels(states, 1001, 8), specs);

        // The budget really did run out — otherwise this fixture is not exercising the path.
        assertThat(fired(reasons, HEAVY_CUT_DESCENDED))
                .as("fixture must out-run the sample budget: fewer sampled levels than the %d present",
                        states)
                .isPositive()
                .isLessThan(states);
        assertThat(fired(reasons, HEAVY_PRIOR_APPLIED))
                .as("SEED.%s must fire — levels were reached with the sample budget spent",
                        HEAVY_PRIOR_APPLIED)
                .isPositive();

        // The claim that actually matters, asserted BEFORE the counters: EVERY level is carved up.
        //
        // Not "the last one" — the descent frontier is priority-ordered, not lexicographic
        // (SEED.frontier_reordered), so which levels fall past the budget is not a positional
        // property and must not be hard-coded. Every state in this fixture is identically heavy, so
        // the invariant is total: no identically-heavy level may be left whole merely because of
        // where the descent reached it. Ablating the prior leaves three of the fourteen whole
        // (measured: states 0, 3 and 6 at zero interior cuts), which this catches.
        //
        // The counter assertions below are tautologically tied to the new code path, so if they
        // came first the ablation would fail on instrumentation rather than on behaviour and this
        // guard would be pinning its own emitter instead of the fix.
        for (int st = 0; st < states; st++) {
            assertThat(interiorCuts(specs, st))
                    .as("state%02d/ is as heavy as every other level, so it must be banded into "
                            + "interior seed ranges rather than left whole as one serial tail", st)
                    .isPositive();
        }

        assertThat(fired(reasons, HEAVY_PRIOR_BANDED))
                .as("SEED.%s must fire — the sampled siblings were heavy, so the prior is heavy",
                        HEAVY_PRIOR_BANDED)
                .isPositive();

        assertTiles(specs);   // I2/I3: still an exact tiling of (⊥, null]
    }

    /**
     * <b>Negative control.</b> The prior must not become "band everything": on a genuine 1:1
     * explosion the sampled levels prove not-heavy, so the unsampled ones must stay whole and the
     * INT-8 behaviour is byte-unchanged.
     */
    @Test
    @Timeout(120)
    void genuineExplosionKeyspaceIsStillLeftWholeByThePrior() throws Exception {
        int states = 14;
        List<NodeSpec> specs = new ArrayList<>();
        Map<String, Long> reasons =
                seedAndCollect(manyAmbiguousExplosionLevels(states, 1001), specs);

        assertThat(fired(reasons, HEAVY_PRIOR_APPLIED))
                .as("the same budget exhaustion must occur on this fixture for the control to mean anything")
                .isPositive();
        assertThat(fired(reasons, HEAVY_PRIOR_BANDED))
                .as("SEED.%s must NOT fire on a 1:1 explosion — the prior must not over-band",
                        HEAVY_PRIOR_BANDED)
                .isZero();
        assertThat(fired(reasons, HEAVY_PRIOR_LEFT_WHOLE))
                .as("SEED.%s must fire — the carried prior correctly says not-heavy",
                        HEAVY_PRIOR_LEFT_WHOLE)
                .isPositive();
        for (int st = 0; st < states; st++) {
            assertThat(interiorCuts(specs, st))
                    .as("state%02d/ is a 1:1 explosion and must still be left whole (INT-8): "
                            + "banding it buys confetti, not parallelism", st)
                    .isZero();
        }

        assertTiles(specs);
    }
}
