/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.policy.NoVictimReason;
import io.varve.swath.engine.policy.RetryReason;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Executor-protocol-contract audit, deliverable (c): a genuinely-contended
 * {@code thief.steal(pool)} run reconciling {@code swath.steal_reason} totals against the number
 * of calls made — <b>conservation, not a specific interleaving</b> (issue #18). {@code THREADS}
 * threads race {@link Thief#steal} against a small, shared, mutating pool of real {@link
 * WorkerState} victims (plus every child a winning split adds), forcing the same stale-snapshot /
 * CAS-loss races {@link Conc3TwoThievesTest} forces deterministically — except here nothing is
 * choreographed: whichever races the scheduler actually produces are what this test must survive.
 *
 * <p>Two of the four outcome buckets need help reaching a nonzero count within a bounded call
 * budget, because this fixture drives no real listing (no {@code RangeScanner}, no {@code
 * commitPage}) — a victim's cursor never advances, so it never exhausts and the shared pool never
 * runs dry on its own. Both are seeded in DETERMINISTICALLY, not hoped for across repeated runs:
 * {@link #UNSPLITTABLE_JUNK_COUNT} open-frontier victims already past the scan ceiling (guaranteed
 * {@code UNSPLITTABLE} the first time any thread reaches one — no probe, no race; see that field's
 * javadoc for why an open frontier, not a bounded byte-adjacent pair) and {@link
 * #EMPTY_POOL_CALLS_PER_THREAD} explicit {@code thief.steal(List.of())} calls per thread
 * (unconditionally {@code NO_VICTIM}/{@code pool_empty} — a real path, exactly what an idle worker
 * observes at quiescence).
 *
 * <p>The invariant asserted is interleaving-independent because every {@link Thief#steal} return
 * path funnels through exactly one terminal {@code record(Outcome, reason)} call (verified by
 * inspection of {@link Thief#steal}/{@link Thief#commit} — see the executor-protocol contract),
 * <b>with one documented exception</b>: a {@code NO_VICTIM} outcome fires a discriminator reason
 * (one of {@link NoVictimReason}'s five specific values) <i>and</i> the aggregate {@code
 * no_splittable_victim} reason in the same call — see {@link NoVictimReason}'s javadoc ("the
 * aggregate always fired alongside exactly one discriminator so the two counter series stay
 * reconcilable"). Summing only the aggregate reason for {@code NO_VICTIM} (never every {@code
 * NO_VICTIM.*} series) avoids double-counting that documented pair; the two are cross-checked
 * against each other below as well, so a divergence there — a second, unrelated
 * engagement-accounting bug — would also fail this test.
 *
 * <p><b>Mutation evidence.</b> A {@code Thief#record} mutated to skip the {@code UNSPLITTABLE}
 * emission fails this test immediately and clearly ({@code expected: 2040L but was: 1932L}); one
 * mutated to skip {@code NO_VICTIM} likewise ({@code but was: 2000L}, exactly the 40 {@code
 * pool_empty} calls lost); the pre-existing {@code RETRY}-double-count mutant this test was
 * originally written to catch still fails against this extended fixture too ({@code but was:
 * 3907L}). None of the four buckets is vacuous.
 *
 * <p><b>A second conservation invariant, over the {@code ALPHABET} "verdict" engagement (added after
 * an independent review found this test reconciled only the four terminal outcome buckets, never any
 * mid-cascade engagement {@code contracts.md} §2.1 cites this test for).</b> See the assertion's own
 * inline comment for the derivation; in short, {@code ThiefPolicy#addAlphabetEngagement} records
 * exactly one of {@code alphabet_chosen}/{@code alphabet_fallback} per selected attempt that reaches
 * a bounded victim's initial pivot placement with a non-null pivot, which is exactly every selected
 * attempt minus the three pre-pivot {@code RetryReason}s and every {@code UNSPLITTABLE} (its one and
 * only source) -- deliberately NOT the same thing as summing every {@code ALPHABET.*} reason:
 * {@code AlphabetDigest#chooseScalar} separately reports its own {@code fallback_out_of_window}/
 * {@code fallback_no_room}/{@code window_gap} marks (zero or more times per attempt, driven by the
 * observed alphabet, not by which attempt this is), which is exactly what an earlier version of this
 * assertion got wrong (it summed the whole {@code ALPHABET} outcome and failed against UNMUTATED code,
 * {@code expected: 21L but was: 42L} -- corrected here to the two verdict reasons only). Both mutants
 * above (double-record / delete {@code Thief#applyEngagements}) turn the corrected assertion red;
 * neither is caught by the four-bucket invariant alone, since the verdict marks are recorded through a
 * different call path ({@code applyEngagements} draining the attempt's engagement list) than the four
 * terminal outcomes (direct {@code Thief#record}/{@code Thief#steal} calls). {@code STRUCTURE}/
 * {@code PIVOT} mid-cascade marks and the {@code STEAL}
 * pacing engagement are NOT reconciled by this test: unlike {@code ALPHABET}, none of them has a
 * single, unconditional call site whose firing condition reduces to already-independently-recorded
 * counters -- each is gated by cascade branches (structure-probe suppression, density-reflection
 * outcome, per-candidate pacing state) that are themselves schedule/topology-dependent under real
 * contention, not computable from the call totals alone without either predicting a race outcome or
 * adding new production instrumentation. Their single-threaded shape is pinned instead by the
 * decision-trace goldens; see {@code contracts.md} §2.1's own citation of this test for the exact,
 * narrowed scope of what conservation it verifies under contention.
 */
final class ThiefStealReasonConservationTest {

    /** Sibling ranges the initial pool starts with — few enough that threads genuinely contend. */
    private static final int SIBLINGS = 6;
    /** Keys per sibling — enough for several generations of splits before a range goes unsplittable. */
    private static final int KEYS_PER_SIBLING = 64;
    private static final int THREADS = 8;
    private static final int ITERATIONS_PER_THREAD = 250;
    /**
     * Dedicated open-frontier victims seeded alongside the wide siblings — deterministically
     * {@code UNSPLITTABLE} the first time any thread reaches one, no probe, no race. A bounded
     * byte-adjacent {@code (X, X+0x00]} pair (the shape {@code ThiefPolicyCascadeTest}'s {@code
     * unsplittable_terminalNullPivotHasNoSafeKeyStrictlyBetweenTheBounds} pins) cannot be used here:
     * {@code estRemaining} computes to exactly {@code 0.0} for any such pair (the trailing {@code
     * 0x00} byte contributes zero weight to the base-256 fraction), so {@code selectVictim} would
     * skip it as {@code all_no_remaining_span} before ever reaching the per-attempt cascade -- it is
     * only reachable directly, bypassing selection, the way that cascade test does. An OPEN
     * FRONTIER ({@code hi == null}) sidesteps this entirely: {@code estRemaining} returns {@code
     * POSITIVE_INFINITY} unconditionally for {@code hi == null} (selection ranks it above every
     * finite-scored candidate, so it can never be starved), and seeding {@code cursor} already past
     * the effective ceiling ({@code StealMath#extrapolate}'s only other precondition, beyond
     * "started": {@code cursor} at or after the ceiling) makes {@link StealMath#extrapolate} return
     * {@code null} with zero probes -- the same {@code UnsplittableReason#NO_PIVOT} terminal, reached
     * through the real {@code selectVictim} + attempt pipeline this time, not bypassing it.
     */
    private static final int UNSPLITTABLE_JUNK_COUNT = 24;
    /**
     * Past {@code StealMath}'s effective ceiling for a whole-bucket-scope {@link Thief} ({@code
     * prefix = new byte[0]}, so {@code prefixCeil} is {@code null} and the effective ceiling is the
     * maximum valid-UTF-8 key, {@code 0xF4 0x8F 0xBF 0xBF}) — {@code 0xF5} alone already unsigned-
     * exceeds that ceiling's first byte, regardless of what (if anything) follows it.
     */
    private static final byte[] JUNK_CURSOR = {(byte) 0xF5};
    /**
     * Explicit empty-pool calls per thread — deterministically {@code NO_VICTIM}/{@code pool_empty}
     * (a real, meaningful path: this is exactly what every idle worker observes at quiescence),
     * rather than hoping the shared pool happens to run dry within the run's bounded call budget.
     */
    private static final int EMPTY_POOL_CALLS_PER_THREAD = 5;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "conservation-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /** The synthetic cut-point before sibling {@code i}: {@code "p<ii>/"} (no real key equals it). */
    private static byte[] cut(int i) {
        return b(String.format("p%02d/", i));
    }

    /** {@code SIBLINGS} prefixes of {@code KEYS_PER_SIBLING} keys each: {@code p<ii>/<kkkkkk>}. */
    private static List<byte[]> wideKeyspace() {
        List<byte[]> keys = new ArrayList<>(SIBLINGS * KEYS_PER_SIBLING);
        for (int s = 0; s < SIBLINGS; s++) {
            for (int k = 0; k < KEYS_PER_SIBLING; k++) {
                keys.add(b(String.format("p%02d/%06d", s, k)));
            }
        }
        return keys;
    }

    /** A junk-range prefix distinct from the wide siblings' {@code p<ii>/} namespace. */
    private static byte[] junkLo(int i) {
        return b(String.format("j%04d/", i));
    }

    /** Sums every {@code swath.steal_reason{outcome=<outcome>,reason=*}} counter, any reason. */
    private static long sumByOutcome(RunMetrics metrics, String outcome) {
        return metrics.registry().getMeters().stream()
                .filter(m -> m.getId().getName().equals("swath.steal_reason"))
                .filter(m -> outcome.equals(m.getId().getTag("outcome")))
                .filter(m -> m instanceof Counter)
                .mapToLong(m -> Math.round(((Counter) m).count()))
                .sum();
    }

    /** Reads exactly one {@code swath.steal_reason{outcome,reason}} counter (0 if never registered). */
    private static long countReason(RunMetrics metrics, String outcome, String reason) {
        return metrics.registry().getMeters().stream()
                .filter(m -> m.getId().getName().equals("swath.steal_reason"))
                .filter(m -> outcome.equals(m.getId().getTag("outcome")) && reason.equals(m.getId().getTag("reason")))
                .filter(m -> m instanceof Counter)
                .mapToLong(m -> Math.round(((Counter) m).count()))
                .sum();
    }

    @Test
    @Timeout(60)
    void contendedStealsConserveEveryStealReasonCount(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = wideKeyspace();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("conservation.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);

            // Seed the UNSPLITTABLE_JUNK_COUNT open-frontier victims FIRST: estRemaining scores
            // hi == null as POSITIVE_INFINITY unconditionally, strictly above every finite-scored
            // wide sibling below, so these are never starved -- each is drained to permanently-cached
            // unsplittable before any wide sibling is ever selected, deterministically, regardless of
            // how the scheduler interleaves the threads. Independent nodes, deliberately NOT part of
            // the siblings' tiling below -- this test asserts steal_reason conservation only, never a
            // tiling invariant.
            List<WorkerState> pool = new CopyOnWriteArrayList<>();
            for (int j = 0; j < UNSPLITTABLE_JUNK_COUNT; j++) {
                byte[] lo = junkLo(j);
                long nodeId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, null, JUNK_CURSOR, null));
                pool.add(WorkerStates.of(nodeId, lo, JUNK_CURSOR, null));
            }

            // Seed SIBLINGS wide, BOUNDED ranges tiling (⊥, p<SIBLINGS>/] -- deliberately no open
            // frontier here (unlike Conc2WideSiblingsTest's seed): an hi == null sibling would ALSO
            // score POSITIVE_INFINITY and, since its cursor never advances in this raw-Thief-only
            // harness, would win every tie against the junk victims above (by iteration order) or,
            // once seeded after them, monopolize selection forever once junk drains (an "unstarted
            // frontier" RETRY loop that never resolves and never lets a bounded sibling be picked
            // again) -- starving CHILD_CREATED for the rest of the run. cursor = lo (a fresh
            // sub-range starts at its range_start), the same convention Conc2WideSiblingsTest uses.
            for (int s = 0; s < SIBLINGS; s++) {
                byte[] lo = (s == 0) ? null : cut(s);
                byte[] hi = cut(s + 1);
                long nodeId = store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, lo, hi, lo, null));
                pool.add(WorkerStates.of(nodeId, lo, lo, hi));
            }

            // Every winning split's child joins the SAME live pool other threads are already racing
            // over — real, not scripted, contention keeps arriving as the run progresses.
            Thief.ChildSink childSink = (childId, childLo, childHi) ->
                    pool.add(WorkerStates.of(childId, childLo, childLo, childHi));
            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS, childSink, metrics);

            ExecutorService exec = Executors.newFixedThreadPool(THREADS);
            CountDownLatch start = new CountDownLatch(1);
            List<AtomicReference<Throwable>> errors = new ArrayList<>();
            List<Future<?>> futures = new ArrayList<>();
            try {
                for (int t = 0; t < THREADS; t++) {
                    AtomicReference<Throwable> err = new AtomicReference<>();
                    errors.add(err);
                    futures.add(exec.submit(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                                thief.steal(pool);
                            }
                            // Deterministic NO_VICTIM/pool_empty calls -- see EMPTY_POOL_CALLS_PER_THREAD.
                            for (int i = 0; i < EMPTY_POOL_CALLS_PER_THREAD; i++) {
                                thief.steal(List.of());
                            }
                        } catch (Throwable th) {
                            err.set(th);
                        }
                    }));
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                exec.shutdownNow();
            }

            for (AtomicReference<Throwable> err : errors) {
                assertThat(err.get()).as("a racing thief thread threw").isNull();
            }

            long totalCalls = (long) THREADS * (ITERATIONS_PER_THREAD + EMPTY_POOL_CALLS_PER_THREAD);

            long noVictim = countReason(metrics, "NO_VICTIM", NoVictimReason.NO_SPLITTABLE_VICTIM.code());
            long retry = sumByOutcome(metrics, "RETRY");
            long unsplittable = sumByOutcome(metrics, "UNSPLITTABLE");
            long childCreated = sumByOutcome(metrics, "CHILD_CREATED");

            // The headline conservation invariant: every one of the totalCalls steal() invocations
            // landed in EXACTLY one of the four outcome buckets, regardless of which races the
            // scheduler actually produced.
            assertThat(noVictim + retry + unsplittable + childCreated)
                    .as("every steal() call lands in exactly one outcome bucket")
                    .isEqualTo(totalCalls);

            // The reconciliation NoVictimReason's own javadoc claims (discriminators sum to the
            // aggregate) -- checked here too, so a second, unrelated engagement-accounting bug in
            // that pairing would also fail this test rather than being silently absorbed above.
            long discriminatorSum = Arrays.stream(NoVictimReason.values())
                    .filter(r -> r != NoVictimReason.NO_SPLITTABLE_VICTIM)
                    .mapToLong(r -> countReason(metrics, "NO_VICTIM", r.code()))
                    .sum();
            assertThat(discriminatorSum)
                    .as("every NO_VICTIM discriminator sums to the aggregate no_splittable_victim count")
                    .isEqualTo(noVictim);

            // Sanity: this is a contention test, not a no-op loop -- real splits must have committed.
            assertThat(childCreated).as("at least some splits committed under contention").isGreaterThan(0);

            // The two previously-vacuous buckets (issue: a 2000-call run of the earlier fixture
            // never observed either, because the fixture drove no real listing -- cursors never
            // advanced, so neither a genuinely exhausted victim nor a genuinely empty pool ever
            // arose). Both are now deterministic, not scheduling-dependent:
            //   - UNSPLITTABLE_JUNK_COUNT open-frontier victims (cursor already past the ceiling)
            //     are ALWAYS resolved to UNSPLITTABLE the first time any thread reaches one -- no
            //     probe, no race, no possible other outcome (see that field's javadoc).
            //   - EMPTY_POOL_CALLS_PER_THREAD x THREADS explicit thief.steal(List.of()) calls are
            //     UNCONDITIONALLY NoVictimReason.POOL_EMPTY -- an exact count, not a lower bound.
            assertThat(unsplittable).as("the open-frontier junk victims resolve to UNSPLITTABLE").isGreaterThan(0);
            assertThat(noVictim).as("the empty-pool calls resolve to NO_VICTIM").isGreaterThan(0);
            assertThat(countReason(metrics, "NO_VICTIM", NoVictimReason.POOL_EMPTY.code()))
                    .as("every explicit empty-pool call lands on POOL_EMPTY, exactly once each")
                    .isEqualTo((long) THREADS * EMPTY_POOL_CALLS_PER_THREAD);

            // A SECOND conservation invariant, this time over the ALPHABET "verdict" engagement -- issue
            // found by an independent review: this test previously reconciled only the four terminal
            // outcome buckets, never any of the mid-cascade engagement categories (ALPHABET/STRUCTURE/
            // PIVOT) contracts.md §2.1 cites this test for. ThiefPolicy#addAlphabetEngagement records
            // exactly one of alphabet_chosen/alphabet_fallback, unconditionally, on the path that reaches
            // a bounded (hi != null) victim's initial pivot placement with a non-null pivot
            // (ThiefPolicy.java:312, immediately before the first key probe is requested). Every OTHER
            // path out of StealAttempt#start() before that point is accounted for by an EXISTING,
            // independently recorded counter:
            //   - UNSPLITTABLE has exactly ONE source in the whole cascade -- ThiefPolicy never returns
            //     MarkUnsplittable except from the m==null branch immediately preceding the verdict's
            //     call site (verified: MarkUnsplittable is constructed nowhere else in ThiefPolicy.java);
            //   - the three RETRY reasons that fire BEFORE that point (UNCHANGED_NONPRODUCTIVE_SNAPSHOT,
            //     CURSOR_AT_OR_PAST_HI, UNSTARTED_FRONTIER -- see RetryReason's own javadoc for why the
            //     other two, RETRY_PIVOT_ADJACENT/BISECT_BUDGET_EXHAUSTED, are deep-cascade RETRYs that
            //     fire only AFTER the verdict already did, as do the executor-owned bound_moved/
            //     cursor_passed_pivot/split_aborted string-literal RETRYs Thief itself emits).
            // So the verdict count == every selected-victim attempt EXCEPT those three early RETRYs and
            // every UNSPLITTABLE -- an exact, schedule-independent identity (it never predicts which
            // CHILD_CREATED/RETRY a given attempt lands on, only which attempts had a chance to reach the
            // verdict's call site at all). Every counter on the right-hand side here is recorded by a
            // call site OTHER than the applyEngagements/applyMutations pipeline the verdict itself goes
            // through (NoVictimReason/RetryReason/terminal-outcome recording are direct
            // Thief#record/Thief#steal calls, never routed through an Engagement list), so this
            // reconciles the verdict against genuinely independent ground truth.
            //
            // NOT sumByOutcome(metrics, "ALPHABET"): that also sums AlphabetDigest#chooseScalar's own
            // three variable, digest-state-dependent fallback marks (ALPHABET.fallback_out_of_window/
            // fallback_no_room/window_gap -- zero or more PER interpolate() call, driven by the observed
            // alphabet, not by which attempt this is) -- summing the whole ALPHABET outcome failed
            // against UNMUTATED code (expected: 21L but was: 42L, a real run's numbers) before this
            // narrowing to the two verdict reasons was applied.
            //
            // Mutation evidence: mutating Thief#applyEngagements to iterate its list TWICE (double-
            // records every engagement, including the verdict mark) turns this assertion red (actual
            // exactly 2x expected: 46L vs 23L, a real run's numbers); deleting the body of
            // Thief#applyEngagements entirely (so no engagement is ever recorded) turns it red too
            // (actual 0, expected > 0 since childCreated > 0 is already asserted above). Both mutants
            // left the test GREEN before this assertion existed; both reverted clean afterward.
            long alphabetVerdict = countReason(metrics, "ALPHABET", "alphabet_chosen")
                    + countReason(metrics, "ALPHABET", "alphabet_fallback");
            long selectedAttempts = totalCalls - noVictim;
            long preAlphabetRetries = countReason(metrics, "RETRY", RetryReason.UNCHANGED_NONPRODUCTIVE_SNAPSHOT.code())
                    + countReason(metrics, "RETRY", RetryReason.CURSOR_AT_OR_PAST_HI.code())
                    + countReason(metrics, "RETRY", RetryReason.UNSTARTED_FRONTIER.code());
            assertThat(alphabetVerdict).as("nonzero ALPHABET verdict marks were recorded at all").isGreaterThan(0);
            assertThat(alphabetVerdict)
                    .as("every ALPHABET verdict engagement (alphabet_chosen/alphabet_fallback) reconciles "
                            + "against selected attempts minus the three pre-pivot-placement RETRY reasons "
                            + "minus every UNSPLITTABLE (the sole MarkUnsplittable source, at the same check "
                            + "site the verdict is guarded by)")
                    .isEqualTo(selectedAttempts - preAlphabetRetries - unsplittable);
        }
    }
}
