/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Focused mechanics of the thief's single-attempt {@link Thief#steal} (algorithms.md
 * §3): victim selection, the pivot/probe, and the two distinct loser paths
 * (early {@code hi != H} vs late {@code SPLIT_ABORTED}). These are single-threaded
 * unit checks of the protocol's branches.
 *
 * <p>DOES NOT cover the cross-cutting CONC-3 / RES-3 / PROP-1 interleavings —
 * those are covered separately.
 */
final class ThiefTest {

    private static final long RUN_ID = 7L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Records the child ids handed to the driver. */
    private static final class RecordingSink implements Thief.ChildSink {
        final List<Long> children = new ArrayList<>();
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { children.add(childNodeId); }
    }

    private static PageFetcher fetcherWithKeys(String... keys) {
        List<byte[]> ks = new ArrayList<>();
        for (String k : keys) {
            ks.add(b(k));
        }
        return MockPageFetcher.builder().keys(ks).build();
    }

    // -------------------------------------------------------------------------

    @Test
    void noVictimWhenPoolEmpty() throws SwathException, InterruptedException {
        Thief thief = Thiefs.of(StubCheckpointStore.returning(1L), fetcherWithKeys("m1"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of())).isEqualTo(Thief.Outcome.NO_VICTIM);
    }

    @Test
    void unstartedFrontierIsTransientRetry_notCachedUnsplittable()
            throws SwathException, InterruptedException {
        // Open frontier (hi == null) with cursor still at lo (⊥, un-started seed): extrapolate has
        // no consumed span to reflect, so its pivot is null — but this is the TRANSIENT case
        // (algorithms.md §3.1, "owner finishes the frontier"). The thief must RETRY and must NOT
        // cache the victim unsplittable; otherwise the seed — which owns the whole keyspace — is
        // excluded from every future steal and the scan collapses to one worker (INT-7's bug).
        WorkerState frontier = WorkerStates.of(1, null, null, null);
        StubCheckpointStore store = StubCheckpointStore.returning(1L);
        Thief thief = Thiefs.of(store, fetcherWithKeys("m1"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(frontier))).isEqualTo(Thief.Outcome.RETRY);
        assertThat(frontier.unsplittable()).as("un-started frontier must NOT be cached unsplittable")
                .isFalse();
        assertThat(store.splitCalls).isZero();   // no probe / no durable split on a null pivot
    }

    @Test
    void exhaustedFrontierAtCeilingIsGenuinelyUnsplittable()
            throws SwathException, InterruptedException {
        // Open frontier whose cursor has reached the prefix ceiling: it has consumed real span
        // (cursor > lo) but there is no forward room below the ceiling, so extrapolate's null is
        // the GENUINE terminal case (not the un-started transient). This must still cache
        // unsplittable and TERMINATE — genuinely unsplittable frontiers stay terminal. prefixCeil
        // of "hot/" is "hot0"; the cursor sits exactly there.
        byte[] prefix = b("hot/");
        WorkerState frontier = WorkerStates.of(1, b("hot/00"), b("hot0"), null);
        StubCheckpointStore store = StubCheckpointStore.returning(1L);
        Thief thief = Thiefs.of(store, fetcherWithKeys("m1"),
                RUN_ID, prefix, ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(frontier))).isEqualTo(Thief.Outcome.UNSPLITTABLE);
        assertThat(frontier.unsplittable()).as("cursor at the ceiling is genuinely unsplittable")
                .isTrue();
        assertThat(store.splitCalls).isZero();
    }

    @Test
    void victimSelectionPicksLargestEstRemaining() throws SwathException, InterruptedException {
        // Both at cursor == lo (no density signal) ⇒ ranked by remaining width. B's (a, z]
        // is far wider than A's (a, c], so B must be the victim.
        WorkerState a = WorkerStates.of(2, b("a"), b("a"), b("c"));
        WorkerState b = WorkerStates.of(3, b("a"), b("a"), b("z"));
        StubCheckpointStore store = StubCheckpointStore.returning(99L);
        Thief thief = Thiefs.of(store, fetcherWithKeys("m1"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(a, b))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(store.lastSplit.victimId()).isEqualTo(3);   // B, the wider remaining span
    }

    @Test
    void successCreatesChildNarrowsHiAndBumpsViaSink() throws SwathException, InterruptedException {
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));
        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        RecordingSink sink = new RecordingSink();
        Thief thief = Thiefs.of(store, fetcherWithKeys("m1"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, sink);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(sink.children).containsExactly(42L);                 // child handed to driver under the lock
        assertThat(store.lastSplit.runId()).isEqualTo(RUN_ID);
        assertThat(store.lastSplit.victimId()).isEqualTo(1);
        assertThat(store.lastSplit.oldHi()).isEqualTo(b("z"));          // H from the snapshot
        // hi narrowed to the pivot m (NOT restored) — victim now owns (lo, m].
        assertThat(victim.hi()).isEqualTo(store.lastSplit.pivot());
        assertThat(KeyBytes.compareUnsigned(victim.hi(), b("z"))).isLessThan(0);
    }

    @Test
    void farAheadPivotWinRecordsPivotAndAlphabetCounters() throws SwathException, InterruptedException {
        // A uniformly-dense bounded victim (density fraction 0.75) that has OBSERVED the alphabet {'p','q'}
        // in the far-ahead gap ('m','z'): the alphabet-aware chooser lands the far-ahead pivot on the
        // populated 'q' (not the plain 'v'), and its upper ("q", z] holds "w" so it commits directly.
        // Both the winning MECHANISM and the alphabet-CHOSEN engagement are counted (§5).
        WorkerState victim = WorkerStates.of(1, b("a"), b("m"), b("z"));
        victim.addKeysEmitted(500);
        victim.recordPage(b("p"), b("q"), 500);   // dense trailing page ⇒ densityFraction == 0.75, digest {'p','q'}
        assertThat(victim.densityFraction()).as("dense drainer ⇒ far-ahead fraction").isGreaterThan(0.5);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        Thief thief = Thiefs.of(store, fetcherWithKeys("w"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(store.lastSplit.pivot()).as("far-ahead pivot landed on the observed 'q'").isEqualTo(b("q"));
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("PIVOT.far_ahead", 0L))
                .as("the far-ahead interpolated pivot won and was counted").isEqualTo(1L);
        assertThat(reasons.getOrDefault("ALPHABET.alphabet_chosen", 0L))
                .as("the alphabet-aware chooser selected a populated scalar and was counted").isEqualTo(1L);
    }

    /**
     * {@code alphabet_pivots} engagement smoke: the IDENTICAL setup as {@link
     * #farAheadPivotWinRecordsPivotAndAlphabetCounters} — a victim whose observed alphabet {@code
     * {'p','q'}} would otherwise deflect the far-ahead pivot onto {@code 'q'} — but with {@code
     * alphabet_pivots=off}. The pivot must fall back to the PLAIN code-point midpoint-of-fraction
     * ({@code 'v'}, the same value {@link StealMath#interpolate(byte[], byte[], double)} without a
     * digest produces), {@code ALPHABET.alphabet_chosen} must stay at zero (never fires — the digest
     * is never consulted), and the once-per-construction {@code TOGGLE.alphabet_pivots_off} mark
     * must be present — the OFF state is provable from metrics alone.
     */
    @Test
    void alphabetPivotsOffFallsBackToThePlainCodePointPivot() throws SwathException, InterruptedException {
        WorkerState victim = WorkerStates.of(1, b("a"), b("m"), b("z"));
        victim.addKeysEmitted(500);
        victim.recordPage(b("p"), b("q"), 500);   // same dense trailing page ⇒ far-ahead fraction 0.75

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        EngineToggles alphabetPivotsOff = EngineToggles.DEFAULT.withAlphabetPivots(false);
        Thief thief = Thiefs.of(store, fetcherWithKeys("w"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics, alphabetPivotsOff);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(store.lastSplit.pivot())
                .as("alphabet_pivots=off: the far-ahead pivot falls back to the plain code-point value")
                .isEqualTo(StealMath.interpolate(b("m"), b("z"), victim.densityFraction()));
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("ALPHABET.alphabet_chosen", 0L))
                .as("the digest is never consulted, so it never deflects the pivot").isZero();
        assertThat(reasons.getOrDefault("ALPHABET.alphabet_fallback", 0L)).isEqualTo(1L);
        assertThat(reasons.getOrDefault("TOGGLE.alphabet_pivots_off", 0L))
                .as("the once-per-construction TOGGLE mark fired").isEqualTo(1L);
    }

    @Test
    void farAheadStepBackFiresAndIsCounted() throws SwathException, InterruptedException {
        // Same dense victim, but the far-ahead upper (~"v", z] is EMPTY (the only key "t" doesn't reach
        // that far): the pivot steps back to the plain byte-midpoint (~"s"), whose upper holds "t" and
        // commits. The step-back FIRE is counted (§5).
        WorkerState victim = WorkerStates.of(1, b("a"), b("m"), b("z"));
        victim.addKeysEmitted(500);
        victim.recordPage(b("l"), b("m"), 500);   // densityFraction == 0.75 ⇒ far-ahead pivot ~"v"

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(StubCheckpointStore.returning(7L), fetcherWithKeys("t"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink(), metrics);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("PIVOT.step_back", 0L))
                .as("the far-ahead step-back fired and was counted").isEqualTo(1L);
    }

    @Test
    void lateLoserSplitAbortedRestoresHiAndRetries() throws SwathException, InterruptedException {
        // The durable CAS rejects us: we already narrowed hi, so we must restore H and RETRY.
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));
        StubCheckpointStore store = StubCheckpointStore.returning(CheckpointStore.SPLIT_ABORTED);
        Thief thief = Thiefs.of(store, fetcherWithKeys("m1"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.RETRY);
        assertThat(store.splitCalls).isEqualTo(1);                      // we did attempt the split
        assertThat(victim.hi()).isEqualTo(b("z"));                      // hi RESTORED to the validated bound
    }

    @Test
    void earlyLoserHiMovedDoesNotTouchHi() throws SwathException, InterruptedException {
        // Simulate a concurrent thief narrowing the bound between our snapshot and our lock,
        // by mutating hi during the probe call. Under the lock victim.hi != H ⇒ EARLY loser:
        // RETRY and DO NOT restore H (that would clobber the winner's narrow / re-open overlap).
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));
        AtomicBoolean once = new AtomicBoolean();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("m1")))
                .interceptor((req, idx, page) -> {
                    if (once.compareAndSet(false, true)) {
                        victim.lock().lock();
                        try {
                            victim.narrowHi(b("p"));   // a rival winner narrowed (lo, p]
                        } finally {
                            victim.lock().unlock();
                        }
                    }
                    return page;
                })
                .build();
        StubCheckpointStore store = StubCheckpointStore.returning(1L);
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.RETRY);
        assertThat(store.splitCalls).isZero();        // we bailed before the durable split
        assertThat(victim.hi()).isEqualTo(b("p"));    // winner's narrow left INTACT (not clobbered)
    }

    @Test
    void emptyUpperProbeRetriesNearerTheCursor() throws SwathException, InterruptedException {
        // The only key sits just past the cursor (a dense left head). The first midpoint
        // probes empty (no key <= z above it); the loop bisects (c, m] until the pivot
        // drops below "ab", whose child then contains it → CHILD_CREATED.
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("z"));
        StubCheckpointStore store = StubCheckpointStore.returning(5L);
        Thief thief = Thiefs.of(store, fetcherWithKeys("ab"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        byte[] pivot = store.lastSplit.pivot();
        assertThat(KeyBytes.compareUnsigned(pivot, b("a"))).isGreaterThan(0);    // m > cursor
        assertThat(KeyBytes.compareUnsigned(pivot, b("ab"))).isLessThan(0);      // child (m, z] holds "ab"
    }

    @Test
    void emptyUpperRetryThatRunsOutOfMidpointsIsTransientRetry_notCachedUnsplittable()
            throws SwathException, InterruptedException {
        // First pivot is "a " within (a, a!]. The probe is empty because the only key is beyond H;
        // the retry midpoint between "a" and "a " is null (only the control sliver remains).
        // This is still transient RETRY: the owner may advance before the next steal attempt, and
        // the thief must not poison the victim with the terminal unsplittable label.
        WorkerState victim = WorkerStates.of(1, b("a"), b("a"), b("a!"));
        StubCheckpointStore store = StubCheckpointStore.returning(5L);
        Thief thief = Thiefs.of(store, fetcherWithKeys("z"),
                RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.RETRY);
        assertThat(victim.unsplittable())
                .as("empty-upper exhaustion is a transient retry, not a cached unsplittable")
                .isFalse();
        assertThat(store.splitCalls).isZero();
    }
}
