/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import io.varve.swath.testkit.ConcurrencyGauges;
import io.varve.swath.testkit.LatencyModels;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangeScanners;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Intra-range speculative readahead unit coverage. Deterministic offline via {@link MockPageFetcher}:
 *
 * <ul>
 *   <li>(a) a dense opaque range with readahead ON is byte-exact / exactly-once vs OFF, and
 *       collapses the serial round-trip chain — the deterministic, scheduling-independent proxy for
 *       "faster under an RTT-dominated latency model" (see the round-trip-structure guard below for
 *       why this replaced the old, fork-contention-flaky wall-clock ratio);</li>
 *   <li>(b) a split (a thief narrowing {@code hi}) mid-readahead discards speculation beyond the new
 *       bound with no leakage and coherent completion;</li>
 *   <li>(c) the adoption buffer stays capped under a pathological non-advancing gap;</li>
 *   <li>(d) toggle OFF is a zero-behavior-change no-op (no {@code READAHEAD.*} counters);</li>
 *   <li>(resume) a crash mid-readahead loses/duplicates nothing — the committed cursor only ever
 *       advances contiguously, so resume from it re-lists the exact residual.</li>
 * </ul>
 */
final class RangeScannerReadaheadTest {

    private static final byte[] NO_PREFIX = new byte[0];

    /**
     * A dense, uniform keyspace over a <b>contiguous</b> alphabet (base-26 lowercase, bytes
     * {@code 0x61-0x7A}, no dead-zone gap): fixed-width 6-letter keys whose lexical order matches
     * their numeric index. A contiguous alphabet is the realistic dense-tail case (UUID/base-N object
     * keys) where the density-reflected guess placement lands on populated values; a sparse alphabet
     * (e.g. decimal/hex with the {@code '9'→'a'} gap) is the proposal's documented degrade-to-serial case.
     */
    private static List<byte[]> denseKeys(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(key(i));
        }
        return keys;
    }

    private static byte[] key(int i) {
        char[] c = new char[6];
        int v = i;
        for (int p = 5; p >= 0; p--) {
            c[p] = (char) ('a' + (v % 26));
            v /= 26;
        }
        return new String(c).getBytes(StandardCharsets.US_ASCII);
    }

    private record Run(List<String> emitted, long apiCalls, long elapsedNanos, Map<String, Long> reasons) {
    }

    private static Run run(PageFetcher fetcher, MockPageFetcher mock, int maxKeys, byte[] hi,
                           ReadaheadConfig cfg) throws Exception {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = RangeScanners.of(fetcher, maxKeys, metrics, cfg);
        List<String> emitted = new ArrayList<>();
        long start = System.nanoTime();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().asString())));
        long elapsed = System.nanoTime() - start;
        return new Run(emitted, mock.apiCalls(), elapsed, metrics.diagnostics(Duration.ZERO).stealReasons());
    }

    private static long sumCategory(Map<String, Long> reasons, String category) {
        return reasons.entrySet().stream()
                .filter(e -> e.getKey().startsWith(category + "."))
                .mapToLong(Map.Entry::getValue).sum();
    }

    /**
     * {@link ReadaheadConfig#on()} defaults with the est-remaining pre-engage gate
     * ({@link ReadaheadConfig#minEngageRemainingPages()}) pinned to 0. Used by pre-existing tests whose
     * `hi`/keyspace fixtures were built for an orthogonal concern (split-narrowing races, stale-hi
     * replenish timing) rather than for exercising the gate's density-extrapolation math.
     * <p>The floor is 0 in the shipped default too (a non-zero floor collapsed a sustained real-world
     * drain — see {@link ReadaheadConfig#minEngageRemainingPages}), so this helper is defensive (pins
     * the pin regardless of the default). See {@link #engageDeferredWhenEstimatedRemainingRunwayIsShort} / {@link
     * #engagesWhenEstimatedRemainingRunwayIsAmple}, which pin an EXPLICIT non-zero floor via {@link
     * #onWithExplicitFloor} to keep the deferral mechanism guarded now that the default is off.
     */
    private static ReadaheadConfig onWithoutEstRemainingGate() {
        return new ReadaheadConfig(true, ReadaheadConfig.DEFAULT_K, ReadaheadConfig.DEFAULT_K,
                ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES, ReadaheadConfig.DEFAULT_PLACEMENT_FACTOR,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, ReadaheadConfig.DEFAULT_DISENGAGE_WINDOW,
                ReadaheadConfig.DEFAULT_DISENGAGE_MIN_ADOPTION, 0);
    }

    /**
     * The est-remaining pre-engage gate is DISABLED by default
     * ({@link ReadaheadConfig#DEFAULT_MIN_ENGAGE_REMAINING_PAGES} == 0). The gate tests below pin
     * an EXPLICIT non-zero floor here so the deferral mechanism (and its {@code
     * engage_deferred_est_remaining} accounting) stays guarded independent of the shipped default.
     */
    private static ReadaheadConfig onWithExplicitFloor(double floor) {
        return new ReadaheadConfig(true, ReadaheadConfig.DEFAULT_K, ReadaheadConfig.DEFAULT_K,
                ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES, ReadaheadConfig.DEFAULT_PLACEMENT_FACTOR,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, ReadaheadConfig.DEFAULT_DISENGAGE_WINDOW,
                ReadaheadConfig.DEFAULT_DISENGAGE_MIN_ADOPTION, floor);
    }

    // ---- (a) ON is byte-exact vs OFF and materially faster under RTT-dominated latency -----------

    @Test
    @Timeout(120)
    void readaheadOnIsByteExactAndFasterThanSerial() throws Exception {
        int n = 4000;
        int maxKeys = 50;                  // 80 pages of the dense tail
        // A realistic dense tail: high-entropy random keys (UUID-shape), where keys one page
        // apart differ in HIGH-order bytes, so the density-reflected guess placement lands well.
        List<byte[]> keys = randomDenseKeys(n, 42L);
        byte[] hi = "zzzzzzzzzzzzzz".getBytes(StandardCharsets.US_ASCII);   // > every 13-letter key
        Duration rtt = Duration.ofMillis(8);

        MockPageFetcher off = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .latencyModel(LatencyModels.uniformFast(1L, rtt, rtt))
                .build();
        Run serial = run(off, off, maxKeys, hi, ReadaheadConfig.off());

        MockPageFetcher on = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .latencyModel(LatencyModels.uniformFast(1L, rtt, rtt))
                .build();
        // Runs under the shipping default (est-remaining floor is 0, so on() engages
        // naturally on this dense tail — no need for the floor-pinning helper). A high-entropy healthy
        // drain keeps adoption well above the disengage floor (window 16), so it stays engaged.
        Run ra = run(on, on, maxKeys, hi, ReadaheadConfig.on());

        // Byte-exact, exactly-once: identical emission to the serial baseline.
        assertThat(ra.emitted()).isEqualTo(serial.emitted());
        assertThat(ra.emitted()).hasSize(n).isSorted().doesNotHaveDuplicates();

        // Engaged and actually adopted speculative pages. (>=1: the tighter disengage window (16)
        // could in principle re-engage once on a noisy stretch of an otherwise healthy drain; the point
        // is that readahead engaged and delivered concurrency, guarded quantitatively just below.)
        assertThat(sumCategory(ra.reasons(), "READAHEAD")).isGreaterThan(0);
        assertThat(ra.reasons().getOrDefault("READAHEAD.engaged", 0L)).isGreaterThanOrEqualTo(1L);

        // --- The speedup guard, expressed as deterministic ROUND-TRIP STRUCTURE (not wall-clock) ---
        //
        // Do not assert this as an absolute wall-clock ratio (e.g. `ra.elapsedNanos() < 0.75 *
        // serial.elapsedNanos()`): under cross-fork CPU contention, parallel forks steal the CPU the K
        // concurrent guess cursors need, so the wall advantage collapses even though the algorithm's
        // pipelining is unchanged — an absolute wall-clock assertion flakes under parallel forks.
        //
        // Under an RTT-dominated store every LIST costs one uniform round-trip, so wall-clock is
        // PROPORTIONAL to the number of *serial, blocking, critical-path* round-trips the owner makes.
        // Readahead's whole point is to replace those with pages fetched CONCURRENTLY ahead of the
        // cursor (adopted / pipelined), so the deterministic analogue of "ON is faster" is "the owner's
        // serial round-trip count collapses well below the serial baseline's page count". Both counters
        // below are functions of the (deterministic) reflected-guess PLACEMENT math, not of thread
        // scheduling, so they are stable to the last unit across fork contention (verified: adopted
        // stays ~87-90 and guess_gap ~24-25 whether forks=1 or forks=8, while only the wall ratio moves).
        long serialPages = serial.apiCalls();                                   // the serial baseline's RTT chain
        long adopted = ra.reasons().getOrDefault("READAHEAD.adopted_page", 0L);
        long guessGap = ra.reasons().getOrDefault("READAHEAD.guess_gap", 0L);   // post-engage serial fallbacks
        // Warm-up pages drained serially BEFORE speculation engages are the only other blocking fetches.
        long ownerSerialRoundTrips = ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES + guessGap;

        // (1) Concurrency actually DELIVERED: the owner adopted the large majority of pages from
        // speculative (concurrent) fetches, not serial round-trips. Fails hard if readahead degenerates
        // to serial (adopted -> 0) — the precise regression the old wall-clock guard protected.
        assertThat(adopted)
                .as("adopted speculative pages (%d) cover most of the %d-page serial drain", adopted, serialPages)
                .isGreaterThanOrEqualTo(serialPages / 2);
        // (2) The deterministic "faster than serial" guard: the owner's blocking, critical-path
        // round-trips collapse to < 0.75× the serial page count, on a scheduling-independent round-trip
        // count. A regression to serial drives guess_gap toward serialPages and trips this.
        assertThat(ownerSerialRoundTrips)
                .as("readahead collapses the %d-page serial RTT chain to %d blocking round-trips (< 0.75×)",
                        serialPages, ownerSerialRoundTrips)
                .isLessThan((long) (serialPages * 0.75));

        // Speculation costs extra LIST calls (the O2 trade), but a bounded amount.
        assertThat(ra.apiCalls()).isGreaterThanOrEqualTo(serial.apiCalls());
        assertThat(ra.apiCalls()).isLessThan(serial.apiCalls() * 4);
    }

    /**
     * N unique high-entropy keys over a CONTIGUOUS alphabet (13-letter base-26 of a random long),
     * sorted — the realistic dense-tail shape: keys one page apart differ in HIGH-order bytes (so
     * density-reflected placement lands well) AND the alphabet has no dead-zone gap (so reflected
     * guesses land on populated values). This is the offline analogue of the falsifier's real tail.
     */
    private static List<byte[]> randomDenseKeys(int n, long seed) {
        SplittableRandom rng = new SplittableRandom(seed);
        TreeSet<String> set = new TreeSet<>();
        while (set.size() < n) {
            long v = rng.nextLong() & Long.MAX_VALUE;
            char[] c = new char[13];
            for (int p = 12; p >= 0; p--) {
                c[p] = (char) ('a' + (int) (v % 26));
                v /= 26;
            }
            set.add(new String(c));
        }
        List<byte[]> keys = new ArrayList<>(n);
        for (String s : set) {
            keys.add(s.getBytes(StandardCharsets.US_ASCII));
        }
        return keys;
    }

    // ---- (d) toggle OFF: zero behavior change, zero READAHEAD counters --------------------------

    @Test
    @Timeout(60)
    void readaheadOffEmitsNoCountersAndIsUnchanged() throws Exception {
        int n = 1000;
        int maxKeys = 50;
        byte[] hi = key(n);
        MockPageFetcher f = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        Run off = run(f, f, maxKeys, hi, ReadaheadConfig.off());

        assertThat(off.emitted()).hasSize(n).isSorted().doesNotHaveDuplicates();
        assertThat(sumCategory(off.reasons(), "READAHEAD")).isZero();
        assertThat(off.apiCalls()).isEqualTo((n + maxKeys - 1) / maxKeys);   // pure serial: ceil(n/maxKeys)
    }

    // ---- (b) split (thief narrows hi) mid-readahead: discard beyond new bound, no leakage --------

    @Test
    @Timeout(60)
    void splitDuringReadaheadDiscardsSpeculationBeyondNewHiWithNoLeakage() throws Exception {
        int n = 3000;
        int maxKeys = 50;
        int triggerIdx = 500;                       // narrow once the owner passes this key
        int narrowIdx = 700;                        // a thief carves off (keys[narrowIdx], hi] to a child
        List<byte[]> keys = randomDenseKeys(n, 7L);
        String triggerKey = new String(keys.get(triggerIdx), StandardCharsets.US_ASCII);
        byte[] narrowKey = keys.get(narrowIdx);
        AtomicReference<byte[]> hi = new AtomicReference<>(
                "zzzzzzzzzzzzzz".getBytes(StandardCharsets.US_ASCII));   // wide at first
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Est-remaining pre-engage gate DISABLED — see #onWithoutEstRemainingGate's javadoc.
        RangeScanner scanner = RangeScanners.of(f, maxKeys, metrics, onWithoutEstRemainingGate());

        List<String> emitted = new ArrayList<>();
        boolean[] narrowed = {false};
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi::get, null,
                (batch, lastKey, completed) -> {
                    batch.forEach(e -> emitted.add(e.key().asString()));
                    // Once the owner has drained past the trigger (readahead has fired guesses well
                    // beyond the future bound), narrow hi like a thief carving off (narrowKey, hi].
                    if (!narrowed[0] && lastKey != null
                            && new String(lastKey, StandardCharsets.US_ASCII).compareTo(triggerKey) >= 0) {
                        hi.set(narrowKey);
                        narrowed[0] = true;
                    }
                });

        // Boundary-belongs-left: the owner keeps exactly (⊥, narrowKey] = keys[0..narrowIdx],
        // byte-exact, exactly once — NO key above the new bound leaked from a speculative page fetched
        // under the old wide bound.
        List<String> expected = new ArrayList<>();
        for (int i = 0; i <= narrowIdx; i++) {
            expected.add(new String(keys.get(i), StandardCharsets.US_ASCII));
        }
        assertThat(emitted).isEqualTo(expected);
        assertThat(emitted).doesNotHaveDuplicates();
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.engaged", 0L)).isEqualTo(1L);
    }

    /**
     * (b2) Split-cancellation counting at the unit level (deterministic regardless of reflected-guess
     * placement): after launching K guesses ahead of the cursor, a narrow to the cursor itself is
     * below every guess, so ALL speculation is discarded as {@code cancelled_split} and the buffer /
     * in-flight set are cleared (their keys are now the child's — no leakage).
     */
    @Test
    @Timeout(60)
    void onHiNarrowedCancelsAllSpeculationBeyondTheNewBound() throws Exception {
        int n = 5000;
        int maxKeys = 50;
        int k = 8;
        byte[] firstKey = key(0);
        byte[] engageCursor = key(maxKeys - 1);
        byte[] hi = key(n);
        MockPageFetcher f = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SpeculativeReadahead ra = new SpeculativeReadahead(f, NO_PREFIX,
                maxKeys, firstKey, engageCursor, hi, k, k, () -> true,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, metrics)) {
            ra.replenish(engageCursor);                 // launch K guesses, all > engageCursor
            for (int i = 0; i < 20 && ra.inFlightCount() > 0; i++) {
                ra.tryNextPage(engageCursor);           // drain completions into the buffer
            }
            int outstanding = ra.inFlightCount() + ra.bufferedPageCount();
            assertThat(outstanding).isGreaterThan(0);

            ra.onHiNarrowed(engageCursor);              // a thief carves everything above the cursor away
            long cancelled = metrics.diagnostics(Duration.ZERO).stealReasons()
                    .getOrDefault("READAHEAD.cancelled_split", 0L);
            assertThat(cancelled).isEqualTo((long) outstanding);
            assertThat(ra.inFlightCount()).isZero();
            assertThat(ra.bufferedPageCount()).isZero();
        }
    }

    /**
     * A speculative fetch that FAULTS right as the scanner is about to
     * ADOPT it (the {@code tryNextPage} pipelined-continuation path, not the background
     * {@code drainCompleted} path already covered elsewhere) must fail SOFT — recorded as
     * {@code speculative_fault} and dropped — never propagated to abort the whole range.
     */
    @Test
    @Timeout(30)
    void pipelinedAwaitFaultFailsSoftInsteadOfPropagating() throws Exception {
        int maxKeys = 50;
        byte[] firstKey = key(0);
        byte[] engageCursor = key(maxKeys - 1);
        byte[] hi = key(5000);
        // A fetcher whose every call sleeps briefly then throws — models a guess that is still
        // in-flight when tryNextPage looks for it (so drainCompleted's own non-blocking check sees
        // isDone()==false) and only faults once genuinely awaited.
        PageFetcher faulty = new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req)
                    throws ListingException {
                try {
                    Thread.sleep(60);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                throw new ListingException("synthetic speculative fetch fault");
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SpeculativeReadahead ra = new SpeculativeReadahead(faulty, NO_PREFIX,
                maxKeys, firstKey, engageCursor, hi, 1, 1, () -> true,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, metrics)) {
            ra.replenish(engageCursor);   // launches exactly one guess (k=1), still in-flight
            assertThat(ra.inFlightCount()).isEqualTo(1);
            // `hi` trivially satisfies `guess <= cursor` (the guess is always < hi by construction),
            // so this simulates the scanner's cursor having caught up to the still-in-flight guess —
            // tryNextPage must await it, absorb the fault, and return null (never throw).
            SpeculativeReadahead.Adopted adopted = ra.tryNextPage(hi);
            assertThat(adopted).isNull();
            assertThat(ra.inFlightCount()).as("the faulted future was consumed, not left dangling").isZero();
        }
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.speculative_fault", 0L)).isGreaterThan(0L);
    }

    /**
     * The one fault the pipelined-continuation await does NOT absorb: a page carrying more entries
     * than the request's max-keys bound condemns the endpoint, so it must propagate out of {@code
     * tryNextPage} rather than being booked as a {@code speculative_fault} and dropped.
     */
    @Test
    @Timeout(30)
    void protocolViolationOnThePipelinedGuessPropagatesInsteadOfFailingSoft() throws Exception {
        int maxKeys = 50;
        byte[] firstKey = key(0);
        byte[] engageCursor = key(maxKeys - 1);
        byte[] hi = key(5000);
        // Sleeps before throwing, exactly like the fail-soft twin above, so the guess is still
        // in-flight when tryNextPage looks for it and the violation surfaces from the AWAITED path.
        PageFetcher overServing = new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req)
                    throws ListingException {
                try {
                    Thread.sleep(60);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                throw ProtocolViolationException.oversizedPage("bucket", maxKeys, maxKeys + 1, 0);
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SpeculativeReadahead ra = new SpeculativeReadahead(overServing, NO_PREFIX,
                maxKeys, firstKey, engageCursor, hi, 1, 1, () -> true,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, metrics)) {
            ra.replenish(engageCursor);   // launches exactly one guess (k=1), still in-flight
            assertThat(ra.inFlightCount()).isEqualTo(1);
            assertThatThrownBy(() -> ra.tryNextPage(hi))
                    .isInstanceOf(ProtocolViolationException.class)
                    .hasMessageContaining("oversized_page");
        }
        assertThat(metrics.diagnostics(Duration.ZERO).stealReasons()
                .getOrDefault("READAHEAD.speculative_fault", 0L))
                .as("the violation was refused outright, not absorbed as a droppable guess fault")
                .isZero();
    }

    /**
     * The intermittently over-serving endpoint: only the speculative guesses trip the response-side
     * entry bound while the ordinary serial continuations keep answering conformingly. Absorbing the
     * violation as a {@code speculative_fault} would let the serial refetch cover the same region and
     * the whole range complete as if nothing had happened — so the range must instead abort on the
     * first violation, wherever on the speculative path it surfaces.
     */
    @Test
    @Timeout(60)
    void protocolViolationOnASpeculativeGuessAbortsTheRangeEvenThoughTheSerialRefetchConforms() throws Exception {
        int maxKeys = 50;
        int n = 2500;   // room for the engage streak (6 full pages) plus a long remaining drain
        byte[] hi = key(n);
        MockPageFetcher serial = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        PageFetcher overServingSpeculation = new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req)
                    throws ListingException {
                throw ProtocolViolationException.oversizedPage("bucket", maxKeys, maxKeys + 1, 0);
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Est-remaining pre-engage gate DISABLED so engagement on `denseKeys`' low-entropy sequential
        // encoding is deterministic here — see #onWithoutEstRemainingGate's javadoc.
        RangeScanner scanner = new RangeScanner(serial, maxKeys, metrics, onWithoutEstRemainingGate(),
                overServingSpeculation, () -> true);

        List<String> emitted = new ArrayList<>();
        assertThatThrownBy(() -> scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().asString()))))
                .isInstanceOf(ProtocolViolationException.class)
                .hasMessageContaining("oversized_page");

        assertThat(emitted)
                .as("the range stopped at the violation instead of draining to the end via serial refetches")
                .hasSizeLessThan(n);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.engaged", 0L))
                .as("readahead actually engaged (the speculative path was the one that tripped)")
                .isGreaterThanOrEqualTo(1L);
        assertThat(reasons.getOrDefault("READAHEAD.speculative_fault", 0L))
                .as("no violation was ever booked as an absorbed, droppable guess fault")
                .isZero();
    }

    /**
     * A speculative fetcher that parks every guess until it is released, then over-serves. The park
     * makes the fault land at a moment the test picks; {@link #awaitFaulted()} then releases the guess
     * and JOINS its carrier thread, which orders the future's exceptional completion before whatever
     * the test does next. That join is what makes the discard/teardown tests below deterministic — a
     * sleep would only make the race likely, never certain.
     */
    private static final class ParkedOverServingFetcher implements PageFetcher {
        private final int maxKeys;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<Thread> carrier = new AtomicReference<>();

        ParkedOverServingFetcher(int maxKeys) {
            this.maxKeys = maxKeys;
        }

        @Override
        public ListPage fetchPage(PageRequest req)
                throws ListingException {
            carrier.set(Thread.currentThread());
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            throw ProtocolViolationException.oversizedPage("bucket", maxKeys, maxKeys + 1, 0);
        }

        @Override
        public StoreCapabilities capabilities() {
            return StoreCapabilities.s3();
        }

        void awaitFaulted() throws InterruptedException {
            started.await();
            Thread parked = carrier.get();
            release.countDown();
            parked.join();
        }
    }

    /** {@link ReadaheadConfig#on()} with a single guess in flight, no disengage gate, no engage gate. */
    private static ReadaheadConfig onWithOneGuess() {
        return new ReadaheadConfig(true, 1, 1,
                ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES, ReadaheadConfig.DEFAULT_PLACEMENT_FACTOR,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, 0,
                ReadaheadConfig.DEFAULT_DISENGAGE_MIN_ADOPTION, 0);
    }

    /**
     * The violation that lands during TEARDOWN. A guess that comes back over-serving only after the
     * range's final page was committed is never drained again — the range returns, and the only code
     * left to look at that future is {@code close}. Cancelling it there and moving on lets the range
     * finish clean while the endpoint's violation goes unreported, since (as in the twin above) every
     * ordinary serial continuation answered conformingly: {@code Future#cancel} returns false for an
     * already-completed future and its result is simply dropped.
     *
     * <p>Deterministic: the guess is released and its carrier thread joined from inside the terminal
     * page's OWN commit callback, so the future is known-completed before teardown runs.
     */
    @Test
    @Timeout(30)
    void protocolViolationLandingAfterTheFinalPageAbortsTheRangeFromTeardown() throws Exception {
        int maxKeys = 50;
        // Exactly the engage streak of full pages, then a short terminal one — so the ONE guess placed
        // at engage is still parked when the range ends and nothing drains it again.
        int n = ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES * maxKeys + 10;
        byte[] hi = key(4000);   // far beyond the real data, so the reflected guess has room to be placed
        MockPageFetcher serial = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        ParkedOverServingFetcher speculation = new ParkedOverServingFetcher(maxKeys);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = new RangeScanner(serial, maxKeys, metrics, onWithOneGuess(),
                speculation, () -> true);

        List<String> emitted = new ArrayList<>();
        assertThatThrownBy(() -> scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> {
                    batch.forEach(e -> emitted.add(e.key().asString()));
                    if (completed) {
                        speculation.awaitFaulted();   // the guess faults AFTER the last drain, before close
                    }
                }))
                .isInstanceOf(ProtocolViolationException.class)
                .hasMessageContaining("oversized_page");

        assertThat(emitted)
                .as("the whole range drained conformingly -- teardown was the only place left to "
                        + "surface the violation")
                .hasSize(n);
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.engaged", 0L)).isEqualTo(1L);
        assertThat(reasons.getOrDefault("READAHEAD.speculative_fault", 0L))
                .as("the violation was refused outright, not absorbed as a droppable guess fault")
                .isZero();
    }

    /**
     * The same hole on the split-narrowing discard: a thief lowering {@code hi} past a guess drops that
     * guess as {@code cancelled_split}, and a guess that had ALREADY come back over-serving must be
     * refused rather than cancelled away — the keys moving to the child says nothing about the endpoint
     * that just broke the protocol serving them.
     */
    @Test
    @Timeout(30)
    void protocolViolationOnAGuessDiscardedByASplitNarrowPropagates() throws Exception {
        int maxKeys = 50;
        byte[] firstKey = key(0);
        byte[] engageCursor = key(maxKeys - 1);
        byte[] hi = key(5000);
        ParkedOverServingFetcher speculation = new ParkedOverServingFetcher(maxKeys);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SpeculativeReadahead ra = new SpeculativeReadahead(speculation, NO_PREFIX,
                maxKeys, firstKey, engageCursor, hi, 1, 1, () -> true,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, metrics)) {
            ra.replenish(engageCursor);   // launches exactly one guess (k=1), parked above the cursor
            assertThat(ra.inFlightCount()).isEqualTo(1);
            speculation.awaitFaulted();   // completed exceptionally, and nothing has observed it

            assertThatThrownBy(() -> ra.onHiNarrowed(engageCursor))
                    .isInstanceOf(ProtocolViolationException.class)
                    .hasMessageContaining("oversized_page");
        }
        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.cancelled_split", 0L))
                .as("the guess was refused, not booked as an ordinary split casualty")
                .isZero();
        assertThat(reasons.getOrDefault("READAHEAD.speculative_fault", 0L)).isZero();
    }

    /**
     * A speculative guess that hits a PERSISTENT, genuine
     * AIMD-voting throttle (a real 503 {@code SlowDown} — {@code ThrottleException.Kind.SLOWDOWN})
     * must never stall the owner's own drain, even though that throttle kind retries UNBOUNDED
     * inside {@link GaugedFetcher} regardless of {@code slotGated} (verified by
     * reading the retry loop: the voting-throttle branch never checks {@code slotGated} and never
     * reaches the {@code PROBE_TRANSIENT_RETRY_CAP} fail-fast, which only guards the NON-voting
     * branch). Uses the EXACT production wrapper ({@link GaugedFetcher} with
     * {@code slotGated=false, reportSuccess=false}, the same construction {@code WorkStealingScan}
     * wires for both the thief's probe fetcher and this readahead unit's speculative fetcher) around
     * a fetcher that throws a genuine {@code SLOWDOWN} on every call, while the ORDINARY serial
     * continuation pages (a separate, healthy fetcher instance) keep succeeding — modeling a guess
     * that happens to land on a persistently-throttled sub-range while the rest of the keyspace is
     * healthy. A small {@code fetchBudgetMillis} keeps the test itself fast; the hard {@code
     * @Timeout} below is the bounded-wall assertion — a regression back to an unconditional await
     * hangs forever and fails the test on timeout, not merely runs slow.
     */
    @Test
    @Timeout(20)
    void speculativeGuessPersistent503NeverStallsOwnerDrainAndFailsSoft() throws Exception {
        int n = 3000;
        int maxKeys = 50;
        List<byte[]> keys = randomDenseKeys(n, 99L);
        byte[] hi = "zzzzzzzzzzzzzz".getBytes(StandardCharsets.US_ASCII);

        // The SERIAL path: a plain, healthy fetcher over the SAME keyspace — every ordinary
        // continuation the owner actually needs succeeds normally.
        MockPageFetcher serialFetcher = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();

        // The SPECULATIVE path: the REAL production wrapper around a fetcher whose EVERY call throws
        // a genuine AIMD-voting SLOWDOWN — persistent, never recovers.
        MockPageFetcher persistentlyThrottled = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .interceptor((req, idx, page) -> {
                    throw ThrottleException.slowDown("synthetic persistent 503");
                })
                .build();
        ConcurrencyGauge gauge = ConcurrencyGauges.of(8);
        // A REAL (interruptible) but tiny backoff sleep — fast retries so the test spins quickly
        // without a busy-loop that would never notice a cancel(true) (Thread.sleep is the checked
        // interruption point the retry loop actually blocks on).
        GaugedFetcher speculativeFetcher = new GaugedFetcher(
                persistentlyThrottled, gauge, false, false, new RunMetrics(new SimpleMeterRegistry()),
                () -> null, millis -> Thread.sleep(Math.min(millis, 5)), RetryPolicy.RIDE_OUT);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        long smallBudgetMillis = 150L;   // fast test; @Timeout above is the real regression guard
        // Disengage-on-low-adoption is DISABLED here (window = 0) so this adversarial test keeps its exact
        // `engaged == 1` assertion — its point is "a persistent 503 never stalls the owner and fails
        // soft", not the low-adoption gate (which would otherwise disengage on zero adoption and
        // re-engage, making `engaged` > 1). The disengage gate is covered directly elsewhere. The
        // est-remaining pre-engage gate is ALSO disabled (0) here: this test's `hi` is a
        // deliberately generous, far-beyond-real-data bound (an unrelated concern to what this test
        // exercises), so it is kept isolated from the new gate rather than hand-verified against it.
        ReadaheadConfig cfg = new ReadaheadConfig(true, ReadaheadConfig.DEFAULT_K, ReadaheadConfig.DEFAULT_K,
                ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES, ReadaheadConfig.DEFAULT_PLACEMENT_FACTOR,
                smallBudgetMillis, 0, ReadaheadConfig.DEFAULT_DISENGAGE_MIN_ADOPTION, 0);
        RangeScanner scanner = new RangeScanner(serialFetcher, maxKeys, metrics, cfg, speculativeFetcher, () -> true);

        List<String> emitted = new ArrayList<>();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().asString())));

        // The run COMPLETES (never hangs) — byte-exact, exactly-once — despite every single
        // speculative guess persistently throttling for the whole drain.
        List<String> expected = new ArrayList<>();
        for (byte[] k : keys) {
            expected.add(new String(k, StandardCharsets.US_ASCII));
        }
        assertThat(emitted).isEqualTo(expected);

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.engaged", 0L))
                .as("readahead actually engaged (the mechanism was exercised, not just the serial baseline)")
                .isEqualTo(1L);
        assertThat(reasons.getOrDefault("READAHEAD.speculative_fault", 0L))
                .as("every persistently-throttled guess failed soft, at least once")
                .isGreaterThan(0L);
        assertThat(reasons.getOrDefault("READAHEAD.adopted_page", 0L))
                .as("nothing was ever adopted -- every guess died to the persistent throttle")
                .isZero();
    }

    /**
     * Stale-hi replenish: {@code consumer.accept} — {@code RangeScanner}'s
     * page-commit callback — can ITSELF narrow {@code hi} (an owner self-split fires from exactly
     * that call site in the real engine). The engage-check and the immediately-following
     * {@code ra.replenish} that run right after the callback returns must act on the FRESHEST bound,
     * not the {@code hiNow} sampled at loop-top BEFORE the callback ran.
     *
     * <p>Timed to land EXACTLY on the engage page's own commit (the callback whose return triggers
     * {@code consecutiveFullSerialPages >= engageAfterFullPages}, constructing a brand-new {@code
     * SpeculativeReadahead} and immediately calling its first {@code replenish} — all within the SAME
     * iteration as the narrow): the test narrows {@code hi} to a key only slightly ahead of that
     * page's own cursor. The newly-engaged {@code SpeculativeReadahead} must be constructed with the
     * FRESH, tight bound, so {@code nextGuess()} finds no meaningful room and places nothing beyond
     * it — using the STALE wide {@code hiNow} sampled before the callback ran would instead place
     * guesses reflecting toward the wrong (far) bound, recognized as invalid only one full iteration
     * late, at the next loop-top's {@code onHiNarrowed}. So {@code cancelled_split == 0} here is the
     * precise regression guard: it is exactly the count of "placed beyond the bound in the first
     * place" waste a stale bound would cause.
     *
     * <p>Uses the CONTIGUOUS base-26 {@link #denseKeys}/{@link #key} keyspace (not the high-entropy
     * random one): its index-to-byte-value mapping is smooth/predictable, so the reflected placement
     * math ({@code StealMath.extrapolate}) reliably overshoots a tight, close-by narrow bound when
     * (and only when) it is constructed against the WIDE stale bound — a high-entropy random keyspace
     * makes the reflected guess position too unpredictable (it can jump straight into empty territory
     * beyond all real data either way) to deterministically distinguish the two behaviors.
     */
    @Test
    @Timeout(30)
    void replenishActsOnFreshHiNotTheStaleValueSampledBeforeTheConsumerRan() throws Exception {
        int n = 4000;
        int maxKeys = 50;
        int engageAfterFullPages = ReadaheadConfig.DEFAULT_ENGAGE_AFTER_FULL_PAGES;   // 6
        int cursorAtEnginePage = engageAfterFullPages * maxKeys - 1;                  // last key of the engage page
        int narrowIdx = cursorAtEnginePage + 10;   // tight margin above the engage page's own cursor
        AtomicReference<byte[]> hi = new AtomicReference<>(key(n));   // wide: > every real key
        MockPageFetcher f = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Est-remaining pre-engage gate DISABLED — this test's whole point is a TIGHT
        // narrow right at the engage page (true remaining there is deliberately ~10 keys), which the
        // new gate would (correctly, for its own purpose) defer on — an unrelated concern to what this
        // test exercises (stale- vs fresh-hi timing). See #onWithoutEstRemainingGate's javadoc.
        RangeScanner scanner = RangeScanners.of(f, maxKeys, metrics, onWithoutEstRemainingGate());

        List<String> emitted = new ArrayList<>();
        int[] pageCount = {0};
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi::get, null,
                (batch, lastKey, completed) -> {
                    batch.forEach(e -> emitted.add(e.key().asString()));
                    pageCount[0]++;
                    // Narrow EXACTLY on the engage page's own commit -- the precise window this test
                    // targets: the engage-check + first replenish (right after this callback returns,
                    // SAME iteration) must see THIS fresh narrow, not the stale hiNow from loop-top.
                    if (pageCount[0] == engageAfterFullPages) {
                        hi.set(key(narrowIdx));
                    }
                });

        // Byte-exact, exactly-once over the FINAL (narrowed) keyspace this run actually owns.
        List<String> expected = new ArrayList<>();
        for (int i = 0; i <= narrowIdx; i++) {
            expected.add(new String(key(i), StandardCharsets.UTF_8));
        }
        assertThat(emitted).isEqualTo(expected);
        assertThat(emitted).doesNotHaveDuplicates();

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.engaged", 0L))
                .as("readahead still engaged at the same page despite the narrow racing its own commit")
                .isEqualTo(1L);
        assertThat(reasons.getOrDefault("READAHEAD.cancelled_split", 0L))
                .as("no guess was ever placed beyond the bound in the first place, so none needed "
                        + "cancelling -- the regression signature (a stale-hi engage/replenish) would "
                        + "show cancelled_split > 0 here instead")
                .isZero();
    }

    // ---- (c) adoption buffer stays capped under a pathological non-advancing gap -----------------

    @Test
    @Timeout(60)
    void adoptionBufferStaysCappedUnderPathologicalGap() throws Exception {
        int n = 5000;
        int maxKeys = 50;
        int k = 4;
        byte[] firstKey = key(0);
        byte[] engageCursor = key(maxKeys - 1);   // one observed page [0, maxKeys-1]
        byte[] hi = key(n);
        MockPageFetcher f = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SpeculativeReadahead ra = new SpeculativeReadahead(f, NO_PREFIX,
                maxKeys, firstKey, engageCursor, hi, k, k, () -> true,
                ReadaheadConfig.DEFAULT_FETCH_BUDGET_MILLIS, metrics)) {
            // Pathological: the cursor NEVER advances (stuck at key(0)); every completed guess lands
            // ahead of it and HOLDs. Replenish many times — the K-slot budget must bound both the
            // buffer and the in-flight set; it must never bloat past K.
            for (int i = 0; i < 50; i++) {
                ra.replenish(key(0));
                // give in-flight guesses time to complete into the buffer
                ra.tryNextPage(key(0));
                assertThat(ra.bufferedPageCount()).isLessThanOrEqualTo(k);
                assertThat(ra.inFlightCount() + ra.bufferedPageCount()).isLessThanOrEqualTo(k);
            }
            // Nothing was adoptable (cursor never advanced past any guess) — no page adopted.
            Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
            assertThat(reasons.getOrDefault("READAHEAD.adopted_page", 0L)).isZero();
        }
    }

    // ---- Engage/disengage gate: engine-level wiring -----------------------------------------

    /**
     * A bucket whose dense stretch is SHORTER than
     * {@code engageAfterFullPages} (6) never engages — exactly the transient-dense-stretch case the plain
     * serial scan absorbs at zero extra cost, which a lower threshold would (wastefully) engage on.
     * Here 5 full truncated pages drain (streak maxes at 5 &lt; 6) before a short terminal page, so no
     * {@code READAHEAD.engaged} is emitted and the emission is exactly the pure-serial baseline.
     */
    @Test
    @Timeout(60)
    void transientDenseStretchBelowTheLongerStreakNeverEngages() throws Exception {
        int maxKeys = 50;
        int n = 255;                       // pages 1..5 full+truncated (streak 5), page 6 = 5 keys, done
        byte[] hi = key(n);
        MockPageFetcher f = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        Run r = run(f, f, maxKeys, hi, ReadaheadConfig.on());

        assertThat(r.emitted()).hasSize(n).isSorted().doesNotHaveDuplicates();
        assertThat(r.reasons().getOrDefault("READAHEAD.engaged", 0L))
                .as("a 5-full-page transient stretch is below the tightened engage streak (6) -> never engages")
                .isZero();
        assertThat(sumCategory(r.reasons(), "READAHEAD")).isZero();   // no speculation at all
    }

    /**
     * The est-remaining pre-engage gate: a range that clears the
     * {@code engageAfterFullPages} streak but is genuinely close to its own {@code hi} — here a 550-key
     * range where the streak's own 300 keys leave only ~250 keys (~5 pages) remaining, far under the
     * pinned {@code minEngageRemainingPages} floor (48 pages) — never engages: every subsequent full page
     * keeps deferring (the streak is not reset) until the range naturally ends. Uses {@link
     * #randomDenseKeys} + a TIGHT (exact-fit) {@code hi}: the gate's density-extrapolation math
     * (`StealMath#estRemaining`, scoped to the streak's own start key) needs keys that diverge in
     * HIGH-order bytes to give a numerically well-behaved estimate — {@link #denseKeys}'s low-entropy
     * shared-prefix sequential encoding does not (see {@link #onWithoutEstRemainingGate}).
     * <p>The floor is 0 in the shipped default (a non-zero floor collapsed a sustained real-world
     * drain), so this test pins an EXPLICIT floor of 48 via {@link #onWithExplicitFloor} to keep the
     * deferral mechanism guarded.
     */
    @Test
    @Timeout(60)
    void engageDeferredWhenEstimatedRemainingRunwayIsShort() throws Exception {
        int maxKeys = 50;
        int n = 550;   // streak(6) = 300 keys emitted; true remaining = 250 keys (~5 pages) << 48-page floor
        List<byte[]> keys = randomDenseKeys(n, 11L);
        byte[] hi = keys.get(keys.size() - 1);   // tight bound: exactly the true max key
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        Run r = run(f, f, maxKeys, hi, onWithExplicitFloor(48.0));

        assertThat(r.emitted()).hasSize(n).isSorted().doesNotHaveDuplicates();   // deferred, still byte-exact
        assertThat(r.reasons().getOrDefault("READAHEAD.engaged", 0L))
                .as("short estimated remaining runway (~5 pages) never clears the 48-page floor -> never engages")
                .isZero();
        assertThat(r.reasons().getOrDefault("READAHEAD.engage_deferred_est_remaining", 0L))
                .as("every full page past the streak threshold deferred on the est-remaining check")
                .isGreaterThanOrEqualTo(1L);
        assertThat(sumCategory(r.reasons(), "READAHEAD"))
                .as("no speculation was ever launched -- deferrals are the ONLY READAHEAD activity")
                .isEqualTo(r.reasons().getOrDefault("READAHEAD.engage_deferred_est_remaining", 0L));
    }

    /**
     * A genuinely long bounded drain (10,000 keys; ~9,700 remaining at the
     * streak-6 engage check, ~194 estimated pages) clears the {@code minEngageRemainingPages} floor
     * comfortably and engages normally — confirms the gate does not (over-)suppress the case it is meant
     * to leave alone (a sustained dense drain).
     * <p>The floor is 0 in the shipped default, so this pins an EXPLICIT floor. Two
     * runs over the SAME keyspace strengthen the guard against the {@code estRemaining} fail-open
     * (INFINITY) branch — which engages regardless of any finite floor and would thus falsely "pass" a
     * naive engage-only assertion: (A) floor 48 (below the real estimate) -> engages; (B) a NEAR-INFINITE
     * floor (1e9 pages, far above any real estimate — the range holds only 10,000 keys / 200 pages) ->
     * DEFERS. Run (B) can only defer if a FINITE real density estimate was computed; had the fail-open
     * INFINITY path engaged run (A), it would engage under the 1e9 floor too (INFINITY is below no floor).
     */
    @Test
    @Timeout(60)
    void engagesWhenEstimatedRemainingRunwayIsAmple() throws Exception {
        int maxKeys = 50;
        int n = 10_000;
        List<byte[]> keys = randomDenseKeys(n, 13L);
        byte[] hi = keys.get(keys.size() - 1);

        // (A) ample runway clears a floor BELOW the real estimate -> engages, never defers.
        MockPageFetcher fa = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        Run a = run(fa, fa, maxKeys, hi, onWithExplicitFloor(48.0));
        assertThat(a.emitted()).hasSize(n).isSorted().doesNotHaveDuplicates();
        assertThat(a.reasons().getOrDefault("READAHEAD.engaged", 0L))
                .as("ample estimated remaining runway (~194 pages) clears the 48-page floor -> engages")
                .isGreaterThanOrEqualTo(1L);
        assertThat(a.reasons().getOrDefault("READAHEAD.engage_deferred_est_remaining", 0L)).isZero();

        // (B) fail-open guard: a NEAR-INFINITE floor (far above any finite estimate — the range holds
        // only 200 pages) must DEFER. The fail-open INFINITY branch (a degenerate/zero-span estimate)
        // would instead ENGAGE under any finite floor, so a defer here proves a FINITE real density
        // estimate — not fail-open — drove the engage in run (A).
        MockPageFetcher fb = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        Run b = run(fb, fb, maxKeys, hi, onWithExplicitFloor(1e9));
        assertThat(b.emitted()).hasSize(n).isSorted().doesNotHaveDuplicates();
        assertThat(b.reasons().getOrDefault("READAHEAD.engaged", 0L))
                .as("a near-infinite floor (1e9) above any FINITE estimate -> defers (NOT the fail-open engage)")
                .isZero();
        assertThat(b.reasons().getOrDefault("READAHEAD.engage_deferred_est_remaining", 0L))
                .as("the real finite density estimate is below the 1e9-page floor -> deferrals recorded")
                .isGreaterThanOrEqualTo(1L);
    }

    /**
     * Disengage-on-low-adoption + re-engage: an engaged range whose speculative guesses NEVER
     * pay off (here every guess-ahead fetch faults, via a throwing speculative fetcher — the deterministic
     * analogue of a real overlap-waste signature: adoption pinned at 0) disengages once its
     * tumbling adoption window fills below the floor, reverts to plain serial, and — since the healthy
     * serial drain keeps producing full un-split pages — re-engages on a fresh streak (counted
     * {@code re_engaged}). The whole drain stays byte-exact and nothing is ever adopted.
     */
    @Test
    @Timeout(60)
    void engagedRangeWithZeroAdoptionDisengagesThenReEngages() throws Exception {
        int maxKeys = 50;
        // 50 pages: room for engage(6) + a full 16-page disengage window + re-engage.
        int n = 2500;
        byte[] hi = key(n);
        MockPageFetcher serial = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        // Every speculative guess-ahead fetch faults -> nothing is ever adoptable -> adoption pinned at 0.
        PageFetcher alwaysFaultingSpeculation = new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req)
                    throws ListingException {
                throw new ListingException("synthetic speculative fault (zero adoption)");
            }

            @Override
            public StoreCapabilities capabilities() {
                return StoreCapabilities.s3();
            }
        };
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        // Est-remaining pre-engage gate DISABLED — this test's point is disengage/
        // re-engage window mechanics under `denseKeys`' low-entropy sequential encoding, which is a
        // poor fit for the gate's density math (see #onWithoutEstRemainingGate's javadoc); isolating it
        // here keeps this test's re-engage assertions independent of the new gate's own behavior.
        RangeScanner scanner = new RangeScanner(serial, maxKeys, metrics, onWithoutEstRemainingGate(),
                alwaysFaultingSpeculation, () -> true);

        List<String> emitted = new ArrayList<>();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().asString())));

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            expected.add(new String(key(i), StandardCharsets.US_ASCII));
        }
        assertThat(emitted).isEqualTo(expected);   // byte-exact / exactly-once despite the engage churn

        Map<String, Long> reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("READAHEAD.engaged", 0L))
                .as("engaged at least twice: the initial engage plus one re-engage after disengaging")
                .isGreaterThanOrEqualTo(2L);
        assertThat(reasons.getOrDefault("READAHEAD.disengaged_low_adoption", 0L))
                .as("the zero-adoption window drove a disengage")
                .isGreaterThanOrEqualTo(1L);
        assertThat(reasons.getOrDefault("READAHEAD.re_engaged", 0L))
                .as("a fresh full-page streak re-engaged after the disengage")
                .isGreaterThanOrEqualTo(1L);
        assertThat(reasons.getOrDefault("READAHEAD.adopted_page", 0L))
                .as("nothing was ever adopted -- every guess faulted")
                .isZero();
    }

    // ---- (resume) crash mid-readahead loses/duplicates nothing at the committed-cursor seam ------

    @Test
    @Timeout(60)
    void crashMidReadaheadResumesExactlyOnceFromCommittedCursor() throws Exception {
        int n = 2000;
        int maxKeys = 50;
        byte[] hi = key(n);

        // Phase 1: run with readahead ON, but "crash" (throw) after ~600 keys. The last key handed to
        // the consumer BEFORE the throw is the durable committed cursor (commit-before-emit: the
        // engine commits the page, then emits — so a crash never leaves an emitted-but-uncommitted key).
        // The crash point (~page 12) is deliberately PAST the engage streak
        // (6 full pages) so the kill genuinely lands MID-READAHEAD (guesses in-flight), not during the
        // pre-engage serial warm-up. The shipped default est-remaining floor is 0, so
        // ReadaheadConfig.on() engages on this dense keyspace without any deferral to work around
        // (a non-zero default floor would defer here, silently reducing this to a serial crash).
        MockPageFetcher f1 = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        RangeScanner s1 = RangeScanners.of(f1, maxKeys, new RunMetrics(new SimpleMeterRegistry()),
                ReadaheadConfig.on());
        List<String> pre = new ArrayList<>();
        AtomicReference<byte[]> committed = new AtomicReference<>();
        try {
            s1.runRange(NO_PREFIX, ListingMode.OBJECTS, null, () -> hi, null,
                    (batch, lastKey, completed) -> {
                        // Simulate the checkpoint: cursor commits to lastKey, THEN we emit.
                        if (lastKey != null) {
                            committed.set(lastKey);
                        }
                        batch.forEach(e -> pre.add(e.key().asString()));
                        if (pre.size() >= 600) {   // ~page 12 -> past the streak-6 engage point
                            throw new RuntimeException("simulated crash mid-readahead");
                        }
                    });
        } catch (RuntimeException expected) {
            assertThat(expected).hasMessageContaining("simulated crash");
        }

        // Phase 2: resume from the durable committed cursor (start_after = last committed key).
        MockPageFetcher f2 = MockPageFetcher.builder().keys(denseKeys(n)).maxKeysCap(maxKeys).build();
        RangeScanner s2 = RangeScanners.of(f2, maxKeys, new RunMetrics(new SimpleMeterRegistry()),
                ReadaheadConfig.on());
        List<String> post = new ArrayList<>();
        s2.runRange(NO_PREFIX, ListingMode.OBJECTS, committed.get(), () -> hi, null,
                (batch, lastKey, completed) -> batch.forEach(e -> post.add(e.key().asString())));

        // The union of committed-pre + resumed-post is the full keyspace, exactly once, in order.
        // Everything AFTER the committed cursor is re-listed on resume; nothing before it is lost.
        List<String> committedPre = new ArrayList<>();
        String committedKey = new String(committed.get(), StandardCharsets.UTF_8);
        for (String key : pre) {
            if (key.compareTo(committedKey) <= 0) {
                committedPre.add(key);
            }
        }
        List<String> union = new ArrayList<>(committedPre);
        union.addAll(post);
        assertThat(union).isSorted().doesNotHaveDuplicates();
        List<String> all = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            all.add(new String(key(i), StandardCharsets.UTF_8));
        }
        assertThat(union).isEqualTo(all);
    }
}
