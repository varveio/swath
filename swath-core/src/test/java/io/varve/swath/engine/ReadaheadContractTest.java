/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.LatencyModels;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangeScanners;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * <b>Adversarial guard for intra-range speculative readahead</b>
 * ({@link SpeculativeReadahead} + {@link RangeScanner}'s page loop): guards the
 * <em>exactly-once</em> contract (I1 commit-before-emit, exactly-once, I9). The focused unit suite
 * ({@link RangeScannerReadaheadTest}) covers the happy-path ON/OFF byte-exactness, speedup,
 * split-no-leakage, buffer cap, and a single crash/resume; this class covers what that left open:
 *
 * <ol>
 *   <li><b>PROP-grade randomized exactly-once</b> ({@link #propertyExactlyOnceAcrossKeyspaces}): over
 *       many seeds, varied alphabets (contiguous, sparse-hex with the {@code '9'->'a'} dead zone,
 *       common-long-prefix, unicode multi-byte), randomized page sizes and per-request latency, and
 *       random mid-run {@code hi}-narrows, the emitted keys are byte-exactly the range content, each
 *       exactly once, in order. This is the primary guard.</li>
 *   <li><b>Degenerate shapes</b>: empty range, single page, guesses landing past end-of-range,
 *       {@code maxKeys == 1}.</li>
 *   <li><b>RES crash/resume torture</b> ({@link #resRepeatedCrashResumeUnionExactlyOnce}): N sequential
 *       kill/resume cycles at randomized points (readahead engaged, mid-await under latency) — the
 *       union of committed segments is the keyspace exactly once (readahead state is process-local and
 *       a crash never advances the committed cursor past a gap).</li>
 *   <li><b>Adversarial adoption edges</b>: a fully-stale buffered page is discarded, never adopted;
 *       the largest adoptable guess wins with boundary-exclusive trim.</li>
 * </ol>
 *
 * <p>Deterministic throughout: fixed seeds, {@link MockPageFetcher}, per-request (order-independent)
 * {@link LatencyModels#uniformFast} timing. Every assertion is designed to genuinely FAIL if the
 * cursor ever advances past a gap (lost key) or re-adopts already-emitted keys (duplicate).
 */
final class ReadaheadContractTest {

    private static final byte[] NO_PREFIX = new byte[0];
    /** A bound strictly above every valid-UTF-8 key (UTF-8 never contains 0xFF) — a non-null hi. */
    private static final byte[] TOP = {(byte) 0xFF};

    // ================================================================================================
    // 1. PROP-grade randomized exactly-once — the big one.
    // ================================================================================================

    @Test
    @Timeout(120)
    void propertyExactlyOnceAcrossKeyspaces() throws Exception {
        int seeds = 100;
        long startNanos = System.nanoTime();
        for (int s = 0; s < seeds; s++) {
            runOnePropertySeed(s);
        }
        long wallMs = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
        // Belt-and-suspenders: the property loop must stay well inside a per-commit budget.
        assertThat(wallMs).as("property loop wall time (ms)").isLessThan(55_000L);
    }

    private void runOnePropertySeed(long seed) throws Exception {
        SplittableRandom rng = new SplittableRandom(seed);
        int alphabet = rng.nextInt(4);
        int n = rng.nextInt(150, 900);
        List<byte[]> keys = randomKeyspace(rng, n, alphabet);
        int size = keys.size();
        int maxKeys = rng.nextInt(4, 80);
        long latMicros = rng.nextInt(0, 2200);
        Duration lat = Duration.ofNanos(latMicros * 1_000L);

        boolean narrow = rng.nextBoolean() && size > 2 * maxKeys + 2;

        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(keys).maxKeysCap(maxKeys)
                .latencyModel(LatencyModels.uniformFast(seed, lat, lat))
                .build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = RangeScanners.of(fetcher, maxKeys, metrics, ReadaheadConfig.on());
        List<byte[]> emitted = new ArrayList<>();

        List<byte[]> expected;
        if (narrow) {
            // A real thief narrows hi to a key that is at least a full page AHEAD of the cursor at the
            // moment the narrow fires, so no key above the new bound has yet been emitted (I3
            // boundary-belongs-left). narrowIdx >= triggerIdx + maxKeys guarantees the page that first
            // reaches triggerKey (last index <= triggerIdx + maxKeys - 1) has not yet crossed narrowKey.
            int triggerIdx = rng.nextInt(0, size - 2 * maxKeys - 1);
            int narrowIdx = triggerIdx + maxKeys + rng.nextInt(0, size - triggerIdx - maxKeys);
            byte[] triggerKey = keys.get(triggerIdx);
            byte[] narrowKey = keys.get(narrowIdx);
            AtomicReference<byte[]> hi = new AtomicReference<>(TOP);
            boolean[] fired = {false};
            scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi::get, null,
                    (batch, lastKey, completed) -> {
                        batch.forEach(e -> emitted.add(e.key().raw()));
                        if (!fired[0] && lastKey != null
                                && KeyBytes.compareUnsigned(lastKey, triggerKey) >= 0) {
                            hi.set(narrowKey);
                            fired[0] = true;
                        }
                    });
            expected = new ArrayList<>(keys.subList(0, narrowIdx + 1));   // (⊥, narrowKey], boundary-left
        } else {
            scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, () -> TOP, null,
                    (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().raw())));
            expected = keys;
        }

        String ctx = "seed=" + seed + " alphabet=" + alphabet + " n=" + size + " maxKeys=" + maxKeys
                + " latMicros=" + latMicros + " narrow=" + narrow;
        assertByteExactInOrder(emitted, expected, ctx);
        // Never a duplicate or gap in emission order (independent of the set equality above).
        assertStrictlyAscendingNoDup(emitted, ctx);
    }

    // ================================================================================================
    // 2. Degenerate shapes.
    // ================================================================================================

    @Test
    @Timeout(60)
    void degenerateEmptyRangeEmitsNothingAndNeverEngages() throws Exception {
        List<byte[]> keys = contiguousKeys(500, 8);   // all >= key "aaaaa..." region
        // hi below the first key: (⊥, hi] is empty, the first page's first key already crosses the bound.
        byte[] hi = "AAAAAAAA".getBytes(StandardCharsets.US_ASCII);   // uppercase < lowercase 'a'
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(50).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = RangeScanners.of(f, 50, metrics, ReadaheadConfig.on());
        List<byte[]> emitted = new ArrayList<>();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, hi,
                null, (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().raw())));
        assertThat(emitted).isEmpty();
        assertThat(sumCategory(reasons(metrics), "READAHEAD")).isZero();
    }

    @Test
    @Timeout(60)
    void degenerateSinglePageNeverEngages() throws Exception {
        int n = 40;
        int maxKeys = 50;   // n < maxKeys -> one non-truncated page
        List<byte[]> keys = contiguousKeys(n, 6);
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = RangeScanners.of(f, maxKeys, metrics, ReadaheadConfig.on());
        List<byte[]> emitted = new ArrayList<>();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, () -> TOP,
                null, (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().raw())));
        assertByteExactInOrder(emitted, keys, "single-page");
        assertThat(sumCategory(reasons(metrics), "READAHEAD")).isZero();
    }

    @Test
    @Timeout(60)
    void degenerateMaxKeysOneIsPureSerialExactlyOnce() throws Exception {
        int n = 400;
        List<byte[]> keys = contiguousKeys(n, 6);
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(1).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = RangeScanners.of(f, 1, metrics, ReadaheadConfig.on());
        List<byte[]> emitted = new ArrayList<>();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, () -> TOP,
                null, (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().raw())));
        assertByteExactInOrder(emitted, keys, "maxKeys=1");
        // A 1-key page has firstKey == lastKey, so the engage gate (firstKey <ᵤ lastKey) never trips.
        assertThat(reasons(metrics).getOrDefault("READAHEAD.engaged", 0L)).isZero();
    }

    @Test
    @Timeout(60)
    void degenerateGuessesPastEndOfRangeStayExactlyOnce() throws Exception {
        // Dense contiguous tail with hi far beyond the last key: readahead engages and fires guesses
        // that land PAST end-of-range (their fetches return empty pages, which absorb() discards).
        int n = 3000;
        int maxKeys = 40;
        List<byte[]> keys = contiguousKeys(n, 8);
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        RangeScanner scanner = RangeScanners.of(f, maxKeys, metrics, ReadaheadConfig.on());
        List<byte[]> emitted = new ArrayList<>();
        scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, null, () -> TOP,
                null, (batch, lastKey, completed) -> batch.forEach(e -> emitted.add(e.key().raw())));
        assertByteExactInOrder(emitted, keys, "guesses-past-end");
        assertThat(reasons(metrics).getOrDefault("READAHEAD.engaged", 0L)).isGreaterThan(0L);
    }

    // ================================================================================================
    // 3. RES — repeated crash/resume torture (union exactly-once at the committed-cursor seam).
    // ================================================================================================

    @Test
    @Timeout(120)
    void resRepeatedCrashResumeUnionExactlyOnce() throws Exception {
        int seeds = 24;
        for (int s = 0; s < seeds; s++) {
            runOneResSeed(s);
        }
    }

    private void runOneResSeed(long seed) throws Exception {
        SplittableRandom rng = new SplittableRandom(seed ^ 0x9E37_79B9L);
        int alphabet = rng.nextInt(4);
        int n = rng.nextInt(600, 1500);
        List<byte[]> keys = randomKeyspace(rng, n, alphabet);
        int size = keys.size();
        int maxKeys = rng.nextInt(8, 50);
        long latMicros = rng.nextInt(0, 1200);
        Duration lat = Duration.ofNanos(latMicros * 1_000L);

        List<byte[]> collected = new ArrayList<>(size);
        AtomicReference<byte[]> committed = new AtomicReference<>(null);   // durable last-emitted cursor
        int cycles = 0;
        boolean done = false;
        while (!done) {
            cycles++;
            assertThat(cycles).as("crash/resume cycles bounded (seed=" + seed + ")").isLessThan(400);

            MockPageFetcher f = MockPageFetcher.builder()
                    .keys(keys).maxKeysCap(maxKeys)
                    .latencyModel(LatencyModels.uniformFast(seed + cycles, lat, lat))
                    .build();
            RangeScanner scanner = RangeScanners.of(f, maxKeys,
                    new RunMetrics(new SimpleMeterRegistry()), ReadaheadConfig.on());

            // Crash after a few pages PAST engagement (>= engageAfterFullPages) so the kill lands while
            // readahead is engaged (guesses in-flight / mid-await under latency), the seam under test.
            int crashAfter = (3 + rng.nextInt(10)) * maxKeys + rng.nextInt(maxKeys);
            byte[] resumeFrom = committed.get();
            List<byte[]> cycleEmitted = new ArrayList<>();
            int[] newThisCycle = {0};
            try {
                scanner.runRange(NO_PREFIX, ListingMode.OBJECTS, resumeFrom, () -> TOP, null,
                        (batch, lastKey, completed2) -> {
                            // Commit-before-emit: the durable cursor advances to this page's lastKey,
                            // THEN the page's keys are emitted. A crash after this leaves no
                            // emitted-but-uncommitted key.
                            if (lastKey != null) {
                                committed.set(lastKey);
                            }
                            for (ListEntry e : batch) {
                                cycleEmitted.add(e.key().raw());
                                newThisCycle[0]++;
                            }
                            if (newThisCycle[0] >= crashAfter) {
                                throw new SimulatedCrash();
                            }
                        });
                done = true;   // ran to completion without a crash
            } catch (SimulatedCrash expected) {
                // resume next iteration from the durable committed cursor
            }
            // Everything emitted this cycle is <= committed (commit-before-emit), so nothing is lost.
            collected.addAll(cycleEmitted);
        }

        String ctx = "RES seed=" + seed + " alphabet=" + alphabet + " n=" + size + " maxKeys=" + maxKeys
                + " cycles=" + cycles;
        assertByteExactInOrder(collected, keys, ctx);
        assertStrictlyAscendingNoDup(collected, ctx);
    }

    private static final class SimulatedCrash extends RuntimeException {
    }

    // ================================================================================================
    // 4. Adversarial adoption edges (direct SpeculativeReadahead).
    // ================================================================================================

    /**
     * A buffered page whose whole content is at/behind the cursor (cursor advanced past its last key)
     * must be DISCARDED as {@code discarded_overlap}, never adopted — no already-emitted key is ever
     * handed back. Drives the buffer full of guesses, then jumps the cursor past the last real key.
     */
    @Test
    @Timeout(60)
    void adoptionFullyStaleBufferedPageIsDiscardedNeverAdopted() throws Exception {
        int n = 5000;
        int maxKeys = 50;
        int k = 8;
        List<byte[]> keys = contiguousKeys(n, 8);
        byte[] firstKey = keys.get(0);
        byte[] engageCursor = keys.get(maxKeys - 1);
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        try (SpeculativeReadahead ra = new SpeculativeReadahead(f, NO_PREFIX, ListingMode.OBJECTS,
                maxKeys, firstKey, engageCursor, TOP, k, k, metrics)) {
            ra.replenish(engageCursor);
            // Drain in-flight guesses into the buffer (they are all > engageCursor, so HELD not adopted).
            drainAllGuessesIntoBuffer(ra, engageCursor);
            assertThat(ra.bufferedPageCount()).as("guesses buffered ahead of the cursor").isGreaterThan(0);

            // Jump the cursor PAST the last real key: every buffered page's last key is now <= cursor.
            byte[] farCursor = keys.get(n - 1);
            SpeculativeReadahead.Adopted a = ra.tryNextPage(farCursor);

            assertThat(a).as("no fully-stale page is ever adopted").isNull();
            assertThat(reasons(metrics).getOrDefault("READAHEAD.discarded_overlap", 0L))
                    .as("the stale pages are counted discarded_overlap").isGreaterThan(0L);
            assertThat(ra.bufferedPageCount()).as("stale pages pruned from the buffer").isZero();
        }
    }

    /**
     * When two (or more) buffered guesses are both adoptable at the cursor ({@code guess <=ᵤ cursor},
     * last {@code > cursor}), the LARGEST guess wins ({@code floorKey}) and its page is handed back
     * trimmed strictly {@code > cursor} — advancing the cursor furthest, never re-emitting the cursor.
     */
    @Test
    @Timeout(60)
    void adoptionLargestGuessWinsWithBoundaryExclusiveTrim() throws Exception {
        int n = 5000;
        int maxKeys = 50;
        int k = 8;
        List<byte[]> keys = contiguousKeys(n, 8);
        byte[] firstKey = keys.get(0);
        byte[] engageCursor = keys.get(maxKeys - 1);
        MockPageFetcher f = MockPageFetcher.builder().keys(keys).maxKeysCap(maxKeys).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());

        // Replay SpeculativeReadahead's chained placement (StealMath.extrapolate is the same math the
        // impl uses) to learn the 2nd guess g2, so we can drive the cursor exactly to it.
        byte[] g1 = StealMath.extrapolate(firstKey, engageCursor, TOP);
        assertThat(g1).as("g1 placed").isNotNull();
        byte[] g2 = StealMath.extrapolate(engageCursor, g1, TOP);
        assertThat(g2).as("g2 placed").isNotNull();
        assertThat(KeyBytes.compareUnsigned(g2, g1)).as("guesses strictly ascend").isGreaterThan(0);

        try (SpeculativeReadahead ra = new SpeculativeReadahead(f, NO_PREFIX, ListingMode.OBJECTS,
                maxKeys, firstKey, engageCursor, TOP, k, k, metrics)) {
            ra.replenish(engageCursor);
            drainAllGuessesIntoBuffer(ra, engageCursor);   // drain all guesses into the buffer, none adoptable yet
            assertThat(ra.bufferedPageCount()).as("g1 and g2 both buffered").isGreaterThanOrEqualTo(2);

            // Cursor now sits exactly at g2: both g1 <ᵤ g2 and g2 are <= cursor. floorKey(cursor) picks
            // g2 (the largest adoptable guess), advancing furthest.
            SpeculativeReadahead.Adopted a = ra.tryNextPage(g2);
            assertThat(a).as("an adoptable page is returned at cursor g2").isNotNull();
            assertThat(a.entries()).isNotEmpty();

            byte[] firstAdopted = a.entries().getFirst().key().rawUnsafe();
            // Boundary-exclusive trim: every adopted key is strictly > the cursor (g2). If g1's page had
            // wrongly won, the first adopted key would fall at/below g2's region.
            for (ListEntry e : a.entries()) {
                assertThat(KeyBytes.compareUnsigned(e.key().rawUnsafe(), g2))
                        .as("adopted key strictly > cursor g2").isGreaterThan(0);
            }
            // It is g2's page: its first key is the smallest real key strictly greater than g2.
            byte[] expectedFirst = firstKeyStrictlyAbove(keys, g2);
            assertThat(Arrays.equals(firstAdopted, expectedFirst))
                    .as("adopted page is the largest guess (g2)'s page").isTrue();
            assertThat(reasons(metrics).getOrDefault("READAHEAD.adopted_page", 0L)).isGreaterThan(0L);
        }
    }

    // ================================================================================================
    // helpers
    // ================================================================================================

    /**
     * Drain every launched speculative guess into the buffer — i.e. wait until nothing is in-flight —
     * so the buffered-count SETUP below is deterministic. {@code tryNextPage(cursor)} is non-blocking
     * when no guess is adoptable at {@code cursor} (as here, where every guess sits ahead of it), so a
     * plain fast spin would blow through hundreds of iterations in microseconds and bet that the
     * speculative fetch virtual threads had ALREADY been scheduled — a bet that loses under cross-fork
     * CPU contention (schedule-sensitive), leaving guesses still in-flight and the
     * buffer short. This yields to those (CPU-starved) threads with a real 1ms sleep and a generous
     * 20s deadline (well under each caller's {@code @Timeout(60)}); it changes NO adoption-ordering
     * assertion — a genuine failure to ever buffer the guesses still surfaces on the caller's own
     * assertion after the deadline.
     */
    private static void drainAllGuessesIntoBuffer(SpeculativeReadahead ra, byte[] cursor) throws Exception {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (ra.inFlightCount() > 0 && System.nanoTime() < deadlineNanos) {
            ra.tryNextPage(cursor);   // move any COMPLETED guesses into the buffer (none adoptable at cursor)
            if (ra.inFlightCount() > 0) {
                Thread.sleep(1);      // yield to the possibly CPU-starved speculative fetch threads
            }
        }
    }

    private static byte[] firstKeyStrictlyAbove(List<byte[]> sortedKeys, byte[] bound) {
        for (byte[] k : sortedKeys) {
            if (KeyBytes.compareUnsigned(k, bound) > 0) {
                return k;
            }
        }
        throw new AssertionError("no key strictly above the bound in the keyspace");
    }

    private static Map<String, Long> reasons(RunMetrics metrics) {
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    private static long sumCategory(Map<String, Long> reasons, String category) {
        return reasons.entrySet().stream()
                .filter(e -> e.getKey().startsWith(category + "."))
                .mapToLong(Map.Entry::getValue).sum();
    }

    private static void assertByteExactInOrder(List<byte[]> actual, List<byte[]> expected, String ctx) {
        assertThat(actual.size()).as("emitted count == expected (" + ctx + ")").isEqualTo(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(Arrays.equals(actual.get(i), expected.get(i)))
                    .as("byte-exact key at index " + i + " (" + ctx + ")").isTrue();
        }
    }

    private static void assertStrictlyAscendingNoDup(List<byte[]> keys, String ctx) {
        for (int i = 1; i < keys.size(); i++) {
            int cmp = Arrays.compareUnsigned(keys.get(i - 1), keys.get(i));
            assertThat(cmp).as("strictly ascending, no duplicate at index " + i + " (" + ctx + ")")
                    .isLessThan(0);
        }
    }

    // ---- keyspace generators -----------------------------------------------------------------------

    /** n distinct fixed-width base-26 lowercase keys in ascending order (a contiguous alphabet). */
    private static List<byte[]> contiguousKeys(int n, int width) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            char[] c = new char[width];
            int v = i;
            for (int p = width - 1; p >= 0; p--) {
                c[p] = (char) ('a' + (v % 26));
                v /= 26;
            }
            keys.add(new String(c).getBytes(StandardCharsets.US_ASCII));
        }
        return keys;
    }

    /**
     * A random distinct keyspace over one of four alphabets, unsigned-sorted:
     * 0 = contiguous base-26; 1 = 12-nibble lowercase HEX (the {@code '9'(0x39) -> 'a'(0x61)} dead
     * zone); 2 = a long common prefix + a short base-26 suffix; 3 = unicode multi-byte (mixed 1/2/3-byte
     * UTF-8 code points).
     */
    private static List<byte[]> randomKeyspace(SplittableRandom rng, int n, int alphabet) {
        TreeSet<byte[]> set = new TreeSet<>(Arrays::compareUnsigned);
        int guard = 0;
        while (set.size() < n && guard++ < n * 64) {
            set.add(randomKey(rng, alphabet));
        }
        return new ArrayList<>(set);
    }

    private static byte[] randomKey(SplittableRandom rng, int alphabet) {
        switch (alphabet) {
            case 1 -> {
                char[] hex = "0123456789abcdef".toCharArray();
                char[] c = new char[12];
                for (int i = 0; i < c.length; i++) {
                    c[i] = hex[rng.nextInt(16)];
                }
                return new String(c).getBytes(StandardCharsets.US_ASCII);
            }
            case 2 -> {
                char[] c = new char[6];
                for (int i = 0; i < c.length; i++) {
                    c[i] = (char) ('a' + rng.nextInt(26));
                }
                return ("shared/long/common/prefix/segment/" + new String(c))
                        .getBytes(StandardCharsets.UTF_8);
            }
            case 3 -> {
                // Mixed-width UTF-8: ASCII (1 byte), Latin-1 supplement (2 bytes), CJK (3 bytes).
                int[] palette = {0x0061, 0x0062, 0x00E9, 0x00FC, 0x00C0, 0x4E2D, 0x6587, 0x00DF};
                StringBuilder sb = new StringBuilder();
                int len = 4 + rng.nextInt(4);
                for (int i = 0; i < len; i++) {
                    sb.appendCodePoint(palette[rng.nextInt(palette.length)]);
                }
                return sb.toString().getBytes(StandardCharsets.UTF_8);
            }
            default -> {
                char[] c = new char[8];
                for (int i = 0; i < c.length; i++) {
                    c[i] = (char) ('a' + rng.nextInt(26));
                }
                return new String(c).getBytes(StandardCharsets.US_ASCII);
            }
        }
    }
}
