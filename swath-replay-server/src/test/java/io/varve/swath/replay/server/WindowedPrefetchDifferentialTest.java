/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ObjectEntry;
import io.varve.swath.replay.fixture.FixtureMetrics;
import io.varve.swath.replay.fixture.SortedFixtures;
import io.varve.swath.replay.fixture.SortedFixtures.IndexEntry;
import io.varve.swath.replay.fixture.SortedFixtures.IndexLoadResult;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.store.ListingStore;
import io.varve.swath.replay.store.Projection;
import io.varve.swath.replay.store.SortedParquetStore;
import io.varve.swath.replay.store.WindowedListingStore;
import io.varve.swath.replay.testkit.HttpProbe;
import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import io.varve.swath.sort.CaptureSorter;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A differential guard for the sequential-window prefetch decorator ({@link WindowedListingStore}).
 * The correctness bar is BYTE-IDENTICAL serving — the wrapped store
 * must be observationally indistinguishable from the bare {@link SortedParquetStore} for every call
 * pattern the pager produces, so a wrong prefetch (a too-short window short-serving a page, a stale
 * or mis-keyed window bleeding a neighbour's rows, a botched {@code fromInclusive} boundary, a
 * projection leak, an eager/LRU eviction that drops a live window) surfaces as a divergence
 * attributable to the decorator by construction.
 *
 * <p>Three layers, all against the REAL sorted Parquet store (no S3, sized-down, seeded):
 * <ol>
 *   <li><b>Pager-level small-window churn</b> ({@link #smallWindowChurnWalksIdenticallyPrefetchOnVsOff})
 *       — the full {@code pager + SortedParquetStore + ListObjectsV2Pager} HTTP walk with prefetch
 *       forced to a tiny window ({@code window-rows=50}, {@code max-windows=2}) so non-final windows,
 *       refills, LRU eviction and eager eviction all churn — asserted byte-identical against the same
 *       walk with prefetch DISABLED. A {@code max-keys=1000} scenario forces {@code limit > window-rows},
 *       exercising the {@code max(windowRows, limit)} fill guard end-to-end.</li>
 *   <li><b>Adversarial store-level call patterns</b> — token re-use, {@code start-after} landing at
 *       exact window boundaries, interleaved prefix/delimiter reads (distinct {@code toExclusive} /
 *       projection — no cross-contamination), maxKeys=1 walks (maximum hit-path), and two interleaved
 *       walks at {@code max-windows=1} (100% window thrash).</li>
 *   <li><b>Property-style sweep</b> ({@link #propertySweepWrappedEqualsBareOverAdversarialKeys}) —
 *       thousands of seeded random probes over an adversarial keyspace (shared prefixes, 1-byte keys,
 *       last-byte twins, a key that is a prefix of another), against a window SMALLER than the fixture
 *       (churn) and LARGER (single final window).</li>
 * </ol>
 */
class WindowedPrefetchDifferentialTest {

    private static final String ENABLED = "swath.replay.prefetch.enabled";
    private static final String WINDOW_ROWS = "swath.replay.prefetch.window-rows";
    private static final String MAX_WINDOWS = "swath.replay.prefetch.max-windows";

    /** Tiny final row groups so a few hundred keys form many row groups — the multi-RG path is real. */
    private static SortConfig manySmallGroups() {
        return SortConfigs.manySmallRowGroups();
    }

    // ------------------------------------------------------------------ Part 1: pager-level churn

    @Test
    void smallWindowChurnWalksIdenticallyPrefetchOnVsOff(@TempDir Path dir) throws Exception {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            keys.add(utf8(String.format("big/%04d", i)));   // one giant shared prefix spanning many windows
        }
        for (int a = 0; a < 3; a++) {
            for (int b = 0; b < 4; b++) {
                keys.add(utf8(String.format("d/a%d/b%d.txt", a, b)));   // delimiter'd hierarchy
            }
        }
        for (int n = 0; n < 8; n++) {
            keys.add(utf8("flat-" + n));
        }
        Path fixture = writeSortedFixture(dir, keys);

        List<Scenario> scenarios = new ArrayList<>();
        // Full flat walk at tiny max-keys forces per-window refills; max-keys=1000 forces limit>window-rows.
        scenarios.addAll(matrix(null, null, null, new int[]{1, 2, 3, 7, 1000}));
        // Giant prefix walk straddles window boundaries at every page size.
        scenarios.addAll(matrix("big/", null, null, new int[]{2, 3, 7}));
        // Delimiter rollup and start-after resume (distinct toExclusive keys — no cross-contamination).
        scenarios.addAll(matrix(null, "/", null, new int[]{2, 3}));
        scenarios.addAll(matrix("big/", null, "big/0100", new int[]{3}));
        scenarios.addAll(matrix(null, null, "big/0150", new int[]{2}));

        Map<String, String> saved = savePrefetchProps();
        try {
            System.setProperty(ENABLED, "true");
            System.setProperty(WINDOW_ROWS, "50");
            System.setProperty(MAX_WINDOWS, "2");
            try (ReplayServer prefetch = new ReplayServer(
                    "127.0.0.1", 0, "bucket", fixture, 2, ServingMode.SORTED)) {
                System.setProperty(ENABLED, "false");
                try (ReplayServer bare = new ReplayServer(
                        "127.0.0.1", 0, "bucket", fixture, 2, ServingMode.SORTED)) {
                    restorePrefetchProps(saved);   // config is captured at construction; safe to restore now
                    prefetch.start();
                    bare.start();
                    assertThat(prefetch.resolvedServingMode()).isEqualTo(ServingMode.SORTED);
                    assertThat(bare.resolvedServingMode()).isEqualTo(ServingMode.SORTED);

                    HttpClient client = HttpClient.newHttpClient();
                    for (Scenario scenario : scenarios) {
                        assertThat(walk(prefetch, client, scenario))
                                .as("prefetch-on vs prefetch-off differ for %s", describe(scenario))
                                .isEqualTo(walk(bare, client, scenario));
                    }
                }
            }
        } finally {
            restorePrefetchProps(saved);
        }
    }

    @Test
    void cachedContinuationBypassesDeterministicallySaturatedColdBacklog() throws Exception {
        ReplayMetrics metrics = new ReplayMetrics();
        BlockingBackingStore backing = new BlockingBackingStore(500, 2, metrics);
        WindowedListingStore windowed = new WindowedListingStore(backing, metrics, 50, 8);

        // Establish a reusable continuation window before arming the backing-reader blockade:
        // cold 10, then an anchored 40-row fill, leaving keys 20..49 cached.
        windowed.rows(null, true, null, 10, Projection.KEYS_ONLY);
        windowed.rows(key("key-009"), false, null, 10, Projection.KEYS_ONLY);
        long claimedBeforeBacklog = counter(metrics, "swath.replay.prefetch.anchor", "event", "claimed");
        backing.blockReads();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<List<ListedObject>>> cold = new ArrayList<>();
            for (int start : new int[]{100, 200, 300, 400}) {
                cold.add(executor.submit(() -> windowed.rows(
                        key(String.format("key-%03d", start)), false, null, 5, Projection.KEYS_ONLY)));
            }

            assertThat(backing.awaitSaturatedBacklog())
                    .as("two reads active and two older cold reads queued behind the backing pool")
                    .isTrue();

            Future<List<ListedObject>> continuation = executor.submit(() ->
                    windowed.rows(key("key-019"), false, null, 10, Projection.KEYS_ONLY));
            assertThat(continuation.get(5, TimeUnit.SECONDS))
                    .extracting(row -> new String(row.key(), StandardCharsets.UTF_8))
                    .containsExactly("key-020", "key-021", "key-022", "key-023", "key-024",
                            "key-025", "key-026", "key-027", "key-028", "key-029");

            assertThat(backing.queuedReads())
                    .as("the continuation completed while older cold work was still queued")
                    .isEqualTo(2);
            assertThat(cold).allMatch(future -> !future.isDone());
            assertThat(counter(metrics, "swath.replay.prefetch.window.hit", null, null)).isEqualTo(1L);
            assertThat(counter(metrics, "swath.replay.prefetch.anchor", "event", "claimed"))
                    .isEqualTo(claimedBeforeBacklog);
            assertThat(metrics.registry().find("swath.replay.parquet.queries.peak").gauge().value())
                    .as("cache-first admission preserves the two-reader backing bound")
                    .isEqualTo(2.0);
            assertThat(backing.peakActiveReads()).isEqualTo(2);

            backing.releaseReads();
            for (Future<List<ListedObject>> future : cold) {
                assertThat(future.get(5, TimeUnit.SECONDS)).hasSize(5);
            }
        } finally {
            backing.releaseReads();
        }
    }

    // -------------------------------------------------- Part 2: adversarial store-level call patterns

    @Test
    void tokenReuseServesIdentically(@TempDir Path dir) throws IOException {
        try (Stores s = buildStores(dir, sequentialKeys(500), 64, 4)) {
            // Replay the SAME (from, toExclusive, limit, projection) call twice: the second is likely a
            // window hit; it must equal the first serve AND the bare store, byte for byte.
            ByteKey from = key(s.sortedKeys.get(120));
            for (int rep = 0; rep < 3; rep++) {
                assertIdentical(s, from, false, null, 25, Projection.KEYS_ONLY, "token-reuse rep " + rep);
            }
            // A re-used continuation token that also lands on a delegate-final tail.
            ByteKey nearEnd = key(s.sortedKeys.get(470));
            for (int rep = 0; rep < 3; rep++) {
                assertIdentical(s, nearEnd, false, null, 100, Projection.KEYS_ONLY, "final-tail reuse rep " + rep);
            }
        }
    }

    @Test
    void startAfterBoundariesServeIdentically(@TempDir Path dir) throws IOException {
        List<String> keys = sequentialKeys(500);
        try (Stores s = buildStores(dir, keys, 64, 4)) {
            // Seed a non-final window over rows [0, 64) keyed toExclusive=null.
            assertIdentical(s, null, true, null, 10, Projection.KEYS_ONLY, "seed");
            // Probe boundaries relative to that 64-row window: window start (index 0), interior,
            // last buffered row (63), first row past the window (64), each inclusive and exclusive,
            // at limits that both fit inside the window and overrun it (forcing refills).
            int[] boundaries = {0, 1, 31, 62, 63, 64, 65};
            int[] limits = {1, 5, 10, 64, 80};
            for (int idx : boundaries) {
                ByteKey from = key(keys.get(idx));
                for (boolean incl : new boolean[]{true, false}) {
                    for (int limit : limits) {
                        assertIdentical(s, from, incl, null, limit, Projection.KEYS_ONLY,
                                "boundary idx=" + idx + " incl=" + incl + " limit=" + limit);
                    }
                }
            }
        }
        // A window that IS delegate-final (window-rows > fixture): every serve is a short-tail hit.
        try (Stores s = buildStores(dir.resolve("final"), keys, 5_000, 4)) {
            assertIdentical(s, null, true, null, 10, Projection.KEYS_ONLY, "seed-final");
            for (int idx = 490; idx < 500; idx++) {
                ByteKey from = key(keys.get(idx));
                assertIdentical(s, from, false, null, 50, Projection.KEYS_ONLY, "final tail idx=" + idx);
                assertIdentical(s, from, true, null, 50, Projection.KEYS_ONLY, "final tail incl idx=" + idx);
            }
        }
    }

    @Test
    void interleavedRangesAndProjectionsDoNotCrossContaminate(@TempDir Path dir) throws IOException {
        List<String> keys = sequentialKeys(600);
        try (Stores s = buildStores(dir, keys, 40, 8)) {
            // Two concurrent "listings" over the same store with DISTINCT toExclusive bounds (as a
            // prefix walk and a delimiter listing produce) interleaved page-by-page, plus a third that
            // differs only in PROJECTION — a window must never serve a neighbour keyed by a different
            // (toExclusive, projection). Every call is checked against the bare store.
            ByteKey boundA = key(keys.get(300));
            ByteKey boundB = key(keys.get(450));
            ByteKey a = null;
            ByteKey b = key(keys.get(150));
            ByteKey c = null;   // same open toExclusive as A but WITH_OWNER projection
            for (int step = 0; step < 40; step++) {
                List<ListedObject> pa = assertIdentical(s, a, false, boundA, 7, Projection.KEYS_ONLY, "walkA step " + step);
                List<ListedObject> pb = assertIdentical(s, b, false, boundB, 5, Projection.KEYS_ONLY, "walkB step " + step);
                List<ListedObject> pc = assertIdentical(s, c, false, boundA, 7, Projection.WITH_OWNER, "walkC step " + step);
                a = advance(pa, a);
                b = advance(pb, b);
                c = advance(pc, c);
            }
        }
    }

    @Test
    void maxKeysOneWalkServesIdentically(@TempDir Path dir) throws IOException {
        List<String> keys = sequentialKeys(400);
        try (Stores s = buildStores(dir, keys, 32, 2)) {
            // maxKeys=1 ⇒ the pager reads limit=2 (lookahead) and advances one row per page: maximum
            // page count, maximum hit path, a refill exactly at every window boundary.
            for (int limit : new int[]{1, 2}) {
                ByteKey from = null;
                int guard = 0;
                while (guard++ < 100_000) {
                    List<ListedObject> page = assertIdentical(s, from, false, null, limit,
                            Projection.KEYS_ONLY, "maxKeys=1 walk limit=" + limit + " guard=" + guard);
                    if (page.isEmpty()) {
                        break;
                    }
                    from = key(page.getFirst().key());   // advance by the single emitted row
                }
                assertThat(guard).as("walk terminated").isLessThan(100_000);
            }
        }
    }

    @Test
    void twoInterleavedWalksAtMaxWindowsOneSurviveTotalThrash(@TempDir Path dir) throws IOException {
        List<String> keys = sequentialKeys(500);
        try (Stores s = buildStores(dir, keys, 40, 1)) {
            // max-windows=1: two independent walks at different positions (same toExclusive=null) evict
            // each other's window on every alternate call ⇒ ~100% miss. Correctness must survive it.
            ByteKey x = null;
            ByteKey y = key(keys.get(250));
            for (int step = 0; step < 60; step++) {
                List<ListedObject> px = assertIdentical(s, x, false, null, 9, Projection.KEYS_ONLY, "thrash X step " + step);
                List<ListedObject> py = assertIdentical(s, y, false, null, 9, Projection.KEYS_ONLY, "thrash Y step " + step);
                x = advance(px, x);
                y = advance(py, y);
            }
        }
    }

    // --------------------------------------------------------------- Part 3: seeded property sweep

    @Test
    void propertySweepWrappedEqualsBareOverAdversarialKeys(@TempDir Path dir) throws IOException {
        List<String> keys = adversarialKeys(2_800, new Random(0xB0FFL));
        // Churn: window smaller than the fixture. Also a window larger than the fixture (single final
        // window). Probes reuse ByteKeys drawn from real keys AND off-key random bytes.
        sweep(dir.resolve("churn"), keys, 37, 3, new Random(0x5EED_0001L), 900);
        sweep(dir.resolve("single"), keys, 50_000, 8, new Random(0x5EED_0002L), 500);
    }

    private void sweep(Path dir, List<String> keys, int windowRows, int maxWindows, Random rng, int probes)
            throws IOException {
        try (Stores s = buildStores(dir, keys, windowRows, maxWindows)) {
            List<String> sorted = s.sortedKeys;
            for (int p = 0; p < probes; p++) {
                ByteKey from = randomBound(rng, sorted, 0.12);
                ByteKey to = randomBound(rng, sorted, 0.60);
                // Draw the two bounds independently and DO NOT normalise them: roughly half of the
                // both-bound probes come out inverted (from >= toExclusive). Those must serve an empty
                // page — exactly the edge we want to keep exercising. assertIdentical proves wrapped ==
                // bare; the extra check below pins the actual contract (inverted/empty range ⇒ no rows),
                // not merely mutual agreement, so a decorator that agreed by both being wrong would fail.
                boolean inclusive = rng.nextBoolean();
                int limit = 1 + rng.nextInt(300);   // spans below AND above window-rows (churn config)
                Projection projection = rng.nextBoolean() ? Projection.WITH_OWNER : Projection.KEYS_ONLY;
                List<ListedObject> page = assertIdentical(s, from, inclusive, to, limit, projection,
                        "sweep probe " + p + " windowRows=" + windowRows);
                if (from != null && to != null && from.compareTo(to) >= 0) {
                    assertThat(page)
                            .as("inverted/empty range (from >= toExclusive) must serve no rows @ probe %d", p)
                            .isEmpty();
                }
            }
        }
    }

    // ------------------------------------------------------------------------------- store harness

    private record Stores(WindowedListingStore wrapped, ListingStore bare, List<String> sortedKeys)
            implements AutoCloseable {
        @Override
        public void close() {
            wrapped.close();
            bare.close();
        }
    }

    private static Stores buildStores(Path dir, List<String> keys, int windowRows, int maxWindows)
            throws IOException {
        Fixture fx = writeSorted(dir, keys);
        SortedParquetStore backing = new SortedParquetStore(fx.files(), fx.index(), new ReplayMetrics(), 2);
        WindowedListingStore wrapped =
                new WindowedListingStore(backing, new ReplayMetrics(), windowRows, maxWindows);
        SortedParquetStore bare = new SortedParquetStore(fx.files(), fx.index(), new ReplayMetrics(), 2);
        List<String> sorted = new ArrayList<>(new TreeSet<>(keys));
        return new Stores(wrapped, bare, sorted);
    }

    /** Serves the same request from wrapped + bare, asserts byte-for-byte identity, returns the page. */
    private static List<ListedObject> assertIdentical(Stores s, ByteKey from, boolean inclusive, ByteKey to,
                                                      int limit, Projection projection, String ctx) {
        List<ListedObject> wrapped = s.wrapped.rows(from, inclusive, to, limit, projection);
        List<ListedObject> bare = s.bare.rows(from, inclusive, to, limit, projection);
        assertThat(encode(wrapped))
                .as("wrapped prefetch store diverged from bare store @ %s", ctx)
                .isEqualTo(encode(bare));
        return wrapped;
    }

    private static ByteKey advance(List<ListedObject> page, ByteKey previous) {
        return page.isEmpty() ? previous : key(page.getLast().key());
    }

    /** Full-fidelity per-row encoding: every projected field, keys as hex so raw bytes survive. */
    private static List<String> encode(List<ListedObject> rows) {
        List<String> out = new ArrayList<>(rows.size());
        for (ListedObject r : rows) {
            out.add(hex(r.key()) + "|" + r.size() + "|" + r.lastModifiedEpochMicros() + "|" + r.etag()
                    + "|" + r.storageClass() + "|" + r.ownerId() + "|" + r.ownerDisplayName()
                    + "|" + r.checksumAlgorithm() + "|" + r.checksumType());
        }
        return out;
    }

    private static ByteKey randomBound(Random rng, List<String> sorted, double nullProbability) {
        double roll = rng.nextDouble();
        if (roll < nullProbability) {
            return null;                                    // open bound
        }
        if (roll < nullProbability + 0.15) {               // off-key random bytes (uncovered positions)
            int len = 1 + rng.nextInt(10);
            byte[] raw = new byte[len];
            rng.nextBytes(raw);
            return ByteKey.copyOf(raw);
        }
        String base = sorted.get(rng.nextInt(sorted.size()));
        byte[] bytes = base.getBytes(StandardCharsets.UTF_8);
        int mut = rng.nextInt(4);
        if (mut == 1 && bytes.length > 0) {               // truncate → lands strictly before a real key
            bytes = Arrays.copyOf(bytes, rng.nextInt(bytes.length + 1));
        } else if (mut == 2) {                            // append → lands strictly after a real key
            byte[] longer = Arrays.copyOf(bytes, bytes.length + 1);
            longer[bytes.length] = (byte) rng.nextInt(256);
            bytes = longer;
        }
        return ByteKey.copyOf(bytes);
    }

    // --------------------------------------------------------------------------- fixture builders

    private record Fixture(List<Path> files, List<IndexEntry> index) {
    }

    private static Fixture writeSorted(Path dir, List<String> keys) throws IOException {
        Files.createDirectories(dir);
        Path capture = Files.createDirectories(dir.resolve("capture"));
        List<String> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(42));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (String k : shuffled) {
                writer.write(object(k));
            }
        }
        Path out = Files.createDirectories(dir.resolve("sorted"));
        new CaptureSorter(manySmallGroups()).sort(capture, out);
        List<Path> files = SortedFixtures.resolveFiles(out);
        IndexLoadResult result = SortedFixtures.loadIndex(files, new FixtureMetrics());
        return new Fixture(files, ((IndexLoadResult.Loaded) result).entries());
    }

    /** Sorted-fixture path for the HTTP servers (Part 1), built the same way as the differential test. */
    private static Path writeSortedFixture(Path dir, List<byte[]> keys) throws IOException {
        Path capture = Files.createDirectories(dir.resolve("capture"));
        List<byte[]> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, new Random(1234));
        try (var writer = ParquetFixtures.open(capture.resolve("part-0.parquet"))) {
            for (byte[] k : shuffled) {
                writer.write(object(k));
            }
        }
        Path out = Files.createDirectories(dir.resolve("sorted"));
        new CaptureSorter(manySmallGroups()).sort(capture, out);
        return out;
    }

    private static List<String> sequentialKeys(int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format("key-%05d", i));
        }
        return keys;
    }

    /** Adversarial keyspace: shared prefixes, 1-byte keys, last-byte twins, prefix-of-another. */
    private static List<String> adversarialKeys(int target, Random rng) {
        TreeSet<String> keys = new TreeSet<>();
        for (char c = 'a'; c <= 'z'; c++) {
            keys.add(String.valueOf(c));                    // 1-byte keys
        }
        // A key that is a prefix of another (and of several), plus near neighbours.
        Collections.addAll(keys, "pre", "pre0", "pre00", "prefix", "prefixed");
        int i = 0;
        while (keys.size() < target) {
            int bucket = i % 3;
            switch (bucket) {
                case 0 -> keys.add(String.format("shared/prefix/%06d", i));   // long shared prefix
                case 1 -> {                                                    // last-byte twins
                    String stem = String.format("twin-%05d-", i);
                    keys.add(stem + "a");
                    keys.add(stem + "b");
                }
                default -> {                                                   // random-ish ascii spread
                    StringBuilder sb = new StringBuilder("r/");
                    int len = 1 + rng.nextInt(6);
                    for (int j = 0; j < len; j++) {
                        sb.append((char) ('0' + rng.nextInt(10)));
                    }
                    sb.append('/').append(i);
                    keys.add(sb.toString());
                }
            }
            i++;
        }
        return new ArrayList<>(keys);
    }

    private static ObjectEntry object(String key) {
        return object(key.getBytes(StandardCharsets.UTF_8));
    }

    private static ObjectEntry object(byte[] key) {
        return ObjectEntries.withOwner(key, "etag");
    }

    // ---------------------------------------------------------------------- HTTP walk harness (Part 1)

    private record Scenario(String prefix, String delimiter, String startAfter, int maxKeys, boolean fetchOwner) {
    }

    private static List<Scenario> matrix(String prefix, String delimiter, String startAfter, int[] maxKeysValues) {
        List<Scenario> scenarios = new ArrayList<>();
        for (int maxKeys : maxKeysValues) {
            for (boolean fetchOwner : new boolean[]{true, false}) {
                scenarios.add(new Scenario(prefix, delimiter, startAfter, maxKeys, fetchOwner));
            }
        }
        return scenarios;
    }

    private static List<String> walk(ReplayServer server, HttpClient client, Scenario scenario)
            throws Exception {
        List<String> pages = new ArrayList<>();
        String token = null;
        for (int guard = 0; guard < 100_000; guard++) {
            StringBuilder q = new StringBuilder("/bucket?list-type=2&encoding-type=url&max-keys=")
                    .append(scenario.maxKeys());
            if (scenario.fetchOwner()) {
                q.append("&fetch-owner=true");
            }
            if (scenario.prefix() != null) {
                q.append("&prefix=").append(HttpProbe.percentEncode(scenario.prefix()));
            }
            if (scenario.delimiter() != null) {
                q.append("&delimiter=").append(HttpProbe.percentEncode(scenario.delimiter()));
            }
            if (token != null) {
                q.append("&continuation-token=").append(HttpProbe.percentEncode(token));
            } else if (scenario.startAfter() != null) {
                q.append("&start-after=").append(HttpProbe.percentEncode(scenario.startAfter()));
            }
            String body = HttpProbe.body(server, client, q.toString());
            pages.add(body);
            if (!"true".equals(HttpProbe.extractTag(body, "IsTruncated"))) {
                return pages;
            }
            token = HttpProbe.extractTag(body, "NextContinuationToken");
            assertThat(token).as("a truncated page must carry a continuation token").isNotNull();
        }
        throw new AssertionError("listing did not terminate for " + describe(scenario));
    }

    private static String describe(Scenario s) {
        return "prefix=" + n(s.prefix()) + " delimiter=" + n(s.delimiter()) + " startAfter=" + n(s.startAfter())
                + " maxKeys=" + s.maxKeys() + " fetchOwner=" + s.fetchOwner();
    }

    private static String n(String v) {
        return v == null ? "-" : v;
    }

    private static long counter(ReplayMetrics metrics, String name, String tagName, String tagValue) {
        var search = metrics.registry().find(name);
        return Math.round(tagName == null ? search.counter().count() : search.tag(tagName, tagValue).counter().count());
    }

    /**
     * A deterministic stand-in for the two-slot sorted backing pool. Calls enter in FIFO order;
     * once armed, two own slots and two older calls park at the semaphore until the test releases
     * them. Metrics are bumped only after acquisition, matching the real reader callbacks.
     */
    private static final class BlockingBackingStore implements ListingStore {
        private final io.varve.swath.replay.testkit.FakeListingStore rows;
        private final Semaphore permits;
        private final ReplayMetrics metrics;
        private final CountDownLatch saturatedBacklog = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean blocking = new AtomicBoolean();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final AtomicInteger queued = new AtomicInteger();

        BlockingBackingStore(int rowCount, int readers, ReplayMetrics metrics) {
            List<ListedObject> all = new ArrayList<>(rowCount);
            for (int i = 0; i < rowCount; i++) {
                all.add(new ListedObject(String.format("key-%03d", i).getBytes(StandardCharsets.UTF_8),
                        i, i, "etag", "STANDARD", null, null, null, null));
            }
            this.rows = new io.varve.swath.replay.testkit.FakeListingStore(all);
            this.permits = new Semaphore(readers, true);
            this.metrics = metrics;
        }

        void blockReads() {
            blocking.set(true);
        }

        boolean awaitSaturatedBacklog() throws InterruptedException {
            return saturatedBacklog.await(5, TimeUnit.SECONDS);
        }

        int queuedReads() {
            return queued.get();
        }

        int peakActiveReads() {
            return peak.get();
        }

        void releaseReads() {
            release.countDown();
        }

        @Override
        public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                       Projection projection) {
            boolean acquired = false;
            boolean countedQueued = false;
            if (blocking.get()) {
                acquired = permits.tryAcquire();
                if (!acquired) {
                    countedQueued = true;
                    if (queued.incrementAndGet() == 2 && active.get() == 2) {
                        saturatedBacklog.countDown();
                    }
                }
            }
            if (!acquired) {
                acquire();
            }
            if (countedQueued) {
                queued.decrementAndGet();
            }
            metrics.parquetReaderAcquired();
            int now = active.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            try {
                if (blocking.get()) {
                    if (now == 2 && queued.get() >= 2) {
                        saturatedBacklog.countDown();
                    }
                    awaitRelease();
                }
                return rows.rows(from, fromInclusive, toExclusive, limit, projection);
            } finally {
                active.decrementAndGet();
                metrics.parquetReaderReleased();
                permits.release();
            }
        }

        private void acquire() {
            try {
                permits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for fake backing reader", e);
            }
        }

        private void awaitRelease() {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted in fake backing read", e);
            }
        }

        @Override
        public void close() {
        }
    }

    // ---------------------------------------------------------------------------------- small utils

    private static ByteKey key(String value) {
        return ByteKey.copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteKey key(byte[] value) {
        return ByteKey.copyOf(value);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            out.append(Character.forDigit((b >>> 4) & 0xF, 16));
            out.append(Character.forDigit(b & 0xF, 16));
        }
        return out.toString();
    }

    private static Map<String, String> savePrefetchProps() {
        Map<String, String> saved = new HashMap<>();
        for (String key : new String[]{ENABLED, WINDOW_ROWS, MAX_WINDOWS}) {
            saved.put(key, System.getProperty(key));
        }
        return saved;
    }

    private static void restorePrefetchProps(Map<String, String> saved) {
        for (Map.Entry<String, String> e : saved.entrySet()) {
            if (e.getValue() == null) {
                System.clearProperty(e.getKey());
            } else {
                System.setProperty(e.getKey(), e.getValue());
            }
        }
    }
}
