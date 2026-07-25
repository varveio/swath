/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.RangePartition;
import io.varve.swath.testkit.RangeScanners;
import io.varve.swath.testkit.RecordingSplitStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * PROP-3 — <b>retry-nearer-cursor on a band wider than its content</b> (algorithms.md §3/§3.1, I4).
 * A victim band {@code (lo, H]} whose <b>content is concentrated in a dense sub-window near
 * {@code lo}</b>
 * with a large <b>empty upper gap</b> up to {@code H} (the shallow-tree + flat-filenames
 * shape) forces the very first steal's naive {@code byteMidpoint} pivot <b>into the empty
 * gap</b>: {@code probe(m, H)} returns nothing. The engine must then <b>retry nearer the
 * cursor</b> — bisecting the lower half {@code (cursor, m]} — and <b>converge into the dense
 * sub-window in {@code O(log(band width))} probes</b>, committing a <b>real</b> split, and
 * <b>without</b> caching the victim {@code unsplittable} (the empty-upper retry path explicitly
 * does not cache — algorithms.md §3). The resulting range set still tiles {@code (lo, H]} (I4).
 *
 * <p>Distinct cause from PROP-1's dense-head skew: here the <i>upper</i> region is empty
 * (band wider than content), not the lower region dense.
 *
 * <p><b>How this is made adversarial.</b> Every probe is the one speculative
 * {@code ListObjectsV2(..., max_keys=1)} call, so a {@link ProbeRecorder} interceptor counts
 * exactly the {@code max_keys == 1} fetches and whether each returned anything. An "empty"
 * probe (zero entries) is one retry iteration. We assert the number of empty probes is
 * <b>logarithmic</b> in the band's code-point width — a regression that linearly scans the gap
 * (one probe per code point) would do thousands and blow the bound, and a regression that
 * gives up and marks the band {@code unsplittable} would never reach {@link Thief.Outcome#CHILD_CREATED}.
 */
final class Prop3BandWiderThanContentTest {

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "prop3-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A recorded probe: its {@code start_after} pivot and whether it returned no key at all. */
    private record Probe(byte[] startAfter, boolean empty) {
    }

    /**
     * Records the engine's speculative 1-key probes (algorithms.md §3). Driving the {@link Thief}
     * directly (no surrounding page scans), the <b>only</b> {@code fetchPage} calls are probes, so
     * every {@code max_keys == 1} request is one probe; a zero-entry result is an "empty" probe =
     * one retry-nearer-cursor iteration. Thread-safe (the steal runs on the caller thread, but the
     * recorder is defensive).
     */
    private static final class ProbeRecorder implements MockPageFetcher.PageInterceptor {
        private final List<Probe> probes = Collections.synchronizedList(new ArrayList<>());

        @Override
        public ListPage intercept(PageRequest req, int callIndex, ListPage computed) {
            if (req.maxKeys() == 1) {
                probes.add(new Probe(req.startAfter(), computed.entries().isEmpty()));
            }
            return computed;
        }

        List<Probe> snapshot() {
            synchronized (probes) {
                return List.copyOf(probes);
            }
        }

        long emptyProbes() {
            return snapshot().stream().filter(Probe::empty).count();
        }
    }

    // -------------------------------------------------------------------------
    // (1) The faithful real-world shape: band (data/jpeg/, data/ocr/] whose keys only fill
    //     data/jpeg/…  The naive midpoint of (cursor, hi) differs at 'j' vs 'o' → pivot ≈
    //     data/l…, which sits in the empty space between the jpeg band and the (excluded) ocr
    //     band. probe(data/l…) is empty; retry-nearer-cursor bisects back into data/jpeg/… and
    //     commits a real split, never caching the band unsplittable.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void commonscreensBandFirstStealRetriesIntoDenseWindow(@TempDir Path dir) throws Exception {
        // Dense sub-window: 8 host-derived jpeg filenames, all under data/jpeg/.  Empty gap above
        // them up to H = data/ocr/ (the next seed cut-point; ocr keys are > H, excluded by <= H).
        byte[] lo = utf8("data/jpeg/");
        byte[] H = utf8("data/ocr/");
        byte[] cursor = utf8("data/jpeg/aaa.jpeg");   // the worker has emitted its first key
        List<byte[]> keyspace = List.of(
                utf8("data/jpeg/aaa.jpeg"), utf8("data/jpeg/bbb.jpeg"), utf8("data/jpeg/ccc.jpeg"),
                utf8("data/jpeg/ddd.jpeg"), utf8("data/jpeg/eee.jpeg"), utf8("data/jpeg/fff.jpeg"),
                utf8("data/jpeg/ggg.jpeg"), utf8("data/jpeg/hhh.jpeg"));

        // The naive first pivot DOES land in the empty upper gap (no key in (firstPivot, H]).
        byte[] firstPivot = ByteMidpoint.between(cursor, H);
        assertThat(probeWouldBeEmpty(firstPivot, H, keyspace))
                .as("first naive midpoint must land in the empty upper gap (band wider than content)")
                .isTrue();
        // …and it is genuinely above the dense jpeg window (i.e. the upper region really is empty).
        assertThat(KeyBytes.compareUnsigned(firstPivot, utf8("data/k")))
                .as("first pivot sits past the jpeg band, in the gap").isGreaterThanOrEqualTo(0);

        StealResult r = runOneSteal(dir, lo, cursor, H, keyspace);

        // Converged into the dense sub-window with a REAL split — not unsplittable, not given up.
        assertThat(r.outcome()).as("retry-nearer-cursor converges to a committed split")
                .isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(r.victimUnsplittable())
                .as("empty-upper retry must NOT cache the band unsplittable (algorithms.md §3)")
                .isFalse();

        // The first probe was the empty-gap one; convergence was logarithmic (here a couple of
        // empty probes), nowhere near a linear scan of the gap.
        List<Probe> probes = r.probes();
        assertThat(probes).as("at least one probe was issued").isNotEmpty();
        assertThat(probes.getFirst().startAfter()).as("first probe is the naive gap pivot").isEqualTo(firstPivot);
        assertThat(probes.getFirst().empty()).as("first probe found nothing (upper gap)").isTrue();
        assertThat(probes.getLast().empty()).as("the converging probe found a key").isFalse();
        assertThat(r.emptyProbes()).as("retry converges in a few empty probes, not a gap scan")
                .isBetween(1L, 6L);

        // A real split landing INSIDE the dense jpeg region (below the gap), child non-empty.
        RangePartition.Split split = onlySplit(r);
        assertThat(KeyBytes.compareUnsigned(lo, split.pivot())).as("lo < pivot").isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(split.pivot(), H)).as("pivot < H").isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(split.pivot(), utf8("data/k")))
                .as("pivot landed inside the dense jpeg window, not in the gap").isLessThan(0);
        assertThat(probeWouldBeEmpty(split.pivot(), H, keyspace))
                .as("the child (pivot, H] is a REAL non-empty range").isFalse();

        // I4: the victim (lo, pivot] and the child (pivot, H] tile (lo, H] with no gap/overlap,
        // and their re-listed key sets union to exactly the band content.
        assertTilesBand(r, lo, H, keyspace);
    }

    // -------------------------------------------------------------------------
    // (2) The O(log) key case: a band whose code-point width is ~4000 but whose content is a tiny
    //     window near lo.  A linear gap-scan would need thousands of probes; retry-nearer-cursor
    //     converges in ~log2(width) empty probes.  We assert a tight logarithmic ceiling that a
    //     linear regression cannot meet.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void wideBandConvergesInLogProbesNotLinearScan(@TempDir Path dir) throws Exception {
        // Dense window 'a'..'h' (8 keys) right above lo; H = k/<U+1000>, so the diverging code
        // point spans 0x61..0x1000 — width 0xF9F (3999) — almost all of it an empty gap.
        byte[] lo = utf8("k/");
        byte[] cursor = utf8("k/a");
        byte[] H = utf8("k/" + new String(Character.toChars(0x1000)));
        List<byte[]> keyspace = new ArrayList<>();
        for (char ch = 'a'; ch <= 'h'; ch++) {
            keyspace.add(utf8("k/" + ch));
        }

        // Diverging code-point width of the band (the bisection's search interval).
        int width = 0x1000 - 0x61;                          // 3999
        int log2Width = 32 - Integer.numberOfLeadingZeros(width);   // ceil-ish: 12

        byte[] firstPivot = ByteMidpoint.between(cursor, H);
        assertThat(probeWouldBeEmpty(firstPivot, H, keyspace))
                .as("first naive midpoint lands in the wide empty gap").isTrue();

        StealResult r = runOneSteal(dir, lo, cursor, H, keyspace);

        assertThat(r.outcome()).as("wide-band steal converges to a committed split")
                .isEqualTo(Thief.Outcome.CHILD_CREATED);
        assertThat(r.victimUnsplittable()).as("wide empty gap must NOT mark the band unsplittable")
                .isFalse();

        long empty = r.emptyProbes();
        // Logarithmic, not linear.  Upper bound: a generous log2(width)+margin — a linear gap-scan
        // (~width empty probes) blows it by orders of magnitude.  Lower bound: a genuine multi-step
        // bisection of a wide gap (not a trivial 1-probe hit), proving the gap really was traversed.
        assertThat(empty).as("empty probes are logarithmic in band width (%d), never linear", width)
                .isLessThanOrEqualTo(log2Width + 4L)
                .isGreaterThanOrEqualTo(5L);
        assertThat(empty).as("a linear gap scan (~%d probes) would fail this", width)
                .isLessThan(width / 16L);

        // First probe in the gap, last probe converged; total probes = empty + 1.
        assertThat(r.probes().getFirst().empty()).as("first probe is the empty gap pivot").isTrue();
        assertThat(r.probes().getLast().empty()).as("the converging probe found a key").isFalse();

        // Real split inside the dense window (pivot's code point <= 'h'), child non-empty, tiles.
        RangePartition.Split split = onlySplit(r);
        assertThat(KeyBytes.compareUnsigned(split.pivot(), utf8("k/i")))
                .as("pivot landed inside the dense 'a'..'h' window").isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(cursor, split.pivot())).as("cursor < pivot").isLessThan(0);
        assertThat(probeWouldBeEmpty(split.pivot(), H, keyspace))
                .as("child (pivot, H] is a real non-empty range").isFalse();
        assertTilesBand(r, lo, H, keyspace);
    }

    // -------------------------------------------------------------------------
    // (3) The band is never permanently disabled: after the first steal narrows it, a SECOND
    //     steal of the narrowed victim still bites (the empty-upper retry path does not cache
    //     unsplittable — algorithms.md §3).  A regression that cached unsplittable on the empty
    //     probe would make the second steal return UNSPLITTABLE.
    // -------------------------------------------------------------------------
    @Test
    @Timeout(60)
    void bandStaysSplittableAcrossRepeatedStealsNeverCached(@TempDir Path dir) throws Exception {
        byte[] lo = utf8("k/");
        byte[] cursor = utf8("k/a");
        byte[] H = utf8("k/" + new String(Character.toChars(0x1000)));
        List<byte[]> keyspace = new ArrayList<>();
        for (char ch = 'a'; ch <= 'h'; ch++) {
            keyspace.add(utf8("k/" + ch));
        }

        ProbeRecorder recorder = new ProbeRecorder();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(recorder).build();
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            long victimId = store.insertNode(
                    new NodeSpec(run.id(), null, NodeKind.RANGE, lo, H, cursor, null));
            WorkerState victim = WorkerStates.of(victimId, lo, cursor, H);
            victim.addKeysEmitted(1);
            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, l, h) -> { });

            // First steal converges into the dense window and narrows the victim.
            assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);
            assertThat(victim.unsplittable()).as("not cached unsplittable after the empty-upper retry").isFalse();
            byte[] firstPivot = victim.hi();

            // Second steal of the (now narrower) victim still finds a real split — the band was
            // never disabled. A "cache unsplittable on empty probe" regression returns UNSPLITTABLE.
            Thief.Outcome second = thief.steal(List.of(victim));
            assertThat(second).as("the band stays splittable across re-steals (never cached)")
                    .isEqualTo(Thief.Outcome.CHILD_CREATED);
            assertThat(victim.unsplittable()).as("still not unsplittable after the second steal").isFalse();
            assertThat(KeyBytes.compareUnsigned(victim.hi(), firstPivot))
                    .as("second split narrowed the victim further").isLessThan(0);

            // Both committed splits replay into a clean tiling of (lo, H].
            assertThat(store.splits()).as("two real splits committed").hasSize(2);

            // Three-interval tiling assertion (the self-completeness check).
            // From the two splits in commit order:
            //   split 0 (first steal):  victim (lo, H] → victim (lo, m1] + child1 (m1, H]
            //   split 1 (second steal): victim (lo, m1] → victim (lo, m2] + child2 (m2, m1]
            // So the three final intervals are (lo, m2], (m2, m1], (m1, H].
            List<RangePartition.Split> twoSplits = store.splits();
            byte[] m1 = twoSplits.get(0).pivot();        // wider first-steal pivot
            byte[] m2 = twoSplits.get(1).pivot();        // nearer second-steal pivot (== victim.hi())
            assertThat(Arrays.equals(m2, victim.hi())).as("m2 == victim.hi() after second steal").isTrue();
            assertThat(Arrays.equals(twoSplits.get(1).oldHi(), m1))
                    .as("split 1 oldHi == m1 (split 0 pivot)").isTrue();

            List<byte[]> vKeys  = relist(fetcher, lo, m2);   // (lo, m2]
            List<byte[]> c2Keys = relist(fetcher, m2, m1);   // (m2, m1]
            List<byte[]> c1Keys = relist(fetcher, m1, H);    // (m1, H]

            TreeSet<byte[]> union3 = new TreeSet<>(Arrays::compareUnsigned);
            union3.addAll(vKeys);
            union3.addAll(c2Keys);
            union3.addAll(c1Keys);
            assertThat(union3).as("no double-emit across three intervals (no overlap)")
                    .hasSize(vKeys.size() + c2Keys.size() + c1Keys.size());

            // Expected: all 8 keyspace keys — all are in (lo, H] by construction.
            TreeSet<byte[]> expected3 = new TreeSet<>(Arrays::compareUnsigned);
            expected3.addAll(keyspace);
            assertThat(union3.size()).as("three-interval union == band content (no gap)")
                    .isEqualTo(expected3.size());
            var u3 = union3.iterator();
            var e3 = expected3.iterator();
            while (u3.hasNext()) {
                assertThat(Arrays.equals(u3.next(), e3.next()))
                        .as("byte-exact three-interval tiling (I4)").isTrue();
            }
        }
    }

    // ---- driver + assertions ---------------------------------------------------

    private record StealResult(Thief.Outcome outcome, boolean victimUnsplittable, byte[] victimHi,
                               List<RangePartition.Split> splits, List<Probe> probes, long emptyProbes,
                               MockPageFetcher fetcher) {
    }

    /**
     * Insert a single victim node {@code (lo, H]} with the given cursor and perform exactly one
     * {@link Thief#steal} against it, capturing the probe trace and the committed split(s).
     */
    private StealResult runOneSteal(Path dir, byte[] lo, byte[] cursor, byte[] H, List<byte[]> keyspace)
            throws Exception {
        ProbeRecorder recorder = new ProbeRecorder();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).interceptor(recorder).build();
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(key(), false, false);
            long victimId = store.insertNode(
                    new NodeSpec(run.id(), null, NodeKind.RANGE, lo, H, cursor, null));
            WorkerState victim = WorkerStates.of(victimId, lo, cursor, H);
            victim.addKeysEmitted(1);   // a density signal so victim selection ranks it splittable

            Thief thief = Thiefs.of(store, fetcher, run.id(), new byte[0], ListingMode.OBJECTS,
                    (id, l, h) -> { });
            Thief.Outcome outcome = thief.steal(List.of(victim));

            return new StealResult(outcome, victim.unsplittable(), victim.hi(),
                    store.splits(), recorder.snapshot(), recorder.emptyProbes(), fetcher);
        }
    }

    private static RangePartition.Split onlySplit(StealResult r) {
        assertThat(r.splits()).as("exactly one split committed").hasSize(1);
        return r.splits().getFirst();
    }

    /**
     * I4 for the band {@code (lo, H]}: re-list the victim {@code (lo, pivot]} and the child
     * {@code (pivot, H]} (via {@link RangeScanner}) and assert their emitted key sets are
     * pairwise-disjoint and union to exactly the band content (every keyspace key in {@code (lo, H]}),
     * and structurally that {@code lo < pivot < H} (boundary belongs LEFT).
     */
    private void assertTilesBand(StealResult r, byte[] lo, byte[] H, List<byte[]> keyspace)
            throws Exception {
        RangePartition.Split split = onlySplit(r);
        byte[] pivot = split.pivot();

        // Structural: lo < pivot < H, and the recorded split's oldHi is the band's H.
        assertThat(Arrays.equals(split.oldHi(), H)).as("split oldHi == band H").isTrue();
        if (lo != null) {
            assertThat(KeyBytes.compareUnsigned(lo, pivot)).as("lo < pivot").isLessThan(0);
        }
        assertThat(KeyBytes.compareUnsigned(pivot, H)).as("pivot < H").isLessThan(0);

        List<byte[]> victimKeys = relist(r.fetcher(), lo, pivot);
        List<byte[]> childKeys = relist(r.fetcher(), pivot, H);

        TreeSet<byte[]> union = new TreeSet<>(Arrays::compareUnsigned);
        union.addAll(victimKeys);
        union.addAll(childKeys);
        assertThat(union).as("no double-emit between victim (lo, pivot] and child (pivot, H]")
                .hasSize(victimKeys.size() + childKeys.size());

        // Expected band content: every keyspace key k with lo < k <= H.
        TreeSet<byte[]> expected = new TreeSet<>(Arrays::compareUnsigned);
        for (byte[] k : keyspace) {
            boolean aboveLo = lo == null || KeyBytes.compareUnsigned(lo, k) < 0;
            boolean atOrBelowH = H == null || KeyBytes.compareUnsigned(k, H) <= 0;
            if (aboveLo && atOrBelowH) {
                expected.add(k);
            }
        }
        assertThat(union.size()).as("victim ∪ child == band content (no gap)").isEqualTo(expected.size());
        var u = union.iterator();
        var e = expected.iterator();
        while (u.hasNext()) {
            assertThat(Arrays.equals(u.next(), e.next())).as("byte-exact band tiling").isTrue();
        }
    }

    /** Re-list {@code (startAfter, hi]} of the whole bucket via {@link RangeScanner}. */
    private static List<byte[]> relist(MockPageFetcher fetcher, byte[] startAfter, byte[] hi)
            throws Exception {
        List<byte[]> out = new ArrayList<>();
        RangeScanners.of(fetcher, 16).runRange(new byte[0], ListingMode.OBJECTS, startAfter, hi, null,
                (batch, lastKey, completed) -> batch.forEach(en -> out.add(en.key().raw())));
        return out;
    }

    /** True iff a {@code probe(m, H)} would be empty — no keyspace key in {@code (m, H]}. */
    private static boolean probeWouldBeEmpty(byte[] m, byte[] H, List<byte[]> keyspace) {
        for (byte[] k : keyspace) {
            boolean aboveM = KeyBytes.compareUnsigned(m, k) < 0;
            boolean atOrBelowH = H == null || KeyBytes.compareUnsigned(k, H) <= 0;
            if (aboveM && atOrBelowH) {
                return false;
            }
        }
        return true;
    }
}
