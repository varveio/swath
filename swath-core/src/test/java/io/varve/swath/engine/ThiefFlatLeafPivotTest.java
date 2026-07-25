/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.SwathException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * <b>Flat-leaf residual.</b> After the parent-empty sliver's structure probe returns
 * {@code null} — a FLAT LEAF: one high-entropy directory of flat FILES with NO sub-directories (a
 * blockchain FC-range dir of {@code .xdr.zst} files), bounded by a far next-dir {@code H} — both
 * byte-midpoint and the {@code delimiter=/} structure probe can only sliver at the cursor. The
 * whole remaining tail then hands off to one child and drains serially (one worker, rest idle).
 * Only DENSITY EXTRAPOLATION can bisect a flat leaf.
 *
 * <p>Before committing the byte-exact sliver, the thief attempts a flat-leaf density-reflected
 * pivot ({@link StealMath#extrapolate}, the same open-frontier reflection) inside the leaf,
 * yielding a BALANCED interior boundary so the victim and child run in parallel. This is an
 * ordinary single-threaded unit guard of the steal mechanics — not the cross-cutting PROP/RES/CONC
 * interleavings.
 */
final class ThiefFlatLeafPivotTest {

    private static final long RUN_ID = 12L;
    private static final byte[] HI = b("led/02");

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RecordingSink implements Thief.ChildSink {
        final List<Long> children = new ArrayList<>();
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { children.add(childNodeId); }
    }

    /**
     * The flat-leaf keyspace: one directory {@code led/0100/} of {@value #FLAT_N} high-entropy flat
     * FILES {@code led/0100/<hhhh>} spread evenly across the full first-hex-digit range (value {@code
     * i*16}, so 0x0000..0xfff0 — no sub-directories). A {@code delimiter=/} probe inside {@code
     * led/0100/} yields only object entries (no CommonPrefix), so the structure probe returns null and
     * the steal falls into the flat-leaf branch.
     */
    private static final int FLAT_N = 4096;

    private static List<byte[]> flatLeafKeys() {
        List<byte[]> ks = new ArrayList<>(FLAT_N);
        for (int i = 0; i < FLAT_N; i++) {
            ks.add(b("led/0100/%04x".formatted(i * 16)));
        }
        return ks;
    }

    /** Files strictly in {@code (lo, hi]} within a keyspace (both bounds unsigned byte-lex). */
    private static long filesIn(List<byte[]> keys, byte[] loExcl, byte[] hiIncl) {
        return keys.stream()
                .filter(k -> KeyBytes.compareUnsigned(k, loExcl) > 0 && KeyBytes.compareUnsigned(k, hiIncl) <= 0)
                .count();
    }

    /** Files strictly in {@code (lo, hi)} (both exclusive). */
    private static long filesInOpen(List<byte[]> keys, byte[] loExcl, byte[] hiExcl) {
        return keys.stream()
                .filter(k -> KeyBytes.compareUnsigned(k, loExcl) > 0 && KeyBytes.compareUnsigned(k, hiExcl) < 0)
                .count();
    }

    @Test
    void bootstrapFlatLeafDensityReflectsBalancedInteriorPivot() throws SwathException, InterruptedException {
        // The flat-leaf bootstrap shape:
        //   lo     = led/0100/          (the leaf directory itself — a bootstrap worker, NOT a real key)
        //   cursor = led/0100/4000      (a real emitted key ~1/4 into the leaf's hex range)
        //   hi     = led/02             (the far next-dir bound; diverges from the cursor at ADJACENT
        //                                code points '1' vs '2', so byte-midpoint slivers)
        // byte-midpoint yields m = cursor·0x20 (the parent-empty sliver). The delimiter=/ structure probe
        // finds NO sub-directory boundary (flat leaf). extrapolate reflects the consumed span (leaf
        // min, cursor] forward to a BALANCED interior pivot, so BOTH halves are populated.
        List<byte[]> keys = flatLeafKeys();
        byte[] cursor = b("led/0100/4000");
        byte[] leafDir = b("led/0100/");
        byte[] leafCeil = StealMath.prefixCeil(leafDir);

        WorkerState victim = WorkerStates.of(1, leafDir, cursor, HI);
        victim.addKeysEmitted(100);   // density signal so estRemaining > 0 (selectable)

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        StubCheckpointStore store = StubCheckpointStore.returning(88L);
        RecordingSink sink = new RecordingSink();
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, sink);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        byte[] pivot = store.lastSplit.pivot();
        byte[] sliver = b("led/0100/4000 ");   // the degenerate cursor-adjacent MIN_SAFE pivot (0x20)

        // NOT the cursor-adjacent sliver (the serial-drain bug).
        assertThat(KeyBytes.compareUnsigned(pivot, sliver))
                .as("pivot must not be the cursor-adjacent MIN_SAFE sliver").isNotZero();

        // The pivot stays strictly inside the leaf directory: leafDir prefix, below prefixCeil(leafDir).
        assertThat(new String(pivot, StandardCharsets.UTF_8))
                .as("pivot stays inside the leaf directory").startsWith("led/0100/");
        assertThat(KeyBytes.compareUnsigned(pivot, leafCeil))
                .as("pivot strictly below prefixCeil(leafDir) — never crosses into the next directory").isLessThan(0);

        // Tiling: cursor < pivot < hi (I2/I3).
        assertThat(KeyBytes.compareUnsigned(cursor, pivot)).isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(pivot, HI)).isLessThan(0);

        // THE parallelism assertion: BOTH the parent (cursor, pivot] and the child (pivot, hi) are
        // populated — a degenerate cursor-adjacent sliver would leave 0 / N (the parent empty, the
        // child inheriting the tail).
        assertThat(filesIn(keys, cursor, pivot)).as("parent (cursor, pivot] non-empty").isGreaterThan(0);
        assertThat(filesInOpen(keys, pivot, HI)).as("child (pivot, hi) non-empty").isGreaterThan(0);

        assertThat(victim.hi()).isEqualTo(pivot);
        assertThat(sink.children).containsExactly(88L);
    }

    @Test
    void childFlatLeafReflectsOwnLoWithZeroExtraProbes() throws SwathException, InterruptedException {
        // A CHILD worker in the same flat leaf: lo is a REAL deep key (a prior split's pivot), not the
        // leaf directory. The reflection reference is the child's own lo — so NO floor probe is issued.
        List<byte[]> keys = flatLeafKeys();
        byte[] cursor = b("led/0100/4000");
        byte[] childLo = b("led/0100/2000");   // a real deep key inside the leaf

        // Bootstrap reference run (lo == leafDir) — it DOES issue the single floor probe.
        WorkerState bootstrap = WorkerStates.of(1, b("led/0100/"), cursor, HI);
        bootstrap.addKeysEmitted(100);
        MockPageFetcher bootFetcher = MockPageFetcher.builder().keys(keys).build();
        Thief bootThief = Thiefs.of(StubCheckpointStore.returning(1L), bootFetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());
        assertThat(bootThief.steal(List.of(bootstrap))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        long bootstrapApiCalls = bootFetcher.apiCalls();

        // Child run — identical mock/shape except lo is a real deep key.
        WorkerState child = WorkerStates.of(1, childLo, cursor, HI);
        child.addKeysEmitted(100);
        MockPageFetcher childFetcher = MockPageFetcher.builder().keys(keys).build();
        StubCheckpointStore childStore = StubCheckpointStore.returning(99L);
        Thief childThief = Thiefs.of(childStore, childFetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, new RecordingSink());
        assertThat(childThief.steal(List.of(child))).isEqualTo(Thief.Outcome.CHILD_CREATED);
        long childApiCalls = childFetcher.apiCalls();

        // ZERO extra probes for the child: the ONLY difference from the bootstrap is the floor probe the
        // bootstrap issues (ref = own lo, no probe). So child == bootstrap - 1.
        assertThat(childApiCalls)
                .as("child reflects its own lo — one fewer LIST (the floor probe) than the bootstrap")
                .isEqualTo(bootstrapApiCalls - 1);

        byte[] pivot = childStore.lastSplit.pivot();
        assertThat(new String(pivot, StandardCharsets.UTF_8)).startsWith("led/0100/");
        assertThat(KeyBytes.compareUnsigned(cursor, pivot)).isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(pivot, HI)).isLessThan(0);
        assertThat(filesIn(keys, cursor, pivot)).as("parent non-empty").isGreaterThan(0);
        assertThat(filesInOpen(keys, pivot, HI)).as("child non-empty").isGreaterThan(0);
    }

    /**
     * No-regression guard: an encode-style leaf WITH uuid sub-directories still takes the delimiter=/
     * STRUCTURE-probe path (the pivot is a real sub-directory boundary), UNCHANGED by the flat-leaf
     * branch — the flat-leaf density reflection is reached only when the structure probe finds nothing.
     */
    @Test
    void leafWithSubdirsStillTakesStructureProbePath() throws SwathException, InterruptedException {
        // A date/uuid shape 2022/03/DD/uNN/f: the cursor is deep in day 05, the bound is the next month.
        // The structure probe backs out to the month 2022/03/ and splits at the median DAY 2022/03/17/ —
        // a sub-directory boundary, NOT a flat-leaf density pivot.
        List<byte[]> keys = new ArrayList<>();
        for (int day = 1; day <= 28; day++) {
            for (int u = 0; u < 10; u++) {
                keys.add(b("2022/03/%02d/u%02d/f".formatted(day, u)));
            }
        }
        WorkerState victim = WorkerStates.of(1, b("2022/03/05/u05/"), b("2022/03/05/u05/f"), b("2022/04"));
        victim.addKeysEmitted(100);

        PageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        StubCheckpointStore store = StubCheckpointStore.returning(55L);
        RecordingSink sink = new RecordingSink();
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, sink);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        // The pivot is the DAY-level sub-directory boundary (structure-probe path), not a flat-leaf pivot.
        assertThat(store.lastSplit.pivot()).as("sub-dir structure boundary, unchanged").isEqualTo(b("2022/03/17/"));
    }
}
