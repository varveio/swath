/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.junit.jupiter.api.Test;

/**
 * The <b>parent-empty degenerate pivot</b> (the symmetric partner of the
 * empty-upper funnel). When a bounded victim has a DEEP cursor and a SHORT {@code hi}
 * that diverge at ADJACENT code points (the baton-passing shape — e.g. cursor
 * {@code led/0100/obj}, hi {@code led/02}), {@link io.varve.swath.model.ByteMidpoint#between}
 * can only return the cursor's immediate MIN_SAFE successor ({@code cursor·0x20}): a
 * byte-exact but useless split whose parent range {@code (cursor, m]} is EMPTY and whose
 * child inherits the ENTIRE remaining tail. The empty-upper probe never fires (the upper
 * IS non-empty), so committing this sliver would drain the tail serially.
 *
 * <p>The thief instead detects the cursor-adjacent sliver and routes it through the SAME
 * demand-driven {@code delimiter=/} structure-probe machinery (with the probe prefix backed
 * out to the cursor's sibling-directory level), yielding a BALANCED median boundary so the
 * victim and child run in parallel.
 *
 * <p>This is an ordinary single-threaded unit guard of the steal mechanics; the
 * cross-cutting PROP-1/RES-3/CONC interleavings are covered separately.
 */
final class ThiefParentEmptyPivotTest {

    private static final long RUN_ID = 11L;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RecordingSink implements Thief.ChildSink {
        final List<Long> children = new ArrayList<>();
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { children.add(childNodeId); }
    }

    /**
     * A blockchain/encode-public shape: one file per directory, {@code led/<NNNN>/obj}. Sibling
     * DIRECTORIES ({@code led/0101/}, {@code led/0102/}, …) are exactly what {@code delimiter=/}
     * at {@code led/} discovers — the populated boundaries the balanced split needs.
     */
    private static PageFetcher siblingDirs(int fromInclusive, int toInclusive) {
        List<byte[]> ks = new ArrayList<>();
        for (int i = fromInclusive; i <= toInclusive; i++) {
            ks.add(b("led/%04d/obj".formatted(i)));
        }
        return MockPageFetcher.builder().keys(ks).build();
    }

    /**
     * A date-partitioned (encode-public) shape {@code YYYY/MM/DD/uNN/f}: month 03, days 01..28,
     * ten uuid directories per day, one file per uuid. The cursor sits DEEP inside day 05 and the
     * bound {@code hi} is the next-month sibling {@code 2022/04}. The committed FIXED 2-segment
     * back-out lands the probe at {@code 2022/03/05/} (the current DAY), whose median boundary is a
     * within-day uuid ({@code 2022/03/05/u08/}) — so close ahead of the cursor the fast drainer races
     * past it (the near-serial tail). The ADAPTIVE back-out instead probes coarse→fine from the
     * cursor∧hi divergence directory ({@code 2022/}) and settles on the MONTH level {@code 2022/03/},
     * whose {@code delimiter=/} children are the DAYS — a durable, far-ahead median.
     */
    private static PageFetcher dateShapedDays() {
        List<byte[]> ks = new ArrayList<>();
        for (int day = 1; day <= 28; day++) {
            for (int u = 0; u < 10; u++) {
                ks.add(b("2022/03/%02d/u%02d/f".formatted(day, u)));
            }
        }
        return MockPageFetcher.builder().keys(ks).build();
    }

    /**
     * A directory wider than one bounded structure probe cannot yield its true median at any
     * affordable cost — the probe would have to enumerate every child, which is what pushed it past
     * its own timeout on `s3://commoncrawl` and left the range unsplit. The truncated regime instead
     * commits the FURTHEST boundary the probe proved: the largest carve available, and the one a fast
     * drainer is least likely to overrun before the CAS.
     */
    @Test
    void aTruncatedStructureProbePivotsOnTheFurthestBoundaryItProved() throws SwathException, InterruptedException {
        // Same baton-passing shape as the balanced case above (hi is the ADJACENT-code-point bound
        // that forces byte-midpoint to sliver, so the structure probe is what decides the pivot) —
        // but with 0101..0199 qualifying, which is more than one probe page returns.
        WorkerState victim = WorkerStates.of(1, b("led/0100/"), b("led/0100/obj"), b("led/02"));
        victim.addKeysEmitted(100);

        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        RecordingSink sink = new RecordingSink();
        Thief thief = Thiefs.of(store, siblingDirs(0, 199),
                RUN_ID, new byte[0], ListingMode.OBJECTS, sink);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        byte[] pivot = store.lastSplit.pivot();
        // The furthest boundary the truncated page carried — NOT its median (which would sit at
        // roughly half the sample, far nearer the cursor), and NOT the cursor-adjacent sliver.
        assertThat(pivot).as("furthest proved boundary from the truncated probe").isEqualTo(b("led/0132/"));
        assertThat(KeyBytes.compareUnsigned(b("led/0100/obj"), pivot)).isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(pivot, b("led/02"))).isLessThan(0);
        assertThat(victim.hi()).isEqualTo(pivot);
    }

    @Test
    void parentEmptySliverBacksOutToDurableCoarserBoundary() throws SwathException, InterruptedException {
        // The date-partitioned baton-passing shape:
        //   lo     = 2022/03/05/u05/       (deep — as if a prior split's pivot)
        //   cursor = 2022/03/05/u05/f      (the last emitted key, deep inside day 05)
        //   hi     = 2022/04               (the next-month sibling; diverges from the cursor at
        //                                   ADJACENT code points '3' vs '4', so byte-midpoint slivers)
        // Byte-midpoint yields m = cursor·0x20 (the parent-empty sliver). A fixed 2-segment back-out
        // would probe 2022/03/05/ (the DAY) and pick a within-day uuid median (2022/03/05/u08/) — a
        // close pivot the fast drainer races past. The adaptive back-out instead finds the coarsest
        // level with a real boundary in (cursor, hi]: the MONTH 2022/03/, whose children are the DAYS,
        // and splits at the median DAY (durable, far ahead of the cursor).
        WorkerState victim = WorkerStates.of(1, b("2022/03/05/u05/"), b("2022/03/05/u05/f"), b("2022/04"));
        victim.addKeysEmitted(100);

        StubCheckpointStore store = StubCheckpointStore.returning(77L);
        RecordingSink sink = new RecordingSink();
        Thief thief = Thiefs.of(store, dateShapedDays(),
                RUN_ID, new byte[0], ListingMode.OBJECTS, sink);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        byte[] pivot = store.lastSplit.pivot();

        // DURABILITY: the pivot is a DAY-level boundary far ahead of the cursor — the median of the
        // days strictly in (2022/03/05/u05/f, 2022/04): days 06..28 → 2022/03/17/. It is NOT the
        // within-day uuid the FIXED back-out picked (2022/03/05/u08/), which this assertion rejects.
        assertThat(pivot).as("durable coarse (DAY-level) median, not the within-day uuid boundary")
                .isEqualTo(b("2022/03/17/"));
        assertThat(new String(pivot, StandardCharsets.UTF_8))
                .as("pivot must not be bunched within the cursor's current day")
                .doesNotStartWith("2022/03/05/u");

        // Tiling sanity: cursor < pivot < hi (I2/I3).
        assertThat(KeyBytes.compareUnsigned(b("2022/03/05/u05/f"), pivot)).isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(pivot, b("2022/04"))).isLessThan(0);

        assertThat(victim.hi()).isEqualTo(pivot);
        assertThat(sink.children).containsExactly(77L);
    }

    /**
     * Pivot-attribution: the parent-empty sliver's coarse->fine adaptive back-out must record a
     * PIVOT.adaptive_structure engagement distinct from the plain empty-upper funnel's
     * PIVOT.structure_probe (see StealStructureProbeTest), so post-hoc analysis can tell which
     * durability mechanism carried the split instead of merging both under one "structure" tag.
     */
    @Test
    void adaptiveBackoutWinIsTaggedDistinctlyFromPlainStructureProbe() throws SwathException, InterruptedException {
        WorkerState victim = WorkerStates.of(1, b("2022/03/05/u05/"), b("2022/03/05/u05/f"), b("2022/04"));
        victim.addKeysEmitted(100);

        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        StubCheckpointStore store = StubCheckpointStore.returning(77L);
        RecordingSink sink = new RecordingSink();
        Thief thief = Thiefs.of(store, dateShapedDays(),
                RUN_ID, new byte[0], ListingMode.OBJECTS, sink, metrics);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        var reasons = metrics.diagnostics(Duration.ZERO).stealReasons();
        assertThat(reasons.getOrDefault("PIVOT.adaptive_structure", 0L))
                .as("the parent-empty sliver's adaptive back-out is tagged PIVOT.adaptive_structure")
                .isPositive();
        assertThat(reasons.getOrDefault("PIVOT.structure_probe", 0L))
                .as("the adaptive back-out must NOT also fire the plain structure_probe tag")
                .isZero();
    }

    @Test
    void parentEmptySliverDiscoversBalancedStructureBoundary() throws SwathException, InterruptedException {
        // The baton-passing shape:
        //   lo     = led/0100/       (deep — as if it were a prior split's pivot)
        //   cursor = led/0100/obj    (the last emitted key)
        //   hi     = led/02          (a SHORT bound; diverges from the cursor at ADJACENT code
        //                             points '1' vs '2', so byte-midpoint can only sliver)
        // Byte-midpoint yields m = cursor·0x20 = "led/0100/obj " (the cursor's immediate
        // successor). The parent (cursor, m] is EMPTY; committing it would hand the child the
        // whole 0101..0199 tail — serial. The thief instead splits at the MEDIAN sibling directory.
        WorkerState victim = WorkerStates.of(1, b("led/0100/"), b("led/0100/obj"), b("led/02"));
        victim.addKeysEmitted(100);   // give it a density signal so estRemaining > 0 (selectable)

        StubCheckpointStore store = StubCheckpointStore.returning(42L);
        RecordingSink sink = new RecordingSink();
        // 0101..0120 = 20 boundaries strictly in (cursor, hi] — inside the structure probe's
        // max_keys, so the probe sees the WHOLE directory and the true median is available.
        Thief thief = Thiefs.of(store, siblingDirs(0, 120),
                RUN_ID, new byte[0], ListingMode.OBJECTS, sink);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        byte[] pivot = store.lastSplit.pivot();
        byte[] sliver = b("led/0100/obj ");   // the degenerate cursor-adjacent pivot

        // NOT the cursor-adjacent sliver (the baton-passing bug).
        assertThat(KeyBytes.compareUnsigned(pivot, sliver))
                .as("pivot must not be the cursor-adjacent MIN_SAFE sliver").isNotZero();

        // A BALANCED split: the median populated sibling directory among led/0101/..led/0120/
        // (20 boundaries strictly in (cursor, hi]) is led/0111/. The parent keeps 0100..0111
        // and the child takes 0111..0120 — both run in parallel (not a whole-tail hand-off).
        assertThat(pivot).as("balanced median sibling-directory boundary").isEqualTo(b("led/0111/"));

        // Sanity on tiling: cursor < pivot < hi (I2/I3).
        assertThat(KeyBytes.compareUnsigned(b("led/0100/obj"), pivot)).isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(pivot, b("led/02"))).isLessThan(0);

        // hi narrowed to the pivot; the child (pivot, hi] was handed to the driver under the lock.
        assertThat(victim.hi()).isEqualTo(pivot);
        assertThat(sink.children).containsExactly(42L);
    }
}
