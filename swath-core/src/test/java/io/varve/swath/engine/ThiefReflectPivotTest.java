/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.error.SwathException;
import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.StubCheckpointStore;
import io.varve.swath.testkit.Thiefs;
import io.varve.swath.testkit.WorkerStates;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The density-reflected empty-upper pivot. When the far-ahead probe and the
 * step-back midpoint both probe empty AND the {@code delimiter=/} structure probe finds nothing,
 * the range's mass sits in a thin low sliver and the uniform-midpoint pivot overshot into vacuum.
 * Instead of walking the blind bisection back toward the cursor one halving at a time, the thief
 * reflects the consumed span {@code (lo, c]} forward under observed density
 * ({@link StealMath#extrapolate}, zero extra I/O) and probes it once: a hit commits at {@code m_r};
 * a miss seeds the existing budgeted bisection at the shorter {@code (c, m_r]} interval.
 *
 * <p>These are ordinary single-attempt unit guards of the steal mechanics; the cross-cutting
 * PROP/RES/CONC interleavings are covered separately.
 */
final class ThiefReflectPivotTest {

    private static final long RUN_ID = 21L;
    /** A far next-dir bound so the uniform interpolate midpoint overshoots the low hex sliver. */
    private static final byte[] HI = b("k/z");

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** A low sliver: {@value #N} keys {@code k/0000..k/00ff} — the real mass occupies a thin
     * low window of the {@code (cursor, HI]} code-point span. */
    private static final int N = 256;

    private static List<byte[]> sliverKeys() {
        List<byte[]> ks = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            ks.add(b("k/%04x".formatted(i)));
        }
        return ks;
    }

    private static long filesIn(List<byte[]> keys, byte[] loExcl, byte[] hiIncl) {
        return keys.stream()
                .filter(k -> KeyBytes.compareUnsigned(k, loExcl) > 0 && KeyBytes.compareUnsigned(k, hiIncl) <= 0)
                .count();
    }

    private static final class RecordingSink implements Thief.ChildSink {
        final List<Long> children = new ArrayList<>();
        @Override public void accept(long childNodeId, byte[] childLo, byte[] childHi) { children.add(childNodeId); }
    }

    private static Map<String, Long> reasons(RunMetrics metrics) {
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    @Test
    void reflectHitCommitsAtTheReflectedPivot() throws SwathException, InterruptedException {
        // cursor early in the sliver (value 0x40): the uniform midpoint of (cursor, k/z] lands far above
        // k/00ff (vacuum) so upperEmpty fires; extrapolate reflects the consumed span forward to a pivot
        // INSIDE the sliver, which probes non-empty and commits directly (no blind bisection).
        List<byte[]> keys = sliverKeys();
        byte[] lo = b("k/0000");
        byte[] cursor = b("k/0040");
        byte[] expectedReflect = StealMath.extrapolate(lo, cursor, HI);
        assertThat(expectedReflect).as("scenario precondition: extrapolate yields a reflected pivot").isNotNull();

        WorkerState victim = WorkerStates.of(1, lo, cursor, HI);
        victim.addKeysEmitted(64);

        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        StubCheckpointStore store = StubCheckpointStore.returning(88L);
        RecordingSink sink = new RecordingSink();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS, sink, metrics);

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        byte[] pivot = store.lastSplit.pivot();
        assertThat(pivot).as("committed pivot IS the reflected pivot m_r").isEqualTo(expectedReflect);
        assertThat(victim.hi()).isEqualTo(expectedReflect);

        Map<String, Long> r = reasons(metrics);
        assertThat(r.getOrDefault("PIVOT.reflect_hit", 0L)).as("reflect probe non-empty").isEqualTo(1L);
        assertThat(r.getOrDefault("PIVOT.reflect", 0L)).as("reflect won the commit").isEqualTo(1L);
        assertThat(r.getOrDefault("PIVOT.bisect", 0L)).as("blind bisection never ran").isZero();

        // Both halves populated (the parallelism win): parent (cursor, m_r], child (m_r, HI].
        assertThat(filesIn(keys, cursor, pivot)).as("parent non-empty").isGreaterThan(0);
        assertThat(filesIn(keys, pivot, HI)).as("child non-empty").isGreaterThan(0);
        assertThat(sink.children).containsExactly(88L);
    }

    @Test
    void reflectEmptyBisectsTheShorterInterval() throws SwathException, InterruptedException {
        // cursor near the TOP of the sliver (value 0xf0): both the uniform midpoint AND the reflected
        // pivot overshoot above k/00ff, so reflect probes empty. The bisection then re-seeds at the
        // SHORTER (cursor, m_r] interval (m_r just above the mass) rather than at the far uniform
        // midpoint — so reflect-on converges in materially fewer probes than reflect-off.
        List<byte[]> keys = sliverKeys();
        byte[] lo = b("k/0000");
        byte[] cursor = b("k/00f0");
        byte[] mReflect = StealMath.extrapolate(lo, cursor, HI);
        assertThat(mReflect).as("scenario precondition: extrapolate yields a reflected pivot").isNotNull();
        assertThat(probeNonEmpty(keys, mReflect, HI))
                .as("scenario precondition: the reflected pivot overshoots the mass (probes empty)").isFalse();

        WorkerState victim = WorkerStates.of(1, lo, cursor, HI);
        victim.addKeysEmitted(240);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        StubCheckpointStore store = StubCheckpointStore.returning(90L);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS,
                new RecordingSink(), metrics);
        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        Map<String, Long> r = reasons(metrics);
        assertThat(r.getOrDefault("PIVOT.reflect_empty", 0L)).as("reflect probed empty").isEqualTo(1L);
        assertThat(r.getOrDefault("PIVOT.reflect", 0L)).as("reflect did NOT win — bisection did").isZero();
        assertThat(r.getOrDefault("PIVOT.bisect", 0L)).as("bisection carried the split").isEqualTo(1L);

        byte[] pivot = store.lastSplit.pivot();
        assertThat(KeyBytes.compareUnsigned(cursor, pivot)).as("cursor < pivot").isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(pivot, HI)).as("pivot < hi").isLessThan(0);
        assertThat(filesIn(keys, pivot, HI)).as("child non-empty").isGreaterThan(0);

        // The load-bearing assertion: the committed pivot is the bisection re-seeded at the SHORTER
        // (cursor, m_r] interval — NOT the far uniform (cursor, midpoint] interval the reflect-off path
        // would bisect. m_r sits just above the mass, so the bisection descends from there.
        assertThat(pivot)
                .as("reflect-empty bisects (cursor, m_r], the shorter interval")
                .isEqualTo(bisectFrom(keys, cursor, mReflect, HI));
    }

    @Test
    void reflectOffPreservesTheUniformBisectionPathByteForByte() throws SwathException, InterruptedException {
        // The reflect-hit scenario, but with reflect OFF: the commit must be the byte-exact blind-bisection
        // pivot (no reflect probe, no reflect counters, the once-per-scan TOGGLE.reflect_off mark present).
        List<byte[]> keys = sliverKeys();
        byte[] lo = b("k/0000");
        byte[] cursor = b("k/0040");

        WorkerState victim = WorkerStates.of(1, lo, cursor, HI);
        victim.addKeysEmitted(64);
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(keys).build();
        StubCheckpointStore store = StubCheckpointStore.returning(77L);
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Thief thief = Thiefs.of(store, fetcher, RUN_ID, new byte[0], ListingMode.OBJECTS,
                new RecordingSink(), metrics, reflectOff());

        assertThat(thief.steal(List.of(victim))).isEqualTo(Thief.Outcome.CHILD_CREATED);

        byte[] pivot = store.lastSplit.pivot();
        byte[] oracle = uniformBisectionPivot(keys, cursor, HI);
        assertThat(pivot).as("reflect off ⇒ the exact pre-unit blind-bisection pivot").isEqualTo(oracle);
        assertThat(pivot).as("differs from the reflected pivot the ON path would have committed")
                .isNotEqualTo(StealMath.extrapolate(lo, cursor, HI));

        Map<String, Long> r = reasons(metrics);
        assertThat(r.getOrDefault("PIVOT.reflect_hit", 0L)).isZero();
        assertThat(r.getOrDefault("PIVOT.reflect_empty", 0L)).isZero();
        assertThat(r.getOrDefault("PIVOT.reflect", 0L)).isZero();
        assertThat(r.getOrDefault("TOGGLE.reflect_off", 0L)).as("once-per-scan ablation mark").isEqualTo(1L);
    }

    private static EngineToggles reflectOff() {
        return EngineToggles.DEFAULT.withReflect(false);
    }

    /** In-test oracle replicating the production empty-upper bisection: start at the uniform f=0.5
     * midpoint (the probed-empty pivot), then halve toward the cursor until a probe is non-empty. */
    private static byte[] uniformBisectionPivot(List<byte[]> keys, byte[] c, byte[] hi) {
        return bisectFrom(keys, c, StealMath.interpolate(c, hi, 0.5), hi);
    }

    /** In-test oracle for the production empty-upper bisection seeded at {@code start} (the probed-empty
     * pivot): halve toward the cursor until a probe is non-empty — exactly the production loop. */
    private static byte[] bisectFrom(List<byte[]> keys, byte[] c, byte[] start, byte[] hi) {
        byte[] m = start;
        for (int i = 0; i < 64; i++) {
            m = ByteMidpoint.between(c, m);
            if (m == null) {
                return null;
            }
            if (probeNonEmpty(keys, m, hi)) {
                return m;
            }
        }
        return null;
    }

    private static boolean probeNonEmpty(List<byte[]> keys, byte[] m, byte[] hi) {
        return keys.stream().anyMatch(k ->
                KeyBytes.compareUnsigned(k, m) > 0 && (hi == null || KeyBytes.compareUnsigned(k, hi) <= 0));
    }
}
