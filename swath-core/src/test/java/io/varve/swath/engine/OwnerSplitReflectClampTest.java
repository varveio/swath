/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Owner-split reflection clamp (gated by {@code reflect}). The decision
 * {@link StealMath#shouldClampToReflected} is exercised directly (pure arithmetic + byte
 * compares) for the two contract cases — a skewed pivot that overshoots the observed mass is clamped
 * down to the reflected pivot, a uniform pivot ({@code m_r >= m}) is left alone — plus a full-engine
 * engagement smoke that the {@code OWNER_SPLIT.pivot_reflect_clamped} counter fires with the toggle on
 * and never with it off, byte-exact either way.
 *
 * <p>Ordinary unit guards of the clamp mechanics — not the PROP-1/RES-3/CONC cross-cutting interleavings.
 */
final class OwnerSplitReflectClampTest {

    private static final int MAX_KEYS = 100;

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- pure decision: shouldClampToReflected --------------------------------------------------

    @Test
    void skewedOvershootClampsToTheReflectedPivot() {
        // The interpolate pivot m overshot the observed mass (m_r strictly below it, inside the mass);
        // est is large and density is uniform (ratio=1 ⇒ the clamped tail clears the observed-mass floor).
        byte[] lo = b("d/00");
        byte[] cursor = b("d/02");
        byte[] m = b("d/08");          // f-interpolated pivot (overshoots)
        byte[] mReflect = b("d/04");   // density-reflected pivot, strictly in (cursor, m)
        byte[] H = b("d/09");

        assertThat(KeyBytes.compareUnsigned(cursor, mReflect)).as("m_r > cursor").isLessThan(0);
        assertThat(KeyBytes.compareUnsigned(mReflect, m)).as("m_r < m (overshoot)").isLessThan(0);

        assertThat(StealMath.shouldClampToReflected(cursor, m, mReflect, lo, H, 100_000.0, 1.0, MAX_KEYS))
                .as("overshoot + clamped tail clears the floor ⇒ clamp")
                .isTrue();
    }

    @Test
    void uniformReflectionAtOrBeyondTheInterpolatedPivotNeverClamps() {
        byte[] lo = b("d/00");
        byte[] cursor = b("d/02");
        byte[] m = b("d/08");
        byte[] H = b("d/09");

        // m_r == m: reflection reached exactly the far-ahead fraction — no overshoot, no clamp.
        assertThat(StealMath.shouldClampToReflected(cursor, m, b("d/08"), lo, H, 100_000.0, 1.0, MAX_KEYS))
                .as("m_r == m ⇒ no clamp")
                .isFalse();
        // m_r > m: reflection reached beyond the far-ahead fraction — the uniform case, no clamp.
        assertThat(StealMath.shouldClampToReflected(cursor, m, b("d/085"), lo, H, 100_000.0, 1.0, MAX_KEYS))
                .as("m_r > m ⇒ no clamp")
                .isFalse();
    }

    @Test
    void nullReflectedPivotNeverClamps() {
        assertThat(StealMath.shouldClampToReflected(b("d/02"), b("d/08"), null, b("d/00"), b("d/09"),
                100_000.0, 1.0, MAX_KEYS))
                .as("no reflected pivot ⇒ no clamp")
                .isFalse();
    }

    @Test
    void overshootWhoseClampedTailIsBelowTheFloorIsNotClamped() {
        // m_r overshoot exists, but est is tiny (100) so the clamped child tail (m_r, H] cannot clear the
        // 2-page floor — the clamp is refused rather than carving a confetti child at the reflected pivot.
        assertThat(StealMath.shouldClampToReflected(b("d/02"), b("d/08"), b("d/04"), b("d/00"), b("d/09"),
                100.0, 1.0, MAX_KEYS))
                .as("overshoot but clamped tail below the floor ⇒ no clamp")
                .isFalse();
    }

    // ---- full-engine engagement smoke -----------------------------------------------------------

    private static final byte[] LO = b("d/00");
    private static final byte[] HI = b("d/05");

    /** A dense clustered directory whose keys leave a code-point gap below {@code HI}, so the far-ahead
     * owner-split pivot overshoots and the reflected pivot lands lower — the clamp's engagement shape. */
    private static List<byte[]> denseFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("d/%06d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    private static Map<String, Long> runScan(Path dir, String label, List<byte[]> keyspace,
                                             EngineToggles toggles) throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder().keys(keyspace).build();
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve(label + ".sqlite"))) {
            RunMeta run = store.openRun(new RunKey("s3", null, "bucket", new byte[0], label,
                    "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl"), false, false);
            store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE, LO, HI, null, null));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles),
                    mock, store, 4, MAX_KEYS, seeds, FilterChain.EMPTY);
            PipelineDrain.collectKeys(2000, engine, emitted);
        }
        assertThat(emitted).as("byte-exact coverage regardless of the clamp").hasSize(keyspace.size());
        return metrics.diagnostics(Duration.ZERO).stealReasons();
    }

    @Test
    @Timeout(60)
    void clampEngagesWithReflectOnAndNeverWithReflectOff(@TempDir Path dir) throws Exception {
        List<byte[]> keyspace = denseFlat(20_000);

        Map<String, Long> on = runScan(dir, "clamp-on", keyspace, EngineToggles.DEFAULT);
        assertThat(on.getOrDefault("OWNER_SPLIT.self_published", 0L))
                .as("owner-splits fired on the dense drain").isGreaterThanOrEqualTo(1L);
        assertThat(on.getOrDefault("OWNER_SPLIT.pivot_reflect_clamped", 0L))
                .as("reflect on: the overshooting owner-split pivot was clamped at least once")
                .isGreaterThanOrEqualTo(1L);

        EngineToggles reflectOff = EngineToggles.DEFAULT.withReflect(false);
        Map<String, Long> off = runScan(dir, "clamp-off", keyspace, reflectOff);
        assertThat(off.getOrDefault("OWNER_SPLIT.pivot_reflect_clamped", 0L))
                .as("reflect off: the clamp never engages")
                .isZero();
        assertThat(off.getOrDefault("TOGGLE.reflect_off", 0L))
                .as("the once-per-scan reflect ablation mark fired")
                .isEqualTo(1L);
    }
}
