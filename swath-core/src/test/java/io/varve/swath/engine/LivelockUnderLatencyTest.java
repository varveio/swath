/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.EngineHarness;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PipelineDrain;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Liveness regression guard for the <b>latency-blind-spot livelock</b>: with the default high
 * worker count ({@code 64}) against a <i>multi-page, latency-bearing</i> keyspace, idle workers
 * steal-split-probe the busiest worker's tail <b>faster than pages return</b>, narrowing each
 * owner's {@code hi} into the empty gap just above its cursor before the page that would advance
 * the cursor comes back. The owner then completes its (now-empty) range without emitting, the
 * real keys are handed to a child, and the waiting thief horde re-narrows the child the same
 * way — an unbounded spin of durable splits and 1-key probes with the page fetch starved.
 * {@code total_emitted} freezes after page 1; {@code splits}/{@code api_calls} climb without bound.
 *
 * <p>Zero-latency mocks never reproduce it (the fetch always keeps up). Here bulk page
 * fetches carry an injected per-page latency while thief probes (maxKeys == 1) stay instant —
 * the exact shape that lets thieves out-race the fetch and the sharpest deterministic
 * reproduction. The scan must complete within a bounded timeout, emit every key exactly once,
 * and stay call-bounded (no probe storm).
 *
 * <p>Progress-gated stealing — a victim is only re-split after it has emitted past the cursor
 * captured at its last steal — is what makes this converge quickly: without it, the livelock
 * never resolves and this test times out.
 */
final class LivelockUnderLatencyTest {

    private static final int WORKERS = 64;          // the real default --max-parallel-listings
    private static final int OBJECTS = 20_000;      // 20 pages at MAX_KEYS=1000
    private static final int MAX_KEYS = 1000;
    private static final Duration BULK_PAGE_LATENCY = Duration.ofMillis(20);
    private static final int EXPECTED_PAGES = OBJECTS / MAX_KEYS;

    static Stream<Arguments> keyspaces() {
        return Stream.of(
                // Deep-nested-prefix shape (the real IHTest/.../Path*/... layout): keys share a
                // long common prefix, so pivots land between deeply-nested cursors.
                Arguments.of("deep-nested", Keyspaces.deepTree(42L, 20, OBJECTS / 20)),
                // Flatter shape: a single dense prefix the stealer must bisect.
                Arguments.of("flat", Keyspaces.singlePrefixFlat(OBJECTS)));
    }

    private static RunKey key(String shape) {
        return new RunKey("s3", null, "bucket", new byte[0], "livelock-" + shape,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("keyspaces")
    @Timeout(45)
    void completesUnderLatencyAt64Workers(String shape, List<byte[]> keyspace, @TempDir Path dir)
            throws Exception {
        MockPageFetcher mock = MockPageFetcher.builder()
                .keys(keyspace)
                // Bulk page fetches are slow (real S3 latency); 1-key thief probes stay instant —
                // the worst case for the fetch-vs-steal race, and the sharpest reproduction.
                .latency(req -> req.maxKeys() > 1 ? BULK_PAGE_LATENCY : Duration.ZERO)
                .build();

        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(shape), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);
            assertThat(seeds).hasSize(1);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    mock, store, WORKERS, MAX_KEYS, seeds, FilterChain.EMPTY);

            PipelineDrain.collectKeys(1000, engine, emitted);
        }

        EngineHarness.assertExactlyOnce(emitted, keyspace);

        // Liveness is guarded by @Timeout above; correctness by assertExactlyOnce. This is only a
        // GENEROUS storm sanity-net: the true livelock spins to 10^6+ unbounded 1-key probes,
        // whereas the fixed engine stays well under this ceiling. The PRECISE probe count is
        // environment-sensitive (idle workers re-probe eligible victims, and that count scales
        // with concurrency / CPU speed), so this deliberately does NOT assert a tight probe
        // bound here — that flaked on slower CI runners. The fix bounds SPLITS (≈1 per emitted
        // page), not probes.
        assertThat(mock.apiCalls())
                .as("no catastrophic probe storm (%d objects, %d expected pages)",
                        OBJECTS, EXPECTED_PAGES)
                .isLessThan(100_000L);
    }
}
