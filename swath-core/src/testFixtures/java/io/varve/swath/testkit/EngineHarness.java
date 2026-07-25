/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListingMode;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Drives the full {@link WorkStealingScan} engine over a fixed keyspace with stealing
 * forced (many workers + small pages), recording every committed split so PROP-1 can
 * prove both the realized emitted-key tiling and the structural range-set tiling. The
 * reusable backbone of the PROP-1 adversarial tests.
 *
 * <p>The efficiency/fan-out half of the engine test matrix lives in its sibling {@link
 * EfficiencyHarness}.
 */
public final class EngineHarness {

    private EngineHarness() {
    }

    /** The outcome of one engine run: the emitted keys (in arrival order) + the split partition. */
    public record Result(List<byte[]> emitted, List<RangePartition.Split> splits, long rootId) {
    }

    /**
     * The fixed run key both harnesses open their throwaway run under. Each run lives in its
     * own temp checkpoint DB, so the key's content hash never has to distinguish two runs.
     */
    static RunKey harnessRunKey() {
        return new RunKey("s3", null, "bucket", new byte[0], "prop1-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /**
     * Run {@code keyspace} through the engine ({@code workers} virtual-thread workers,
     * page size {@code maxKeys}) over a fresh checkpoint DB under {@code ckptDir}.
     */
    public static Result run(List<byte[]> keyspace, int workers, int maxKeys, Path ckptDir)
            throws Exception {
        return run(MockPageFetcher.builder().keys(keyspace).build(), workers, maxKeys, ckptDir);
    }

    /**
     * As {@link #run(List, int, int, Path)}, but drives a caller-supplied {@code fetcher} instead
     * of building a zero-latency one internally — e.g. one with a {@link
     * LatencyModel} wired in, to confirm the result is byte-exact regardless of injected latency.
     */
    public static Result run(MockPageFetcher fetcher, int workers, int maxKeys, Path ckptDir)
            throws Exception {
        try (SqliteCheckpointStore sqlite = SqliteCheckpointStore.open(ckptDir.resolve("ckpt.sqlite"))) {
            RecordingSplitStore store = new RecordingSplitStore(sqlite);
            RunMeta run = store.openRun(harnessRunKey(), false, false);
            long rootId = store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS),
                    fetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY);

            List<byte[]> emitted = PipelineDrain.collectKeys(1000, engine);
            return new Result(emitted, store.splits(), rootId);
        }
    }

    /**
     * The PROP-1 headline assertion: the emitted keys are byte-exactly the keyspace, each
     * exactly once (no gap, no overlap on the realized keyspace), AND the durable range set
     * reconstructed from the recorded splits tiles {@code (⊥, ⊤]} (I2/I3, structural).
     */
    public static void assertExactlyOnceAndTiles(Result r, List<byte[]> keyspace) {
        assertExactlyOnce(r.emitted(), keyspace);
        // Structural range-set tiling, independent of what was emitted.
        RangePartition.assertTilesFromSplits(r.rootId(), r.splits());
    }

    /**
     * The correctness half of the generality matrix: byte-exact set equality between the
     * emitted keys and {@code keyspace}, each key exactly once (no gap, no overlap on the
     * realized keyspace). Unlike {@link #assertExactlyOnceAndTiles}, this does NOT assert the
     * structural range-set tiling — the efficiency runs seed via {@code SHALLOW} (many seed
     * ranges, no single-root split chain), so structural tiling is the orchestrator's own
     * adversarial test, not this harness's concern.
     */
    public static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(KeyBytes::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(KeyBytes::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(emitted).as("no key emitted twice (no overlap)").hasSize(distinctEmitted.size());
        assertThat(distinctEmitted).as("every key emitted exactly once (no gap)")
                .hasSize(distinctKeyspace.size());
        var ek = distinctEmitted.iterator();
        var kk = distinctKeyspace.iterator();
        while (ek.hasNext()) {
            assertThat(Arrays.equals(ek.next(), kk.next())).as("byte-exact key (I10)").isTrue();
        }
    }
}
