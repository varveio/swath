/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.ParquetWriterPool;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PageBatches;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Key properties: the --sort staging dir is the visible
 * {@link ListCommand#SORT_STAGING_DIR} directory, and the five on-disk states — fresh, crashed
 * without {@code --sort}, crashed mid-{@code --sort}, complete sorted, and complete unsorted — are
 * mutually distinguishable by {@code _SUCCESS} / {@code _staging} / {@code manifest.sorted} /
 * {@code data/part-w*} alone.
 *
 * <p>Without a visible {@code _staging} marker and a {@code manifest.sorted} flag, the classifier
 * below could not tell a crashed {@code --sort} from a fresh dir, nor a sorted-complete from an
 * unsorted-complete.
 */
final class VisibleStagingAndCrashStateContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ARGS_HASH = "crash-state-hash";
    private static final int MAX_KEYS = 32;

    // ==================== visible staging dir ====================

    @Test
    void stagingDirIsVisible_staging_notHiddenSwathSortSegments(@TempDir Path tmp) throws Exception {
        // The staging dir NAME is the product's choice; the contract requires the visible "_staging".
        assertThat(ListCommand.SORT_STAGING_DIR)
                .as("part B: the --sort staging dir must be the visible '_staging', not a hidden dot-dir")
                .isEqualTo("_staging");

        // Realize a mid-sort layout using the product-chosen name and assert the visible-name contract.
        Path root = Files.createDirectories(tmp.resolve("out"));
        Path staging = root.resolve(ListCommand.SORT_STAGING_DIR);
        Files.createDirectories(staging);
        Files.writeString(staging.resolve("segment-0-x.parquet"), "x");

        assertThat(Files.isDirectory(root.resolve("_staging")))
                .as("visible _staging/ present in a mid-sort state").isTrue();
        assertThat(Files.isDirectory(root.resolve(".swath-sort-segments")))
                .as("hidden .swath-sort-segments/ must not be used").isFalse();
    }

    // ==================== crash-state legibility ====================

    /** The five distinguishable on-disk conditions of a dataset root. */
    private enum State { FRESH, CRASHED_NOSORT, CRASHED_SORT, COMPLETE_SORTED, COMPLETE_UNSORTED }

    @Test
    @Timeout(120)
    void fourCrashStatesAreMutuallyDistinguishable(@TempDir Path tmp) throws Exception {
        Map<State, Path> roots = new EnumMap<>(State.class);
        roots.put(State.FRESH, buildFresh(tmp));
        roots.put(State.CRASHED_NOSORT, buildCrashedNoSort(tmp));
        roots.put(State.CRASHED_SORT, buildCrashedSort(tmp));
        roots.put(State.COMPLETE_SORTED, buildCompleteSorted(tmp));
        roots.put(State.COMPLETE_UNSORTED, buildCompleteUnsorted(tmp));

        // Each built state must classify to exactly its own meaning...
        Map<State, State> observed = new LinkedHashMap<>();
        for (Map.Entry<State, Path> e : roots.entrySet()) {
            observed.put(e.getKey(), classify(e.getValue()));
        }
        for (State s : State.values()) {
            assertThat(observed.get(s))
                    .as("state %s must be classifiable as %s from (_SUCCESS,_staging,manifest.sorted,part-w) "
                            + "— got %s", s, s, observed.get(s))
                    .isEqualTo(s);
        }
        // ...and no two states may collide onto the same classification.
        assertThat(observed.values()).doesNotHaveDuplicates();
    }

    /**
     * The consumer's crash-state classifier — reads ONLY the four contract signals: {@code _SUCCESS}
     * present, visible {@code _staging/} present, {@code manifest.sorted} (true/false/absent), and
     * whether {@code data/part-w*} finals exist. Returns {@code null} when those signals do not match
     * any of the five expected states.
     */
    private static State classify(Path root) throws IOException {
        boolean success = Files.exists(root.resolve("_SUCCESS"));
        boolean staging = Files.isDirectory(root.resolve("_staging"));
        Boolean sorted = manifestSorted(root);
        boolean partW = hasPartW(root);

        if (success && !staging) {
            if (Boolean.TRUE.equals(sorted)) {
                return State.COMPLETE_SORTED;
            }
            if (Boolean.FALSE.equals(sorted)) {
                return State.COMPLETE_UNSORTED;
            }
            return null;   // complete but manifest.sorted is absent ⇒ cannot tell sorted vs unsorted
        }
        if (!success && staging) {
            return State.CRASHED_SORT;
        }
        if (!success && !staging && partW) {
            return State.CRASHED_NOSORT;
        }
        if (!success && !staging && !partW) {
            return State.FRESH;
        }
        return null;
    }

    private static Boolean manifestSorted(Path root) throws IOException {
        Path mf = root.resolve("manifest.json");
        if (!Files.exists(mf)) {
            return null;
        }
        JsonNode s = MAPPER.readTree(mf.toFile()).get("sorted");
        return (s != null && s.isBoolean()) ? s.asBoolean() : null;
    }

    private static boolean hasPartW(Path root) throws IOException {
        Path data = root.resolve("data");
        if (!Files.isDirectory(data)) {
            return false;
        }
        try (Stream<Path> s = Files.list(data)) {
            return s.anyMatch(p -> p.getFileName().toString().startsWith("part-w"));
        }
    }

    // --- state builders ---

    private static Path buildFresh(Path tmp) throws IOException {
        return Files.createDirectories(tmp.resolve("fresh"));
    }

    private static Path buildCrashedNoSort(Path tmp) throws IOException {
        Path root = Files.createDirectories(tmp.resolve("crashed-nosort"));
        Path data = Files.createDirectories(root.resolve("data"));
        Files.writeString(data.resolve("part-w0-00000.parquet"), "x");   // a rotated part, no _SUCCESS
        return root;
    }

    private static Path buildCrashedSort(Path tmp) throws IOException {
        Path root = Files.createDirectories(tmp.resolve("crashed-sort"));
        // Staging dir created with the PRODUCT-chosen name — the classifier expects the visible one.
        Path staging = Files.createDirectories(root.resolve(ListCommand.SORT_STAGING_DIR));
        Files.writeString(staging.resolve("segment-0-x.parquet"), "x");
        return root;   // no _SUCCESS, no manifest
    }

    private static Path buildCompleteSorted(Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("complete-sorted"));
        Path stagingDir = outputDir.resolve(ListCommand.SORT_STAGING_DIR);
        Files.createDirectories(stagingDir);
        Path db = tmp.resolve("cs.sqlite");

        RunContext ctx = RunContext.create();
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(120);
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);
            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec(),
                    store, run.id(), 4, seeds, sortConfig(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE,
                    false);
        }
        return outputDir;
    }

    private static Path buildCompleteUnsorted(Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("complete-unsorted"));
        try (ParquetWriterPool pool =
                     new ParquetWriterPool(outputDir, ParquetSchema.canonical(), ARGS_HASH, 1, 64 * 1024, 8)) {
            for (int p = 0; p < 12; p++) {
                pool.submit(PageBatches.batch(0, p, p * 1000, p * 1000 + 1000));
            }
        }
        return outputDir;
    }

    // --- sorted harness plumbing ---

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    private static SortConfig sortConfig() {
        return SortConfigs.base()
                .withSegmentEntries(4);
    }
}
