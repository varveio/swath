/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

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
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.ParquetWriterPool;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMode;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two properties of the on-disk sort output: (1) the manifest is self-describing — sortedness, sort
 * key, and per-file rowCount/minKey/maxKey are all present, with a strict cross-file minKey/maxKey
 * disjointness invariant across the whole publish; (2) every sorted final uses the uniform
 * {@code data/part-NNNNN.parquet} naming, never a {@code sorted-*.parquet} legacy name.
 *
 * <p>Everything is read through raw {@link com.fasterxml.jackson} JSON + the filesystem, so the
 * class compiles against current code and fails only at assertion time (no not-yet-written Java
 * API is referenced).
 */
final class SortOutputLayoutContractTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ARGS_HASH = "sort-layout-hash";
    private static final int MAX_KEYS = 32;

    private static RunKey sortKey() {
        return new RunKey("s3", null, "bucket", new byte[0], ARGS_HASH,
                "WORK_STEALING", ListingMode.OBJECTS, "", "parquet",
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), true);
    }

    private static ListRunner.ParquetSpec spec() {
        return new ListRunner.ParquetSpec(new byte[0], 256, MAX_KEYS, FilterChain.EMPTY,
                2, 1024, 16, ARGS_HASH, null, null, 0L, 0L, "");
    }

    /** Small segment gate ⇒ many segments; tiny final-file roll ⇒ MANY final parts (≥2 guaranteed). */
    private static SortConfig multiFinalConfig() {
        return SortConfigs.base()
                .withSegmentEntries(4)
                .withFinalFileBytes(1L);
    }

    @Test
    @Timeout(120)
    void sortedManifest_isSelfDescribing_andCrossFileMinMaxDisjoint(@TempDir Path tmp) throws Exception {
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(60);
        Path outputDir = publishSorted(tmp, keyspace);
        DatasetLayout layout = DatasetLayout.of(outputDir);

        JsonNode manifest = MAPPER.readTree(layout.manifest().toFile());

        // --- top-level sortedness metadata (NEW) ---
        assertThat(manifest.has("sorted"))
                .as("manifest.json must carry a top-level boolean 'sorted' for a --sort run").isTrue();
        assertThat(manifest.path("sorted").asBoolean(false))
                .as("a --sort run publishes sorted==true").isTrue();
        assertThat(manifest.hasNonNull("sortKey"))
                .as("manifest.json must carry a non-null 'sortKey' for a --sort run").isTrue();

        JsonNode files = manifest.path("files");
        assertThat(files.isArray()).isTrue();
        assertThat(files.size())
                .as("test forces >1 final part so the cross-file min/max invariant is meaningful")
                .isGreaterThanOrEqualTo(2);

        // Iterate files[] in LEXICAL filename order (== global key order for %05d names).
        List<JsonNode> ordered = new ArrayList<>();
        files.forEach(ordered::add);
        ordered.sort(Comparator.comparing(f -> bareName(f.path("key").asText())));

        String prevMax = null;
        for (JsonNode f : ordered) {
            // --- per-file metadata (NEW) ---
            assertThat(f.path("rowCount").isIntegralNumber())
                    .as("each files[] entry needs an integer rowCount").isTrue();
            assertThat(f.path("rowCount").asLong())
                    .as("rowCount > 0 for a real part").isPositive();
            assertThat(f.hasNonNull("minKey")).as("each sorted files[] entry needs minKey").isTrue();
            assertThat(f.hasNonNull("maxKey")).as("each sorted files[] entry needs maxKey").isTrue();

            String minKey = f.path("minKey").asText();
            String maxKey = f.path("maxKey").asText();
            assertThat(compareUnsigned(minKey, maxKey))
                    .as("per-file minKey <= maxKey (unsigned byte)").isLessThanOrEqualTo(0);

            // Cross-file strict disjointness: maxKey(i) < minKey(i+1) (unsigned byte).
            if (prevMax != null) {
                assertThat(compareUnsigned(prevMax, minKey))
                        .as("cross-file invariant files[i].maxKey < files[i+1].minKey").isLessThan(0);
            }
            prevMax = maxKey;

            // --- CROSS-CHECK the manifest is not lying: real first/last key of the part ---
            Path part = layout.resolveKey(f.path("key").asText());
            List<String> partKeys = ParquetReads.keys(part);
            assertThat(partKeys).isNotEmpty();
            assertThat(f.path("rowCount").asLong())
                    .as("manifest rowCount == real row count of the part").isEqualTo(partKeys.size());
            assertThat(minKey)
                    .as("manifest minKey == real first key of the sorted part").isEqualTo(partKeys.get(0));
            assertThat(maxKey)
                    .as("manifest maxKey == real last key of the sorted part")
                    .isEqualTo(partKeys.get(partKeys.size() - 1));
        }
    }

    @Test
    @Timeout(120)
    void unsortedManifest_saysSortedFalse_andStillCarriesRowCount(@TempDir Path tmp) throws Exception {
        Path outputDir = publishUnsorted(tmp);
        DatasetLayout layout = DatasetLayout.of(outputDir);

        JsonNode manifest = MAPPER.readTree(layout.manifest().toFile());
        assertThat(manifest.has("sorted"))
                .as("manifest.json must carry a top-level boolean 'sorted' even for a no-sort run").isTrue();
        assertThat(manifest.path("sorted").asBoolean(true))
                .as("a no-sort run publishes sorted==false").isFalse();

        JsonNode files = manifest.path("files");
        assertThat(files.size()).isGreaterThanOrEqualTo(1);
        for (JsonNode f : files) {
            assertThat(f.path("rowCount").isIntegralNumber())
                    .as("per-file rowCount present even for a no-sort run").isTrue();
            assertThat(f.path("rowCount").asLong()).isPositive();
        }
    }

    @Test
    @Timeout(120)
    void sortedFinals_useUniformPartName_noSortedPrefixRemains(@TempDir Path tmp) throws Exception {
        Path outputDir = publishSorted(tmp, Keyspaces.singlePrefixFlat(60));
        List<Path> parts = DatasetLayout.of(outputDir).dataParts();
        assertThat(parts.size()).isGreaterThanOrEqualTo(2);
        for (Path p : parts) {
            String name = p.getFileName().toString();
            assertThat(name)
                    .as("sorted finals must be uniform data/part-NNNNN.parquet (5-digit)")
                    .matches("part-\\d{5}\\.parquet");
            assertThat(name)
                    .as("no legacy sorted-*.parquet may remain after publish")
                    .doesNotStartWith("sorted-");
        }
    }

    @Test
    @Timeout(120)
    void sortedParts_together_containEveryInputKey_exactlyOnce_inGlobalOrder(@TempDir Path tmp) throws Exception {
        // Completeness ACROSS the rolled parts. The tests above check each part's manifest metadata
        // (rowCount, minKey/maxKey, naming) against that part's own contents, and the cross-file invariant
        // maxKey(i) < minKey(i+1) — every one of which a merge that DROPPED an entire part's worth of rows
        // still satisfies (the survivors stay self-consistent and disjoint). Nothing pinned that the parts
        // TOGETHER carry the whole input. Concatenating them in name order (== global key order) and
        // comparing against the LISTED INPUT keyspace closes that: no lost row, no duplicated row, in order.
        List<byte[]> keyspace = Keyspaces.singlePrefixFlat(60);
        Path outputDir = publishSorted(tmp, keyspace);

        List<Path> parts = DatasetLayout.of(outputDir).dataParts();   // already name-sorted
        assertThat(parts.size())
                .as("this config rolls >= 2 parts, so cross-part completeness is meaningful")
                .isGreaterThanOrEqualTo(2);

        List<String> concatenated = new ArrayList<>();
        for (Path part : parts) {
            concatenated.addAll(ParquetReads.keys(part));
        }

        List<String> expected = keyspace.stream()
                .sorted(Arrays::compareUnsigned)
                .map(k -> new String(k, StandardCharsets.UTF_8))
                .toList();
        assertThat(concatenated)
                .as("the rolled parts, concatenated in name order, ARE the input keyspace — exactly once "
                        + "each, in global sorted order (a merge that lost a part's rows must fail here)")
                .containsExactlyElementsOf(expected);
    }

    // --- harness ---

    /** Run the real listing → sort → merge → publish path; returns the dataset root with ≥2 finals. */
    private static Path publishSorted(Path tmp, List<byte[]> keyspace) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("out"));
        Path stagingDir = outputDir.resolve(".swath-sort-segments");
        Files.createDirectories(stagingDir);
        Path db = tmp.resolve("c.sqlite");

        RunContext ctx = RunContext.create();
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db, ctx.metrics())) {
            RunMeta run = store.openRun(sortKey(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), true);

            MockPageFetcher fetcher = MockPageFetcher.builder().keys(keyspace).maxKeysCap(MAX_KEYS).build();
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec(),
                    store, run.id(), 4, seeds, multiFinalConfig(), SortMode.OBJECTS,
                    EngineToggles.DEFAULT, TraceSink.NONE,
                    false);
        }
        return outputDir;
    }

    /** Run the real unsorted writer-pool publish; returns the dataset root (data/part-w* finals). */
    private static Path publishUnsorted(Path tmp) throws Exception {
        Path outputDir = Files.createDirectories(tmp.resolve("unsorted"));
        try (ParquetWriterPool pool =
                     new ParquetWriterPool(outputDir, ParquetSchema.canonical(), ARGS_HASH, 1, 64 * 1024, 8)) {
            for (int p = 0; p < 12; p++) {
                pool.submit(batch(0, p, p * 1000, p * 1000 + 1000));
            }
        }
        return outputDir;
    }

    private static PageBatch batch(long nodeId, long seq, int from, int to) {
        List<ListEntry> entries = new ArrayList<>();
        for (int i = from; i < to; i++) {
            entries.add(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8(String.format("n%d/key-%08d", nodeId, i)),
                    100, 1_700_000_000_000_000L, "etag", "STANDARD", null, true, null, null));
        }
        return new PageBatch(nodeId, seq, entries);
    }

    private static String bareName(String key) {
        int slash = key.lastIndexOf('/');
        return slash < 0 ? key : key.substring(slash + 1);
    }

    private static int compareUnsigned(String a, String b) {
        return Arrays.compareUnsigned(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
