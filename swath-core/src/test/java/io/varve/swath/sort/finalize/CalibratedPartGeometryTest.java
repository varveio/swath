/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.parquet.fixture.ParquetEntryReader;
import io.varve.swath.output.sorted.SortedDatasetCommitter;
import io.varve.swath.output.sorted.SortedDatasetCoordinator;
import io.varve.swath.output.sorted.SortedDatasetResult;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.output.sorted.StaleFinalSweep;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortBenchCorpus;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.spill.PageCompression;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproducibility guard for the production calibrated-byte part target. The same corpus and
 * configuration must yield the same parts regardless of how many encoders run and in what order they
 * finish, so that a part set can be compared across runs and across encoder counts by digest.
 */
final class CalibratedPartGeometryTest {
    private static final long FINAL_FILE_BYTES = 64L << 10;
    private static final long MERGE_BUDGET_BYTES = 512L << 20;
    private static final int SEGMENTS = 4;
    private static final int PAGES_PER_SEGMENT = 120;
    private static final int ROWS_PER_PAGE = 60;

    private final ListEntryComparator comparator = new ListEntryComparator();

    @Test
    void calibratedGeometryIsIdenticalAcrossEncoderCounts(@TempDir Path root) throws IOException {
        List<Path> corpus = corpus(root);

        Geometry serial = geometry(root.resolve("n1"), corpus, 1);
        Geometry four = geometry(root.resolve("n4"), corpus, 4);
        Geometry eight = geometry(root.resolve("n8"), corpus, 8);

        assertThat(serial.parts()).as("the corpus must roll several calibrated parts")
                .hasSizeGreaterThan(5);
        assertThat(four.parts()).as("four encoders: %s", four.parts())
                .isEqualTo(serial.parts());
        assertThat(eight.parts()).as("eight encoders: %s", eight.parts())
                .isEqualTo(serial.parts());
        assertThat(four.keys()).isEqualTo(serial.keys());
        assertThat(eight.keys()).isEqualTo(serial.keys());
    }

    @Test
    void repeatedCalibratedRunsProduceTheSameGeometry(@TempDir Path root) throws IOException {
        List<Path> corpus = corpus(root);

        Geometry first = geometry(root.resolve("first"), corpus, 4);
        Geometry second = geometry(root.resolve("second"), corpus, 4);

        assertThat(second.parts()).as("second run: %s", second.parts()).isEqualTo(first.parts());
        assertThat(second.keys()).isEqualTo(first.keys());
    }

    /**
     * Stage one immutable corpus whose pages overlap across segments, so routing exercises both
     * whole-page forwarding and cluster merging. Keys carry high-entropy suffixes because a corpus
     * that compresses almost perfectly would reach the byte target in too few parts to compare.
     */
    private List<Path> corpus(Path root) throws IOException {
        Path master = Files.createDirectories(root.resolve("corpus"));
        List<Path> segments = new ArrayList<>(SEGMENTS);
        for (int segment = 0; segment < SEGMENTS; segment++) {
            List<List<ListEntry>> pages = new ArrayList<>(PAGES_PER_SEGMENT);
            for (int page = 0; page < PAGES_PER_SEGMENT; page++) {
                List<ListEntry> rows = new ArrayList<>(ROWS_PER_PAGE);
                for (int row = 0; row < ROWS_PER_PAGE; row++) {
                    long value = (page * 1_000L + row) * SEGMENTS + segment;
                    rows.add(SortTestSupport.object(String.format("prefix/%05d/%016x%016x/s%02d",
                            page, value * 0x9e3779b97f4a7c15L,
                            Long.rotateLeft(value * 0xc2b2ae3d27d4eb4fL, 29), segment)));
                }
                rows.sort(comparator);
                pages.add(rows);
            }
            segments.add(SortTestSupport.writePages(
                    master.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), pages,
                    SortMode.OBJECTS, PageCompression.LZ4));
        }
        return List.copyOf(segments);
    }

    /**
     * Run one arm over hard links to the shared corpus, so every arm consumes byte-identical staging
     * inputs while still owning the staging directory the sorted path destroys.
     */
    private Geometry geometry(Path root, List<Path> corpus, int encoders) throws IOException {
        Path staging = Files.createDirectories(root.resolve("_staging"));
        Path output = Files.createDirectories(root.resolve("data"));
        List<Path> inputs = SortBenchCorpus.hardLinkCorpus(corpus, staging);
        SortConfig config = SortConfigs.base()
                .withMergeParallelism(encoders)
                .withFanIn(10_000)
                .withMergeBudgetBytes(MERGE_BUDGET_BYTES)
                .withFinalFileBytes(FINAL_FILE_BYTES);
        SortRun run = new SortRun(config, comparator, DuplicateHook.NO_OP, EqualKeyPolicy.ALLOW,
                SortMetrics.NO_OP, SortedFileWriterFactory.DEFAULT, () -> -1,
                StaleFinalSweep.OWN_PARTS_ONLY);
        SortedDatasetResult result = new SortedDatasetCoordinator(run).transform(inputs, output,
                staging, SortedDatasetCommitter.NO_OP, ignored -> { }, FinalPassListener.NO_OP);

        assertThat(result.finalizationParallelism())
                .as("the arm must actually admit the encoders it compares")
                .isEqualTo(encoders);
        assertThat(result.mergePasses()).as("cascade must not reshape the routed input").isEqualTo(1);
        List<Part> parts = new ArrayList<>(result.finalFiles().size());
        List<String> keys = new ArrayList<>();
        for (Path file : result.finalFiles()) {
            List<String> partKeys = keys(file);
            keys.addAll(partKeys);
            parts.add(new Part(file.getFileName().toString(), partKeys.getFirst(),
                    partKeys.getLast(), partKeys.size(), Files.size(file), digest(file)));
        }
        return new Geometry(List.copyOf(parts), List.copyOf(keys));
    }

    private static List<String> keys(Path file) throws IOException {
        List<String> keys = new ArrayList<>();
        try (ParquetEntryReader reader = new ParquetEntryReader(file)) {
            while (reader.hasNext()) {
                keys.add(reader.next().key().asString());
            }
        }
        return keys;
    }

    private static String digest(Path file) throws IOException {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        } catch (NoSuchAlgorithmException unsupported) {
            throw new UncheckedIOException(new IOException(unsupported));
        }
    }

    /** One published part's identity: its name, boundary keys, row count, and exact bytes. */
    private record Part(String name, String firstKey, String lastKey, int rows, long bytes,
                        String digest) {
    }

    private record Geometry(List<Part> parts, List<String> keys) {
    }
}
