/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.sorted;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.output.parquet.sorted.SortedParquetStamp;
import io.varve.swath.output.parquet.sorted.SortedParquetWriterFactory;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.EqualKeyPolicy;
import io.varve.swath.sort.FinalPassListener;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortConfigs;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.finalize.SortTestSupport;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link SortedDatasetCoordinator} wired with {@link SortedParquetWriterFactory} (instead of
 * {@link SortedFileWriterFactory#DEFAULT}): the published final file(s) carry the sortedness
 * stamp and small row groups; staging uses page-run files.
 */
class SortedDatasetCoordinatorStampingTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    @Test
    void finalFileCarriesTheStampAndSmallRowGroupsSegmentsDoNot(@TempDir Path root) throws IOException {
        Path output = Files.createDirectories(root.resolve("out"));
        Path staging = Files.createDirectories(root.resolve("out/_staging"));

        // Enough distinct, fixed-width keys and a tiny row-group knob to force >1 row group in the
        // final file — proving the stamped writer's row-group sizing actually took effect end-to-end.
        List<ListEntry> a = new ArrayList<>();
        List<ListEntry> b = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String key = String.format("%08d", i) + "x".repeat(190);
            (i % 2 == 0 ? a : b).add(objectEntry(key));
        }
        Path seg0 = writeSegment(staging, "seg-0.parquet", a);
        Path seg1 = writeSegment(staging, "seg-1.parquet", b);

        SortConfig config = SortConfigs.base().withFinalRowGroupBytes(4096);
        SortedParquetWriterFactory stampedFactory = new SortedParquetWriterFactory(config, SortMode.OBJECTS);
        SortedDatasetCoordinator transform = new SortedDatasetCoordinator(new SortRun(config, cmp, DuplicateHook.NO_OP,
                EqualKeyPolicy.ALLOW, SortMetrics.NO_OP, stampedFactory,
                SortRun.PROCESS_SOFT_FD_LIMIT, StaleFinalSweep.OWN_PARTS_ONLY));

        SortedDatasetResult result = transform.transform(
                List.of(seg0, seg1), output, staging, SortedDatasetCommitter.NO_OP,
                units -> { }, FinalPassListener.NO_OP);

        assertThat(result.finalFiles()).hasSize(1);
        Path finalFile = result.finalFiles().get(0);

        Optional<SortedParquetStamp> stamp = SortedParquetStamp.read(finalFile);
        assertThat(stamp).isPresent();
        assertThat(stamp.get().mode()).isEqualTo(SortMode.OBJECTS);

        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(finalFile))) {
            assertThat(reader.getRowGroups().size()).isGreaterThan(1);
        }
    }

    private Path writeSegment(Path dir, String name, List<ListEntry> sorted) throws IOException {
        return SortTestSupport.writePageRun(
                dir.resolve(name.replace(".parquet", StagingNames.PAGE_RUN_SUFFIX)), sorted, cmp);
    }

    private static ObjectEntry objectEntry(String key) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, null, null, null, false, null, null, null, null);
    }
}
