/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.output.parquet.DatasetLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A FRESH (non-resumed) parquet run must not inherit a stale dataset's markers/parts.
 * {@link DatasetDirGuard#clearDatasetForFreshRun} is what {@code runEngineParquet}/{@code runSortedParquet}
 * call — only when {@code run.resumed() == false} (a resume MUST keep its finalized parts + markers) —
 * right after {@code openParquetDir()}, before any writing. Here we exercise the clearing directly
 * (the call-site guard is a plain {@code if (!run.resumed())}); a resumed run's survival is guarded by
 * {@link SortPublishedReentryTest}, which plants a {@code data/} part and asserts it survives reentry.
 */
final class ClearDatasetForFreshRunTest {

    @Test
    void clearsStaleMarkersAndDataPartsButRecreatesAnEmptyDataDir(@TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("out"));
        DatasetLayout layout = DatasetLayout.of(outputDir);

        // A completely stale prior dataset sharing this -o dir: a foreign identity, a false "complete"
        // marker, a consumer manifest, a symlink, plus an EXTRA data/ part from a wider prior run.
        Files.writeString(layout.success(), "");
        Files.writeString(layout.state(), "{\"args_hash\":\"foreign\",\"run_id\":42}");
        Files.writeString(layout.manifest(), "{\"sourceBucket\":\"stale\",\"files\":[]}");
        Files.writeString(layout.symlink(), "data/part-wZ-99999.parquet\n");
        Path dataDir = Files.createDirectories(layout.dataDir());
        Path staleWide = dataDir.resolve("part-wZ-99999.parquet");
        Path staleSorted = dataDir.resolve("part-00007.parquet");
        Files.writeString(staleWide, "stale wide part from a prior run");
        Files.writeString(staleSorted, "stale sorted part from a prior sorted run");

        // The staging dir MUST NOT be touched (the sort path owns it).
        Path stagingDir = Files.createDirectories(outputDir.resolve(ListCommand.SORT_STAGING_DIR));
        Path stagingLeftover = stagingDir.resolve("seg-0.parquet");
        Files.writeString(stagingLeftover, "owned by the sort path, not clearDatasetForFreshRun");

        DatasetDirGuard.clearDatasetForFreshRun(outputDir);

        // All four root markers are gone.
        assertThat(layout.success()).doesNotExist();
        assertThat(layout.state()).doesNotExist();
        assertThat(layout.manifest()).doesNotExist();
        assertThat(layout.symlink()).doesNotExist();
        // data/ still exists but is now EMPTY — the stale extra parts are gone.
        assertThat(dataDir).isDirectory();
        assertThat(layout.dataParts()).isEmpty();
        assertThat(staleWide).doesNotExist();
        assertThat(staleSorted).doesNotExist();
        // The staging dir and its content are untouched.
        assertThat(stagingLeftover).exists();
    }

    @Test
    void createsAnEmptyDataDirWhenNonePreexists(@TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("out"));
        DatasetLayout layout = DatasetLayout.of(outputDir);

        DatasetDirGuard.clearDatasetForFreshRun(outputDir);

        assertThat(layout.dataDir()).isDirectory();
        assertThat(layout.dataParts()).isEmpty();
    }
}
