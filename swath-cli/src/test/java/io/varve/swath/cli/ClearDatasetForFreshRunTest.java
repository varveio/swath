/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A FRESH (non-resumed) parquet run must not inherit a stale dataset's markers/parts.
 * {@link DatasetDirGuard#prepareDatasetForFreshRun} is what {@code runEngineParquet}/{@code runSortedParquet}
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
        Manifest.write(outputDir, "stale", "message swath { required binary key (STRING); }",
                List.of(), false, null);
        Files.writeString(layout.symlink(), "data/part-wZ-99999.parquet\n");
        Path dataDir = Files.createDirectories(layout.dataDir());
        Path staleWide = dataDir.resolve("part-wZ-99999.parquet");
        Path staleSorted = dataDir.resolve("part-00007.parquet");
        Files.writeString(staleWide, "stale wide part from a prior run");
        Files.writeString(staleSorted, "stale sorted part from a prior sorted run");

        // The staging dir MUST NOT be touched (the sort path owns it).
        Path stagingDir = Files.createDirectories(outputDir.resolve(ListCommand.SORT_STAGING_DIR));
        Path stagingLeftover = stagingDir.resolve("seg-0.parquet");
        Files.writeString(stagingLeftover, "owned by the sort path, not prepareDatasetForFreshRun");

        DatasetDirGuard.FreshDirectoryState guarded =
                DatasetDirGuard.guardFreshRunDatasetDir(outputDir, true, false);
        DatasetDirGuard.prepareDatasetForFreshRun(outputDir, "new-hash", 84L, guarded);

        // All four root markers are gone.
        assertThat(layout.success()).doesNotExist();
        assertThat(Manifest.readIdentity(outputDir)).contains(new Manifest.Identity("new-hash", 84L));
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

        DatasetDirGuard.FreshDirectoryState guarded =
                DatasetDirGuard.guardFreshRunDatasetDir(outputDir, false, false);
        DatasetDirGuard.prepareDatasetForFreshRun(outputDir, "hash", 1L, guarded);

        assertThat(layout.dataDir()).isDirectory();
        assertThat(layout.dataParts()).isEmpty();
        assertThat(Manifest.readIdentity(outputDir)).contains(new Manifest.Identity("hash", 1L));
    }

    @Test
    void markerlessPartLookingFilesAreNeverOwnershipEvidenceOrDeleted(@TempDir Path root)
            throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("foreign"));
        Path dataDir = Files.createDirectories(outputDir.resolve("data"));
        Path personalJsonl = Files.writeString(dataDir.resolve("part-personal.jsonl"), "mine\n");
        Path personalTsv = Files.writeString(dataDir.resolve("part-personal.tsv"), "mine\n");
        Path personalParquet = Files.writeString(dataDir.resolve("part-personal.parquet"), "mine\n");

        assertThatThrownBy(() -> DatasetDirGuard.guardFreshRunDatasetDir(outputDir, true, true))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("no swath dataset");
        assertThatThrownBy(() -> DatasetDirGuard.prepareDatasetForFreshRun(
                outputDir, "hash", 1L, DatasetDirGuard.FreshDirectoryState.OWNED))
                .isInstanceOf(InvalidArgsException.class)
                .hasMessageContaining("no durable swath ownership evidence");

        assertThat(personalJsonl).hasContent("mine\n");
        assertThat(personalTsv).hasContent("mine\n");
        assertThat(personalParquet).hasContent("mine\n");
    }

    @Test
    void incompleteDatasetWithEarlyOwnershipMarkerCanRestart(@TempDir Path root) throws Exception {
        Path outputDir = Files.createDirectories(root.resolve("incomplete"));
        DatasetLayout layout = DatasetLayout.of(outputDir);
        Manifest.writeState(outputDir, "old-hash", 7L);
        Path oldPart = Files.writeString(
                Files.createDirectories(layout.dataDir()).resolve("part-w0-00000.jsonl"), "old\n");

        DatasetDirGuard.FreshDirectoryState guarded =
                DatasetDirGuard.guardFreshRunDatasetDir(outputDir, false, true);
        DatasetDirGuard.prepareDatasetForFreshRun(outputDir, "new-hash", 8L, guarded);

        assertThat(oldPart).doesNotExist();
        assertThat(Manifest.readIdentity(outputDir)).contains(new Manifest.Identity("new-hash", 8L));
    }
}
