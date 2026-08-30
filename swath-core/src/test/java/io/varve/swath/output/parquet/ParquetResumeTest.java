/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ParquetResume#discardNonFinalized} (I6/RES-4): the hard-crash path — a
 * real {@code part-*.parquet} left on disk that the checkpoint does not record as
 * finalized is deleted on resume; recorded-finalized parts and unrelated files
 * survive.
 *
 * <p>The leftover is produced exactly as a kill-9 would leave one: a {@link
 * PartWriter} that wrote real rows and was {@link PartWriter#discard() discard}ed —
 * the writer handle is released <b>without</b> the finalizing fsync and the part is
 * never recorded in the checkpoint, so its rows are not durable and resume must throw
 * it away.
 */
final class ParquetResumeTest {

    /** Write real {@link io.varve.swath.model.ListEntry} rows to a part; finalize (footer fsync) or discard it. */
    private static void writePart(Path path, boolean finalize, String... keys) throws Exception {
        PartWriter w = new PartWriter(path, ParquetSchema.canonical());
        for (String k : keys) {
            w.write(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8(k), 1L, 1_700_000_000_000_000L,
                    "etag", "STANDARD", null, true, null, null));
        }
        if (finalize) {
            w.close();      // footer + fsync ⇒ durable, recorded-finalized part
        } else {
            w.discard();    // release the handle without the finalizing fsync ⇒ a hard-crash leftover
        }
    }

    @Test
    void discardsNonFinalizedPartsKeepsFinalizedAndOtherFiles(@TempDir Path dir) throws Exception {
        // Parts live under <root>/data/; the canonical finalized names are data/-prefixed.
        Path data = DatasetLayout.of(dir).dataDir();
        Files.createDirectories(data);
        Path finalized0 = data.resolve("part-w0-00000.parquet");
        Path finalized1 = data.resolve("part-w1-00000.parquet");
        Path leftover = data.resolve("part-w0-00001.parquet");   // written + discarded, never recorded finalized
        Path manifest = DatasetLayout.of(dir).manifest();
        writePart(finalized0, true, "a", "b");
        writePart(finalized1, true, "c", "d");
        writePart(leftover, false, "e", "f");
        Files.writeString(manifest, "{\"parts\":[]}");

        ParquetResume.discardNonFinalized(dir,
                Set.of("data/part-w0-00000.parquet", "data/part-w1-00000.parquet"));

        // Finalized parts survive and remain readable, footer-complete parquet.
        assertThat(finalized0).exists();
        assertThat(finalized1).exists();
        assertThat(ParquetReads.keys(finalized0)).containsExactly("a", "b");
        assertThat(ParquetReads.keys(finalized1)).containsExactly("c", "d");

        assertThat(leftover).doesNotExist();                         // discarded — its rows aren't durable
        assertThat(manifest).exists();                               // non-part files untouched
        assertThat(Files.readString(manifest)).isEqualTo("{\"parts\":[]}");
    }

    @Test
    void missingDirectoryIsANoOp(@TempDir Path dir) throws Exception {
        ParquetResume.discardNonFinalized(dir.resolve("nope"), Set.of());   // does not throw
    }

    @Test
    void missingCheckpointFinalizedPartRefusesBeforeDeletingAnything(@TempDir Path dir)
            throws Exception {
        Path data = DatasetLayout.of(dir).dataDir();
        Files.createDirectories(data);
        Path stale = data.resolve("part-w0-00001.parquet");
        writePart(stale, false, "tail");

        assertThatThrownBy(() -> ParquetResume.discardNonFinalized(
                dir, Set.of("data/part-w0-00000.parquet")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("checkpoint-finalized Parquet part is missing");

        assertThat(stale).as("validation precedes the destructive stale-part sweep").exists();
    }

    @Test
    void checkpointFinalizedPartPathCannotEscapeTheDataset(@TempDir Path dir) throws Exception {
        Files.createDirectories(DatasetLayout.of(dir).dataDir());

        assertThatThrownBy(() -> ParquetResume.discardNonFinalized(
                dir, Set.of("data/../outside.parquet")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("unsafe checkpoint-finalized Parquet part path");
    }

    @Test
    void symlinkedDataDirectoryIsRefusedBeforeDeletingOutsideParts(@TempDir Path dir)
            throws Exception {
        Path outside = Files.createDirectory(dir.resolve("outside"));
        Path outsidePart = Files.writeString(outside.resolve("part-w0-00001.parquet"), "outside");
        Files.createSymbolicLink(DatasetLayout.of(dir).dataDir(), outside);

        assertThatThrownBy(() -> ParquetResume.discardNonFinalized(dir, Set.of()))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("data path is not an ordinary directory");

        assertThat(outsidePart).exists();
    }

    @Test
    void missingCheckpointFinalizedSortSegmentRefusesResume(@TempDir Path dir) throws Exception {
        Path staging = Files.createDirectory(dir.resolve("_staging"));
        assertThatThrownBy(() -> ParquetResume.discardNonFinalizedSegments(
                staging, Set.of("seg-1.pageseg")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("sort staging segment is missing");
    }

    @Test
    void checkpointFinalizedSortSegmentMustBeABarePageRunName(@TempDir Path dir) throws Exception {
        Path staging = Files.createDirectory(dir.resolve("_staging"));

        assertThatThrownBy(() -> ParquetResume.discardNonFinalizedSegments(
                staging, Set.of("../escape.pageseg")))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("unsafe retained sort staging segment name");
    }

    @Test
    void sortSweepDeletesOnlyOwnedStagingExtensions(@TempDir Path dir) throws Exception {
        Path staging = Files.createDirectory(dir.resolve("_staging"));
        Path retained = Files.writeString(staging.resolve("seg-keep.pageseg"), "keep");
        Path stalePageRun = Files.writeString(staging.resolve("seg-stale.pageseg"), "stale");
        Path staleLegacy = Files.writeString(staging.resolve("merge-stale.parquet"), "stale");
        Path unrelated = Files.writeString(staging.resolve("operator-note.txt"), "leave me");

        ParquetResume.discardNonFinalizedSegments(staging, Set.of("seg-keep.pageseg"));

        assertThat(retained).exists();
        assertThat(stalePageRun).doesNotExist();
        assertThat(staleLegacy).doesNotExist();
        assertThat(unrelated).exists();
    }
}
