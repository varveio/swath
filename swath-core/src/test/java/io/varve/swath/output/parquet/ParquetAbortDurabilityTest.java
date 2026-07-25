/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static io.varve.swath.output.parquet.ParquetPoolTestSupport.parts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.varve.swath.error.OutputException;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.testkit.ParquetReads;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * I6 / RES-4 durability under abort and finalize-failure in {@link ParquetWriterPool}:
 *
 * <ol>
 *   <li><b>Abort must not finalize the discarded open part.</b> {@link
 *       PartWriter#discard()} releases the handle <i>without</i> an fsync, and the
 *       part is deleted and never enters the manifest. Do not finalize (call
 *       {@link PartWriter#close()}, which fsyncs) before deleting a discarded part:
 *       a crash in that window would leave a durably-finalized part the abort
 *       intended to throw away.</li>
 *   <li><b>A finalize failure must clean up its own partial part.</b> {@code
 *       finalizeCurrent} deletes the partial part on failure and surfaces the error
 *       directly, rather than relying on a later {@code abort()} to clean up: once
 *       the {@code joined} latch is set, {@code abort()} is a no-op and would leave
 *       the half-written file orphaned.</li>
 * </ol>
 */
class ParquetAbortDurabilityTest {

    private static PageBatch batch(long nodeId, int n) {
        List<ListEntry> entries = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            entries.add(ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8(String.format("k/%06d", i)),
                    1234, 1_700_000_000_000_000L, "etag", "STANDARD", null, true, null, null));
        }
        return new PageBatch(nodeId, 0, entries);
    }

    @Test
    void abortDiscardsTheOpenPartLeavingNoFinalizedFileNorManifestEntry(@TempDir Path dir) throws Exception {
        ParquetWriterPool pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "h",
                1, Long.MAX_VALUE, 8);   // huge rotation target ⇒ the part never finalizes mid-stream
        pool.submit(batch(0, 1000));

        // The open (non-finalized) part file exists on disk while being written.
        await().atMost(Duration.ofSeconds(5)).until(() -> !parts(dir).isEmpty());
        Path open = parts(dir).getFirst();
        assertThat(open).exists();

        pool.abort();

        // Discarded: the open part is gone, and no manifest claims it as durable.
        assertThat(parts(dir)).as("open part must be discarded on abort").isEmpty();
        Path manifest = DatasetLayout.of(dir).manifest();
        if (Files.exists(manifest)) {
            assertThat(Files.readString(manifest)).doesNotContain(open.getFileName().toString());
        }
    }

    @Test
    void rotatedFinalizedPartsSurviveAbort(@TempDir Path dir) throws Exception {
        // Small rotation target ⇒ early batches finalize into durable parts; a later
        // open part is then discarded by abort, but the finalized ones remain.
        ParquetWriterPool pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "h",
                1, 4096, 64);
        for (int i = 0; i < 40; i++) {
            pool.submit(batch(0, 1000));
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> !parts(dir).isEmpty());
        int finalizedBefore = parts(dir).size();
        assertThat(finalizedBefore).isGreaterThan(0);

        pool.abort();

        List<Path> after = parts(dir);
        assertThat(after).as("finalized parts are durable across abort").hasSizeGreaterThanOrEqualTo(finalizedBefore);
        // Every surviving part is a readable, footer-complete parquet (truly finalized).
        for (Path p : after) {
            assertThat(ParquetReads.keys(p)).isNotEmpty();
        }
    }

    @Test
    void finalizeFailureDeletesThePartialPartAndSurfacesTheError(@TempDir Path dir) throws Exception {
        assumeTrue(Files.getFileStore(dir).supportsFileAttributeView("posix"),
                "POSIX permissions needed to force a finalize failure");

        ParquetWriterPool pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "h",
                1, Long.MAX_VALUE, 8);
        pool.submit(batch(0, 1000));

        await().atMost(Duration.ofSeconds(5)).until(() -> !parts(dir).isEmpty());
        Path open = parts(dir).getFirst();

        // Make the part file read-only: writer.close() still flushes the footer through
        // its already-open fd, but close()'s fsync (a fresh WRITE open) is denied → the
        // finalize throws. The pool must delete the partial file (dir stays writable) and
        // surface the failure rather than orphaning it behind the `joined` latch.
        Set<PosixFilePermission> readOnly = EnumSet.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ);
        Files.setPosixFilePermissions(open, readOnly);
        try {
            assertThatThrownBy(pool::close).isInstanceOf(OutputException.class);
            assertThat(parts(dir)).as("the un-finalizable partial part must be deleted, not orphaned").isEmpty();
        } finally {
            if (Files.exists(open)) {
                Files.setPosixFilePermissions(open, EnumSet.allOf(PosixFilePermission.class));
            }
        }
    }
}
