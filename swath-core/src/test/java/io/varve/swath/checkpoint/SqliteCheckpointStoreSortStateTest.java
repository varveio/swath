/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListingMode;
import io.varve.swath.output.OutputFormat;
import java.nio.file.Path;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code --sort} run-meta state: {@code sort_enabled} (drives the resume mode-mismatch refusal)
 * and the {@code sort_phase} LISTING → MERGING → PUBLISHED machine round-trip through
 * {@link SqliteCheckpointStore}, incl. the idempotent {@code ALTER TABLE} backfill onto a
 * pre-sort checkpoint DB.
 */
final class SqliteCheckpointStoreSortStateTest {

    private static RunKey key(boolean sortEnabled) {
        return new RunKey("s3", null, "bucket", new byte[0], "hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", OutputFormat.PARQUET.name(),
                new SoftRestoreContext(false, null, null, false, false, "out", false, null, null), sortEnabled);
    }

    @Test
    void freshSortRunStartsInListingAndRoundTripsPhases(@TempDir Path tmp) throws Exception {
        Path db = tmp.resolve("c.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(true), false, false);
            assertThat(run.sortEnabled()).isTrue();
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.LISTING);

            store.setSortPhase(run.id(), SortPhase.MERGING);
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.MERGING);
            store.setSortPhase(run.id(), SortPhase.PUBLISHED);
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.PUBLISHED);

            RunMeta resumed = store.openRun(key(true), true, false);
            assertThat(resumed.resumed()).isTrue();
            assertThat(resumed.sortEnabled()).as("sort_enabled survives resume for the mismatch check").isTrue();
            assertThat(store.sortPhase(resumed.id())).isEqualTo(SortPhase.PUBLISHED);
        }
    }

    @Test
    void nonSortRunHasNullPhaseAndDisabledFlag(@TempDir Path tmp) throws Exception {
        Path db = tmp.resolve("c.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(false), false, false);
            assertThat(run.sortEnabled()).isFalse();
            assertThat(store.sortPhase(run.id())).isNull();
        }
    }

    @Test
    void backfillsSortColumnsOntoAPreSortCheckpointDb(@TempDir Path tmp) throws Exception {
        Path db = tmp.resolve("legacy.sqlite");
        // A minimal pre-sort run_meta (no sort_enabled / sort_phase columns), like an older DB.
        try (var c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath())) {
            CheckpointStoreTestSupport.writePreContextRunMeta(c);
        }
        // Opening the store migrates the missing columns; a fresh sort run then works end-to-end.
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            RunMeta run = store.openRun(key(true), false, false);
            assertThat(run.sortEnabled()).isTrue();
            assertThat(store.sortPhase(run.id())).isEqualTo(SortPhase.LISTING);
        }
    }
}
