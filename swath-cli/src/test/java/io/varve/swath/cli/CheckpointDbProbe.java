/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.checkpoint.RunStatus;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * A shared, read-only probe over a checkpoint sqlite file — asserts what the checkpoint durably
 * recorded WITHOUT going through {@link io.varve.swath.checkpoint.SqliteCheckpointStore}'s typed
 * API.
 *
 * <p><b>Why raw JDBC, not {@code SqliteCheckpointStore.open}.</b> Several call sites read the file
 * at lifecycle moments a second typed store cannot safely reach:
 * <ul>
 *   <li>while a {@code SqliteCheckpointStore} is STILL open on the same file in the same JVM
 *       (e.g. {@code FatalErrorMarksRunFailedTest} asserts mid-run status inside the store's own
 *       try-with-resources block) — opening a second store there would spin up a second writer
 *       thread and re-run the {@code CREATE TABLE}/migration DDL against a file a live store
 *       already owns, exercising a concurrent-writer path production code never actually takes;</li>
 *   <li>immediately after an external process is {@code kill -9}'d (crash-resume tests) or after
 *       a CLI invocation returns, where the probe only needs to read already-committed state
 *       without resurrecting the writer machinery (or a fresh WAL/DDL pass) at all.</li>
 * </ul>
 * A lightweight, connection-only read matches the precedent {@code SqliteCheckpointStore} itself
 * sets with {@code readLatestRun}: a raw {@link DriverManager} connection, no writer thread, no
 * DDL. This probe follows the same shape rather than opening a second typed store per read.
 */
final class CheckpointDbProbe {

    private CheckpointDbProbe() {
    }

    /** Total committed {@code listing_node} rows across the whole file (single-run test DBs only). */
    static long nodeCount(Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM listing_node")) {
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }

    /** The most recently inserted run's {@code run_meta.status} (single-run test DBs only). */
    static String runStatus(Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT status FROM run_meta ORDER BY id DESC LIMIT 1")) {
            assertThat(rs.next()).as("run_meta row exists").isTrue();
            return rs.getString(1);
        }
    }

    /** {@code run_meta.status} for a specific run id. */
    static String runStatus(Path db, long runId) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             PreparedStatement ps = c.prepareStatement("SELECT status FROM run_meta WHERE id=?")) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("run_meta row exists").isTrue();
                return rs.getString(1);
            }
        }
    }

    /** {@code run_meta.status} for a specific run id, as the typed {@link RunStatus} enum. */
    static RunStatus runStatusEnum(Path db, long runId) throws Exception {
        return RunStatus.valueOf(runStatus(db, runId));
    }

    /** The most recently inserted run's {@code run_meta.fatal_error} (single-run test DBs only). */
    static boolean fatalError(Path db) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT fatal_error FROM run_meta ORDER BY id DESC LIMIT 1")) {
            assertThat(rs.next()).as("run_meta row exists").isTrue();
            return rs.getInt(1) != 0;
        }
    }
}
