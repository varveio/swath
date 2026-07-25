/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.model.ListingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Shared checkpoint-store test scaffolding, package-private to {@code io.varve.swath.checkpoint}'s
 * own test tree (nothing outside swath-core touches {@link RunKey}/{@link SqliteCheckpointStore}
 * test doubles, so a testFixtures home would be speculative surface):
 * <ul>
 *   <li>the {@code b()}/{@code key()} helpers duplicated across {@link SqliteCheckpointStoreTest}
 *       and {@link SqliteCheckpointStoreMetricsTest};</li>
 *   <li>{@link #writePreContextRunMeta} and the {@link #PRE_CONTEXT_RUN_META_DDL} behind it — a
 *       checkpoint DB at the current schema version whose {@code run_meta} stops at the base
 *       columns, used to prove the idempotent {@code ALTER TABLE} backfill migrates it cleanly
 *       ({@link SqliteCheckpointStoreTest}, {@link SqliteCheckpointStoreSortStateTest},
 *       {@link CheckpointSchemaTest}) and, unstamped, to stand in for a foreign checkpoint
 *       ({@link CheckpointSchemaVersionTest}).</li>
 * </ul>
 */
final class CheckpointStoreTestSupport {

    private CheckpointStoreTestSupport() {
    }

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static RunKey key(String argsHash) {
        return new RunKey("s3", null, "bucket", b("p/"), argsHash,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    /**
     * Turn an empty DB into the state a column addition has to migrate: an in-version checkpoint
     * (stamped {@link CheckpointSchema#SCHEMA_VERSION}, since every version the gate accepts is
     * stamped) whose {@link #PRE_CONTEXT_RUN_META_DDL} {@code run_meta} the run-context/sort columns
     * still have to be backfilled onto.
     */
    static void writePreContextRunMeta(Connection c) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute("PRAGMA user_version=" + CheckpointSchema.SCHEMA_VERSION);
            st.execute(PRE_CONTEXT_RUN_META_DDL);
        }
    }

    /**
     * A minimal pre-context/pre-sort {@code run_meta} schema — no {@code no_sign_request}/
     * {@code profile}/{@code region}/{@code fetch_owner}/{@code raw_output}/{@code output_path}/
     * {@code sort_enabled}/{@code sort_phase} columns — modelling a checkpoint DB written before
     * those columns joined the schema, for the idempotent {@code ALTER TABLE} backfill migration
     * tests.
     */
    static final String PRE_CONTEXT_RUN_META_DDL =
            "CREATE TABLE run_meta ("
                    + "id INTEGER PRIMARY KEY, store_scheme TEXT NOT NULL, endpoint TEXT, "
                    + "bucket TEXT NOT NULL, prefix BLOB NOT NULL, args_hash TEXT NOT NULL, "
                    + "strategy TEXT NOT NULL, filter_spec TEXT, output_format TEXT, "
                    + "mode TEXT NOT NULL CHECK (mode IN ('OBJECTS','VERSIONS')), "
                    + "started_at INTEGER NOT NULL, finished_at INTEGER, "
                    + "status TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')))";
}
