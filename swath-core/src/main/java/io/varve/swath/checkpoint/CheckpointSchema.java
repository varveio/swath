/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.error.CheckpointException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * The checkpoint DB's schema: the {@code CREATE TABLE}/{@code CREATE INDEX} DDL, the version
 * stamp that says the file is a swath checkpoint this build understands, plus the idempotent
 * {@code ALTER TABLE ADD COLUMN} migration that brings an older checkpoint DB up to the current
 * column set. Lives apart from {@link SqliteCheckpointStore}'s runtime (writer thread, DAO, CAS)
 * because it is touched on exactly one code path — {@link #checkVersion} then {@link #apply} during
 * {@code open}, before the writer thread exists — and nowhere else.
 *
 * <p>Apart from the version refusal, this class carries no logger, metric, or exception identity
 * of its own: failures propagate as the raw {@link SQLException} so {@code open} still wraps them
 * into the same {@code CheckpointException} with the same message it always did.
 */
final class CheckpointSchema {

    /**
     * The checkpoint schema version, stamped into SQLite's {@code PRAGMA user_version} at creation
     * and required to match exactly on every open. All DDL here is {@code IF NOT EXISTS}/additive,
     * so without the stamp a foreign, damaged, or future-version database would be silently
     * half-adopted instead of refused.
     */
    static final int SCHEMA_VERSION = 1;

    /** {@code user_version} of a database no swath has stamped — including any non-swath SQLite file. */
    private static final int UNSTAMPED = 0;

    private CheckpointSchema() {
    }

    /**
     * Refuse anything that is not a checkpoint at {@link #SCHEMA_VERSION}, and report whether this
     * open is the one creating the database.
     *
     * <p><b>Runs before the connection's setup PRAGMAs.</b> Two {@code SELECT}-shaped reads are all
     * the gate needs, and a file swath is about to refuse must be left byte-identical — whereas
     * {@code journal_mode=WAL} is a persistent header change. So a foreign or unstamped file is
     * refused without swath having written a single byte to it.
     *
     * @param checkpointLabel the checkpoint's path (or in-memory marker), for the refusal message
     * @return true when the database is empty and unstamped, i.e. this open is creating it
     */
    static boolean checkVersion(Statement st, String checkpointLabel) throws SQLException, CheckpointException {
        int version = userVersion(st);
        boolean fresh = version == UNSTAMPED && isEmpty(st);
        if (!fresh && version != SCHEMA_VERSION) {
            throw new CheckpointException(versionRefusal(version, checkpointLabel));
        }
        return fresh;
    }

    /**
     * Create any missing tables/indexes and backfill any {@link #RUN_META_CONTEXT_COLUMNS} an
     * already-existing {@code run_meta} predates, stamping a database this open created. Idempotent,
     * and identical in outcome for a fresh DB and for one an earlier build wrote at this schema
     * version: {@link #BASE_DDL} declares only the columns that have existed since the first
     * release, and every column added since is declared once in {@link #RUN_META_CONTEXT_COLUMNS}
     * and applied by the same migration for fresh and older DBs alike.
     *
     * <p><b>Must run inside a transaction the caller commits.</b> SQLite makes both DDL and
     * {@code PRAGMA user_version} transactional, so committing them together is what keeps creation
     * atomic: without it, a crash (or a WAL tail lost to power failure) between the last
     * {@code CREATE TABLE} and the stamp would leave a durable, unstamped, half-built schema that
     * every later open — including {@code --restart} and {@code resume}, both of which run behind
     * this gate — would refuse until the operator deleted the file by hand.
     *
     * @param fresh {@link #checkVersion}'s verdict: stamp only a database this open created
     */
    static void apply(Statement st, boolean fresh) throws SQLException {
        for (String ddl : BASE_DDL) {
            st.execute(ddl);
        }
        migrateRunMetaContextColumns(st);

        if (fresh) {
            st.execute("PRAGMA user_version=" + SCHEMA_VERSION);
        }
    }

    private static int userVersion(Statement st) throws SQLException {
        try (ResultSet rs = st.executeQuery("PRAGMA user_version")) {
            return rs.next() ? rs.getInt(1) : UNSTAMPED;
        }
    }

    /** True for a database with no objects at all — i.e. one this open is creating. */
    private static boolean isEmpty(Statement st) throws SQLException {
        try (ResultSet rs = st.executeQuery("SELECT count(*) FROM sqlite_master")) {
            return rs.next() && rs.getInt(1) == 0;
        }
    }

    /**
     * There is no migration path. Version {@value #SCHEMA_VERSION} is the first swath ever stamped
     * and it ships with the first release, so no checkpoint in the field carries a lower one: an
     * {@link #UNSTAMPED} database is a foreign or damaged file (or one from a pre-release build
     * predating the stamp), and a higher version is a newer swath's. Either way the remedy is a
     * fresh run (or a newer swath), never an in-place upgrade.
     */
    private static String versionRefusal(int found, String checkpointLabel) {
        String preamble = "checkpoint schema version mismatch at " + checkpointLabel + ": found "
                + found + ", expected " + SCHEMA_VERSION;
        if (found > SCHEMA_VERSION) {
            return preamble + " (written by a newer swath); upgrade swath to read it, "
                    + "or start a fresh run";
        }
        return preamble + " (not a swath checkpoint, or one written before swath stamped a schema "
                + "version); start a fresh run";
    }

    /**
     * Run-context columns on {@code run_meta}: name → SQLite column DDL fragment (after the name).
     * Declared <b>only</b> here — {@link #BASE_DDL}'s {@code run_meta} deliberately stops at
     * {@code status}, so a fresh DB gains these through the very same
     * {@link #migrateRunMetaContextColumns} backfill an older checkpoint DB does (schema-migration-safe,
     * and one place to add the next column).
     *
     * <p><b>Order is the contract:</b> appended in this order, they reproduce the column order a
     * pre-single-sourcing swath wrote inline in {@code run_meta}'s {@code CREATE TABLE}.
     */
    private static final String[][] RUN_META_CONTEXT_COLUMNS = {
            {"no_sign_request", "INTEGER NOT NULL DEFAULT 0"},
            {"profile", "TEXT"},
            {"region", "TEXT"},
            {"fetch_owner", "INTEGER NOT NULL DEFAULT 0"},
            {"raw_output", "INTEGER NOT NULL DEFAULT 0"},
            {"output_path", "TEXT"},
            // --sort: sort_enabled gates the resume mode-mismatch refusal;
            // sort_phase (LISTING/MERGING/PUBLISHED) drives sort resume dispatch. Both backfill onto
            // an older checkpoint DB (default off / NULL) exactly like the other context columns.
            {"sort_enabled", "INTEGER NOT NULL DEFAULT 0"},
            {"sort_phase", "TEXT"},
            // NULLABLE, no DEFAULT — an older checkpoint DB backfills every row NULL, which
            // reads as "not fatal" (resumable), exactly matching a fresh row before anything has
            // flagged it. Only the two fatal marks (markRunFatalUnlessFinished, markRunUnresumable)
            // ever set it to 1; the broken-pipe FAILED path (markRunFinished) deliberately never
            // touches it (INT-12 stays normally resumable).
            {"fatal_error", "INTEGER"},
            // --request-payer: same shape as no_sign_request — backfills to 0/off on
            // an older checkpoint DB, soft-restored on resume exactly like no_sign_request.
            {"request_payer", "INTEGER NOT NULL DEFAULT 0"},
            // The resolved output destination kind (STDOUT/FILE/DIRECTORY, stored as the
            // enum name) and the raw --output-type override string (file|dir, or NULL when the
            // user did not force one). Persisted so resume no longer has to re-infer the kind from
            // the path. Both are NULLABLE with no DEFAULT — an older checkpoint DB backfills NULL
            // (kind unknown / no override) exactly like the other context columns.
            {"destination_kind", "TEXT"},
            {"output_type", "TEXT"},
            // The readable label-tagged canonical IDENTITY string (e.g.
            // "scheme=s3;endpoint=;bucket=b;...;fetch_owner=true"). Persisted at run creation and
            // recomputed-and-compared on resume so every IDENTITY option — not just the
            // filter/format/sort triple — refuses a changed resume, naming the changed column.
            // NULLABLE with no DEFAULT — an older checkpoint DB backfills NULL (no identity gate)
            // exactly like the other context columns.
            {"identity_spec", "TEXT"},
    };

    /** Add any {@link #RUN_META_CONTEXT_COLUMNS} the existing {@code run_meta} does not already have. */
    private static void migrateRunMetaContextColumns(Statement st) throws SQLException {
        Set<String> existing = new HashSet<>();
        try (ResultSet rs = st.executeQuery("PRAGMA table_info(run_meta)")) {
            while (rs.next()) {
                existing.add(rs.getString("name"));
            }
        }
        for (String[] col : RUN_META_CONTEXT_COLUMNS) {
            if (!existing.contains(col[0])) {
                st.execute("ALTER TABLE run_meta ADD COLUMN " + col[0] + " " + col[1]);
            }
        }
    }

    /**
     * Tables/indexes as they stood before any run-context column existed — i.e. exactly the schema
     * the oldest supported checkpoint DB has. Everything added since is single-sourced in
     * {@link #RUN_META_CONTEXT_COLUMNS}.
     */
    private static final String[] BASE_DDL = {
            "CREATE TABLE IF NOT EXISTS run_meta ("
                    + "id INTEGER PRIMARY KEY, store_scheme TEXT NOT NULL, endpoint TEXT, "
                    + "bucket TEXT NOT NULL, prefix BLOB NOT NULL, args_hash TEXT NOT NULL, "
                    + "strategy TEXT NOT NULL, filter_spec TEXT, output_format TEXT, "
                    + "mode TEXT NOT NULL CHECK (mode IN ('OBJECTS','VERSIONS')), "
                    + "started_at INTEGER NOT NULL, finished_at INTEGER, "
                    + "status TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETED','FAILED')))",
            "CREATE TABLE IF NOT EXISTS listing_node ("
                    + "id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES run_meta(id), "
                    + "parent_id INTEGER REFERENCES listing_node(id), "
                    + "kind TEXT NOT NULL CHECK (kind IN ('RANGE','PREFIX','INVENTORY_FILE')), "
                    + "range_start BLOB, range_end BLOB, cursor BLOB, opaque_token TEXT, "
                    + "durable_cursor BLOB, key_marker BLOB, version_id_marker TEXT, inventory_uri TEXT, "
                    + "status TEXT NOT NULL CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED')), "
                    + "generation INTEGER NOT NULL DEFAULT 0, owner_lease TEXT, "
                    + "pages_emitted INTEGER NOT NULL DEFAULT 0, api_calls INTEGER NOT NULL DEFAULT 0, "
                    + "unsplittable INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL)",
            "CREATE INDEX IF NOT EXISTS idx_node_ready ON listing_node(run_id, status)",
            "CREATE TABLE IF NOT EXISTS part_file ("
                    + "id INTEGER PRIMARY KEY, run_id INTEGER NOT NULL REFERENCES run_meta(id), "
                    + "writer_id INTEGER NOT NULL, path TEXT NOT NULL, format TEXT NOT NULL, "
                    + "finalized INTEGER NOT NULL DEFAULT 0, rows INTEGER NOT NULL DEFAULT 0, "
                    + "bytes INTEGER NOT NULL DEFAULT 0)",
    };
}
