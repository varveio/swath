/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The checkpoint schema is a persistence contract: a DB written by one swath build must be
 * readable by another. Since the run-context columns are single-sourced in
 * {@link CheckpointSchema} and reach <i>every</i> DB — fresh or legacy — through the same
 * {@code ALTER TABLE ADD COLUMN} backfill, the two paths must land on a byte-identical schema.
 */
final class CheckpointSchemaTest {

    /**
     * {@code run_meta}'s columns, in order, exactly as every released swath has written them.
     * Pinned literally because the equivalence check below cannot see a reorder: shuffling
     * {@code CheckpointSchema}'s context-column table moves the columns in the fresh <i>and</i>
     * migrated DB alike, keeping them equal to each other while diverging from what is already
     * on disk in the field.
     */
    private static final List<String> RUN_META_COLUMNS_IN_ORDER = List.of(
            "id", "store_scheme", "endpoint", "bucket", "prefix", "args_hash", "strategy",
            "filter_spec", "output_format", "mode", "started_at", "finished_at", "status",
            "no_sign_request", "profile", "region", "fetch_owner", "raw_output", "output_path",
            "sort_enabled", "sort_phase", "fatal_error", "request_payer",
            "destination_kind", "output_type", "identity_spec");

    /**
     * The FULL durable schema of every persisted table — {@code table|name|type|notnull|default|pk} in {@code cid} order
     * — captured from a build at {@code e7b80f25} and pinned literally. This is the <b>external
     * oracle</b>: the fresh-vs-migrated equivalence check below compares two DBs built from the SAME
     * current definitions, so they move together and it is structurally blind to any drift that
     * changes both paths at once (a retyped column, a changed default, an edit to
     * {@code listing_node}/{@code part_file}). Only a literal captured from a prior build can catch
     * that, and it must: an existing checkpoint file in the field has this schema, and resume reads it.
     *
     * <p>If a future change intends to alter the schema, this constant is updated deliberately, in the
     * same commit, with a migration — never silently re-captured to make a red test green. The
     * {@code destination_kind}/{@code output_type} rows and {@code identity_spec} were added later
     * via the same {@code ALTER TABLE ADD COLUMN} backfill as the other context
     * columns (everything else is unchanged from the original capture).
     */
    private static final List<String> HEAD_TABLE_INFO = List.of(
            "run_meta|id|INTEGER|0|null|1",
            "run_meta|store_scheme|TEXT|1|null|0",
            "run_meta|endpoint|TEXT|0|null|0",
            "run_meta|bucket|TEXT|1|null|0",
            "run_meta|prefix|BLOB|1|null|0",
            "run_meta|args_hash|TEXT|1|null|0",
            "run_meta|strategy|TEXT|1|null|0",
            "run_meta|filter_spec|TEXT|0|null|0",
            "run_meta|output_format|TEXT|0|null|0",
            "run_meta|mode|TEXT|1|null|0",
            "run_meta|started_at|INTEGER|1|null|0",
            "run_meta|finished_at|INTEGER|0|null|0",
            "run_meta|status|TEXT|1|null|0",
            "run_meta|no_sign_request|INTEGER|1|0|0",
            "run_meta|profile|TEXT|0|null|0",
            "run_meta|region|TEXT|0|null|0",
            "run_meta|fetch_owner|INTEGER|1|0|0",
            "run_meta|raw_output|INTEGER|1|0|0",
            "run_meta|output_path|TEXT|0|null|0",
            "run_meta|sort_enabled|INTEGER|1|0|0",
            "run_meta|sort_phase|TEXT|0|null|0",
            "run_meta|fatal_error|INTEGER|0|null|0",
            "run_meta|request_payer|INTEGER|1|0|0",
            "run_meta|destination_kind|TEXT|0|null|0",
            "run_meta|output_type|TEXT|0|null|0",
            "run_meta|identity_spec|TEXT|0|null|0",
            "listing_node|id|INTEGER|0|null|1",
            "listing_node|run_id|INTEGER|1|null|0",
            "listing_node|parent_id|INTEGER|0|null|0",
            "listing_node|kind|TEXT|1|null|0",
            "listing_node|range_start|BLOB|0|null|0",
            "listing_node|range_end|BLOB|0|null|0",
            "listing_node|cursor|BLOB|0|null|0",
            "listing_node|opaque_token|TEXT|0|null|0",
            "listing_node|durable_cursor|BLOB|0|null|0",
            "listing_node|key_marker|BLOB|0|null|0",
            "listing_node|version_id_marker|TEXT|0|null|0",
            "listing_node|inventory_uri|TEXT|0|null|0",
            "listing_node|status|TEXT|1|null|0",
            "listing_node|generation|INTEGER|1|0|0",
            "listing_node|owner_lease|TEXT|0|null|0",
            "listing_node|pages_emitted|INTEGER|1|0|0",
            "listing_node|api_calls|INTEGER|1|0|0",
            "listing_node|unsplittable|INTEGER|1|0|0",
            "listing_node|updated_at|INTEGER|1|null|0",
            "part_file|id|INTEGER|0|null|1",
            "part_file|run_id|INTEGER|1|null|0",
            "part_file|writer_id|INTEGER|1|null|0",
            "part_file|path|TEXT|1|null|0",
            "part_file|format|TEXT|1|null|0",
            "part_file|finalized|INTEGER|1|0|0",
            "part_file|rows|INTEGER|1|0|0",
            "part_file|bytes|INTEGER|1|0|0");

    @Test
    void freshDbAndMigratedLegacyDbHaveIdenticalSchemas(@TempDir Path dir) throws Exception {
        Path fresh = dir.resolve("fresh.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(fresh)) {
            assertThat(store).isNotNull();
        }

        Path legacy = dir.resolve("legacy.sqlite");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + legacy.toAbsolutePath())) {
            CheckpointStoreTestSupport.writePreContextRunMeta(c);
        }
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(legacy)) {
            assertThat(store).isNotNull();
        }

        // The stored CREATE TABLE/INDEX text: strictest possible comparison, and the one that
        // catches a column re-added to the base DDL only (which a legacy DB, whose run_meta
        // already exists, would never gain).
        assertThat(schemaText(legacy))
                .as("migrated legacy checkpoint DB has the same stored schema as a fresh one")
                .isEqualTo(schemaText(fresh));

        for (String table : List.of("run_meta", "listing_node", "part_file")) {
            assertThat(tableInfo(legacy, table))
                    .as("%s columns (name/type/notnull/default/pk, in order)", table)
                    .isEqualTo(tableInfo(fresh, table));
        }
    }

    @Test
    void runMetaColumnOrderMatchesEveryReleasedBuild(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("fresh.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(db)) {
            assertThat(store).isNotNull();
        }
        assertThat(tableInfo(db, "run_meta").stream().map(col -> col.split("\\|")[0]).toList())
                .isEqualTo(RUN_META_COLUMNS_IN_ORDER);
    }

    private static List<String> schemaText(Path db) throws Exception {
        List<String> rows = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")) {
            while (rs.next()) {
                rows.add(rs.getString(1) + "|" + rs.getString(2) + "|" + rs.getString(3));
            }
        }
        return rows;
    }

    /** {@code name|type|notnull|default|pk} per column, in {@code cid} order. */
    private static List<String> tableInfo(Path db, String table) throws Exception {
        List<String> cols = new ArrayList<>();
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (rs.next()) {
                cols.add(rs.getString("name") + "|" + rs.getString("type") + "|"
                        + rs.getInt("notnull") + "|" + rs.getString("dflt_value") + "|"
                        + rs.getInt("pk"));
            }
        }
        return cols;
    }

    /**
     * Guards the durable schema against the drift the equivalence check cannot see: anything that
     * moves the fresh and migrated paths together, or that touches a table the run-context backfill
     * never mentions.
     */
    @Test
    void durableSchemaStillMatchesTheShippedBuild(@TempDir Path dir) throws Exception {
        Path fresh = dir.resolve("fresh.sqlite");
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(fresh)) {
            assertThat(store).isNotNull();
        }

        List<String> actual = new ArrayList<>();
        for (String table : List.of("run_meta", "listing_node", "part_file")) {
            for (String row : tableInfo(fresh, table)) {
                actual.add(table + "|" + row);
            }
        }
        assertThat(actual)
                .as("durable schema (all tables, full column metadata) matches e7b80f25 plus the "
                        + "5f-4a destination_kind/output_type and 5f-4c-2 identity_spec backfill columns")
                .isEqualTo(HEAD_TABLE_INFO);
    }
}
