/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.ReplayMetrics;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DuckDbListingStoreTest {

    @Test
    void returnsAllRowsInAscendingKeyOrderForAnOpenRange() throws Exception {
        try (DuckDbListingStore store = store(obj("a/1", 1), obj("b/1", 4), obj("a/3", 3), obj("a/2", 2))) {
            assertThat(keys(store.rows(null, true, null, 1000, Projection.WITH_OWNER)))
                    .containsExactly("a/1", "a/2", "a/3", "b/1");
        }
    }

    @Test
    void honorsExclusiveLowerAndExclusiveUpperBounds() throws Exception {
        try (DuckDbListingStore store = store(obj("a/1", 1), obj("a/2", 2), obj("a/3", 3), obj("b/1", 4))) {
            List<ListedObject> rows = store.rows(
                    key("a/1"), false, key("b/1"), 1000, Projection.KEYS_ONLY);
            assertThat(keys(rows)).containsExactly("a/2", "a/3");
        }
    }

    @Test
    void honorsInclusiveLowerBoundAndLimit() throws Exception {
        try (DuckDbListingStore store = store(obj("a/1", 1), obj("a/2", 2), obj("a/3", 3))) {
            List<ListedObject> rows = store.rows(key("a/1"), true, null, 2, Projection.KEYS_ONLY);
            assertThat(keys(rows)).containsExactly("a/1", "a/2");
        }
    }

    @Test
    void closeIsIdempotent() throws Exception {
        DuckDbListingStore store = store(obj("a/1", 1));
        store.close();
        store.close();
    }

    @Test
    void recordsParquetQueryMetrics() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ReplayMetrics metrics = new ReplayMetrics(registry);
        try (DuckDbListingStore store = store(metrics, obj("a/1", 1), obj("a/2", 2))) {
            store.rows(null, true, null, 1000, Projection.WITH_OWNER);
        }

        assertThat(registry.find("swath.replay.parquet.query.latency").timer().count()).isEqualTo(1);
        assertThat(registry.find("swath.replay.parquet.query.rows").summary().totalAmount()).isEqualTo(2.0);
    }

    @Test
    void loadsLegacyParquetWithoutOwnerDisplayNameOrChecksumType(@TempDir Path dir) throws Exception {
        Path parquet = dir.resolve("legacy.parquet");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             var st = connection.createStatement()) {
            st.execute("""
                    COPY (
                        SELECT
                            'OBJECT'::VARCHAR AS row_type,
                            'legacy/1'::BLOB AS key,
                            10::BIGINT AS size,
                            TIMESTAMP '2026-01-01 00:00:00' AS last_modified,
                            'etag-10'::VARCHAR AS etag,
                            'STANDARD'::VARCHAR AS storage_class,
                            'owner-10'::VARCHAR AS owner_id,
                            'CRC32'::VARCHAR AS checksum_algorithm
                    ) TO %s (FORMAT PARQUET)
                    """.formatted(sqlString(parquet)));
        }

        try (DuckDbListingStore store = new DuckDbListingStore(parquet)) {
            List<ListedObject> rows = store.rows(null, true, null, 1000, Projection.WITH_OWNER);
            ListedObject object = rows.getFirst();
            assertThat(ByteKeys.utf8(object.key())).isEqualTo("legacy/1");
            assertThat(object.ownerId()).isEqualTo("owner-10");
            assertThat(object.ownerDisplayName()).isNull();
            assertThat(object.checksumAlgorithm()).isEqualTo("CRC32");
            assertThat(object.checksumType()).isEqualTo("FULL_OBJECT");
        }
    }

    private static DuckDbListingStore store(Row... rows) throws Exception {
        return store(new ReplayMetrics(), rows);
    }

    private static DuckDbListingStore store(ReplayMetrics metrics, Row... rows) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        try (var st = connection.createStatement()) {
            st.execute("""
                    CREATE TEMP TABLE listing_objects(
                        key BLOB,
                        size BIGINT,
                        last_modified TIMESTAMP,
                        etag VARCHAR,
                        storage_class VARCHAR,
                        owner_id VARCHAR,
                        owner_display_name VARCHAR,
                        checksum_algorithm VARCHAR,
                        checksum_type VARCHAR)
                    """);
        }
        try (PreparedStatement ps = connection.prepareStatement("""
                INSERT INTO listing_objects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """)) {
            for (Row row : rows) {
                ps.setBytes(1, bytes(row.key()));
                ps.setLong(2, row.size());
                ps.setTimestamp(3, Timestamp.from(Instant.parse("2026-01-01T00:00:00Z")));
                ps.setString(4, "etag-" + row.size());
                ps.setString(5, "STANDARD");
                ps.setString(6, "owner-" + row.size());
                ps.setString(7, "owner-display-" + row.size());
                ps.setString(8, "CRC32");
                ps.setString(9, "FULL_OBJECT");
                ps.addBatch();
            }
            ps.executeBatch();
        }
        return new DuckDbListingStore(connection, metrics);
    }

    private static Row obj(String key, long size) {
        return new Row(key, size);
    }

    private static ByteKey key(String value) {
        return ByteKey.copyOf(bytes(value));
    }

    private static List<String> keys(List<ListedObject> rows) {
        return rows.stream().map(r -> ByteKeys.utf8(r.key())).toList();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String sqlString(Path path) {
        return "'" + path.toAbsolutePath().toString().replace("'", "''") + "'";
    }

    private record Row(String key, long size) {
    }
}
