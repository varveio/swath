/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.protocol.ByteKeys;
import io.varve.swath.replay.protocol.ListedObject;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DuckDB {@link ResultSet} → {@link ListedObject} mapping for {@link DuckDbListingStore}. The
 * query projection and its epoch-microsecond timestamp conversion live together, so JDBC conversion
 * cannot introduce a JVM-default-timezone dependency at the storage seam.
 */
final class RowMapper {

    private RowMapper() {
    }

    /**
     * Reads one row from a result set whose columns are exactly {@code key_hex, size,
     * last_modified_epoch_micros, etag, storage_class, owner_id, owner_display_name,
     * checksum_algorithm, checksum_type}.
     *
     * <p>The SQL projection uses DuckDB's {@code epoch_us(last_modified)} rather than JDBC's
     * {@code Timestamp} conversion. The latter has varied with the JVM default timezone for
     * Parquet {@code TIMESTAMPTZ} values; an epoch count is the capture's canonical UTC instant.
     */
    static ListedObject read(ResultSet rs) throws SQLException {
        byte[] key = ByteKeys.fromHex(rs.getString("key_hex"));
        long lastModifiedEpochMicros = rs.getLong("last_modified_epoch_micros");
        if (rs.wasNull()) {
            lastModifiedEpochMicros = 0L;
        }
        return new ListedObject(key, rs.getLong("size"), lastModifiedEpochMicros,
                rs.getString("etag"), rs.getString("storage_class"),
                rs.getString("owner_id"), rs.getString("owner_display_name"),
                rs.getString("checksum_algorithm"), rs.getString("checksum_type"));
    }

    /** SQL single-quoted string literal with embedded quotes doubled. */
    static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
