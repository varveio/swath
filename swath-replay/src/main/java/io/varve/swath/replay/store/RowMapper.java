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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Shared DuckDB {@link ResultSet} → {@link ListedObject} mapping for every {@link ListingStore} that
 * queries a swath capture through DuckDB. Both {@link DuckDbListingStore} (role 1, materialised) and
 * {@link SortedParquetStore} (role 2, bounded {@code read_parquet}) SELECT the same nine column
 * aliases in the same order, so the row shape lives here once rather than in each store — the point
 * of the seam is that a differential attributes disagreements to <em>routing</em>, never to a
 * divergent row decode.
 */
final class RowMapper {

    /** {@code last_modified} is a UTC timestamp in the capture; read it in UTC to avoid a TZ shift. */
    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private RowMapper() {
    }

    /**
     * Reads one row from a result set whose columns are exactly {@code key_hex, size, last_modified,
     * etag, storage_class, owner_id, owner_display_name, checksum_algorithm, checksum_type}.
     */
    static ListedObject read(ResultSet rs) throws SQLException {
        byte[] key = ByteKeys.fromHex(rs.getString("key_hex"));
        return new ListedObject(key, rs.getLong("size"), micros(rs.getTimestamp("last_modified", UTC)),
                rs.getString("etag"), rs.getString("storage_class"),
                rs.getString("owner_id"), rs.getString("owner_display_name"),
                rs.getString("checksum_algorithm"), rs.getString("checksum_type"));
    }

    private static long micros(Timestamp timestamp) {
        if (timestamp == null) {
            return 0L;
        }
        Instant instant = timestamp.toInstant();
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
    }

    /** SQL single-quoted string literal with embedded quotes doubled. */
    static String sqlString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
