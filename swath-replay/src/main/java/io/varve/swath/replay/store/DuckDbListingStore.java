/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.store;

import io.varve.swath.replay.protocol.ByteKey;
import io.varve.swath.replay.protocol.ListedObject;
import io.varve.swath.replay.server.ReplayMetrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link ListingStore} backed by DuckDB over a swath Parquet capture. Unsorted captures are
 * materialised once into a temp-database {@code listing_objects} table; range reads are then
 * {@code key >|>= ? AND key < ? ORDER BY key LIMIT ?}. No secondary index is created: DuckDB does
 * not use one for the ~50%-selective ranges or the {@code ORDER BY} these reads issue.
 *
 * <p>This store honors only range bounds — prefix, delimiter, start-after and pagination live in the
 * pager above it. Owner columns are always projected (cheap here), so {@link Projection} is accepted
 * but not used to prune.
 *
 * <p><b>It is order-agnostic, and that hides a fixture's disorder from every caller.</b> Serving an
 * unsorted capture is this store's purpose — the {@code ORDER BY key} above is what makes that
 * correct — so a file whose rows are in no particular order is served here exactly as a sorted one
 * is, and a caller that wanted to know the input was unsorted will not learn it here. The sorted
 * tiers ({@link SortedParquetStore}'s skip-scan, the simulator's streaming tier) check the ascent of
 * the rows they physically step over, in the loops they already run; matching that here would mean a
 * dedicated scan of a whole materialized table, which buys a caller that has already chosen the
 * order-agnostic store nothing it can act on. Stated rather than fixed: a consumer that needs an
 * unsorted input refused must route the fixture through a tier that reads it in physical order.
 */
public final class DuckDbListingStore implements ListingStore {

    private final DuckDbConnectionPool pool;
    private final ReplayMetrics metrics;
    private final Path tempDatabase;

    public DuckDbListingStore(Path parquet) {
        this(parquet, new ReplayMetrics(), defaultConnectionCount());
    }

    public DuckDbListingStore(Path parquet, ReplayMetrics metrics, int connectionCount) {
        this(openMaterializedPool(parquet, connectionCount), metrics);
    }

    public DuckDbListingStore(Connection connection, ReplayMetrics metrics) {
        this(new OpenedPool(List.of(connection), null), metrics);
    }

    private DuckDbListingStore(OpenedPool opened, ReplayMetrics metrics) {
        List<Connection> pooled = opened.connections();
        if (pooled.isEmpty()) {
            throw new IllegalArgumentException("DuckDB connection pool must contain at least one connection");
        }
        this.pool = new DuckDbConnectionPool(pooled,
                "interrupted waiting for DuckDB replay connection",
                "DuckDB replay connection pool rejected a returned connection",
                "failed to close DuckDB store");
        this.metrics = metrics;
        this.tempDatabase = opened.tempDatabase();
    }

    /** The CPU-bounded default DuckDB connection count; also the natural request-concurrency bound. */
    public static int defaultConnectionCount() {
        return Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors()));
    }

    @Override
    public List<ListedObject> rows(ByteKey from, boolean fromInclusive, ByteKey toExclusive, int limit,
                                   Projection projection) {
        Connection connection = pool.borrow();
        try {
            var pageSample = metrics.startPageReadTimer();
            try {
                return query(connection, from, fromInclusive, toExclusive, limit);
            } finally {
                metrics.recordPageRead(pageSample);
            }
        } finally {
            pool.release(connection);
        }
    }

    @Override
    public void close() {
        RuntimeException failure = pool.close();
        if (tempDatabase != null) {
            try {
                Files.deleteIfExists(tempDatabase);
                Files.deleteIfExists(Path.of(tempDatabase.toString() + ".wal"));
            } catch (IOException e) {
                failure = accumulate(failure, "failed to delete temporary DuckDB store", e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException accumulate(RuntimeException failure, String message, Exception cause) {
        if (failure == null) {
            return new IllegalStateException(message, cause);
        }
        failure.addSuppressed(cause);
        return failure;
    }

    private List<ListedObject> query(Connection connection, ByteKey from, boolean fromInclusive,
                                     ByteKey toExclusive, int limit) {
        Query query = buildQuery(from, fromInclusive, toExclusive);
        metrics.parquetReaderAcquired();
        var sample = metrics.startParquetQueryTimer();
        int rows = 0;
        boolean success = false;
        try (PreparedStatement ps = connection.prepareStatement(query.sql())) {
            int index = 1;
            for (byte[] value : query.params()) {
                ps.setBytes(index++, value);
            }
            ps.setInt(index, limit);
            try (ResultSet rs = ps.executeQuery()) {
                List<ListedObject> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(RowMapper.read(rs));
                }
                rows = out.size();
                success = true;
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to query replay store", e);
        } finally {
            metrics.recordParquetQuery(sample, rows, success);
            metrics.parquetReaderReleased();
        }
    }

    private static Query buildQuery(ByteKey from, boolean fromInclusive, ByteKey toExclusive) {
        StringBuilder sql = new StringBuilder("""
                SELECT hex(key) AS key_hex, size, epoch_us(last_modified) AS last_modified_epoch_micros,
                       etag, storage_class, owner_id,
                       owner_display_name, checksum_algorithm, checksum_type
                FROM listing_objects
                WHERE 1 = 1
                """);
        List<byte[]> params = new ArrayList<>();
        if (from != null) {
            sql.append(fromInclusive ? " AND key >= ?" : " AND key > ?");
            params.add(from.toByteArray());
        }
        if (toExclusive != null) {
            sql.append(" AND key < ?");
            params.add(toExclusive.toByteArray());
        }
        sql.append(" ORDER BY key LIMIT ?");
        return new Query(sql.toString(), params);
    }

    private static OpenedPool openMaterializedPool(Path parquet, int connectionCount) {
        if (connectionCount < 1) {
            throw new IllegalArgumentException("DuckDB connection count must be at least 1");
        }
        Path database = createTempDatabase();
        List<Connection> out = new ArrayList<>(connectionCount);
        try {
            Connection owner = openConnection(database);
            createListingTable(owner, parquet);
            out.add(owner);
            for (int i = 1; i < connectionCount; i++) {
                out.add(openConnection(database));
            }
            return new OpenedPool(out, database);
        } catch (SQLException | RuntimeException e) {
            for (Connection connection : out) {
                try {
                    connection.close();
                } catch (SQLException closeError) {
                    e.addSuppressed(closeError);
                }
            }
            try {
                Files.deleteIfExists(database);
                Files.deleteIfExists(Path.of(database.toString() + ".wal"));
            } catch (IOException deleteError) {
                e.addSuppressed(deleteError);
            }
            throw new IllegalStateException("failed to open replay store " + parquet, e);
        }
    }

    private static Path createTempDatabase() {
        try {
            Path path = Files.createTempFile("swath-s3-replay-", ".duckdb");
            Files.delete(path);
            return path;
        } catch (IOException e) {
            throw new IllegalStateException("failed to create temporary DuckDB store", e);
        }
    }

    private static void createListingTable(Connection connection, Path parquet) throws SQLException {
        String path = sqlString(parquetInput(parquet));
        try (var st = connection.createStatement()) {
            st.execute("CREATE TEMP VIEW replay_parquet_source AS SELECT * FROM read_parquet(%s)".formatted(path));
            Set<String> columns = describeColumns(connection, "replay_parquet_source");
            String key = "VARCHAR".equals(columnType(connection, "replay_parquet_source", "key"))
                    ? "encode(key)"
                    : "key";
            String ownerDisplayName = optionalColumn(columns, "owner_display_name", "NULL::VARCHAR");
            String checksumType = optionalColumn(columns, "checksum_type",
                    "CASE WHEN checksum_algorithm IS NOT NULL THEN 'FULL_OBJECT' ELSE NULL END");
            st.execute("""
                    CREATE TABLE listing_objects AS
                    SELECT %s AS key, size, last_modified, etag, storage_class, owner_id,
                           %s AS owner_display_name,
                           checksum_algorithm,
                           %s AS checksum_type
                    FROM replay_parquet_source
                    WHERE row_type = 'OBJECT'
                    """.formatted(key, ownerDisplayName, checksumType));
        }
    }

    private static String columnType(Connection connection, String relation, String column) throws SQLException {
        try (var st = connection.createStatement();
             ResultSet rs = st.executeQuery("DESCRIBE " + relation)) {
            while (rs.next()) {
                if (column.equals(rs.getString("column_name"))) {
                    return rs.getString("column_type");
                }
            }
        }
        throw new SQLException("missing required replay column: " + column);
    }

    private static Set<String> describeColumns(Connection connection, String relation) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (var st = connection.createStatement();
             ResultSet rs = st.executeQuery("DESCRIBE " + relation)) {
            while (rs.next()) {
                columns.add(rs.getString("column_name"));
            }
        }
        return columns;
    }

    private static String optionalColumn(Set<String> columns, String name, String fallback) {
        return columns.contains(name) ? name : fallback;
    }

    private static String parquetInput(Path path) {
        Path absolute = path.toAbsolutePath();
        if (Files.isDirectory(absolute)) {
            return absolute.resolve("*.parquet").toString();
        }
        return absolute.toString();
    }

    private static Connection openConnection(Path database) throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:" + database.toAbsolutePath());
    }

    private static String sqlString(String value) {
        return RowMapper.sqlString(value);
    }

    private record Query(String sql, List<byte[]> params) {
    }

    /** The raw materialised connections and their backing temp-database file, before pooling. */
    private record OpenedPool(List<Connection> connections, Path tempDatabase) {
    }
}
