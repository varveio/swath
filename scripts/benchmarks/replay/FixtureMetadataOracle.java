/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/** Independent Parquet oracle for exact fixture-backed name/size/time during warmup. */
final class FixtureMetadataOracle {
    private FixtureMetadataOracle() { }

    static Plan plan(String fixtureGlob, String protocol, int partitionCount) throws Exception {
        if (!List.of("s3", "gcs", "azure").contains(protocol) || partitionCount < 0) {
            throw new IllegalArgumentException("invalid metadata oracle protocol/partition count");
        }
        String path = fixtureGlob.replace("'", "''");
        Properties properties = new Properties();
        properties.setProperty("jdbc_stream_results", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             Statement statement = connection.createStatement()) {
            long total;
            try (ResultSet result = statement.executeQuery("SELECT count(*) FROM read_parquet('" + path + "')")) {
                result.next();
                total = result.getLong(1);
            }
            if (partitionCount > 0 && total < partitionCount) {
                throw new IllegalArgumentException("partitioned metadata run needs at least one key per client");
            }
            String type;
            try (ResultSet result = statement.executeQuery(
                    "SELECT typeof(key) FROM read_parquet('" + path + "') LIMIT 1")) {
                type = result.next() ? result.getString(1) : "BLOB";
            }
            String keyExpression = switch (type.toUpperCase(Locale.ROOT)) {
                case "BLOB" -> "key";
                case "VARCHAR" -> "encode(key)";
                default -> throw new IllegalStateException("unsupported fixture key type " + type);
            };
            List<MetadataVerifier> builders = new ArrayList<>(partitionCount);
            for (int i = 0; i < partitionCount; i++) builders.add(new MetadataVerifier());
            MetadataVerifier all = new MetadataVerifier();
            byte[] prior = null;
            long index = 0;
            try (ResultSet rows = statement.executeQuery("SELECT " + keyExpression
                    + ", size, epoch_us(last_modified) FROM read_parquet('" + path + "')")) {
                while (rows.next()) {
                    byte[] key = rows.getBytes(1);
                    if (key == null || prior != null && Arrays.compareUnsigned(prior, key) >= 0) {
                        throw new IllegalStateException("fixture metadata rows are not strictly byte-sorted");
                    }
                    long size = rows.getLong(2); // JDBC null -> 0, matching replay's RowMapper.
                    long micros = rows.getLong(3); // JDBC null -> 0, matching replay's RowMapper.
                    long unit = wireEpochUnit(protocol, micros);
                    all.accept(key, size, unit);
                    if (partitionCount > 0) {
                        int owner = (int) Math.min(partitionCount - 1,
                                index * partitionCount / total);
                        builders.get(owner).accept(key, size, unit);
                    }
                    prior = key;
                    index++;
                }
            }
            if (index != total) throw new IllegalStateException("fixture count changed during metadata scan");
            List<Digest> expected = new ArrayList<>(partitionCount);
            for (MetadataVerifier builder : builders) expected.add(builder.finish());
            return new Plan(protocol, all.finish(), List.copyOf(expected));
        }
    }

    static long wireEpochUnit(String protocol, long epochMicros) {
        return switch (protocol) {
            case "s3" -> Math.floorDiv(epochMicros, 1000L);
            case "gcs" -> epochMicros;
            case "azure" -> Math.floorDiv(epochMicros, 1_000_000L);
            default -> throw new IllegalArgumentException("unknown metadata protocol");
        };
    }

    /** Streams the fixture and hashes only direct blobs in a delimiter listing. */
    static Digest planDelimiter(String fixtureGlob, String protocol, String prefix,
                                String delimiter) throws Exception {
        if (!List.of("s3", "gcs", "azure").contains(protocol) || prefix == null
                || delimiter == null || delimiter.isEmpty()) {
            throw new IllegalArgumentException("invalid delimiter metadata scope");
        }
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] delimiterBytes = delimiter.getBytes(StandardCharsets.UTF_8);
        String path = fixtureGlob.replace("'", "''");
        Properties properties = new Properties();
        properties.setProperty("jdbc_stream_results", "true");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:", properties);
             Statement statement = connection.createStatement()) {
            String type;
            try (ResultSet result = statement.executeQuery(
                    "SELECT typeof(key) FROM read_parquet('" + path + "') LIMIT 1")) {
                type = result.next() ? result.getString(1) : "BLOB";
            }
            String keyExpression = switch (type.toUpperCase(Locale.ROOT)) {
                case "BLOB" -> "key";
                case "VARCHAR" -> "encode(key)";
                default -> throw new IllegalStateException("unsupported fixture key type " + type);
            };
            MetadataVerifier direct = new MetadataVerifier();
            byte[] prior = null;
            try (ResultSet rows = statement.executeQuery("SELECT " + keyExpression
                    + ", size, epoch_us(last_modified) FROM read_parquet('" + path + "')")) {
                while (rows.next()) {
                    byte[] key = rows.getBytes(1);
                    if (key == null || prior != null && Arrays.compareUnsigned(prior, key) >= 0) {
                        throw new IllegalStateException("fixture delimiter rows are not strictly byte-sorted");
                    }
                    if (startsWith(key, prefixBytes)
                            && !containsFrom(key, delimiterBytes, prefixBytes.length)) {
                        direct.accept(key, rows.getLong(2), wireEpochUnit(protocol, rows.getLong(3)));
                    }
                    prior = key;
                }
            }
            return direct.finish();
        }
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) if (value[i] != prefix[i]) return false;
        return true;
    }

    private static boolean containsFrom(byte[] value, byte[] needle, int from) {
        for (int i = from; i <= value.length - needle.length; i++) {
            int j = 0;
            while (j < needle.length && value[i + j] == needle[j]) j++;
            if (j == needle.length) return true;
        }
        return false;
    }

    record Digest(long count, String sha256) { }
    record Plan(String protocol, Digest total, List<Digest> partitions) { }
}
