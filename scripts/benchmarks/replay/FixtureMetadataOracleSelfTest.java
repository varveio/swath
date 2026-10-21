/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.bench;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;

/** Independent fixture metadata digest and adversarial size/time checks. */
public final class FixtureMetadataOracleSelfTest {
    private FixtureMetadataOracleSelfTest() { }

    public static void main(String[] args) throws Exception {
        Path file = Files.createTempFile("swath-metadata-oracle-", ".parquet");
        Files.delete(file);
        Path delimitedFile = Files.createTempFile("swath-delimiter-metadata-", ".parquet");
        Files.delete(delimitedFile);
        try {
            try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                 var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE fixture(key BLOB, size BIGINT, last_modified TIMESTAMPTZ)");
                statement.execute("INSERT INTO fixture VALUES "
                        + "(encode('a'), 10, TIMESTAMPTZ '1969-12-31 23:59:59.999999+00'),"
                        + "(encode('b'), 20, TIMESTAMPTZ '1970-01-01 00:00:00+00'),"
                        + "(encode('c'), 30, TIMESTAMPTZ '1970-01-01 00:00:01.234567+00')");
                statement.execute("COPY fixture TO '" + file.toString().replace("'", "''")
                        + "' (FORMAT PARQUET)");
            }
            for (String protocol : List.of("s3", "gcs", "azure")) {
                var plan = FixtureMetadataOracle.plan(file.toString(), protocol, 2);
                require(plan.total().count() == 3 && plan.partitions().size() == 2,
                        protocol + " total and partition count");
                var unpartitioned = FixtureMetadataOracle.plan(file.toString(), protocol, 0);
                require(unpartitioned.partitions().isEmpty()
                        && unpartitioned.total().equals(plan.total()),
                        protocol + " unpartitioned digest");
                MetadataVerifier complete = new MetadataVerifier();
                long[] times = {-1, 0, 1_234_567};
                long[] sizes = {10, 20, 30};
                for (int i = 0; i < 3; i++) {
                    complete.accept(raw("" + (char) ('a' + i)), sizes[i],
                            FixtureMetadataOracle.wireEpochUnit(protocol, times[i]));
                }
                require(plan.total().equals(complete.finish()), protocol + " metadata digest");

                MetadataVerifier first = new MetadataVerifier();
                first.accept(raw("a"), 10, FixtureMetadataOracle.wireEpochUnit(protocol, -1));
                first.accept(raw("b"), 20, FixtureMetadataOracle.wireEpochUnit(protocol, 0));
                MetadataVerifier second = new MetadataVerifier();
                second.accept(raw("c"), 30, FixtureMetadataOracle.wireEpochUnit(protocol, 1_234_567));
                require(plan.partitions().get(0).equals(first.finish())
                        && plan.partitions().get(1).equals(second.finish()),
                        protocol + " contiguous partition metadata");

                MetadataVerifier wrongSize = new MetadataVerifier();
                wrongSize.accept(raw("a"), 11, FixtureMetadataOracle.wireEpochUnit(protocol, -1));
                wrongSize.accept(raw("b"), 20, FixtureMetadataOracle.wireEpochUnit(protocol, 0));
                wrongSize.accept(raw("c"), 30, FixtureMetadataOracle.wireEpochUnit(protocol, 1_234_567));
                require(!plan.total().equals(wrongSize.finish()), protocol + " wrong size must fail");

                MetadataVerifier wrongTime = new MetadataVerifier();
                wrongTime.accept(raw("a"), 10,
                        FixtureMetadataOracle.wireEpochUnit(protocol, -1) + 1);
                wrongTime.accept(raw("b"), 20, FixtureMetadataOracle.wireEpochUnit(protocol, 0));
                wrongTime.accept(raw("c"), 30, FixtureMetadataOracle.wireEpochUnit(protocol, 1_234_567));
                require(!plan.total().equals(wrongTime.finish()), protocol + " wrong wire time must fail");
            }
            require(FixtureMetadataOracle.wireEpochUnit("s3", -1) == -1,
                    "S3 negative micros floor to millis");
            require(FixtureMetadataOracle.wireEpochUnit("azure", -1) == -1,
                    "Azure negative micros floor to seconds");
            boolean refusedExcessPartitions = false;
            try {
                FixtureMetadataOracle.plan(file.toString(), "s3", 4);
            } catch (IllegalArgumentException expected) {
                refusedExcessPartitions = true;
            }
            require(refusedExcessPartitions, "empty metadata partition refused");
            try (var connection = DriverManager.getConnection("jdbc:duckdb:");
                 var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE delimited(key BLOB, size BIGINT, last_modified TIMESTAMPTZ)");
                statement.execute("INSERT INTO delimited VALUES "
                        + "(encode('a/'),1,TIMESTAMPTZ '1970-01-01 00:00:00+00'),"
                        + "(encode('a/1'),2,TIMESTAMPTZ '1970-01-01 00:00:01+00'),"
                        + "(encode('a/sub/2'),3,TIMESTAMPTZ '1970-01-01 00:00:02+00'),"
                        + "(encode('a/sub/3'),4,TIMESTAMPTZ '1970-01-01 00:00:03+00'),"
                        + "(encode('b'),5,TIMESTAMPTZ '1970-01-01 00:00:04+00')");
                statement.execute("COPY delimited TO '" + delimitedFile.toString().replace("'", "''")
                        + "' (FORMAT PARQUET)");
            }
            for (String protocol : List.of("s3", "gcs", "azure")) {
                MetadataVerifier expected = new MetadataVerifier();
                expected.accept(raw("a/"), 1, FixtureMetadataOracle.wireEpochUnit(protocol, 0));
                expected.accept(raw("a/1"), 2,
                        FixtureMetadataOracle.wireEpochUnit(protocol, 1_000_000));
                require(FixtureMetadataOracle.planDelimiter(delimitedFile.toString(), protocol,
                        "a/", "/").equals(expected.finish()), protocol + " direct delimiter objects");
            }
        } finally {
            Files.deleteIfExists(file);
            Files.deleteIfExists(delimitedFile);
        }
        System.out.println("FixtureMetadataOracleSelfTest passed");
    }

    private static byte[] raw(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private static void require(boolean condition, String label) {
        if (!condition) throw new AssertionError(label);
    }
}
