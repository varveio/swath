/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * INT-6: the Parquet parts swath writes are read back correctly by
 * <b>DuckDB</b> (a different reader than parquet-mr) — union row count, schema, and
 * values all hold. {@code ParquetCanonicalContractTest} pins the same contract via
 * parquet-mr; this is the independent-engine (DuckDB) check. Skips cleanly where the
 * {@code duckdb} CLI is not installed.
 */
@Tag("integration")
class Int6DuckDbReadIT {

    @Test
    void duckDbReadsCanonicalPartsWithCorrectCountsSchemaAndValues(@TempDir Path dir) throws Exception {
        String duckdb = locateDuckDb();
        if (duckdb == null) {
            abort("duckdb CLI not on PATH — INT-6 DuckDB readback skipped");
        }

        byte[] multibyteKey = "café/data/🚀.bin".getBytes(StandardCharsets.UTF_8);
        List<ListEntry> entries = List.of(
                ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.of(multibyteKey), 123L, 1_700_000_000_000_000L,
                        "d41d8cd98f00b204e9800998ecf8427e-3", "GLACIER", null, true, null, null),
                ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8("plain/a.txt"), 7L, 1_700_000_000_000_000L,
                        "abc", "STANDARD", null, true, null, null),
                new CommonPrefixEntry(KeyBytes.ofUtf8("plain/")));

        try (ParquetWriterPool pool = new ParquetWriterPool(dir, ParquetSchema.canonical(), "h",
                1, Long.MAX_VALUE, 8)) {
            pool.submit(new PageBatch(0, 0, entries));
        }
        String glob = dir.toAbsolutePath() + "/data/*.parquet";

        // Union row count == what we wrote (3), split by row_type (2 objects + 1 prefix).
        assertThat(duck(duckdb, "SELECT count(*) FROM read_parquet('" + glob + "')")).isEqualTo("3");
        assertThat(duck(duckdb, "SELECT count(*) FROM read_parquet('" + glob + "') WHERE row_type='OBJECT'"))
                .isEqualTo("2");
        assertThat(duck(duckdb, "SELECT count(*) FROM read_parquet('" + glob + "') WHERE row_type='COMMON_PREFIX'"))
                .isEqualTo("1");

        // Canonical schema is legible to DuckDB: typed columns project cleanly.
        // DESCRIBE exposes the type as `column_type` (not `data_type`, which is the
        // information_schema.columns name) — duckdb binds the column name strictly.
        String types = duck(duckdb, "SELECT lower(string_agg(column_name || ':' || column_type, ',' ORDER BY column_name))"
                + " FROM (DESCRIBE SELECT * FROM read_parquet('" + glob + "'))");
        assertThat(types).contains("key:varchar").contains("size:bigint").contains("etag:varchar")
                .contains("row_type:varchar").contains("is_delete_marker:boolean");

        // ETag stored verbatim (multipart hex-N), and size NULL for the common-prefix row.
        assertThat(duck(duckdb, "SELECT etag FROM read_parquet('" + glob + "') WHERE size=123"))
                .isEqualTo("d41d8cd98f00b204e9800998ecf8427e-3");
        assertThat(duck(duckdb, "SELECT count(*) FROM read_parquet('" + glob + "')"
                + " WHERE row_type='COMMON_PREFIX' AND size IS NULL")).isEqualTo("1");

        // Byte-exact multibyte key survives (compare as hex to avoid shell encoding issues).
        String keyHex = duck(duckdb, "SELECT lower(hex(key)) FROM read_parquet('" + glob + "') WHERE size=123");
        assertThat(keyHex).isEqualTo(toHex(multibyteKey));
    }

    /**
     * INT-6 "union == bucket", read by DuckDB over <b>actual {@link ListRunner} output</b> (not
     * hand-submitted synthetic rows): a multi-part listing of N keys reads back as exactly N rows,
     * N distinct keys, all OBJECT. The parquet-mr equivalent is
     * {@code ParquetListRunnerTest}; this is the independent-engine (DuckDB) check.
     */
    @Test
    void duckDbUnionEqualsBucketOverActualListRunnerOutput(@TempDir Path dir) throws Exception {
        String duckdb = locateDuckDb();
        if (duckdb == null) {
            abort("duckdb CLI not on PATH — INT-6 DuckDB union==bucket skipped");
        }

        int n = 12_345;
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(n)).build();
        // Small part target forces several parts, so "union of parts" is a real union.
        var spec = new ListRunner.ParquetSpec(new byte[0], 64, 1000, FilterChain.EMPTY,
                3, 128 * 1024, 16, "argshash-int6", null, null, 0L, 0L, "");
        var stats = new ListRunner().runToParquet(RunContext.create(), fetcher, dir, spec);
        assertThat(stats.objects()).isEqualTo((long) n);

        String glob = dir.toAbsolutePath() + "/data/*.parquet";
        String expected = String.valueOf(n);
        assertThat(duck(duckdb, "SELECT count(*) FROM read_parquet('" + glob + "')"))
                .as("union of parts == bucket").isEqualTo(expected);
        assertThat(duck(duckdb, "SELECT count(DISTINCT key) FROM read_parquet('" + glob + "')"))
                .as("each key exactly once across parts").isEqualTo(expected);
        assertThat(duck(duckdb, "SELECT count(*) FROM read_parquet('" + glob + "') WHERE row_type='OBJECT'"))
                .isEqualTo(expected);
    }

    private static String toHex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }

    /** Run a one-shot duckdb query, returning the single scalar result (trimmed). */
    private static String duck(String duckdb, String sql) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(duckdb, "-noheader", "-list", "-c", sql);
        pb.redirectErrorStream(false);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        String err = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (!p.waitFor(60, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IOException("duckdb timed out");
        }
        if (p.exitValue() != 0) {
            throw new IOException("duckdb failed (" + p.exitValue() + "): " + err);
        }
        return out;
    }

    private static String locateDuckDb() {
        for (String candidate : new String[]{"duckdb", "/usr/local/bin/duckdb", "/usr/bin/duckdb",
                System.getProperty("user.home") + "/.duckdb/cli/latest/duckdb"}) {
            try {
                Process p = new ProcessBuilder(candidate, "--version").start();
                if (p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0) {
                    return candidate;
                }
            } catch (IOException | InterruptedException ignored) {
                if (Thread.interrupted()) {
                    return null;
                }
            }
        }
        return null;
    }
}
