/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.runtime.ListRunner;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Immutable, read-only checkpoint catalog lookup for retained benchmark staging. */
final class BenchmarkCheckpointCatalog {

    private static final String CHECKPOINT_DIR = ".swath";
    private static final String CHECKPOINT_FILE = "checkpoint.sqlite";

    private BenchmarkCheckpointCatalog() {
    }

    record Authority(long runId, String argsHash, List<TrackedSegment> segments) {
    }

    record TrackedSegment(Path path, long rows, long bytes) {
    }

    static Authority read(Path output, Path staging, Manifest.Identity identity) throws IOException {
        if (identity.runId() == null || identity.runId() <= 0 || identity.argsHash().isBlank()) {
            throw new IllegalArgumentException("external staging requires a complete checkpoint-backed run identity: "
                    + output);
        }
        Path success = output.resolve(Manifest.SUCCESS_FILE_NAME);
        if (!Files.isRegularFile(success, LinkOption.NOFOLLOW_LINKS) || Files.size(success) != 0) {
            throw new IllegalArgumentException("external staging requires a completed _SUCCESS dataset: " + output);
        }
        Path checkpoint = output.resolve(CHECKPOINT_DIR).resolve(CHECKPOINT_FILE);
        if (!Files.isRegularFile(checkpoint, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("external staging requires an existing checkpoint: " + checkpoint);
        }
        refuseJournalCompanion(checkpoint.resolveSibling(checkpoint.getFileName() + "-wal"));
        refuseJournalCompanion(checkpoint.resolveSibling(checkpoint.getFileName() + "-shm"));

        String url = "jdbc:sqlite:" + checkpoint.toUri().toASCIIString() + "?mode=ro&immutable=1";
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA query_only=ON");
            requireMatchingRun(connection, identity);
            List<TrackedSegment> segments = trackedSegments(connection, identity.runId(), staging);
            if (segments.isEmpty()) {
                throw new IllegalArgumentException("checkpoint corpus contains no original page-run inputs: " + staging);
            }
            return new Authority(identity.runId(), identity.argsHash(), List.copyOf(segments));
        } catch (SQLException e) {
            throw new IOException("failed immutable checkpoint catalog read at " + checkpoint, e);
        }
    }

    private static void requireMatchingRun(Connection connection, Manifest.Identity identity)
            throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT args_hash, status, sort_enabled, sort_phase FROM run_meta WHERE id=?")) {
            query.setLong(1, identity.runId());
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalArgumentException("state run_id is absent from the retained checkpoint: "
                            + identity.runId());
                }
                String argsHash = rows.getString(1);
                String status = rows.getString(2);
                boolean sortEnabled = rows.getInt(3) != 0;
                String sortPhase = rows.getString(4);
                if (!identity.argsHash().equals(argsHash)) {
                    throw new IllegalArgumentException("state args_hash does not match checkpoint run_id="
                            + identity.runId());
                }
                if (!"COMPLETED".equals(status) || !sortEnabled || !"PUBLISHED".equals(sortPhase)) {
                    throw new IllegalArgumentException("retained checkpoint run is not completed sorted PUBLISHED: run_id="
                            + identity.runId() + " status=" + status + " sort_enabled=" + sortEnabled
                            + " sort_phase=" + sortPhase);
                }
                if (rows.next()) {
                    throw new IllegalArgumentException("checkpoint contains duplicate run identity " + identity.runId());
                }
            }
        }
    }

    private static List<TrackedSegment> trackedSegments(Connection connection, long runId, Path staging)
            throws SQLException, IOException {
        List<TrackedSegment> segments = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT path, format, finalized, rows, bytes FROM part_file WHERE run_id=? ORDER BY id")) {
            query.setLong(1, runId);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String recordedPath = rows.getString(1);
                    String format = rows.getString(2);
                    boolean finalized = rows.getInt(3) != 0;
                    long recordedRows = rows.getLong(4);
                    long recordedBytes = rows.getLong(5);
                    String requiredPrefix = "seg-" + runId + "-";
                    Path relative = Path.of(recordedPath);
                    if (!finalized || !ListRunner.SORT_SEGMENT_FORMAT.equals(format)
                            || relative.getNameCount() != 1
                            || !relative.getFileName().toString().equals(recordedPath)
                            || !recordedPath.startsWith(requiredPrefix)
                            || !recordedPath.endsWith(StagingNames.PAGE_RUN_SUFFIX)) {
                        throw new IllegalArgumentException("checkpoint contains a non-original sort segment row: "
                                + recordedPath + " format=" + format + " finalized=" + finalized);
                    }
                    Path segment = staging.resolve(relative).toAbsolutePath().normalize();
                    if (!segment.getParent().equals(staging) || !Files.isRegularFile(segment,
                            LinkOption.NOFOLLOW_LINKS)) {
                        throw new IllegalArgumentException("checkpoint segment is missing or escapes staging: " + segment);
                    }
                    if (Files.size(segment) != recordedBytes || recordedRows <= 0 || recordedBytes <= 0) {
                        throw new IllegalArgumentException("checkpoint segment metadata disagrees with disk: " + segment);
                    }
                    segments.add(new TrackedSegment(segment, recordedRows, recordedBytes));
                }
            }
        }
        return segments;
    }

    private static void refuseJournalCompanion(Path path) {
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("immutable benchmark checkpoint has live SQLite companion: " + path);
        }
    }
}
