/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Filesystem/checkpoint observations shared by the crash-and-resume test harnesses. */
final class SigkillResumeHarnessSupport {

    private SigkillResumeHarnessSupport() {
    }

    /** How many output parts the checkpoint has durably recorded as finalized, and their row total. */
    record FinalizedParts(long count, long rows) {
    }

    /**
     * The checkpoint's durably-committed finalized parts — the rows a resume carries into the dataset
     * it publishes. Read over a second connection while the run that writes them is still live (WAL),
     * so a harness can arm a crash on "at least one part is durable" rather than on a guess.
     */
    static FinalizedParts finalizedParts(Path ckpt) throws IOException {
        if (!Files.exists(ckpt)) {
            return new FinalizedParts(0, 0);
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + ckpt.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*), COALESCE(SUM(rows), 0) FROM part_file WHERE finalized=1")) {
            rs.next();
            return new FinalizedParts(rs.getLong(1), rs.getLong(2));
        } catch (SQLException e) {
            if (transientlyUnreadable(e)) {
                return new FinalizedParts(0, 0);
            }
            throw new IOException("failed reading finalized parts from " + ckpt, e);
        }
    }

    /** Sum of the checkpoint's monotonic per-node committed-page counters. */
    static long listingPagesEmitted(Path ckpt) throws IOException {
        if (!Files.exists(ckpt)) {
            return 0;
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + ckpt.toAbsolutePath());
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COALESCE(SUM(pages_emitted), 0) FROM listing_node")) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            if (transientlyUnreadable(e)) {
                return 0;
            }
            throw new IOException("failed reading listing progress from " + ckpt, e);
        }
    }

    /** A checkpoint whose DDL has not landed yet, or that a live writer holds — poll again, don't fail. */
    private static boolean transientlyUnreadable(SQLException e) {
        String message = e.getMessage();
        return message != null
                && (message.contains("no such table") || message.contains("database is locked"));
    }
}
