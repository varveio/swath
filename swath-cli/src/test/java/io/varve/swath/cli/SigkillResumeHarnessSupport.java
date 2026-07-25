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

/** Filesystem/checkpoint observations shared by the external-SIGKILL test harness. */
final class SigkillResumeHarnessSupport {

    private SigkillResumeHarnessSupport() {
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
            String message = e.getMessage();
            if (message != null && (message.contains("no such table") || message.contains("database is locked"))) {
                return 0;
            }
            throw new IOException("failed reading listing progress from " + ckpt, e);
        }
    }
}
