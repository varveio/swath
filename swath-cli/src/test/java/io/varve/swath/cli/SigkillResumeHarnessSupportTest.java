/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SigkillResumeHarnessSupportTest {

    @Test
    void listingPagesEmittedReadsMonotonicSumWithoutCreatingMissingCheckpoint(@TempDir Path tmp)
            throws Exception {
        Path missing = tmp.resolve("missing.sqlite");
        assertThat(SigkillResumeHarnessSupport.listingPagesEmitted(missing)).isZero();
        assertThat(missing).doesNotExist();

        Path ckpt = tmp.resolve("ckpt.sqlite");
        try (var ignored = DriverManager.getConnection("jdbc:sqlite:" + ckpt.toAbsolutePath())) {
            assertThat(SigkillResumeHarnessSupport.listingPagesEmitted(ckpt)).isZero();
        }
        try (var c = DriverManager.getConnection("jdbc:sqlite:" + ckpt.toAbsolutePath());
             var st = c.createStatement()) {
            st.execute("CREATE TABLE listing_node (pages_emitted INTEGER NOT NULL)");
            st.execute("INSERT INTO listing_node VALUES (3), (7), (0)");
        }
        assertThat(SigkillResumeHarnessSupport.listingPagesEmitted(ckpt)).isEqualTo(10);

        try (var c = DriverManager.getConnection("jdbc:sqlite:" + ckpt.toAbsolutePath());
             var st = c.createStatement()) {
            st.execute("UPDATE listing_node SET pages_emitted = pages_emitted + 1 WHERE pages_emitted = 0");
        }
        assertThat(SigkillResumeHarnessSupport.listingPagesEmitted(ckpt)).isEqualTo(11);
    }
}
