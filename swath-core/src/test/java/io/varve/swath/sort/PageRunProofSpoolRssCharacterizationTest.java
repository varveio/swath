/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in characterization of touched proof mappings as process RSS, including arena unmap. */
@Tag("perf")
class PageRunProofSpoolRssCharacterizationTest {

    private static final long TARGET_EXTENT = 128L * 1024 * 1024;
    private static final long RSS_NOISE_ALLOWANCE = 64L * 1024 * 1024;

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void touchedMappingIsRssVisibleAndArenaCloseReleasesItsAddressSpace(@TempDir Path root)
            throws Exception {
        assumeTrue(readVmRssBytes() > 0, "/proc/self/status VmRSS is unavailable");
        int slots = Math.toIntExact(Math.ceilDiv(TARGET_EXTENT, PageRunProofSpool.slotBytes()));
        long extent = PageRunProofSpool.logicalBytes(slots);
        Path path = root.resolve("rss-proof.tmp");
        PageRunProofSpool.Stats stats = new PageRunProofSpool.Stats(SortMetrics.NO_OP);
        PageRunProofSpool.Writer writer = new PageRunProofSpool.Writer(path, slots, stats);
        long beforeTouch;
        long touched;
        try {
            beforeTouch = readVmRssBytes();
            byte[] key = {(byte) 0xa5};
            for (int slot = 0; slot < slots; slot++) {
                writer.markOpen(slot);
                writer.writeKey(slot, PageRunProofSpool.KeyField.FIRST_MIN, key);
                writer.finish(slot, 0, 0, 0, -1, -1, 0, false);
            }
            touched = readVmRssBytes();
        } finally {
            writer.close();
        }
        long afterUnmap;
        try {
            afterUnmap = awaitRssAtMost(beforeTouch + RSS_NOISE_ALLOWANCE, Duration.ofSeconds(10));
        } finally {
            PageRunProofSpool.delete(path, stats);
        }

        long touchedDelta = touched - beforeTouch;
        assertThat(touchedDelta)
                .as("touching nearly every mapped page must be visible in process RSS")
                .isGreaterThan(extent / 2)
                .isLessThanOrEqualTo(extent + RSS_NOISE_ALLOWANCE);
        assertThat(afterUnmap)
                .as("closing the foreign-memory arena must release the mapping from process RSS")
                .isLessThanOrEqualTo(beforeTouch + RSS_NOISE_ALLOWANCE);
        System.out.printf("PROOF_SPOOL_RSS_RESULT extent_bytes=%d before_touch_bytes=%d "
                        + "touched_bytes=%d touched_delta_bytes=%d after_unmap_bytes=%d "
                        + "noise_allowance_bytes=%d%n",
                extent, beforeTouch, touched, touchedDelta, afterUnmap, RSS_NOISE_ALLOWANCE);
    }

    private static long awaitRssAtMost(long target, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        long current;
        do {
            current = readVmRssBytes();
            if (current > 0 && current <= target) {
                return current;
            }
            Thread.sleep(20);
        } while (System.nanoTime() < deadline);
        return current;
    }

    private static long readVmRssBytes() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    String digits = line.replaceAll("[^0-9]", "");
                    return digits.isEmpty() ? -1 : Long.parseLong(digits) * 1024L;
                }
            }
        } catch (IOException | RuntimeException ignored) {
            // The assumption at the call site skips unsupported platforms.
        }
        return -1;
    }
}
