/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The manifest-finalize window (streaming each multi-GB part through MD5) can run for
 * minutes without emitting a single liveness signal; unless it advances progress explicitly,
 * {@code LivenessWatchdog} trips its total-freeze tripwire on a run that is still doing
 * useful work.
 *
 * <p>This pins the mechanism that prevents that — {@link ListRunner#md5HexWithLivenessProgress} advances the real
 * progress signal ({@link RunMetrics#progressSignal()}) and bumps the {@code SORT/finalize_progress_tick}
 * engagement counter in proportion to BYTES consumed — WITHOUT writing a multi-GB fixture (it injects a
 * tiny byte stride, and files a few MiB wide so the reader loops several times). It deliberately is NOT
 * the separately maintained adversarial watchdog-level guard; it proves the progress emission
 * fires, stays byte-keyed (honest — no bytes moved, no tick), and that the digest algorithm stays
 * byte-for-byte compatible with {@code DigestUtils.md5Hex}.
 */
final class ListRunnerFinalizeProgressTest {

    // Larger than the reader's 1 MiB internal buffer so the read loop iterates several times, making the
    // per-chunk byte-keyed tick fire repeatedly regardless of short-read behavior of the OS.
    private static final int FILE_BYTES = 4 * 1024 * 1024;   // 4 MiB
    private static final long STRIDE = 512 * 1024;           // 512 KiB

    @Test
    void finalizeDigestTicksLivenessProgressAndCounterAsBytesAreConsumed(@TempDir Path dir)
            throws IOException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Path part = writeRandomFile(dir.resolve("part-0.parquet"), FILE_BYTES);

        long before = metrics.progressSignal();
        String md5 = ListRunner.md5HexWithLivenessProgress(part, metrics, STRIDE);

        assertThat(md5)
                .as("digest must equal the prior DigestUtils.md5Hex value byte-for-byte")
                .isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(part)));
        long ticks = metrics.progressSignal() - before;
        assertThat(ticks)
                .as("byte-keyed liveness ticks must advance progressSignal repeatedly during a "
                        + "multi-MiB finalize (proportional to bytes, not a single end-of-file tick)")
                .isGreaterThanOrEqualTo(2L);
        assertThat(finalizeTickCount(metrics))
                .as("each liveness tick also bumps the SORT/finalize_progress_tick engagement counter")
                .isEqualTo((double) ticks);
    }

    @Test
    void strideLargerThanFileEmitsNoFreeTick(@TempDir Path dir) throws IOException {
        RunMetrics metrics = new RunMetrics(new SimpleMeterRegistry());
        Path part = writeRandomFile(dir.resolve("part-0.parquet"), FILE_BYTES);

        long before = metrics.progressSignal();
        // Honest, not a timer: with a stride wider than the whole file, no full stride of bytes is
        // consumed, so NO tick is emitted — a truly stalled read (zero bytes moving) would likewise emit
        // nothing and correctly let the watchdog trip, rather than an unconditional timer masking a hang.
        String md5 = ListRunner.md5HexWithLivenessProgress(part, metrics, 1L << 30);

        assertThat(md5).isEqualTo(DigestUtils.md5Hex(Files.readAllBytes(part)));
        assertThat(metrics.progressSignal() - before)
                .as("no byte stride crossed => no unconditional/free liveness tick")
                .isZero();
        assertThat(finalizeTickCount(metrics)).isZero();
    }

    private static double finalizeTickCount(RunMetrics metrics) {
        Counter c = metrics.registry().find("swath.steal_reason")
                .tags("outcome", "SORT", "reason", "finalize_progress_tick")
                .counter();
        return c == null ? 0.0 : c.count();
    }

    private static Path writeRandomFile(Path path, int bytes) throws IOException {
        byte[] data = new byte[bytes];
        new Random(42).nextBytes(data);
        Files.write(path, data);
        return path;
    }
}
