/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.replay.testkit.ObjectEntries;
import io.varve.swath.replay.testkit.ParquetFixtures;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplaySigtermTest {
    @TempDir Path temp;

    @Test
    void sigtermRunsBoundedIdempotentServeShutdownHook() throws Exception {
        Path fixture = temp.resolve("part.parquet");
        try (var writer = ParquetFixtures.open(fixture)) {
            writer.write(ObjectEntries.bare("a"));
        }
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Path log = temp.resolve("serve.log");
        Process process = new ProcessBuilder(java, "-Djdk.nio.maxCachedBufferSize=262144",
                "-cp", System.getProperty("java.class.path"), ReplayServerApp.class.getName(),
                "serve", "--fixture", fixture.toString(), "--bucket", "bucket",
                "--serving-mode", "duckdb", "--port", "0", "--stop-timeout", "2s")
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < deadline && process.isAlive()
                    && !Files.readString(log).contains("swath_replay endpoint=")) {
                Thread.sleep(25);
            }
            assertThat(Files.readString(log)).contains("swath_replay endpoint=");
            process.destroy(); // SIGTERM on the Linux test host.
            assertThat(process.waitFor(5, TimeUnit.SECONDS)).isTrue();
            assertThat(Files.readString(log)).contains("swath_replay_shutdown_hook complete");
        } finally {
            process.destroyForcibly();
        }
    }
}
