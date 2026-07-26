/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.SafeInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link BearerTokenSupplier} backed by an external command (run through {@code /bin/sh -c}),
 * whose stdout — trimmed of surrounding whitespace — is the token. Deliberately provider-agnostic:
 * {@code gcloud auth print-access-token} for GCS, a Workload-Identity-Federation exchange script, or
 * any other bearer-minting command all work the same way, so no cloud-provider SDK or credential
 * library is pulled into {@code swath-s3} for this (module boundary: {@code
 * docs/internals/build-and-modules.md}).
 *
 * <p>The token is cached and re-minted only after {@code refreshInterval} elapses since the last
 * successful run — a fixed cadence, not real expiry-awareness (the raw string alone carries no
 * portable expiry). Size {@code refreshInterval} comfortably under the token source's real TTL
 * (default 45 min; GCS/Google OAuth access tokens are typically valid ~1 h).
 *
 * <p>Thread-safe: concurrent callers during a refresh block on the same in-flight command rather
 * than racing separate processes.
 */
public final class ProcessBearerTokenSupplier implements BearerTokenSupplier {

    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(30);

    private final List<String> command;
    private final Duration refreshInterval;

    private volatile String cachedToken;
    private volatile Instant fetchedAt = Instant.MIN;

    public ProcessBearerTokenSupplier(String shellCommand, Duration refreshInterval) {
        this.command = List.of("/bin/sh", "-c", shellCommand);
        this.refreshInterval = refreshInterval;
    }

    @Override
    public synchronized String token() {
        Instant now = Instant.now();
        if (cachedToken == null || now.isAfter(fetchedAt.plus(refreshInterval))) {
            cachedToken = run();
            fetchedAt = now;
        }
        return cachedToken;
    }

    private String run() {
        Process process;
        try {
            process = new ProcessBuilder(command).start();
        } catch (IOException e) {
            throw new BearerTokenCommandException("failed to start --bearer-token-command", e);
        }
        // Drain stdout/stderr concurrently (not sequentially): a child that writes enough to the
        // stream we read second can otherwise block on a full pipe buffer while we sit blocked
        // reading the first, deadlocking both sides.
        AtomicReference<String> stdoutRef = new AtomicReference<>("");
        AtomicReference<String> stderrRef = new AtomicReference<>("");
        Thread stdoutReader = Thread.ofVirtual().start(() -> stdoutRef.set(readAllQuietly(process.getInputStream())));
        Thread stderrReader = Thread.ofVirtual().start(() -> stderrRef.set(readAllQuietly(process.getErrorStream())));
        try {
            stdoutReader.join();
            stderrReader.join();
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new BearerTokenCommandException("interrupted while reading --bearer-token-command output", e);
        }
        String stdout = stdoutRef.get().strip();
        String stderr = stderrRef.get().strip();
        boolean finished;
        try {
            finished = process.waitFor(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new BearerTokenCommandException("interrupted while running --bearer-token-command", e);
        }
        if (!finished) {
            process.destroyForcibly();
            throw new BearerTokenCommandException(
                    "--bearer-token-command did not exit within " + COMMAND_TIMEOUT);
        }
        int exitValue = process.exitValue();
        if (exitValue != 0) {
            throw new BearerTokenCommandException("--bearer-token-command exited " + exitValue
                    + (stderr.isEmpty() ? "" : ": " + SafeInput.logText(stderr)));
        }
        if (stdout.isEmpty()) {
            throw new BearerTokenCommandException("--bearer-token-command produced no output");
        }
        return stdout;
    }

    private static String readAllQuietly(InputStream in) {
        try {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }
}
