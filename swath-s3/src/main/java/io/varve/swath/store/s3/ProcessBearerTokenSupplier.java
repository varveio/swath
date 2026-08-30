/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.SafeInput;
import java.io.ByteArrayOutputStream;
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

    private static final Duration DEFAULT_COMMAND_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration TERMINATION_TIMEOUT = Duration.ofSeconds(2);
    static final int MAX_CAPTURE_BYTES = 64 * 1024;
    static final int MAX_DIAGNOSTIC_CHARS = 4096;

    private final List<String> command;
    private final Duration refreshInterval;
    private final Duration commandTimeout;

    private volatile String cachedToken;
    private volatile Instant fetchedAt = Instant.MIN;

    public ProcessBearerTokenSupplier(String shellCommand, Duration refreshInterval) {
        this(shellCommand, refreshInterval, DEFAULT_COMMAND_TIMEOUT);
    }

    /** Test-only: a short {@code commandTimeout} so a hung-process test doesn't wait 30s. */
    ProcessBearerTokenSupplier(String shellCommand, Duration refreshInterval, Duration commandTimeout) {
        this.command = List.of("/bin/sh", "-c", shellCommand);
        this.refreshInterval = refreshInterval;
        this.commandTimeout = commandTimeout;
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

    /**
     * Redacts the command, and exists for that reason alone. {@link S3Config} is a record holding
     * this supplier, so {@code S3Config.toString()} recurses in here — one {@code log.debug("config
     * {}", config)} anywhere downstream would otherwise put the operator's
     * {@code --bearer-token-command} in a log, and nothing obliges that command to merely MINT a
     * token ({@code 'echo <token>'} is a plausible spelling). Without this override the default
     * {@code Object.toString()} makes that safe by accident; this makes it safe by construction.
     */
    @Override
    public String toString() {
        return "ProcessBearerTokenSupplier[command=" + SafeInput.REDACTED_SECRET
                + ", refreshInterval=" + refreshInterval + "]";
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
        // reading the first, deadlocking both sides. These reader threads run independently of —
        // and start before — the waitFor() below, so bounding the process's lifetime there doesn't
        // reintroduce that deadlock.
        AtomicReference<CapturedOutput> stdoutRef = new AtomicReference<>(CapturedOutput.EMPTY);
        AtomicReference<CapturedOutput> stderrRef = new AtomicReference<>(CapturedOutput.EMPTY);
        Thread stdoutReader = Thread.ofVirtual().start(() -> stdoutRef.set(readBounded(process.getInputStream())));
        Thread stderrReader = Thread.ofVirtual().start(() -> stderrRef.set(readBounded(process.getErrorStream())));
        // waitFor() MUST run before joining the reader threads: a hung child that keeps its pipes
        // open (the common case — alive but not yet producing output) never triggers stream EOF, so
        // join() alone could block forever and the commandTimeout below would never be reached.
        // token() is synchronized, so that would stall every subsequent signing call indefinitely.
        boolean finished;
        try {
            finished = process.waitFor(commandTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            terminate(process, stdoutReader, stderrReader);
            Thread.currentThread().interrupt();
            throw new BearerTokenCommandException("interrupted while running --bearer-token-command", e);
        }
        if (!finished) {
            terminate(process, stdoutReader, stderrReader);
            throw new BearerTokenCommandException(
                    "--bearer-token-command did not exit within " + commandTimeout);
        }
        // The process has exited (or was just destroyed above), so its pipes are closed and these
        // joins return promptly.
        try {
            if (!joinReaders(stdoutReader, stderrReader, TERMINATION_TIMEOUT)) {
                // stdout/stderr cannot be closed safely while a reader may hold the
                // ProcessPipeInputStream monitor. Abandon the daemon virtual readers; the detached
                // fd holder's eventual exit releases both pipes. This bounds the caller at the
                // cost of two readers/fds for each persistently misbehaving command invocation.
                closeProcessStdin(process);
                throw new BearerTokenCommandException(
                        "--bearer-token-command output streams did not close within " + TERMINATION_TIMEOUT);
            }
            closeReadableProcessStreams(process);
        } catch (InterruptedException e) {
            closeProcessStdin(process);
            Thread.currentThread().interrupt();
            throw new BearerTokenCommandException("interrupted while reading --bearer-token-command output", e);
        }
        CapturedOutput stdoutOutput = stdoutRef.get();
        CapturedOutput stderrOutput = stderrRef.get();
        String stdout = stdoutOutput.text().strip();
        String stderr = stderrOutput.text().strip();
        int exitValue = process.exitValue();
        if (exitValue != 0) {
            throw new BearerTokenCommandException("--bearer-token-command exited " + exitValue
                    + (stderr.isEmpty() ? "" : ": " + boundedDiagnostic(stderr))
                    + (stderrOutput.overflowed() ? " [stderr truncated]" : ""));
        }
        if (stdoutOutput.overflowed()) {
            throw new BearerTokenCommandException(
                    "--bearer-token-command stdout exceeded " + MAX_CAPTURE_BYTES + " bytes");
        }
        if (stdout.isEmpty()) {
            throw new BearerTokenCommandException("--bearer-token-command produced no output");
        }
        return stdout;
    }

    private static String boundedDiagnostic(String stderr) {
        StringBuilder safe = new StringBuilder(Math.min(stderr.length(), MAX_DIAGNOSTIC_CHARS));
        for (int offset = 0; offset < stderr.length();) {
            int codePoint = stderr.codePointAt(offset);
            String escaped = SafeInput.logText(new String(Character.toChars(codePoint)));
            if (safe.length() + escaped.length() > MAX_DIAGNOSTIC_CHARS) {
                break;
            }
            safe.append(escaped);
            offset += Character.charCount(codePoint);
        }
        return safe.toString();
    }

    private record CapturedOutput(String text, boolean overflowed) {
        private static final CapturedOutput EMPTY = new CapturedOutput("", false);
    }

    /** Drain the whole pipe to preserve child-process liveness while retaining only a fixed prefix. */
    private static CapturedOutput readBounded(InputStream in) {
        ByteArrayOutputStream captured = new ByteArrayOutputStream(MAX_CAPTURE_BYTES);
        boolean overflowed = false;
        byte[] buffer = new byte[8192];
        try {
            int read;
            while ((read = in.read(buffer)) >= 0) {
                int remaining = MAX_CAPTURE_BYTES - captured.size();
                int keep = Math.min(read, Math.max(0, remaining));
                if (keep > 0) {
                    captured.write(buffer, 0, keep);
                }
                overflowed |= keep < read;
            }
        } catch (IOException e) {
            // A process killed on timeout commonly closes its pipes underneath these readers.
        }
        return new CapturedOutput(captured.toString(StandardCharsets.UTF_8), overflowed);
    }

    private static void terminate(Process process, Thread stdoutReader, Thread stderrReader) {
        closeProcessStdin(process);
        List<ProcessHandle> descendants = process.descendants().toList();
        descendants.reversed().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();

        long deadline = System.nanoTime() + TERMINATION_TIMEOUT.toNanos();
        awaitExit(process.toHandle(), deadline);
        for (ProcessHandle descendant : descendants) {
            awaitExit(descendant, deadline);
        }
        try {
            boolean readersClosed = joinReaders(stdoutReader, stderrReader, remaining(deadline));
            if (readersClosed) {
                closeReadableProcessStreams(process);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean awaitExit(ProcessHandle process, long deadline) {
        while (process.isAlive() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !process.isAlive();
    }

    private static boolean joinReaders(Thread stdoutReader, Thread stderrReader, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        joinUntil(stdoutReader, deadline);
        joinUntil(stderrReader, deadline);
        return !stdoutReader.isAlive() && !stderrReader.isAlive();
    }

    private static void joinUntil(Thread thread, long deadline) throws InterruptedException {
        long nanos = remaining(deadline).toNanos();
        if (nanos > 0 && thread.isAlive()) {
            thread.join(Duration.ofNanos(nanos));
        }
    }

    private static Duration remaining(long deadline) {
        return Duration.ofNanos(Math.max(0L, deadline - System.nanoTime()));
    }

    private static void closeReadableProcessStreams(Process process) {
        closeQuietly(process.getInputStream());
        closeQuietly(process.getErrorStream());
    }

    private static void closeProcessStdin(Process process) {
        closeQuietly(process.getOutputStream());
    }

    private static void closeQuietly(AutoCloseable stream) {
        try {
            stream.close();
        } catch (Exception ignored) {
            // Best-effort teardown after exit/timeout; the command outcome remains authoritative.
        }
    }
}
