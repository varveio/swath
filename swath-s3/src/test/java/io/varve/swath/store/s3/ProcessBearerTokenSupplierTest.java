/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.observability.SafeInput;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ProcessBearerTokenSupplier} runs an arbitrary shell command and treats its trimmed stdout
 * as the token, caching it for {@code refreshInterval}. Every case here uses a real {@code /bin/sh}
 * subprocess (no mocking the process layer) since the caching/refresh/failure behavior IS the
 * subprocess-handling logic under test.
 */
class ProcessBearerTokenSupplierTest {

    @Test
    void returnsTrimmedStdout() {
        var supplier = new ProcessBearerTokenSupplier("printf '  tok-123  \\n'", Duration.ofMinutes(45));
        assertThat(supplier.token()).isEqualTo("tok-123");
    }

    @Test
    void cachesWithinTheRefreshInterval() {
        var supplier = new ProcessBearerTokenSupplier(
                "echo tok-$(date +%s%N)", Duration.ofMinutes(45));
        String first = supplier.token();
        String second = supplier.token();
        assertThat(second).isEqualTo(first);
    }

    @Test
    void reRunsTheCommandOnceTheRefreshIntervalHasElapsed() throws InterruptedException {
        var supplier = new ProcessBearerTokenSupplier(
                "echo tok-$(date +%s%N)", Duration.ofMillis(1));
        String first = supplier.token();
        Thread.sleep(10);
        String second = supplier.token();
        assertThat(second).isNotEqualTo(first);
    }

    @Test
    void throwsOnNonZeroExit() {
        var supplier = new ProcessBearerTokenSupplier("echo denied >&2; exit 7", Duration.ofMinutes(45));
        assertThatThrownBy(supplier::token)
                .isInstanceOf(BearerTokenCommandException.class)
                .hasMessageContaining("exited 7")
                .hasMessageContaining("denied");
    }

    @Test
    void throwsOnEmptyOutput() {
        var supplier = new ProcessBearerTokenSupplier("true", Duration.ofMinutes(45));
        assertThatThrownBy(supplier::token)
                .isInstanceOf(BearerTokenCommandException.class)
                .hasMessageContaining("no output");
    }

    @Test
    void throwsWhenTheCommandCannotBeStarted() {
        var supplier = new ProcessBearerTokenSupplier(
                "/definitely/not/a/real/binary-xyz", Duration.ofMinutes(45));
        // Not "cannot start" -- /bin/sh -c itself starts fine and reports the exec failure via a
        // non-zero exit, exactly like a real misconfigured --bearer-token-command would.
        assertThatThrownBy(supplier::token).isInstanceOf(BearerTokenCommandException.class);
    }

    @Test
    void largeStderrDoesNotDeadlockAgainstSmallStdout() {
        // Regression guard for reading stdout/stderr concurrently instead of sequentially: a
        // command that fills the stderr pipe buffer before writing (small) stdout must not hang.
        var supplier = new ProcessBearerTokenSupplier(
                "yes err | head -c 200000 >&2; echo tok", Duration.ofMinutes(45));
        assertThat(supplier.token()).isEqualTo("tok");
    }

    @Test
    void stdoutBeyondTheCaptureLimitIsDrainedButRejected() {
        var supplier = new ProcessBearerTokenSupplier(
                "yes x | head -c " + (ProcessBearerTokenSupplier.MAX_CAPTURE_BYTES + 1),
                Duration.ofMinutes(45));

        assertThatThrownBy(supplier::token)
                .isInstanceOf(BearerTokenCommandException.class)
                .hasMessageContaining("stdout exceeded")
                .hasMessageContaining(Integer.toString(ProcessBearerTokenSupplier.MAX_CAPTURE_BYTES));
    }

    @Test
    void oversizedFailureStderrIsTruncatedToTheCaptureLimit() {
        var supplier = new ProcessBearerTokenSupplier(
                "yes e | head -c " + (ProcessBearerTokenSupplier.MAX_CAPTURE_BYTES + 100_000)
                        + " >&2; exit 9",
                Duration.ofMinutes(45));

        assertThatThrownBy(supplier::token)
                .isInstanceOf(BearerTokenCommandException.class)
                .hasMessageContaining("exited 9")
                .hasMessageContaining("stderr truncated")
                .satisfies(error -> assertThat(error.getMessage().length())
                        .isLessThan(ProcessBearerTokenSupplier.MAX_DIAGNOSTIC_CHARS + 100));
    }

    /**
     * Regression guard (CodeRabbit review, PR #23): a process that stays alive without closing its
     * pipes must be killed and reported at {@code commandTimeout}, not hang {@link
     * ProcessBearerTokenSupplier#token()} forever. The original bug joined the stdout/stderr reader
     * threads BEFORE calling {@code waitFor(timeout)} — since a live child's pipes stay open until
     * it exits, that join never returned, so the timeout check below it was unreachable. Because
     * {@code token()} is {@code synchronized}, that would have stalled every subsequent signing call
     * indefinitely, not just this one. Uses a short injected {@code commandTimeout} so the test
     * itself doesn't wait out the real 30s default.
     */
    @Test
    void killsAndReportsAHungProcessAtTheCommandTimeoutInsteadOfHangingForever() {
        var supplier = new ProcessBearerTokenSupplier("sleep 300", Duration.ofMinutes(45), Duration.ofMillis(300));
        assertThatThrownBy(supplier::token)
                .isInstanceOf(BearerTokenCommandException.class)
                .hasMessageContaining("did not exit within");
    }

    @Test
    void timeoutKillsAStubbornDescendantAndBoundsCleanup(@TempDir Path dir) throws Exception {
        Path pidFile = dir.resolve("descendant.pid");
        var supplier = new ProcessBearerTokenSupplier(
                "(trap '' TERM; while :; do sleep 1; done) & echo $! > '" + pidFile + "'; wait",
                Duration.ofMinutes(45), Duration.ofMillis(300));

        long started = System.nanoTime();
        assertThatThrownBy(supplier::token)
                .isInstanceOf(BearerTokenCommandException.class)
                .hasMessageContaining("did not exit within");
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));

        long descendantPid = Long.parseLong(Files.readString(pidFile).strip());
        assertThat(ProcessHandle.of(descendantPid).map(ProcessHandle::isAlive).orElse(false))
                .as("the timed-out helper's stubborn descendant was reaped")
                .isFalse();
    }

    @Test
    void detachedDescendantHoldingStdoutCannotMakeCleanupBlockForever(@TempDir Path dir)
            throws Exception {
        Path pidFile = dir.resolve("detached.pid");
        var supplier = new ProcessBearerTokenSupplier(
                "(sleep 300 & echo $! > '" + pidFile + "') & "
                        + "i=0; while [ ! -s '" + pidFile + "' ] && [ $i -lt 1000 ]; "
                        + "do i=$((i + 1)); done; echo token",
                Duration.ofMinutes(45), Duration.ofSeconds(5));

        long started = System.nanoTime();
        try {
            assertThatThrownBy(supplier::token)
                    .isInstanceOf(BearerTokenCommandException.class)
                    .hasMessageContaining("output streams did not close within");
            assertThat(Duration.ofNanos(System.nanoTime() - started))
                    .isLessThan(Duration.ofSeconds(5));
        } finally {
            for (int i = 0; i < 40 && !Files.exists(pidFile); i++) {
                Thread.sleep(25);
            }
            if (Files.exists(pidFile)) {
                long pid = Long.parseLong(Files.readString(pidFile).strip());
                ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly);
            }
        }
    }

    /**
     * The command must not be recoverable from a stringified supplier. {@link S3Config} is a record
     * holding this supplier, so {@code S3Config.toString()} recurses in here — one {@code
     * log.debug("config {}", config)} downstream would otherwise print the operator's
     * {@code --bearer-token-command}, and nothing obliges that command to merely MINT a token
     * ({@code 'echo <token>'} is a plausible spelling). The default {@code Object.toString()} would
     * make this pass by accident; the explicit override makes it hold by construction, and this
     * test fails if someone deletes it or turns the class into a record.
     */
    @Test
    void toStringRedactsTheCommandAndSurvivesBeingNestedInAnS3ConfigRecord() {
        var supplier = new ProcessBearerTokenSupplier("echo eyJhbGciOiJSUzI1NiRealTokenHere",
                Duration.ofMinutes(45));

        assertThat(supplier.toString())
                .doesNotContain("eyJhbGci")
                .doesNotContain("echo")
                .contains(SafeInput.REDACTED_SECRET);

        // The realistic leak path: the supplier stringified as a component of the config record.
        S3Config config = new S3Config(
                null, null, false,
                S3Config.DEFAULT_MAX_PARALLEL,
                S3Config.DEFAULT_MAX_ATTEMPTS,
                S3Config.DEFAULT_ATTEMPT_TIMEOUT,
                S3Config.DEFAULT_API_CALL_TIMEOUT,
                null,
                S3Config.DEFAULT_PROBE_ATTEMPT_TIMEOUT,
                supplier);
        assertThat(config.toString()).doesNotContain("eyJhbGci").doesNotContain("echo");
    }

    /** The resolved token must not be recoverable from a stringified identity either. */
    @Test
    void identityToStringRedactsTheToken() {
        assertThat(new BearerTokenIdentity("eyJhbGciOiJSUzI1NiRealTokenHere").toString())
                .doesNotContain("eyJhbGci")
                .contains(SafeInput.REDACTED_SECRET);
    }
}
