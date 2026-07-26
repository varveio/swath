/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

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
}
