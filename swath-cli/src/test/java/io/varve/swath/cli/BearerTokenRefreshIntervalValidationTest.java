/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * {@code --bearer-token-refresh-interval} only means anything alongside {@code
 * --bearer-token-command}: alone it used to be silently ignored, and the run would sign with SigV4
 * instead. That slip is most likely on {@code swath resume}, where the command is {@link
 * ResumeClass#FREE} and has to be re-passed by hand — so the failure mode was "confusing auth error
 * against a bearer-only endpoint", or a run against whatever ambient identity the SDK chain found,
 * rather than the operator's actual mistake. Reject it at exit 2 instead.
 */
class BearerTokenRefreshIntervalValidationTest {

    @Test
    void refreshIntervalWithoutACommandIsRejected() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--region", "us-east-1",
                "--bearer-token-refresh-interval", "10m");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .satisfies(e -> assertThat(ExitCodes.forThrowable(e)).isEqualTo(2))
                .hasMessageContaining("--bearer-token-refresh-interval")
                .hasMessageContaining("--bearer-token-command");
    }

    /**
     * The same rejection on {@code swath resume}, which is where the slip is likeliest: the command
     * is never persisted, so an operator re-passing the pair by hand can easily carry over only the
     * interval. Both flags are forwarded onto the delegated {@link ListCommand}, so one guard covers
     * both commands.
     */
    @Test
    void refreshIntervalWithoutACommandIsRejectedOnResumeToo() {
        ResumeCommand resume = new ResumeCommand();
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--region", "us-east-1");
        resume.bearer.refreshInterval = "10m";
        cmd.connection.bearer.copyFrom(resume.bearer);

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--bearer-token-refresh-interval");
    }

    /**
     * The guard must not fire on the overwhelmingly common case — neither flag passed — which is
     * what {@code null} out of {@code resolveBearerTokenSupplier()} encodes. A malformed interval
     * alongside a command still reaches the parser, proving the early return did not swallow it.
     */
    @Test
    void aCommandWithAnIntervalStillParsesTheInterval() {
        ListCommand cmd = new ListCommand();
        new CommandLine(cmd).parseArgs("s3://bucket/prefix", "--checkpoint", "none",
                "--region", "us-east-1",
                "--bearer-token-command", "printf token",
                "--bearer-token-refresh-interval", "not-a-duration");

        assertThatThrownBy(cmd::call)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("bearer-token-refresh-interval");
    }
}
