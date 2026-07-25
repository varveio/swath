/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Pins {@link TerminalCapabilities}: independent per-fd probing (the JDK-25
 * {@code isatty} fix).
 */
class TerminalCapabilitiesTest {

    // ---- the full 2x2 stdout/stderr matrix ---------------------------------------------------
    // Via the FdProbe seam -- no real controlling terminal exists under a build.

    @Test
    void stdoutTerminalStderrTerminal() {
        TerminalCapabilities term = new TerminalCapabilities(fd -> true);
        assertThat(term.stdoutIsTerminal()).isTrue();
        assertThat(term.stderrIsTerminal()).isTrue();
    }

    @Test
    void stdoutTerminalStderrPiped() {
        // The JDK-25 bug this replaces: System.console() != null conflates stdin AND
        // stdout, so a real terminal on stdout with piped stdin was wrongly classified non-tty.
        // A per-fd probe must answer each fd independently.
        TerminalCapabilities term = new TerminalCapabilities(fd -> fd == TerminalCapabilities.STDOUT_FD);
        assertThat(term.stdoutIsTerminal()).isTrue();
        assertThat(term.stderrIsTerminal()).isFalse();
    }

    @Test
    void stdoutPipedStderrTerminal() {
        TerminalCapabilities term = new TerminalCapabilities(fd -> fd == TerminalCapabilities.STDERR_FD);
        assertThat(term.stdoutIsTerminal()).isFalse();
        assertThat(term.stderrIsTerminal()).isTrue();
    }

    @Test
    void stdoutPipedStderrPiped() {
        TerminalCapabilities term = new TerminalCapabilities(fd -> false);
        assertThat(term.stdoutIsTerminal()).isFalse();
        assertThat(term.stderrIsTerminal()).isFalse();
    }

    @Test
    void realProbeNeverThrowsUnderATestRunner() {
        // No real controlling terminal exists under a build/CI JVM -- the production probe must
        // resolve to a boolean (almost always false here), never throw.
        TerminalCapabilities real = new TerminalCapabilities();
        assertThat(real.stdoutIsTerminal()).isFalse();
        assertThat(real.stderrIsTerminal()).isFalse();
    }

}
