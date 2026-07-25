/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.OutputException;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Guards the terminal-disposition contract: whatever escapes a command, the process ends
 * with a deterministic exit code AND a {@code swath:} terminal marker — never a bare "docker exit 1,
 * no line".
 *
 * <p>Two escape shapes, two seams:
 * <ul>
 *   <li><b>An escaping {@link Exception}</b> is routed by picocli's {@code execute()} to
 *       {@link App}'s execution-exception handler, which maps it to {@link ExitCodes#UNEXPECTED}
 *       and prints {@code swath: unexpected error: ...}. This is fully unit-testable through
 *       {@link App#commandLine()}.</li>
 *   <li><b>An escaping {@link Error}</b> (or any {@link Throwable} that is not an {@link Exception})
 *       is NOT routed to that handler — the handler is typed {@code (Exception ex, ...)} — so it
 *       propagates straight out of {@code execute()}. In production the ONLY thing between it and
 *       the JVM default handler (bare exit, no marker) is {@code App}'s {@code catch (Throwable)}
 *       backstop. {@code App.main} calls {@code System.exit}, which JDK 25 can no longer trap, so the
 *       backstop logic is extracted into the package-private {@code
 *       App.executeWithBackstop(CommandLine, String[], PrintStream)} seam: it runs a given
 *       {@link CommandLine}, and for ANY escaping {@link Throwable} prints the {@code swath:
 *       unexpected error} marker and returns {@link ExitCodes#UNEXPECTED} instead of propagating.
 *       That seam is what {@code #escapingErrorReachesTheThrowableBackstopSeam} drives directly with
 *       an Error-throwing subcommand, exercising the real backstop end to end.</li>
 * </ul>
 */
class AppThrowableBackstopTest {

    @Command(name = "boom")
    static class ThrowingExceptionCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            throw new RuntimeException("simulated business-logic failure");
        }
    }

    @Command(name = "boomerror")
    static class ThrowingErrorCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            throw new Error("simulated fatal Error escaping the command");
        }
    }

    @Command(name = "domain-failure")
    static class ThrowingDomainFailureCommand implements Callable<Integer> {
        @Override
        public Integer call() throws OutputException {
            throw new OutputException("disk is full");
        }
    }

    @Command(name = "wrapped-domain-failure")
    static class ThrowingWrappedDomainFailureCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            throw new IllegalStateException("wrapper", new OutputException("disk is full"));
        }
    }

    @Test
    void domainFailureWithExitOneIsNotRenderedAsUnexpected() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("domain-failure", new ThrowingDomainFailureCommand());
        cmd.setErr(new PrintWriter(err));

        int exit = cmd.execute("domain-failure");

        assertThat(exit).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(err.toString()).contains("swath: disk is full")
                .doesNotContain("unexpected error")
                .doesNotContain(OutputException.class.getName());
    }

    /**
     * {@code -qq} silences logging (down to {@code OFF}, per {@link CliLoggingTest}) but must NOT
     * silence the {@code swath: …} terminal error line — that line goes through {@code
     * cmd.getErr()} in {@link App#handleExecutionException}, bypassing SLF4J entirely.
     */
    @Test
    void quietQqDoesNotSuppressTheTerminalErrorLine() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("domain-failure", new ThrowingDomainFailureCommand());
        cmd.setErr(new PrintWriter(err));

        int exit = cmd.execute("-qq", "domain-failure");

        assertThat(exit).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(err.toString()).contains("swath: disk is full");
    }

    @Test
    void wrappedDomainFailureWithExitOneIsAlsoRenderedAsADomainError() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("wrapped-domain-failure", new ThrowingWrappedDomainFailureCommand());
        cmd.setErr(new PrintWriter(err));

        int exit = cmd.execute("wrapped-domain-failure");

        assertThat(exit).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(err.toString()).contains("swath: disk is full")
                .doesNotContain("unexpected error")
                .doesNotContain(OutputException.class.getName());
    }

    /**
     * An escaping {@link Exception} → {@link ExitCodes#UNEXPECTED} and a {@code swath: unexpected
     * error} terminal marker, via the real {@link App#commandLine()} execution-exception handler.
     */
    @Test
    void escapingExceptionYieldsUnexpectedExitAndTerminalMarker() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("boom", new ThrowingExceptionCommand());
        cmd.setErr(new PrintWriter(err));

        int exit = cmd.execute("boom");

        assertThat(exit).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(err.toString())
                .as("terminal disposition marker for an escaping Exception")
                .contains("swath: unexpected error");
    }

    /**
     * An escaping {@link Error} propagates out of {@code execute()} unhandled — picocli does not
     * route it to the {@code (Exception ex, ...)} handler, so no marker is emitted at this seam.
     * This documents (and locks) the gap that {@code App.main}'s {@code catch (Throwable)} backstop
     * exists to close: without it a true {@link Error} would reach the JVM default handler and exit
     * with a bare code and no {@code swath:} line. The backstop itself calls {@code System.exit} and
     * is not in-process unit-testable — see the class javadoc SEAM LIMITATION.
     */
    @Test
    void escapingErrorPropagatesPastPicocliExecute_onlyMainThrowableBackstopCatchesIt() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("boomerror", new ThrowingErrorCommand());
        cmd.setErr(new PrintWriter(err));

        assertThatThrownBy(() -> cmd.execute("boomerror"))
                .as("a true Error is not routed to the Exception-typed handler")
                .isInstanceOf(Error.class)
                .hasMessageContaining("simulated fatal Error");

        assertThat(err.toString())
                .as("execute() emits no marker for an Error; only main's Throwable backstop does")
                .doesNotContain("swath: unexpected error");
    }

    /**
     * Drives the REAL backstop, not just the seam it exists to compensate for: an {@link Error}
     * escaping a subcommand's {@code call()} propagates past picocli's {@code execute()} (as the
     * previous test documents) and is caught by {@code App.executeWithBackstop(CommandLine,
     * String[], PrintStream)}'s {@code catch (Throwable)}, which is the ONLY thing between it and a
     * bare, markerless process exit in production.
     *
     * <p>MUTATION REQUIREMENT: changing the product's {@code return ExitCodes.UNEXPECTED;} to
     * {@code return ExitCodes.SUCCESS;}, or removing the {@code err.println(...)} call, MUST turn
     * this test RED — assertion (a) pins the exit code, assertion (b) pins the marker text.
     */
    @Test
    void escapingErrorReachesTheThrowableBackstopSeam() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(out, true, StandardCharsets.UTF_8);
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("boomerror", new ThrowingErrorCommand());

        int exit = App.executeWithBackstop(cmd, new String[] {"boomerror"}, err);

        assertThat(exit).as("the backstop's terminal exit code for an escaping Error")
                .isEqualTo(ExitCodes.UNEXPECTED);
        String captured = out.toString(StandardCharsets.UTF_8);
        assertThat(captured)
                .as("the backstop's terminal disposition marker for an escaping Error")
                .contains("swath: unexpected error")
                .contains("simulated fatal Error escaping the command");
    }
}
