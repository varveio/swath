/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.OutputException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Pins the two halves of "a full disk is legible from outside the process".
 *
 * <p>Both were missing together, and the combination left a large listing fleet unable to see disk
 * exhaustion at all: buckets filled a 30 GiB workspace, exited 1, and put nothing on stderr but
 * {@code swath: parquet writer failed}. An external runner classifying that run had neither a code
 * nor a message to go on, so it read the failure as memory pressure and retried on ever-larger
 * machines with the same disk.
 *
 * <ul>
 *   <li><b>The code</b> — {@link OutputException} exits {@link ExitCodes#DISK_FULL} when the cause
 *       chain holds an out-of-space {@link IOException}, and the generic {@code 1} otherwise. This
 *       is the signal a runner should classify on, because it is contractual.</li>
 *   <li><b>The message</b> — the terminal {@code swath:} line carries the cause chain, not just
 *       the domain exception's own stage name.</li>
 * </ul>
 */
class DiskFullClassificationTest {

    /** The message Linux gives {@code ENOSPC}, which is all the JDK surfaces. */
    private static final String ENOSPC = "No space left on device";

    @Command(name = "disk-full")
    static class DiskFullCommand implements Callable<Integer> {
        @Override
        public Integer call() throws OutputException {
            throw new OutputException("parquet writer failed", new IOException(ENOSPC));
        }
    }

    @Command(name = "multiline")
    static class MultiLineCauseCommand implements Callable<Integer> {
        @Override
        public Integer call() throws OutputException {
            throw new OutputException("parquet writer failed",
                    new IOException("write failed\n\tat part-00007.parquet\n" + ENOSPC));
        }
    }

    @Command(name = "output-failure")
    static class OtherOutputFailureCommand implements Callable<Integer> {
        @Override
        public Integer call() throws OutputException {
            throw new OutputException("parquet writer failed", new IOException("Permission denied"));
        }
    }

    // ---- the code -------------------------------------------------------------------------

    @Test
    void outOfSpaceCauseGetsItsOwnExitCode() {
        var failure = new OutputException("parquet writer failed", new IOException(ENOSPC));

        assertThat(failure.exitCode()).isEqualTo(ExitCodes.DISK_FULL);
        assertThat(ExitCodes.forThrowable(failure)).isEqualTo(ExitCodes.DISK_FULL);
    }

    /**
     * The real chain is deeper than one link — Arrow/Parquet wrap the OS error before the writer
     * pool wraps that — so the search must not stop at the immediate cause.
     */
    @Test
    void outOfSpaceIsFoundThroughAWrappedCauseChain() {
        var failure = new OutputException("parquet writer failed",
                new RuntimeException("write failed", new IOException(ENOSPC)));

        assertThat(failure.exitCode()).isEqualTo(ExitCodes.DISK_FULL);
    }

    /**
     * Both conditions, in both spellings, at both depths. Which of the two a platform produces —
     * the {@code strerror} wording or the symbolic name — is not ours to predict, and neither is
     * how many times something re-wraps it on the way up. Testing the cross product is what stops
     * a regression that keeps the chain walk for one spelling and loses it for another.
     */
    @Test
    void everySpellingOfOutOfSpaceIsRecognisedAtAnyDepth() {
        for (String message : new String[] {
            ENOSPC, "ENOSPC", "Disk quota exceeded", "EDQUOT", "write failed: ENOSPC",
        }) {
            assertThat(new OutputException("parquet writer failed", new IOException(message))
                    .exitCode())
                .as("immediate cause: %s", message)
                .isEqualTo(ExitCodes.DISK_FULL);
            assertThat(new OutputException("parquet writer failed",
                    new RuntimeException("write failed", new IOException(message))).exitCode())
                .as("wrapped cause: %s", message)
                .isEqualTo(ExitCodes.DISK_FULL);
        }
    }

    /** Every other sink failure keeps the generic code: the disk code must stay actionable. */
    @Test
    void otherOutputFailuresStayOnTheGenericCode() {
        assertThat(new OutputException("parquet writer failed", new IOException("Permission denied"))
                .exitCode()).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(new OutputException("parquet writer failed").exitCode())
                .isEqualTo(ExitCodes.UNEXPECTED);
    }

    /** A different domain exception is not reclassified just because a full disk is underneath. */
    @Test
    void onlyOutputFailuresClaimTheDiskCode() {
        var failure = new CheckpointException("checkpoint corrupt", new IOException(ENOSPC));

        assertThat(ExitCodes.forThrowable(failure)).isNotEqualTo(ExitCodes.DISK_FULL);
    }

    @Test
    void theProcessExitsWithTheDiskCodeEndToEnd() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("disk-full", new DiskFullCommand());
        cmd.setErr(new PrintWriter(err));

        assertThat(cmd.execute("disk-full")).isEqualTo(ExitCodes.DISK_FULL);
    }

    @Test
    void anOrdinaryOutputFailureStillExitsOneEndToEnd() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("output-failure", new OtherOutputFailureCommand());
        cmd.setErr(new PrintWriter(err));

        assertThat(cmd.execute("output-failure")).isEqualTo(ExitCodes.UNEXPECTED);
    }

    // ---- the message ----------------------------------------------------------------------

    @Test
    void theTerminalLineCarriesTheCauseNotJustTheStage() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("disk-full", new DiskFullCommand());
        cmd.setErr(new PrintWriter(err));

        cmd.execute("disk-full");

        assertThat(err.toString())
                .as("the stage alone is what the field reports saw, and it was not diagnosable")
                .contains("swath: parquet writer failed: " + ENOSPC);
    }

    /** One line under the coordinator's lock — a stack trace would be splice-able. */
    @Test
    void theTerminalLineStaysOneLine() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("disk-full", new DiskFullCommand());
        cmd.setErr(new PrintWriter(err));

        cmd.execute("disk-full");

        assertThat(err.toString().strip().lines()).hasSize(1);
    }

    /**
     * Stripping the ends is not enough — an embedded line break would split the record that the
     * stderr coordinator's lock exists to keep whole, which is the guarantee above.
     */
    @Test
    void anEmbeddedLineBreakIsFlattened() {
        var failure = new OutputException("parquet writer failed",
                new IOException("write failed\n\tat part-00007.parquet\r\n" + ENOSPC));

        assertThat(App.messageChain(failure)).doesNotContain("\n").doesNotContain("\r");
        assertThat(App.messageChain(failure))
                .isEqualTo("parquet writer failed: write failed \tat part-00007.parquet " + ENOSPC);
    }

    @Test
    void aMultiLineCauseStillPrintsAsOneTerminalLine() {
        StringWriter err = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.addSubcommand("multiline", new MultiLineCauseCommand());
        cmd.setErr(new PrintWriter(err));

        cmd.execute("multiline");

        assertThat(err.toString().strip().lines()).hasSize(1);
    }

    @Test
    void aCauselessDomainErrorRendersExactlyAsBefore() {
        assertThat(App.messageChain(new OutputException("parquet writer failed")))
                .isEqualTo("parquet writer failed");
    }

    /**
     * {@code new IOException(cause)} takes {@code cause.toString()} as its own message, so an
     * un-deduplicated walk renders the same text twice.
     */
    @Test
    void aRepeatedMessageIsNotPrintedTwice() {
        var inner = new IOException(ENOSPC);

        assertThat(App.messageChain(new OutputException("parquet writer failed", inner)))
                .isEqualTo("parquet writer failed: " + ENOSPC);
        assertThat(App.messageChain(new OutputException(ENOSPC, inner)))
                .isEqualTo(ENOSPC);
    }

    /** A messageless link still names itself, or the chain silently shortens. */
    @Test
    void aMessagelessCauseStillNamesItself() {
        assertThat(App.messageChain(new OutputException("parquet writer failed", new IllegalStateException())))
                .isEqualTo("parquet writer failed: IllegalStateException");
    }

    /** A self-referential chain must terminate rather than spin. */
    @Test
    void aCyclicChainTerminates() {
        var a = new IOException("a");
        var b = new IOException("b", a);
        a.initCause(b);

        assertThat(App.messageChain(b)).startsWith("b: a");
    }
}
