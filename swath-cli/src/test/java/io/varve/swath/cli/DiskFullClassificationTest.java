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
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Command;

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
            "write failed (EDQUOT)",
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

    @Test
    void classificationIsLocaleStable() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr"));

            assertThat(new OutputException("parquet writer failed", new IOException("DISK QUOTA EXCEEDED"))
                    .exitCode()).isEqualTo(ExitCodes.DISK_FULL);
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void aSymbolicNameInAFileSystemPathDoesNotClaimTheDiskCode() {
        var cause = new FileSystemException("/tmp/ENOSPC/results", null, "Permission denied");

        assertThat(new OutputException("parquet writer failed", cause).exitCode())
                .isEqualTo(ExitCodes.UNEXPECTED);
    }

    @Test
    void aFileSystemExceptionWithoutAReasonDoesNotInspectItsPath() {
        var cause = new NoSuchFileException("/tmp/ENOSPC/results");

        assertThat(new OutputException("parquet writer failed", cause).exitCode())
                .isEqualTo(ExitCodes.UNEXPECTED);
    }

    @Test
    void symbolicNamesMustBeDiagnosticTokens() {
        assertThat(new OutputException("parquet writer failed", new IOException("pathENOSPCfile"))
                .exitCode()).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(new OutputException("parquet writer failed", new IOException("edquotation failure"))
                .exitCode()).isEqualTo(ExitCodes.UNEXPECTED);
    }

    @Test
    void otherOutputFailuresStayOnTheGenericCode() {
        assertThat(new OutputException("parquet writer failed", new IOException("Permission denied"))
                .exitCode()).isEqualTo(ExitCodes.UNEXPECTED);
        assertThat(new OutputException("parquet writer failed").exitCode())
                .isEqualTo(ExitCodes.UNEXPECTED);
    }

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

    @Test
    void controlCharactersAreEscaped() {
        var failure = new OutputException("parquet writer failed",
                new IOException("write failed\n\tat \u001bpart-00007.parquet\0\r\n" + ENOSPC));

        assertThat(App.messageChain(failure))
                .isEqualTo("parquet writer failed: write failed\\x0a\\x09at "
                        + "\\x1bpart-00007.parquet\\x00\\x0d\\x0a" + ENOSPC);
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
    void anAdjacentRepeatedMessageIsNotPrintedTwice() {
        var inner = new IOException(ENOSPC);

        assertThat(App.messageChain(new OutputException("parquet writer failed", inner)))
                .isEqualTo("parquet writer failed: " + ENOSPC);
        assertThat(App.messageChain(new OutputException(ENOSPC, inner)))
                .isEqualTo(ENOSPC);
    }

    @Test
    void anIOExceptionCauseWrapperDoesNotRepeatItsCauseText() {
        var inner = new IOException(ENOSPC);
        var wrapper = new IOException(inner);

        assertThat(App.messageChain(new OutputException("parquet writer failed", wrapper)))
                .isEqualTo("parquet writer failed: " + wrapper.getMessage());
    }

    @Test
    void distinctSuffixMessagesArePreserved() {
        assertThat(App.messageChain(new OutputException("write failed", new IOException("failed"))))
                .isEqualTo("write failed: failed");
    }

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

        assertThat(App.messageChain(b)).isEqualTo("b: a");
    }
}
