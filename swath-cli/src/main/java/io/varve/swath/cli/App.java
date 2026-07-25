/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.SwathException;
import io.varve.swath.observability.SafeInput;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.UnmatchedArgumentException;

/**
 * The {@code swath} entry point (Picocli). Verbs: {@code list}, {@code resume}. The
 * execution exception handler maps every terminal throwable to its documented exit
 * code via {@link ExitCodes} (UNIT-exit).
 */
@Command(
        name = "swath",
        mixinStandardHelpOptions = true,
        versionProvider = App.VersionProvider.class,
        description = "High-performance object-store lister.",
        subcommands = {
                ListCommand.class,
                ResumeCommand.class,
                DumpRunCommand.class,
                HelpCommand.class,
        })
public final class App implements Callable<Integer>, GlobalOptions.Carrier {

    /** Keeps {@code --version} aligned with the Gradle archive manifest. */
    public static final class VersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            String version = App.class.getPackage().getImplementationVersion();
            return new String[] {"swath " + (version == null ? "development" : version)};
        }
    }

    /** A leading {@code scheme://} — matches any store URI (today only {@code s3://}), so a bare
     * URI at the top level (no verb) can be recognized without hard-coding the scheme list. */
    private static final Pattern URI_SHAPED = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*://[\\s\\S]*");

    @Mixin
    final GlobalOptions global = new GlobalOptions();

    @Override
    public GlobalOptions globalOptions() {
        return global;
    }

    @Override
    public Integer call() {
        // No subcommand → usage error on stderr, exit 2 (incomplete invocation).
        CommandLine.usage(this, System.err);
        return CommandLine.ExitCode.USAGE;   // 2
    }

    /** Build the configured {@link CommandLine} (shared by main and tests). */
    public static CommandLine commandLine() {
        CommandLine cmd = new CommandLine(new App())
                .setExecutionExceptionHandler(App::handleExecutionException)
                .setCaseInsensitiveEnumValuesAllowed(true);
        CommandLine.IExecutionStrategy runLast = new CommandLine.RunLast();
        cmd.setExecutionStrategy(parseResult -> {
            CommandLine parsed = parseResult.commandSpec().commandLine();
            CliLogging.configure(GlobalOptions.effectiveVerbosity(parsed), GlobalOptions.effectiveQuietLevel(parsed));
            return runLast.execute(parseResult);
        });
        CommandLine completion = new CommandLine(new AutoComplete.GenerateCompletion());
        completion.getCommandSpec().name("completion").usageMessage().hidden(true);
        cmd.addSubcommand("completion", completion);
        CommandLine.IParameterExceptionHandler defaultHandler = cmd.getParameterExceptionHandler();
        cmd.setParameterExceptionHandler(
                (ex, args) -> handleParameterException(ex, args, defaultHandler));
        return cmd;
    }

    /**
     * Explicit verbs, no default command: a bare {@code swath s3://bucket/prefix} does
     * not implicitly list — it recovers the convenience of a default command, with none of its
     * ambiguity, via a "did you mean" hint pointing at {@code swath list}. Every other parse error
     * (typo'd subcommand, unknown option, …) keeps picocli's own message/suggestions/exit code.
     */
    private static int handleParameterException(ParameterException ex, String[] args,
            CommandLine.IParameterExceptionHandler defaultHandler) throws Exception {
        if (ex instanceof UnmatchedArgumentException unmatchedEx && ex.getCommandLine().getParent() == null) {
            List<String> unmatched = unmatchedEx.getUnmatched();
            if (!unmatched.isEmpty() && URI_SHAPED.matcher(unmatched.get(0)).matches()) {
                PrintWriter err = ex.getCommandLine().getErr();
                String safeUnmatched = SafeInput.logText(unmatched.get(0));
                err.printf("swath: '%s' is not a command%n", safeUnmatched);
                err.printf("did you mean: swath list %s?%n", safeUnmatched);
                err.println("Try 'swath --help'.");
                err.flush();
                return ex.getCommandLine().getCommandSpec().exitCodeOnInvalidInput();
            }
        }
        return defaultHandler.handleParseException(ex, args);
    }

    private static int handleExecutionException(Exception ex, CommandLine cmd,
                                                CommandLine.ParseResult parseResult) {
        int code = ExitCodes.forThrowable(ex);
        SwathException domain = domainException(ex);
        if (domain != null) {
            cmd.getErr().println("swath: " + domain.getMessage());
        } else if (code == ExitCodes.UNEXPECTED) {
            PrintWriter err = cmd.getErr();
            err.println("swath: unexpected error: " + ex);
            if (GlobalOptions.effectiveVerbosity(cmd) > 0) {
                ex.printStackTrace(err);
            }
        } else if (code != ExitCodes.SUCCESS) {
            cmd.getErr().println("swath: " + ex.getMessage());
        }
        return code;
    }

    private static SwathException domainException(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current instanceof SwathException domain) {
                return domain;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(executeWithBackstop(args, System.err));
    }

    /**
     * Run the CLI and return the process exit code, guaranteeing a terminal disposition marker.
     *
     * <p>Throwable backstop. picocli's {@code execute()} routes a thrown {@link Exception}
     * to {@link #handleExecutionException}, but lets an {@link Error} (or anything the handler
     * itself failed on) propagate — which would otherwise reach the JVM default handler and exit
     * with a bare code and NO {@code swath:} terminal marker. This wrapper emits a single
     * actionable terminal line and a deterministic exit code so a run ALWAYS ends with a
     * disposition marker. Extracted from {@code main} so the backstop is unit-testable without
     * a {@code System.exit} (which JDK 25 can no longer trap).
     */
    static int executeWithBackstop(String[] args, PrintStream err) {
        return executeWithBackstop(commandLine(), args, err);
    }

    /** Backstop over a given {@link CommandLine} — the injectable seam that makes the {@code
     * catch (Throwable)} path unit-testable (a test can register an Error-throwing subcommand). */
    static int executeWithBackstop(CommandLine cmd, String[] args, PrintStream err) {
        try {
            return cmd.execute(args);
        } catch (Throwable t) {
            err.println("swath: unexpected error: " + t);
            return ExitCodes.UNEXPECTED;
        }
    }
}
