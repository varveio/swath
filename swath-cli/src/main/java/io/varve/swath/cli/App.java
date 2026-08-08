/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.error.SwathException;
import io.varve.swath.observability.SafeInput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
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
        footer = "Built by Varve: https://varve.io",
        subcommands = {
                ListCommand.class,
                ResumeCommand.class,
                DumpRunCommand.class,
                HelpCommand.class,
        })
public final class App implements Callable<Integer>, GlobalOptions.Carrier {

    /**
     * Provides the five factual {@code --version} lines: stable name/version, Varve attribution,
     * source repository, commit, and Java runtime.
     */
    public static final class VersionProvider implements CommandLine.IVersionProvider {
        /** Manifest attribute stamped by the build (see {@code swath.java-conventions.gradle.kts}). */
        private static final String COMMIT_ATTRIBUTE = "Implementation-Commit";

        /** Displayed commit length — enough to be unambiguous, short enough to read. */
        private static final int SHORT_COMMIT_LENGTH = 12;

        private static final String BRAND_LINE = "Built by Varve: https://varve.io";
        private static final String SOURCE_LINE = "Source: https://github.com/varveio/swath";

        @Override
        public String[] getVersion() {
            return format(App.class.getPackage().getImplementationVersion(), readCommit(), runtimeVersion());
        }

        /**
         * Renders the five version-information lines. The first is stable ASCII {@code swath
         * X.Y.Z}; a missing commit is rendered as {@code Commit: unavailable}. Package-private
         * and pure so the formatting is testable without a packaged jar to read a manifest from.
         *
         * @param version the manifest's {@code Implementation-Version}, or null outside a jar
         * @param commit the manifest's {@code Implementation-Commit}, or null when unavailable
         * @param runtime the current Java runtime version
         */
        static String[] format(String version, String commit, String runtime) {
            String line = "swath " + (version == null ? "development" : version);
            return new String[] {
                line,
                BRAND_LINE,
                SOURCE_LINE,
                "Commit: " + displayCommit(commit),
                "Runtime: " + runtime,
            };
        }

        private static String displayCommit(String commit) {
            if (commit == null || commit.isBlank() || commit.equals("unknown")) {
                return "unavailable";
            }
            return commit.substring(0, Math.min(SHORT_COMMIT_LENGTH, commit.length()));
        }

        private static String runtimeVersion() {
            return System.getProperty("java.runtime.version", System.getProperty("java.version"));
        }

        /**
         * Reads the commit from the manifest of the jar this class was loaded from.
         *
         * <p>{@code Package.getImplementationVersion()} only exposes the standard version
         * attribute, so the manifest is opened directly. Running from a classes directory
         * (development, tests) has no jar and yields null, which {@link #format} renders as
         * {@code Commit: unavailable}; version reporting therefore degrades rather than failing.
         */
        private static String readCommit() {
            try {
                java.security.CodeSource source = App.class.getProtectionDomain().getCodeSource();
                if (source == null || source.getLocation() == null) {
                    return null;
                }
                java.io.File location = new java.io.File(source.getLocation().toURI());
                if (!location.isFile()) {
                    return null;
                }
                try (java.util.jar.JarFile jar = new java.util.jar.JarFile(location)) {
                    java.util.jar.Manifest manifest = jar.getManifest();
                    return manifest == null
                            ? null
                            : manifest.getMainAttributes().getValue(COMMIT_ATTRIBUTE);
                }
            } catch (Exception e) {
                // Version reporting must never be the reason a command fails.
                return null;
            }
        }
    }

    /** A leading {@code scheme://} — matches any store URI (today only {@code s3://}), so a bare
     * URI at the top level (no verb) can be recognized without hard-coding the scheme list. */
    private static final Pattern URI_SHAPED = Pattern.compile("^[A-Za-z][A-Za-z0-9+.-]*://[\\s\\S]*");

    /** How deep {@link #messageChain} follows a chain — see {@code ExitCodes.MAX_CHAIN_DEPTH}. */
    private static final int MAX_MESSAGE_CHAIN_DEPTH = 32;

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
            // The deepest parsed level, not parseResult.commandSpec().commandLine() (always the
            // root): a leaf-placed -v/-q (`swath list -vv …`) must be visible here too, and
            // GlobalOptions.mergedLevel needs the genuine leaf to sum root+leaf without double
            // counting when there is no subcommand at all (`swath -vv --help`).
            List<CommandLine> parsedLevels = parseResult.asCommandLineList();
            CommandLine leaf = parsedLevels.get(parsedLevels.size() - 1);
            CliLogging.configure(GlobalOptions.effectiveVerbosity(leaf), GlobalOptions.effectiveQuietLevel(leaf));
            // Log events share stderr with the live progress record and the summary block, so the
            // console appender writes through the one coordinator that serializes all of them.
            CliLogging.serializeThrough(StderrCoordinator.shared());
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
        // A terminal error line is the run's last word on stderr: end live progress permanently
        // first, so no frame still being formatted can land after it — then write the whole record
        // under the coordinator's lock, so a log event cannot splice a multi-line stack trace or
        // land after the error either. picocli's own writer keeps doing the writing: an embedding
        // application may have redirected it.
        StderrCoordinator coordinator = StderrCoordinator.shared();
        coordinator.finishProgress();
        int code = ExitCodes.forThrowable(ex);
        SwathException domain = domainException(ex);
        if (domain != null) {
            recordError(coordinator, cmd, err -> err.println("swath: " + messageChain(domain)));
        } else if (code == ExitCodes.UNEXPECTED) {
            recordError(coordinator, cmd, err -> {
                err.println("swath: unexpected error: " + ex);
                if (GlobalOptions.effectiveVerbosity(cmd) > 0) {
                    ex.printStackTrace(err);
                }
            });
        } else if (code != ExitCodes.SUCCESS) {
            recordError(coordinator, cmd, err -> err.println("swath: " + ex.getMessage()));
        }
        return code;
    }

    /** One error record on picocli's writer, complete and flushed, under the stderr coordinator's lock. */
    private static void recordError(StderrCoordinator coordinator, CommandLine cmd,
            Consumer<PrintWriter> body) {
        coordinator.record(() -> {
            PrintWriter err = cmd.getErr();
            body.accept(err);
            err.flush();
        });
    }

    private static SwathException domainException(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current instanceof SwathException domain) {
                return domain;
            }
        }
        return null;
    }

    /**
     * A domain error's message followed by every distinct cause message, joined with {@code ": "}
     * as one physical line so the error coordinator can serialize the record intact.
     *
     * <p>Adjacent duplicate messages and the repeated cause text from {@code new IOException(cause)}
     * are emitted once.
     */
    static String messageChain(Throwable throwable) {
        var text = new StringBuilder();
        var visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        int depthLeft = MAX_MESSAGE_CHAIN_DEPTH;
        Throwable previous = null;
        String previousPart = null;
        for (Throwable current = throwable;
                current != null && depthLeft > 0 && visited.add(current);
                current = current.getCause()) {
            depthLeft--;
            String message = current.getMessage();
            String part = message == null || message.isBlank()
                ? current.getClass().getSimpleName()
                : SafeInput.logText(message);
            if (text.isEmpty()) {
                text.append(part);
            } else if (!part.equals(previousPart) && !repeatsWrappedCause(previous, current)) {
                text.append(": ").append(part);
            }
            previous = current;
            previousPart = part;
        }
        return text.toString();
    }

    private static boolean repeatsWrappedCause(Throwable wrapper, Throwable cause) {
        return wrapper instanceof IOException
                && wrapper.getCause() == cause
                && cause.toString().equals(wrapper.getMessage());
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
            StderrCoordinator.shared().finishProgress();
            err.println("swath: unexpected error: " + t);
            return ExitCodes.UNEXPECTED;
        }
    }
}
