/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.io.OutputStream;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configures the application logger from the CLI's global verbosity/quiet flags. */
final class CliLogging {

    /** Logback's own name for fd 2 ({@code ConsoleAppender#getTarget}) — the one fd this serializes. */
    private static final String STDERR_TARGET = "System.err";

    private CliLogging() {
    }

    /**
     * Route the log's console output through the CLI's {@link StderrCoordinator}, so a log event
     * and a live progress frame cannot splice each other on the one file descriptor they share.
     *
     * <p>Logback's console appender is re-pointed at {@link StderrCoordinator#logStream()} rather
     * than replaced by a coordinator-aware appender class: the shipped {@code logback.xml} lives in
     * swath-core, and naming a swath-cli class in it would both break core's own logging and put
     * terminal policy back in the layer that must not have it. Reflective for the same reason
     * {@link #configure} is — Logback is the shipped binding, and an embedding application that
     * supplies another one keeps its own logging policy.
     *
     * <p>The alternative the design considered — suppressing the display while logging is active —
     * does not work: the default level is WARN, so a run can emit a line at any moment and there is
     * no "quiet period" to display into.
     *
     * <p><b>Only the stderr console appender, and only once.</b> The fd this coordinates is fd 2, so
     * an appender pointed anywhere else is left exactly as its owner configured it: re-pointing a
     * {@code FileAppender} (or a stdout console appender) at stderr would close its stream and
     * silently redirect an embedding application's log file into this process's terminal. Re-running
     * on an appender already routed here is a no-op, so repeat invocations in one JVM neither stack
     * wrappers nor churn streams.
     */
    static void serializeThrough(StderrCoordinator coordinator) {
        try {
            Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            Class<?> loggerClass = Class.forName("ch.qos.logback.classic.Logger");
            Class<?> consoleAppenderClass = Class.forName("ch.qos.logback.core.ConsoleAppender");
            if (!loggerClass.isInstance(root)) {
                return;
            }
            Object appenders = loggerClass.getMethod("iteratorForAppenders").invoke(root);
            for (Iterator<?> it = (Iterator<?>) appenders; it.hasNext(); ) {
                Object appender = it.next();
                if (!consoleAppenderClass.isInstance(appender)
                        || !STDERR_TARGET.equals(consoleAppenderClass.getMethod("getTarget").invoke(appender))) {
                    continue;
                }
                Object current = consoleAppenderClass.getMethod("getOutputStream").invoke(appender);
                if (coordinator.ownsLogStream(current)) {
                    continue;
                }
                consoleAppenderClass.getMethod("setOutputStream", OutputStream.class)
                        .invoke(appender, coordinator.logStream());
            }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // Non-Logback binding, or an appender that will not take a stream: the CLI still runs,
            // it just loses the ordering guarantee it cannot enforce from outside.
        }
    }

    /**
     * Configures the {@code io.varve.swath} log level from the merged {@code -v}/{@code -q} counts
     * ({@link GlobalOptions#effectiveVerbosity} / {@link GlobalOptions#effectiveQuietLevel}).
     * Quiet wins over verbosity when both are given: {@code -qq} (or higher) turns logging off
     * entirely, {@code -q} drops it to ERROR, and only an unquieted invocation consults {@code
     * verbosity} (0 WARN, 1 INFO, 2 DEBUG, 3+ TRACE).
     */
    static void configure(int verbosity, int quietLevel) {
        String level = quietLevel >= 2 ? "OFF"
                : quietLevel == 1 ? "ERROR"
                : switch (verbosity) {
                    case 0 -> "WARN";
                    case 1 -> "INFO";
                    case 2 -> "DEBUG";
                    default -> "TRACE";
                };
        try {
            Object logger = LoggerFactory.getLogger("io.varve.swath");
            Class<?> loggerClass = Class.forName("ch.qos.logback.classic.Logger");
            Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
            if (!loggerClass.isInstance(logger)) {
                return;
            }
            Object logbackLevel = levelClass.getMethod("valueOf", String.class).invoke(null, level);
            loggerClass.getMethod("setLevel", levelClass).invoke(logger, logbackLevel);
        } catch (ReflectiveOperationException ignored) {
            // Logback is the shipped binding. If an embedding application supplies another SLF4J
            // binding, leave its logging policy alone rather than making the CLI fail to start.
        }
    }
}
