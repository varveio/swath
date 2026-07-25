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
     */
    static void serializeThrough(StderrCoordinator coordinator) {
        try {
            Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
            Class<?> loggerClass = Class.forName("ch.qos.logback.classic.Logger");
            Class<?> streamAppenderClass = Class.forName("ch.qos.logback.core.OutputStreamAppender");
            if (!loggerClass.isInstance(root)) {
                return;
            }
            Object appenders = loggerClass.getMethod("iteratorForAppenders").invoke(root);
            OutputStream stream = coordinator.logStream();
            for (Iterator<?> it = (Iterator<?>) appenders; it.hasNext(); ) {
                Object appender = it.next();
                if (streamAppenderClass.isInstance(appender)) {
                    streamAppenderClass.getMethod("setOutputStream", OutputStream.class)
                            .invoke(appender, stream);
                }
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
