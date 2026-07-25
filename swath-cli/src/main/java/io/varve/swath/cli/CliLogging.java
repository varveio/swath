/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import org.slf4j.LoggerFactory;

/** Configures the application logger from the CLI's global verbosity/quiet flags. */
final class CliLogging {

    private CliLogging() {
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
