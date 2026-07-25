/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import org.slf4j.LoggerFactory;

/** Configures the application logger from the CLI's global verbosity flags. */
final class CliLogging {

    private CliLogging() {
    }

    static void configure(int verbosity) {
        String level = switch (verbosity) {
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
