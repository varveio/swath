/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Pins {@link CliLogging#configure}'s {@code (verbosity, quietLevel)} → Logback level mapping:
 * quiet wins over verbosity whenever both are given.
 */
class CliLoggingTest {

    private final Logger logger = (Logger) LoggerFactory.getLogger("io.varve.swath");

    @Test
    void mapsEveryVerbosityLevelWhenUnquieted() {
        Level original = logger.getLevel();
        try {
            CliLogging.configure(0, 0);
            assertThat(logger.getLevel()).isEqualTo(Level.WARN);
            CliLogging.configure(1, 0);
            assertThat(logger.getLevel()).isEqualTo(Level.INFO);
            CliLogging.configure(2, 0);
            assertThat(logger.getLevel()).isEqualTo(Level.DEBUG);
            CliLogging.configure(3, 0);
            assertThat(logger.getLevel()).isEqualTo(Level.TRACE);
            CliLogging.configure(9, 0);
            assertThat(logger.getLevel()).isEqualTo(Level.TRACE);   // 3+ all clamp to TRACE
        } finally {
            logger.setLevel(original);
        }
    }

    @Test
    void quietLevelOneDropsToError() {
        Level original = logger.getLevel();
        try {
            CliLogging.configure(0, 1);
            assertThat(logger.getLevel()).isEqualTo(Level.ERROR);
        } finally {
            logger.setLevel(original);
        }
    }

    @Test
    void quietLevelTwoOrMoreTurnsLoggingOff() {
        Level original = logger.getLevel();
        try {
            CliLogging.configure(0, 2);
            assertThat(logger.getLevel()).isEqualTo(Level.OFF);
            CliLogging.configure(0, 5);
            assertThat(logger.getLevel()).isEqualTo(Level.OFF);
        } finally {
            logger.setLevel(original);
        }
    }

    @Test
    void quietWinsOverVerbosityWhenBothArePassed() {
        Level original = logger.getLevel();
        try {
            CliLogging.configure(3, 1);
            assertThat(logger.getLevel()).isEqualTo(Level.ERROR);
            CliLogging.configure(3, 2);
            assertThat(logger.getLevel()).isEqualTo(Level.OFF);
        } finally {
            logger.setLevel(original);
        }
    }
}
