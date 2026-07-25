/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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

    /**
     * The coordinator owns fd 2 and nothing else. An embedding application's file appender — or a
     * stdout console appender — must come out of {@code serializeThrough} untouched: re-pointing one
     * at stderr closes its stream and silently redirects its log file into this process's terminal.
     */
    @Test
    void onlyTheStderrConsoleAppenderIsRoutedThroughTheCoordinator(@TempDir Path dir) throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        FileAppender<ILoggingEvent> file = fileAppender(root.getLoggerContext(), dir.resolve("app.log"));
        ConsoleAppender<ILoggingEvent> stdout = consoleAppender(root.getLoggerContext(), "System.out");
        root.addAppender(file);
        root.addAppender(stdout);
        OutputStream fileStream = file.getOutputStream();
        OutputStream stdoutStream = stdout.getOutputStream();
        try {
            CliLogging.serializeThrough(new StderrCoordinator(() -> System.err));

            assertThat(file.getOutputStream()).as("a file appender is not this coordinator's to re-point")
                    .isSameAs(fileStream);
            assertThat(stdout.getOutputStream()).as("fd 1 is a different file descriptor")
                    .isSameAs(stdoutStream);
            root.warn("still_going_to_its_own_file");
            file.stop();
            assertThat(Files.readString(dir.resolve("app.log")))
                    .as("its stream was never closed out from under it")
                    .contains("still_going_to_its_own_file");
        } finally {
            root.detachAppender(file);
            root.detachAppender(stdout);
            stdout.stop();
            CliLogging.serializeThrough(StderrCoordinator.shared());
        }
    }

    @Test
    void routingAnAlreadyRoutedAppenderIsANoOp() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        ConsoleAppender<ILoggingEvent> stderr = consoleAppender(root.getLoggerContext(), "System.err");
        root.addAppender(stderr);
        try {
            StderrCoordinator coordinator = new StderrCoordinator(() -> System.err);
            CliLogging.serializeThrough(coordinator);
            OutputStream routed = stderr.getOutputStream();

            CliLogging.serializeThrough(coordinator);

            assertThat(stderr.getOutputStream())
                    .as("a repeat invocation neither stacks wrappers nor churns the stream")
                    .isSameAs(routed);
        } finally {
            root.detachAppender(stderr);
            stderr.stop();
            CliLogging.serializeThrough(StderrCoordinator.shared());
        }
    }

    private static FileAppender<ILoggingEvent> fileAppender(LoggerContext context, Path file) {
        FileAppender<ILoggingEvent> appender = new FileAppender<>();
        appender.setContext(context);
        appender.setFile(file.toString());
        appender.setEncoder(encoder(context));
        appender.start();
        return appender;
    }

    private static ConsoleAppender<ILoggingEvent> consoleAppender(LoggerContext context, String target) {
        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
        appender.setContext(context);
        appender.setTarget(target);
        appender.setEncoder(encoder(context));
        appender.start();
        return appender;
    }

    private static PatternLayoutEncoder encoder(LoggerContext context) {
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%msg%n");
        encoder.start();
        return encoder;
    }
}
