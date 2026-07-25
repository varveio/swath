/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.varve.swath.observability.RunProgressReporter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.store.FirstRequestMarkerFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.PageGate;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Start-of-listing marker (W + engine-toggle-default flag), first-request/first-page
 * markers, and SEED-phase progress during a HUNG SEED — full end-to-end through {@link
 * ListCommand#call()} with a {@link PageGate}-parked fetch. The session {@link
 * RunProgressReporter} is started by the CLI around the whole run, so the seed window is covered
 * by the same lifecycle that later covers listing and the merge.
 */
final class SeedZeroProgressHeartbeatTest {

    @Test
    @Timeout(30)
    void hungSeedEmitsFirstRequestMarkerAndSeedShapedProgress(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        PageGate gate = new PageGate(req -> true);   // parks the seed's very first fetch
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of("data/a".getBytes(StandardCharsets.UTF_8), "data/b".getBytes(StandardCharsets.UTF_8)))
                .interceptor(gate.interceptor())
                .build();

        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://bucket/data/";
        cmd.connection.endpointUrl = "http://localhost:4566";
        cmd.connection.region = "us-east-1";
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = db.toString();
        // Checkpointed runs require a resumable directory-dataset destination; FILE-kind text
        // output is structurally ephemeral and requires --checkpoint none.
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = dir.resolve("out-dataset").toString();
        cmd.fetcherOverride = fetcher;
        // Default cadence and no --progress flag: off a terminal that leaves the structured log
        // record as the run's progress surface, which is what this test reads. (An explicit
        // --progress-interval would opt the run into the operator DISPLAY instead, and
        // --no-progress would leave no progress surface at all.) The activation delay -- not the
        // cadence -- puts the first record well inside the parked seed.
        cmd.terminalOverride = new TerminalCapabilities(fd -> false);

        Logger listCommandLogger =
                (Logger) LoggerFactory.getLogger(ListCommand.class);
        Logger progressLogger =
                (Logger) LoggerFactory.getLogger(RunProgressReporter.class);
        Logger markerLogger =
                (Logger) LoggerFactory.getLogger(FirstRequestMarkerFetcher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level prevCmd = listCommandLogger.getLevel();
        Level prevProgress = progressLogger.getLevel();
        Level prevMarker = markerLogger.getLevel();
        listCommandLogger.setLevel(Level.INFO);
        progressLogger.setLevel(Level.INFO);
        markerLogger.setLevel(Level.INFO);
        listCommandLogger.addAppender(appender);
        progressLogger.addAppender(appender);
        markerLogger.addAppender(appender);

        try {
            CompletableFuture<Integer> callFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return cmd.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            gate.awaitFetched();   // the seed's first request is now parked in-flight

            assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                    .anyMatch(m -> m.startsWith("list_first_request_issued")))
                    .as("the first-request marker must fire before the gated fetch returns")
                    .isTrue();

            // Wait for at least one progress record to appear while the seed is still parked --
            // and it must be SEED-shaped: zero probes completed against the probe budget, with the
            // age of the last completed one climbing. A listing-shaped line would be all zeros here
            // (no objects, no pages, no workers exist during seeding) and could not tell a healthy
            // seed from a hung one, which is the whole point of the phase.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            String seedLine = null;
            while (System.nanoTime() < deadline && seedLine == null) {
                seedLine = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                        .filter(m -> m.startsWith("progress ") && m.contains("phase=seeding"))
                        .findFirst().orElse(null);
                if (seedLine == null) {
                    Thread.sleep(20);
                }
            }
            assertThat(seedLine)
                    .as("a seed-phase progress record must fire while the seed is hung, not just silence")
                    .isNotNull()
                    .contains("probes=0")
                    .contains("probe_budget=")
                    .contains("last_probe_age_ms=");
            assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                    .anyMatch(m -> m.startsWith("list_first_page_returned")))
                    .as("the gated page has not returned yet -- must not have logged first_page_returned early")
                    .isFalse();

            gate.release();
            int exit = callFuture.get(15, TimeUnit.SECONDS);
            assertThat(exit).isEqualTo(ExitCodes.SUCCESS);

            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages.stream().anyMatch(m -> m.startsWith("list_first_page_returned"))).isTrue();
            String startLine = messages.stream().filter(m -> m.startsWith("list_command_start")).findFirst()
                    .orElseThrow(() -> new AssertionError("no list_command_start log line emitted"));
            assertThat(startLine).contains("max_parallel_listings=").contains("engine_toggles_default=");
        } finally {
            listCommandLogger.detachAppender(appender);
            progressLogger.detachAppender(appender);
            markerLogger.detachAppender(appender);
            listCommandLogger.setLevel(prevCmd);
            progressLogger.setLevel(prevProgress);
            markerLogger.setLevel(prevMarker);
        }
    }
}
