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
 * markers, and a zero-progress heartbeat during a HUNG SEED phase (before {@link
 * RunProgressReporter}'s own engine-phase reporter would otherwise start) — full end-to-end
 * through {@link ListCommand#call()} with a {@link PageGate}-parked fetch.
 */
final class SeedZeroProgressHeartbeatTest {

    @Test
    @Timeout(30)
    void hungSeedEmitsFirstRequestMarkerAndAZeroProgressHeartbeat(@TempDir Path dir) throws Exception {
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
        cmd.liveness.progressInterval = "50ms";   // tiny window so the seed-phase heartbeat ticks fast

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

            // Wait for at least one zero-progress heartbeat line to appear while the seed is
            // still parked -- proves RunProgressReporter ticks even with nothing committed yet.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            boolean heartbeatSeen = false;
            while (System.nanoTime() < deadline) {
                heartbeatSeen = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                        .anyMatch(m -> m.startsWith("progress ") && m.contains("total_emitted=0"));
                if (heartbeatSeen) {
                    break;
                }
                Thread.sleep(20);
            }
            assertThat(heartbeatSeen)
                    .as("a zero-progress heartbeat must fire while the seed is hung, not just silence")
                    .isTrue();
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
