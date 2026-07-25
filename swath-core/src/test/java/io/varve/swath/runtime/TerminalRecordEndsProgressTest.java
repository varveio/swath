/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.observability.ProgressSink;
import io.varve.swath.observability.RunProgressReporter;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Where progress ends, driven by a REAL run rather than a direct {@code emitSummary} call. The
 * reporter under test is the CLI's: it spans the whole command, so it outlives the one {@code
 * ListRunner} opens and closes around the pipeline, and a tick it schedules can land in the window
 * between the run's terminal log records and the emit that ends progress. A frame printed after the
 * run's last word is the interleaving W6 exists to prevent.
 */
final class TerminalRecordEndsProgressTest {

    /** The tick cadence — short enough that many frames fall inside {@link #TERMINAL_LOG_DWELL}. */
    private static final Duration TICK = Duration.ofMillis(5);

    /**
     * How long the terminal log record dwells before the emit that follows it, so the window between
     * them is wide enough that a still-running ticker WILL fire inside it. Without the dwell the
     * ordering holds by luck on a fast machine and the test proves nothing.
     */
    private static final long TERMINAL_LOG_DWELL_MILLIS = 250L;

    private static final String PROGRESS = "progress_frame";
    private static final String TERMINAL = "terminal_record";

    /** Records the terminal log record in the same sequence the progress frames land in. */
    private static final class DwellingAppender extends AppenderBase<ILoggingEvent> {

        private final List<String> sequence;

        DwellingAppender(List<String> sequence) {
            this.sequence = sequence;
        }

        @Override
        protected void append(ILoggingEvent event) {
            if (!event.getFormattedMessage().startsWith("list_run_summary")) {
                return;
            }
            sequence.add(TERMINAL);
            try {
                Thread.sleep(TERMINAL_LOG_DWELL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @Timeout(60)
    void noProgressFrameLandsAfterTheRunsFirstTerminalRecord(@TempDir Path dir) throws Exception {
        List<String> sequence = Collections.synchronizedList(new ArrayList<>());
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(1500)).build();

        Logger runnerLog = (Logger) LoggerFactory.getLogger(ListRunner.class);
        Level previousLevel = runnerLog.getLevel();
        DwellingAppender appender = new DwellingAppender(sequence);
        appender.start();
        runnerLog.setLevel(Level.INFO);
        runnerLog.addAppender(appender);

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("c.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            List<Node> seeds = store.loadResumable(run.id(), false);

            RunContext ctx = RunContext.create();
            ctx.metrics().setProgressSink(new ProgressSink() {
                @Override
                public void accept(ProgressEvent event) {
                    sequence.add(PROGRESS);
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }
            });

            // The CLI's own reporter, started around the whole command — the one ListRunner's
            // nested start() joins rather than owns.
            try (RunProgressReporter session = RunProgressReporter.start(ctx.metrics(), TICK)) {
                while (!sequence.contains(PROGRESS)) {
                    Thread.sleep(TICK.toMillis());
                }
                new ListRunner().runWorkStealing(
                        ctx, fetcher, new StringWriter(), spec(), store, run.id(), 4, seeds);
            }
        } finally {
            runnerLog.detachAppender(appender);
            runnerLog.setLevel(previousLevel);
            appender.stop();
        }

        assertThat(sequence).contains(PROGRESS, TERMINAL);
        assertThat(sequence.subList(sequence.indexOf(TERMINAL), sequence.size()))
                .as("progress ends before the run's last word, so nothing repaints over it")
                .containsOnly(TERMINAL);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "progress-order-hash",
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    private static ListRunner.Spec spec() {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true,
                8000, 1000, FilterChain.EMPTY, null, null);
    }
}
