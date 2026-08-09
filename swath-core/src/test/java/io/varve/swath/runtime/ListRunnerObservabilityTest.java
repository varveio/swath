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
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageRequest;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

final class ListRunnerObservabilityTest {

    private static ListRunner.Spec jsonl(int maxKeys) {
        return new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 8, maxKeys, FilterChain.EMPTY, null, null);
    }

    private static RunKey key() {
        return new RunKey("s3", null, "bucket", new byte[0], "observability-hash",
                "SEQUENTIAL", ListingMode.OBJECTS, "", "jsonl");
    }

    @Test
    void listingBytesAreUnchangedWithMetricsProgressAndLoggingActive() throws Exception {
        List<byte[]> keys = List.of(b("a"), b("b"), b("c"));
        StringWriter observabilityOff = new StringWriter();
        StringWriter observabilityOn = new StringWriter();

        RunContext off = new RunContext(new CancellationToken(), new CompositeMeterRegistry());
        new ListRunner().run(off, MockPageFetcher.builder().keys(keys).build(), observabilityOff, jsonl(2));
        new ListRunner().run(RunContext.create(), MockPageFetcher.builder().keys(keys).build(), observabilityOn, jsonl(2));

        assertThat(observabilityOn.toString().getBytes(StandardCharsets.UTF_8))
                .isEqualTo(observabilityOff.toString().getBytes(StandardCharsets.UTF_8));
        assertThat(observabilityOn.toString()).doesNotContain("list_run_summary").doesNotContain("progress ");
    }

    @Test
    void logsAreConfiguredForStderrOnly() throws Exception {
        String logback = Files.readString(Path.of("src/main/resources/logback.xml"));

        assertThat(logback).contains("<target>System.err</target>");
        assertThat(logback).doesNotContain("<target>System.out</target>");
    }

    @Test
    void checkpointedSequentialProducerSeesBoundRunIdInsideFork(@TempDir Path dir) throws Exception {
        AtomicLong observedRunId = new AtomicLong(-1L);
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("a"), b("b")))
                .interceptor((PageRequest req, int callIndex, ListPage computed) -> {
                    observedRunId.set(RunContext.runIdOrNone());
                    return computed;
                })
                .build();

        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("runid.sqlite"))) {
            RunMeta run = store.openRun(key(), false, false);
            store.insertNode(NodeSpec.rootRange(run.id()));
            Node node = store.loadResumable(run.id(), false).getFirst();

            new ListRunner().runCheckpointed(RunContext.create(), fetcher, new StringWriter(), jsonl(1),
                    store, run.id(), node);

            assertThat(observedRunId.get()).isEqualTo(run.id());
        }
    }

    @Test
    void completedRunReportsNoPhaseShapedProgress() throws Exception {
        RunContext ctx = RunContext.create();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("a"), b("b"), b("c")))
                .build();

        new ListRunner().run(ctx, fetcher, new StringWriter(), jsonl(2));

        // A finished run offers no phase-shaped progress: the terminal summary owns that surface,
        // and a display must not keep painting listing counters over it. The run-level fields stay
        // valid in every phase.
        ProgressEvent event = ctx.metrics().progressEvent(Duration.ofSeconds(1));
        assertThat(event.phase()).isEqualTo(Phase.COMPLETE);
        assertThat(event.listing()).isNull();
        assertThat(event.completion()).isNull();
        assertThat(event.sessionElapsed()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void sequentialProgressReportsOneWorkerWhileFetchIsInFlight() throws Exception {
        RunContext ctx = RunContext.create();
        AtomicReference<ProgressEvent> observed = new AtomicReference<>();
        MockPageFetcher fetcher = MockPageFetcher.builder()
                .keys(List.of(b("a"), b("b")))
                .interceptor((PageRequest req, int callIndex, ListPage computed) -> {
                    observed.set(ctx.metrics().progressEvent(Duration.ofMillis(1)));
                    return computed;
                })
                .build();

        new ListRunner().run(ctx, fetcher, new StringWriter(), jsonl(2));

        assertThat(observed.get().listing().concurrencyTarget()).isEqualTo(1L);
        assertThat(observed.get().listing().inFlight()).isEqualTo(1L);
    }

    @Test
    void runSummaryLogLineCarriesEfficiencyAndResourceFields() throws Exception {
        Logger logger =
                (Logger) LoggerFactory.getLogger(ListRunner.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level previous = logger.getLevel();
        logger.setLevel(Level.INFO);
        logger.addAppender(appender);
        try {
            new ListRunner().run(RunContext.create(),
                    MockPageFetcher.builder().keys(List.of(b("a"), b("b"), b("c"))).build(),
                    new StringWriter(), jsonl(2));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }

        String summaryLine = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("list_run_summary"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_run_summary log line emitted"));
        String diagnosticsLine = appender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("list_run_diagnostics"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no list_run_diagnostics log line emitted"));

        assertThat(summaryLine)
                .as("the -v line must carry all three clocks, same as the JSON report and the "
                        + "stderr summary, so a machine consumer scraping this line never disagrees "
                        + "with the other two surfaces about the session's actual wall clock")
                .contains("duration_ms=")
                .contains("listing_duration_ms=")
                .contains("session_duration_ms=")
                .contains("recovered_objects=")
                .contains("api_calls_per_1k_objects=")
                .contains("peak_rss_bytes=")
                .contains("peak_heap_bytes=")
                .contains("cpu_seconds=")
                .contains("cpu_efficiency=");
        assertThat(diagnosticsLine)
                .contains("strategy=SEQUENTIAL")
                .contains("strategy_why=checkpoint_none")
                .contains("steal_reasons=")
                .contains("probe_fetches=")
                .contains("empty_upper_bisections=")
                .contains("splits_committed=")
                .contains("unsplittable_victims=")
                .contains("split_guard_aborts=")
                .contains("peak_in_flight=")
                .contains("time_to_first_steal_ms=")
                .contains("time_to_peak_in_flight_ms=")
                .contains("pages=")
                .contains("fetched_keys=")
                .contains("mean_keys_per_page=")
                .contains("short_truncated_pages=")
                .contains("throttle_events=")
                .contains("transient_events=")
                .contains("aimd_votes=")
                .contains("aimd_target_reductions=");
    }

    /**
     * A broken pipe is a clean exit-0 termination but
     * NOT a completed run — {@code swath.run.duration}/{@code swath.run.throughput}
     * must stay unrecorded so a post-hoc "how long did runs take" aggregate never counts a
     * truncated run, while {@code swath.output.broken_pipe} still fires as the truncation signal.
     * Drives a real broken-pipe termination at the {@link OutputStage} seam (a {@link Writer} that
     * throws the platform's broken-pipe {@link IOException} on write), rather than a process-level
     * IT, to observe the meters directly.
     */
    @Test
    void brokenPipeRunSuppressesRunCompletionMetersButStillRecordsTheBrokenPipeCounter() throws Exception {
        RunContext ctx = RunContext.create();
        Writer brokenPipeWriter = new Writer() {
            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                throw new IOException("Broken pipe");
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };

        new ListRunner().run(ctx, MockPageFetcher.builder().keys(List.of(b("a"), b("b"))).build(),
                brokenPipeWriter, jsonl(2));

        assertThat(ctx.metrics().registry().find("swath.output.broken_pipe").counter().count())
                .as("broken pipe truncation signal fires")
                .isEqualTo(1.0);
        assertThat(ctx.metrics().registry().find("swath.run.duration").timer().count())
                .as("swath.run.duration must have ZERO samples -- a broken-pipe run is not a completed run")
                .isEqualTo(0L);
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
