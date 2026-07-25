/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.store.PageRequest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;

/**
 * A RETRYABLE fault's log line names the call class and key range that faulted, so a fault STORM is
 * attributable from the log alone.
 *
 * <p>REGRESSION: a probe-timeout storm once emitted ~1300 {@code s3_timeout} lines carrying only
 * {@code bucket} and {@code type} — indistinguishable, from the log, between a sick network and one
 * mis-budgeted call class. It was the latter (every timeout was a {@code structure_probe}, none were
 * worker pages), but that could only be recovered from the JSON run summary's per-call-class
 * histograms. Against the un-attributed line {@link #retryableTimeoutLineNamesTheCallClassAndRange}
 * fails. See {@code docs/ops/dev/field-investigations.md}.
 *
 * <p><b>Asserted at DEBUG, which is the tier these lines ship at.</b> The retryable fault family is
 * auto-retried and each line duplicates a counter recorded immediately beside it, so emitting one
 * per fault at WARN put a healthy run's ordinary transients on the operator's stderr. The aggregate
 * reaches the operator instead as the run summary's {@code throttled}/{@code retried}/{@code
 * errors} line — a storm is one line there rather than ~1300 here. What survives that move, and is
 * what this test guards, is the ATTRIBUTION: whoever turns these lines on to diagnose a storm must
 * be able to read the faulting call class and key range off them.
 */
class S3FaultLogAttributionTest {

    /** The fault lines are deliberately emitted under {@link S3PageFetcher}'s logger name. */
    private static final Logger FAULT_LOGGER = (Logger) LoggerFactory.getLogger(S3PageFetcher.class);

    private ListAppender<ILoggingEvent> appender;
    private Level previousLevel;

    @BeforeEach
    void attachAppender() {
        appender = new ListAppender<>();
        appender.start();
        FAULT_LOGGER.addAppender(appender);
        previousLevel = FAULT_LOGGER.getLevel();
        FAULT_LOGGER.setLevel(Level.DEBUG);
    }

    @AfterEach
    void detachAppender() {
        FAULT_LOGGER.detachAppender(appender);
        appender.stop();
        FAULT_LOGGER.setLevel(previousLevel);
    }

    /**
     * The single DEBUG line starting with {@code tag}. A faulting PROBE fetch legitimately emits two
     * lines — the fault line itself plus {@code slow_probe_exemplar} (any exception path is an
     * unconditional exemplar candidate) — so the assertion selects by tag rather than by count.
     */
    private String faultLine(String tag) {
        List<String> matching = appender.list.stream()
                .filter(e -> e.getLevel() == Level.DEBUG)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith(tag))
                .toList();
        assertThat(matching).as("exactly one %s line", tag).hasSize(1);
        return matching.get(0);
    }

    @Test
    void retryableTimeoutLineNamesTheCallClassAndRange() {
        S3FaultClassifier classifier =
                new S3FaultClassifier("bucket", new RunMetrics(new SimpleMeterRegistry()));
        // The exact shape that stormed: a delimiter=/ structure probe deep inside a dense prefix.
        PageRequest structureProbe = PageRequest.objectsDelimited(
                "corpus/".getBytes(StandardCharsets.UTF_8),
                "/".getBytes(StandardCharsets.UTF_8),
                "corpus/2024/region-a/shard-0007".getBytes(StandardCharsets.UTF_8),
                32);

        classifier.classify(
                ApiCallAttemptTimeoutException.builder().message("attempt timed out").build(),
                new S3FaultClassifier.FaultContext(
                        S3PageFetcher.callClass(structureProbe),
                        new String(structureProbe.prefix(), StandardCharsets.UTF_8),
                        new String(structureProbe.startAfter(), StandardCharsets.UTF_8)));

        assertThat(faultLine("s3_timeout"))
                .as("a storm of these must be attributable to a call class and a key range")
                .contains("call_class=" + RunMetrics.CALL_CLASS_STRUCTURE_PROBE)
                .contains("prefix=corpus/")
                .contains("start_after=corpus/2024/region-a/shard-0007");
    }

    /** The whole fetch path wires the context through — not just a direct classifier call. */
    @Test
    void fetcherThreadsTheContextThroughOnAFaultingFetch() {
        FakeS3Client client = FakeS3Client.throwing(
                ApiCallAttemptTimeoutException.builder().message("attempt timed out").build());
        PageRequest pivotProbe = PageRequest.objects(
                "data/".getBytes(StandardCharsets.UTF_8), null, 1);

        assertThatThrownBy(() -> new S3PageFetcher(client, "bucket").fetchPage(pivotProbe))
                .isInstanceOf(Exception.class);

        assertThat(faultLine("s3_timeout"))
                .contains("call_class=" + RunMetrics.CALL_CLASS_PIVOT_PROBE)
                .contains("prefix=data/");
    }
}
