/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.error.ThrottleException;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.ParquetReads;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fresh run whose seed hits an attempt-timeout burst that outlasts
 * {@code TransientRetryFetcher.MAX_TRANSIENT_RETRIES} rides it out rather than aborting at the cap:
 * once the store heals, the seed completes, the engine lists the whole keyspace, and the run exits
 * NORMALLY (exit 0). Death from a sustained burst is owned solely by the {@code LivenessWatchdog}
 * (exercised in the sibling {@code SeedRideOutWatchdogStopContractTest}), never by the per-fetch
 * retry count.
 *
 * <p>The mock throws {@code ATTEMPT_TIMEOUT} for the seed's FIRST logical probe {@value #STORM_THROWS}
 * consecutive times — strictly more than {@code TransientRetryFetcher.MAX_TRANSIENT_RETRIES} — before
 * healing, so a run that gave up at that count would abort STUCK (exit 75) with an EMPTY listing
 * instead of riding it out to completion.
 *
 * <p>The injected <b>backoff sleeper</b> ({@code cmd.backoffSleeperOverride}) makes the ride-out's
 * backoffs no-ops, so the burst runs in milliseconds instead of tens of real seconds.
 *
 * <p><b>Coordination.</b> A package-private {@code ListCommand.backoffSleeperOverride} field of type
 * {@code TransientRetryFetcher.Sleeper} (null in production), plumbed into BOTH the seed's
 * {@code TransientRetryFetcher} and the engine's {@code WorkStealingScan} package-private constructor,
 * mirroring the existing {@code fetcherOverride} seam. The {@code TransientRetryFetcher.Sleeper}
 * nested type must stay {@code public} so this {@code cli}-package test can bind the field across
 * packages.
 */
final class SeedRideOutHealsContractTest {

    private static final String BUCKET = "bucket";
    private static final String ENDPOINT = "http://localhost:4566";
    private static final String REGION = "us-east-1";
    private static final String PREFIX = "data/";
    /** Consecutive over-one-logical-fetch transients before healing: strictly more than
     * TransientRetryFetcher.MAX_TRANSIENT_RETRIES (8). */
    private static final int STORM_THROWS = 12;
    private static final List<byte[]> KEYS = List.of(
            b("data/a/1"), b("data/a/2"), b("data/b/1"), b("data/b/2"), b("data/c/1"));

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** No-op backoff sleeper — removes the multi-second burst backoffs so the ride-out runs fast. */
    private static final TransientRetryFetcher.Sleeper NOOP_SLEEPER = millis -> { /* deterministic: no real sleep */ };

    private ListCommand baseCommand(Path db, PageFetcher fetcher) {
        ListCommand cmd = new ListCommand();
        cmd.uri = "s3://" + BUCKET + "/" + PREFIX;
        cmd.connection.endpointUrl = ENDPOINT;
        cmd.connection.region = REGION;
        cmd.connection.noSignRequest = true;
        cmd.checkpoint.location = db.toString();
        cmd.fetcherOverride = fetcher;
        cmd.backoffSleeperOverride = NOOP_SLEEPER;
        return cmd;
    }

    @Test
    @Timeout(60)
    void seedStormPastTheCap_ridesOut_thenHealsAndCompletesExit0(@TempDir Path dir) throws Exception {
        Path db = dir.resolve("c.sqlite");
        Path out = dir.resolve("out");

        AtomicInteger thrown = new AtomicInteger();
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(KEYS)
                .interceptor((req, idx, page) -> {
                    // A single sustained burst on the seed's FIRST logical probe: STORM_THROWS consecutive
                    // ATTEMPT_TIMEOUTs (no page served in between), then the store heals for the rest of
                    // the run (seed tail + engine listing).
                    if (thrown.get() < STORM_THROWS) {
                        thrown.incrementAndGet();
                        throw ThrottleException.attemptTimeout("t=0 fleet attempt-timeout storm");
                    }
                    return page;
                })
                .build();

        ListCommand cmd = baseCommand(db, fetcher);
        cmd.output.format = OutputFormat.PARQUET;
        cmd.output.destination = out.toString();

        int exit = cmd.call();

        assertThat(exit)
                .as("a storm that HEALS is ridden out to a NORMAL exit 0 — not killed STUCK at attempt 9")
                .isEqualTo(ExitCodes.SUCCESS);
        assertThat(thrown.get())
                .as("the ride-out endured >= 10 transients on one logical fetch (past the legacy cap of 9)")
                .isEqualTo(STORM_THROWS);
        assertThat(parquetKeys(out))
                .as("the full keyspace is listed exactly once after the storm heals")
                .isEqualTo(expectedKeys());
        assertThat(CheckpointDbProbe.runStatus(db))
                .as("the run reaches genuine COMPLETED (never a resumable STUCK)")
                .isEqualTo("COMPLETED");
        assertThat(CheckpointDbProbe.fatalError(db)).as("a healed storm is never fatal").isFalse();
    }

    // ---- reads over output (checkpoint reads: CheckpointDbProbe) -----------

    private static Set<String> expectedKeys() {
        Set<String> ks = new LinkedHashSet<>();
        for (byte[] k : KEYS) {
            ks.add(new String(k, StandardCharsets.UTF_8));
        }
        return ks;
    }

    private static Set<String> parquetKeys(Path outputDir) throws Exception {
        Set<String> keys = new LinkedHashSet<>();
        for (Path part : DatasetLayout.of(outputDir).dataParts()) {
            keys.addAll(ParquetReads.keys(part));
        }
        return keys;
    }
}
