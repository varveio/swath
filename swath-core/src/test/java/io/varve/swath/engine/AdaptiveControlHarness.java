/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeKind;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.error.CancelledException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.pipeline.Msg;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.EngineContexts;
import io.varve.swath.testkit.MockPageFetcher;
import io.varve.swath.testkit.SeedSteps;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Shared harness for the four adaptive-composition test classes ({@code TailTimeoutRideOutTest},
 * {@code TimeoutShedGateReopenTest}, {@code Aimd503BackoffRecoveryTest}, {@code InProcessSigkillResumeTest}).
 * A failure class can live in the COMPOSITION of individually-correct mechanisms even when none of
 * them is wrong in isolation, so this harness races the adaptive controllers — AIMD, the timeout shed, the
 * latency freeze, the demand gate, the confetti gate, structure-probe suppression, the watchdog —
 * against each other under a realistic timeline. The gauge's shed/clean windows are real wall-clock
 * (25–40&nbsp;s / 10&nbsp;s) and are not reachable by driving {@link WorkStealingScan} directly.
 * {@link GaugeClock} compresses them (see its javadoc); this harness drives a REAL {@link
 * WorkStealingScan} over that compressed clock with the scripted latency/fault timeline encoded in
 * the caller's {@link MockPageFetcher}.
 *
 * <p>Each test scripts its OWN timeline (latency profile / fault window / keyspace) through the
 * fetcher fixtures ({@code LatencyModels}, {@code MockPageFetcher.PageInterceptor}) and asserts on
 * robust scaled thresholds — never exact counts — so the classes stay deterministic (seeded,
 * compressed windows) and CI-stable. Deep tier (latency-injecting): tagged {@code deep}, excluded from
 * the fast per-commit suite.
 *
 * <p>These classes live in {@code io.varve.swath.engine} (not a separate sub-package) because
 * they need package-private access to {@link WorkStealingScan#gauge()} to read the live {@code
 * effectiveT}/demand-gate state — the same reason {@code DenseShapeCollapseSignatureTest} sits here.
 */
final class AdaptiveControlHarness {

    private AdaptiveControlHarness() {
    }

    /** One engine run's observable outcome: emitted keys, the live gauge, the metrics, and the stop cause. */
    record Result(String name, List<byte[]> emitted, int seedCount, ConcurrencyGauge gauge,
                  RunMetrics metrics, Throwable failure) {

        /** A {@code CAT.reason} steal-reason engagement count (0 if never fired). */
        long steal(String key) {
            return metrics.diagnostics(Duration.ZERO).stealReasons().getOrDefault(key, 0L);
        }

        int effectiveT() {
            return gauge.effectiveT();
        }

        /** The run drained to quiescence (no cancel / no fatal error). */
        boolean completed() {
            return failure == null;
        }
    }

    /**
     * Drive a full {@link WorkStealingScan} over {@code engineFetcher} (whose latency/fault fixtures
     * encode the regime's scripted timeline) with {@code gaugeClock} compressing the adaptive
     * windows. Seeds on a SEPARATE plain (instant, fault-free) fetcher so the timeline touches only the
     * engine's own fetches. Blocks until the run drains or the {@code token} cancels it; a cancellation
     * (mapped to {@link CancelledException}) is captured as {@link Result#failure()} with the partial
     * emitted keys preserved, never rethrown.
     */
    static Result run(String name, List<byte[]> keyspace, MockPageFetcher engineFetcher,
                      EngineToggles toggles, int workers, int maxKeys, SeedMode seedMode,
                      GaugeClock gaugeClock, RetryConfig retry, CancellationToken token,
                      RunMetrics metrics, Path dir)
            throws Exception {
        return runCore(name, keyspace, engineFetcher, toggles, workers, maxKeys, seedMode, null,
                gaugeClock, retry, token, metrics, dir);
    }

    /**
     * As {@link #run}, but seeds an EXPLICIT list of range bounds instead of a {@link SeedMode} probe —
     * so a test can place a tail range whose FIRST read faults with {@code est == 0} (thus un-stealable,
     * keeping a fault burst concentrated on ONE logical fetch so it can trip the retry cap).
     * Each {@link RangeBound} becomes a fresh RANGE seed node.
     */
    static Result runWithSeeds(String name, List<byte[]> keyspace, MockPageFetcher engineFetcher,
                               EngineToggles toggles, int workers, int maxKeys, List<RangeBound> seeds,
                               GaugeClock gaugeClock, RetryConfig retry, CancellationToken token,
                               RunMetrics metrics, Path dir)
            throws Exception {
        return runCore(name, keyspace, engineFetcher, toggles, workers, maxKeys, null, seeds,
                gaugeClock, retry, token, metrics, dir);
    }

    /** A half-open {@code (start, end]} range seed (null start = ⊥, null end = frontier). */
    record RangeBound(byte[] start, byte[] end) {
    }

    private static Result runCore(String name, List<byte[]> keyspace, MockPageFetcher engineFetcher,
                                  EngineToggles toggles, int workers, int maxKeys, SeedMode seedMode,
                                  List<RangeBound> explicitSeeds, GaugeClock gaugeClock, RetryConfig retry,
                                  CancellationToken token, RunMetrics metrics, Path dir)
            throws Exception {
        Files.createDirectories(dir);
        MockPageFetcher seedFetcher = MockPageFetcher.builder().keys(keyspace).build();
        List<byte[]> emitted = new ArrayList<>(keyspace.size());
        Throwable failure = null;
        ConcurrencyGauge gauge;
        int seedCount;
        try (SqliteCheckpointStore store = SqliteCheckpointStore.open(dir.resolve("ckpt.sqlite"))) {
            RunMeta run = store.openRun(key(name), false, false);
            if (explicitSeeds != null) {
                for (RangeBound rb : explicitSeeds) {
                    // cursor := rangeStart (lo): the RangeScanner uses cursor as start_after, so a fresh
                    // bounded seed must begin at its lower bound — exactly SeedStep's bounded-seed shape.
                    store.insertNode(new NodeSpec(run.id(), null, NodeKind.RANGE,
                            rb.start(), rb.end(), rb.start(), null));
                }
                seedCount = explicitSeeds.size();
            } else if (seedMode == null) {
                store.insertNode(NodeSpec.rootRange(run.id()));
                seedCount = 1;
            } else {
                List<NodeSpec> specs = SeedSteps.of(seedFetcher, new byte[0], workers, metrics, toggles)
                        .seedSpecs(run.id(), seedMode);
                store.insertNodes(specs);
                seedCount = specs.size();
            }
            List<Node> seeds = store.loadResumable(run.id(), false);

            metrics.markRunStarted();
            WorkStealingScan engine = new WorkStealingScan(
                    EngineContexts.of(run.id(), new byte[0], ListingMode.OBJECTS, metrics).withToggles(toggles).withRetryConfig(retry),
                    engineFetcher, store, workers, maxKeys, seeds, FilterChain.EMPTY, gaugeClock);
            gauge = engine.gauge();

            RunContext ctx = new RunContext(token, new SimpleMeterRegistry(), metrics);
            try {
                // Local drain, NOT PipelineDrain: a Failure is rethrown as StopSignal so the catch
                // below can keep the partial `emitted` on a cooperative cancel instead of failing.
                new Pipeline<PageBatch>(5000).run(ctx, engine, (c, in) -> {
                    while (true) {
                        Msg<PageBatch> msg = in.receive();
                        if (msg instanceof End<PageBatch>) {
                            return;
                        }
                        if (msg instanceof Failure<PageBatch> f) {
                            throw new StopSignal(f.cause());
                        }
                        if (msg instanceof Item<PageBatch> item) {
                            for (ListEntry e : item.value().entries()) {
                                emitted.add(e.key().raw());
                            }
                        }
                    }
                });
            } catch (StopSignal s) {
                Throwable cause = s.getCause();
                if (cause instanceof CancelledException) {
                    failure = cause;   // cooperative cancel (watchdog / retry cap): partial emitted kept
                } else {
                    throw (cause instanceof Exception e) ? e : new IllegalStateException(cause);
                }
            } catch (CancelledException ce) {
                // A cooperative cancel (watchdog / retry cap) surfaces as the producer throwing
                // CancelledException straight out of Pipeline.run, rather than as a Failure message.
                failure = ce;
            }
        }
        return new Result(name, emitted, seedCount, gauge, metrics, failure);
    }

    /** Unchecked carrier so the drain lambda can surface a pipeline {@code Failure} cause. */
    private static final class StopSignal extends RuntimeException {
        StopSignal(Throwable cause) {
            super(cause);
        }
    }

    private static RunKey key(String name) {
        return new RunKey("s3", null, "bucket", new byte[0], "regime-" + name,
                "WORK_STEALING", ListingMode.OBJECTS, "", "jsonl");
    }

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Byte-exact set equality: every keyspace key emitted exactly once (no gap, no overlap). */
    static void assertExactlyOnce(List<byte[]> emitted, List<byte[]> keyspace) {
        TreeSet<byte[]> distinctKeyspace = new TreeSet<>(Arrays::compareUnsigned);
        distinctKeyspace.addAll(keyspace);
        TreeSet<byte[]> distinctEmitted = new TreeSet<>(Arrays::compareUnsigned);
        distinctEmitted.addAll(emitted);
        assertThat(emitted).as("no key emitted twice (no overlap)").hasSize(distinctEmitted.size());
        assertThat(distinctEmitted).as("every key emitted exactly once (no gap)")
                .hasSize(distinctKeyspace.size());
        var actual = distinctEmitted.iterator();
        var expected = distinctKeyspace.iterator();
        while (actual.hasNext()) {
            assertThat(Arrays.equals(actual.next(), expected.next())).as("byte-exact key (I10)").isTrue();
        }
    }
}
