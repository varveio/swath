/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.runtime;

import io.micrometer.core.instrument.Timer;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.PartFinalize;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.engine.EngineContext;
import io.varve.swath.engine.EngineToggles;
import io.varve.swath.engine.RetryConfig;
import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.engine.WorkStealingScan;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.OutputException;
import io.varve.swath.error.SwathException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ListingMode;
import io.varve.swath.model.PageBatch;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.Phase;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunProgressReporter;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.StopReason;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.BrokenPipe;
import io.varve.swath.output.CountingWriter;
import io.varve.swath.output.EntryFormatter;
import io.varve.swath.output.Formatters;
import io.varve.swath.output.ListingStatistics;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.OutputStage;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.ParquetOutputStage;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.ParquetSchema;
import io.varve.swath.output.parquet.ParquetWriterPool;
import io.varve.swath.output.parquet.ParquetWriterPoolConfig;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.output.parquet.PartListener;
import io.varve.swath.pipeline.Pipeline;
import io.varve.swath.sort.DuplicateHook;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SegmentCorruptionException;
import io.varve.swath.sort.SegmentSink;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortLane;
import io.varve.swath.sort.SortLaneMeters;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.SortMode;
import io.varve.swath.sort.SortPagePacker;
import io.varve.swath.sort.SortRun;
import io.varve.swath.sort.SortTransform;
import io.varve.swath.sort.SortTransformResult;
import io.varve.swath.sort.SortedFileIndex;
import io.varve.swath.sort.SortedFileWriter;
import io.varve.swath.sort.SortedFileWriterFactory;
import io.varve.swath.sort.SortedParquetWriter;
import io.varve.swath.sort.SortedParquetWriterFactory;
import io.varve.swath.store.ListPage;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.PageRequest;
import io.varve.swath.store.StoreCapabilities;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wires a single listing run for the {@code list} verb: a sequential
 * {@link ScanProducer} for non-checkpointed runs, a sequential
 * {@link CheckpointedScanProducer} for the single-checkpoint-node path, or the
 * multi-worker {@code WorkStealingScan} engine for checkpointed, resumable multi-node
 * runs — all feed the same bounded {@link Pipeline}, drained by a text
 * {@link OutputStage} or the {@link ParquetWriterPool}.
 */
public final class ListRunner {

    private static final Logger log = LoggerFactory.getLogger(ListRunner.class);

    /**
     * The process exit code a CLASSIFIED fatal crash reports in the JSON summary. Every
     * classification that reaches {@code RunMetrics#recordFatalErrorClass} is a merge/publish
     * failure rethrown as an {@code OutputException}, whose {@code exitCode()} is 1 — the same value
     * {@code ExitCodes.forThrowable} hands the process. Kept as a literal because {@code ExitCodes}
     * lives in the CLI module (core must not depend on it); a new classification whose exit code is NOT
     * 1 must thread its own code here rather than reuse this constant.
     */
    private static final int CLASSIFIED_FATAL_EXIT_CODE = 1;

    /** Text-sink spec. */
    public record Spec(byte[] prefix, OutputFormat format, boolean escape, int queueCapacity, int maxKeys,
                       FilterChain filters, Duration progressInterval, JsonRunSummaryWriter.Config jsonSummary) {

        public Spec withFilters(FilterChain filters) {
            return new Spec(prefix, format, escape, queueCapacity, maxKeys, filters, progressInterval, jsonSummary);
        }

        public Spec withProgressInterval(Duration progressInterval) {
            return new Spec(prefix, format, escape, queueCapacity, maxKeys, filters, progressInterval, jsonSummary);
        }

        public Spec withJsonSummary(JsonRunSummaryWriter.Config jsonSummary) {
            return new Spec(prefix, format, escape, queueCapacity, maxKeys, filters, progressInterval, jsonSummary);
        }
    }

    /**
     * Parquet-sink spec.
     *
     * @param rotationIntervalNanos rotate a lane's open part once it has been open this long, even
     *                              below {@code targetBytes} — bounds the resume RPO; {@code 0}
     *                              disables the time trigger
     * @param rotationMaxRows rotate once a lane's open part has this many rows, even below
     *                        {@code targetBytes}; {@code 0} disables the row-count trigger
     */
    public record ParquetSpec(byte[] prefix, int queueCapacity, int maxKeys, FilterChain filters,
                              int numWriters, long targetBytes, int writerQueueCapacity, String argsHash,
                              Duration progressInterval, JsonRunSummaryWriter.Config jsonSummary,
                              long rotationIntervalNanos, long rotationMaxRows, String bucket) {

        public ParquetSpec withProgressInterval(Duration progressInterval) {
            return new ParquetSpec(prefix, queueCapacity, maxKeys, filters, numWriters, targetBytes,
                    writerQueueCapacity, argsHash, progressInterval, jsonSummary, rotationIntervalNanos,
                    rotationMaxRows, bucket);
        }

        public ParquetSpec withJsonSummary(JsonRunSummaryWriter.Config jsonSummary) {
            return new ParquetSpec(prefix, queueCapacity, maxKeys, filters, numWriters, targetBytes,
                    writerQueueCapacity, argsHash, progressInterval, jsonSummary, rotationIntervalNanos,
                    rotationMaxRows, bucket);
        }

        public ParquetSpec withRotationIntervalNanos(long rotationIntervalNanos) {
            return new ParquetSpec(prefix, queueCapacity, maxKeys, filters, numWriters, targetBytes,
                    writerQueueCapacity, argsHash, progressInterval, jsonSummary, rotationIntervalNanos,
                    rotationMaxRows, bucket);
        }

        public ParquetSpec withRotationMaxRows(long rotationMaxRows) {
            return new ParquetSpec(prefix, queueCapacity, maxKeys, filters, numWriters, targetBytes,
                    writerQueueCapacity, argsHash, progressInterval, jsonSummary, rotationIntervalNanos,
                    rotationMaxRows, bucket);
        }

        public ParquetSpec withBucket(String bucket) {
            return new ParquetSpec(prefix, queueCapacity, maxKeys, filters, numWriters, targetBytes,
                    writerQueueCapacity, argsHash, progressInterval, jsonSummary, rotationIntervalNanos,
                    rotationMaxRows, bucket);
        }
    }

    // ---- text sinks (jsonl / tsv / table) -----------------------------------

    public ListingStatistics run(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec)
            throws SwathException, InterruptedException {
        return run(ctx, fetcher, out, spec, RetryConfig.DEFAULT);
    }

    /**
     * Full overload: {@code retryConfig} threads the CLI-resolved transient-retry
     * policy (RIDE_OUT vs BOUNDED) + injectable backoff sleeper into the sequential path's retry
     * decorator, mirroring the seed/engine paths. The shorter overload defaults to
     * {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics run(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                 RetryConfig retryConfig)
            throws SwathException, InterruptedException {

        CountingWriter countingOut = new CountingWriter(out);
        EntryFormatter formatter = Formatters.text(spec.format(), countingOut, spec.escape());
        OutputStage outputStage = new OutputStage(formatter);
        ScanProducer producer = new ScanProducer(observedSequentialFetcher(ctx, fetcher, retryConfig),
                spec.prefix(), spec.maxKeys(), spec.filters());
        Function<Duration, RunSummary> summary =
                el -> ctx.metrics().summary(el, "SEQUENTIAL", 1L, countingOut.bytesWritten());

        return this.<RuntimeException>runLifecycle(ctx, LifecyclePlan.<RuntimeException>builder()
                .strategy("SEQUENTIAL").strategyWhy("checkpoint_none").concurrencyTarget(1L)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info("list_run_start strategy={} checkpointed={} output_format={}",
                        "SEQUENTIAL", false, spec.format()))
                .producer(producer).consumerStage(outputStage)
                .jsonSummaryConfig(spec.jsonSummary()).snapshotSummary(summary).terminalSummary(summary)
                .outputStage(outputStage).statistics(outputStage::statistics)
                .drain(textDrain(formatter, outputStage))
                .complete(() -> { })
                .epilogue(textOutputEpilogue(ctx, spec.format(), countingOut, outputStage))
                .build());
    }

    /**
     * Checkpointed text run: scans {@code node} with
     * commit-before-emit (I1), so a kill leaves {@code emitted ⊆ clean-run} with no
     * duplicates on resume (at-most-once for text, RES-1/RES-2). Marks the run
     * COMPLETED only on clean termination.
     */
    public ListingStatistics runCheckpointed(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                              CheckpointStore store, long runId, Node node)
            throws SwathException, InterruptedException {
        return runCheckpointed(ctx, fetcher, out, spec, store, runId, node, RetryConfig.DEFAULT);
    }

    /**
     * Full overload: {@code retryConfig} threads the sequential path's retry decorator, as in
     * {@link #run(RunContext, PageFetcher, Writer, Spec, RetryConfig)}. The shorter overload
     * defaults to {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics runCheckpointed(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                              CheckpointStore store, long runId, Node node,
                                              RetryConfig retryConfig)
            throws SwathException, InterruptedException {

        CountingWriter countingOut = new CountingWriter(out);
        EntryFormatter formatter = Formatters.text(spec.format(), countingOut, spec.escape());
        OutputStage outputStage = new OutputStage(formatter);
        CheckpointedScanProducer producer = new CheckpointedScanProducer(
                observedSequentialFetcher(ctx, fetcher, retryConfig), store, node, spec.prefix(), spec.maxKeys(),
                spec.filters());
        Function<Duration, RunSummary> summary =
                el -> ctx.metrics().summary(el, "SEQUENTIAL", 1L, countingOut.bytesWritten());

        return this.<RuntimeException>runLifecycle(ctx, LifecyclePlan.<RuntimeException>builder()
                .strategy("SEQUENTIAL").strategyWhy("single_checkpoint_node").concurrencyTarget(1L).runId(runId)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info("list_run_start strategy={} checkpointed={} run_id={} output_format={}",
                        "SEQUENTIAL", true, runId, spec.format()))
                .producer(producer).consumerStage(outputStage)
                .jsonSummaryConfig(spec.jsonSummary()).snapshotSummary(summary).terminalSummary(summary)
                .outputStage(outputStage).statistics(outputStage::statistics)
                .drain(textDrain(formatter, outputStage))
                // Broken pipe is a clean CLI termination, but the stdout stream was truncated:
                // do not record the run as COMPLETED or resume would skip work.
                .complete(() -> store.markRunFinished(runId,
                        outputStage.wasBrokenPipe() ? RunStatus.FAILED : RunStatus.COMPLETED))
                .epilogue(textOutputEpilogue(ctx, spec.format(), countingOut, outputStage))
                .build());
    }

    /**
     * Checkpointed Parquet run: commit-before-emit listing plus the
     * exactly-once durable-cursor model (I6). On each part finalize the
     * {@link PartListener} records the part + advances {@code durable_cursor} in one
     * checkpoint transaction (the commit point, after the footer fsync).
     * {@code existingParts} are carried into the manifest on resume; non-finalized
     * parts must already have been discarded by the caller.
     */
    public ListingStatistics runToParquetCheckpointed(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                       ParquetSpec spec, CheckpointStore store, long runId,
                                                       Node node, List<PartInfo> existingParts)
            throws SwathException, InterruptedException {
        return runToParquetCheckpointed(ctx, fetcher, outputDir, spec, store, runId, node, existingParts,
                RetryConfig.DEFAULT);
    }

    /**
     * Full overload: {@code retryConfig} threads the sequential path's retry decorator, as in
     * {@link #run(RunContext, PageFetcher, Writer, Spec, RetryConfig)}. The shorter overload
     * defaults to {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics runToParquetCheckpointed(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                       ParquetSpec spec, CheckpointStore store, long runId,
                                                       Node node, List<PartInfo> existingParts,
                                                       RetryConfig retryConfig)
            throws SwathException, InterruptedException {

        CheckpointedScanProducer producer = new CheckpointedScanProducer(
                observedSequentialFetcher(ctx, fetcher, retryConfig), store, node, spec.prefix(), spec.maxKeys(),
                spec.filters());
        ParquetWriterPool pool = new ParquetWriterPool(outputDir, ParquetSchema.canonical(), spec.argsHash(),
                spec.numWriters(), spec.targetBytes(), spec.writerQueueCapacity(),
                new ParquetWriterPoolConfig(spec.bucket(), partFinalizedListener(store, runId), existingParts,
                        spec.rotationIntervalNanos(), spec.rotationMaxRows(), ctx.metrics()));
        ParquetOutputStage stage = new ParquetOutputStage(pool);
        Function<Duration, RunSummary> summary = el -> ctx.metrics().summary(el, "SEQUENTIAL",
                pool.committedPartCount(), pool.committedBytes());

        return this.<RuntimeException>runLifecycle(ctx, LifecyclePlan.<RuntimeException>builder()
                .strategy("SEQUENTIAL").strategyWhy("single_checkpoint_node").concurrencyTarget(1L).runId(runId)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info("list_run_start strategy={} checkpointed={} run_id={} output_format={}",
                        "SEQUENTIAL", true, runId, "PARQUET"))
                .producer(producer).consumerStage(stage)
                .jsonSummaryConfig(spec.jsonSummary()).snapshotSummary(summary).terminalSummary(summary)
                .statistics(stage::statistics)
                .drain(poolDrain(pool))
                // Clean close: every COMPLETED node's kept rows are now durable. Latch
                // durable_cursor=cursor so a node whose tail pages were all filtered out
                // (cursor > last durable kept row) is still output-complete (I6, §4.5) and
                // resume treats the run as done instead of reopening it.
                .complete(() -> {
                    store.markOutputComplete(runId);
                    store.markRunFinished(runId, RunStatus.COMPLETED);
                })
                .epilogue(() -> ctx.metrics().recordOutput("parquet", "written",
                        pool.committedPartCount(), pool.committedBytes()))
                .build());
    }

    /**
     * Engine-backed text run: seeds {@link WorkStealingScan} from the full
     * {@code seeds} list (all resumable nodes) with {@code workerCount} virtual-thread
     * workers and feeds the same {@link Pipeline} + {@link OutputStage} as
     * {@link #runCheckpointed}. Marks the run COMPLETED only on clean termination.
     */
    public ListingStatistics runWorkStealing(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                             CheckpointStore store, long runId, int workerCount, List<Node> seeds)
            throws SwathException, InterruptedException, IOException {
        return runWorkStealing(ctx, fetcher, out, spec, store, runId, workerCount, seeds, true);
    }

    public ListingStatistics runWorkStealing(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                             CheckpointStore store, long runId, int workerCount, List<Node> seeds,
                                             boolean ownerSplitEnabled)
            throws SwathException, InterruptedException, IOException {
        return runWorkStealing(ctx, fetcher, out, spec, store, runId, workerCount, seeds,
                ownerSplitEnabled ? EngineToggles.DEFAULT
                        : EngineToggles.DEFAULT.withOwnerSplit(false));
    }

    /**
     * Full-toggle overload: {@code toggles} is the {@code --engine-toggle} ablation
     * namespace threaded into {@link WorkStealingScan} (see {@link EngineToggles}
     * for the exact mechanical effect of each).
     */
    public ListingStatistics runWorkStealing(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                             CheckpointStore store, long runId, int workerCount, List<Node> seeds,
                                             EngineToggles toggles)
            throws SwathException, InterruptedException, IOException {
        return runWorkStealing(ctx, fetcher, out, spec, store, runId, workerCount, seeds, toggles, TraceSink.NONE);
    }

    /**
     * Full overload: {@code trace} is the opt-in JSONL flight recorder seam
     * threaded into {@link WorkStealingScan}, the same path as {@code toggles}.
     */
    public ListingStatistics runWorkStealing(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                             CheckpointStore store, long runId, int workerCount, List<Node> seeds,
                                             EngineToggles toggles, TraceSink trace)
            throws SwathException, InterruptedException, IOException {
        return runWorkStealing(ctx, fetcher, out, spec, store, runId, workerCount, seeds, toggles, trace,
                RetryConfig.DEFAULT, OutputPublisher.NONE);
    }

    /**
     * Full overload: {@code retryConfig} is the transient-retry policy + injectable
     * backoff sleeper threaded into {@link WorkStealingScan}'s {@code GaugedFetcher}s — the CLI selects
     * {@link io.varve.swath.engine.RetryPolicy#BOUNDED} when both watchdog windows are disabled and a
     * test injects a no-op sleeper. Every shorter overload defaults it to {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics runWorkStealing(RunContext ctx, PageFetcher fetcher, Writer out, Spec spec,
                                             CheckpointStore store, long runId, int workerCount, List<Node> seeds,
                                             EngineToggles toggles, TraceSink trace,
                                             RetryConfig retryConfig, OutputPublisher publisher)
            throws SwathException, InterruptedException, IOException {

        CountingWriter countingOut = new CountingWriter(out);
        EntryFormatter formatter = Formatters.text(spec.format(), countingOut, spec.escape());
        OutputStage outputStage = new OutputStage(formatter);
        WorkStealingScan producer = new WorkStealingScan(
                new EngineContext(runId, spec.prefix(), ListingMode.OBJECTS, ctx.metrics(), toggles, trace, retryConfig),
                fetcher, store, workerCount, spec.maxKeys(), seeds, spec.filters());
        Function<Duration, RunSummary> summary =
                el -> ctx.metrics().summary(el, "WORK_STEALING", 1L, countingOut.bytesWritten());

        return this.<IOException>runLifecycle(ctx, LifecyclePlan.<IOException>builder()
                .strategy("WORK_STEALING").strategyWhy("checkpointed_resumable_nodes").runId(runId)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info(
                        "list_run_start strategy={} checkpointed={} run_id={} output_format={} worker_count={}",
                        "WORK_STEALING", true, runId, spec.format(), workerCount))
                .producer(producer).consumerStage(outputStage)
                .jsonSummaryConfig(spec.jsonSummary()).snapshotSummary(summary).terminalSummary(summary)
                .outputStage(outputStage).statistics(outputStage::statistics)
                .drain(textDrain(formatter, outputStage))
                .complete(() -> {
                    // The writer has flushed and closed into its staged temp file. Publish the completed
                    // output (an atomic rename into place) BEFORE the run is recorded complete, so a run is
                    // never marked COMPLETED while its output is still unpublished. A publish failure records
                    // the run FAILED and aborts completion — no success summary is written. Broken pipe skips
                    // publication for the same reason it skips COMPLETED: the stream was truncated, so there
                    // is nothing durable to publish.
                    if (!outputStage.wasBrokenPipe()) {
                        try {
                            publisher.publish();
                        } catch (IOException e) {
                            store.markRunFinished(runId, RunStatus.FAILED);
                            throw e;
                        }
                    }
                    // Broken pipe is a clean CLI termination, but the stdout stream was truncated:
                    // do not record the run as COMPLETED or resume would skip work.
                    store.markRunFinished(runId,
                            outputStage.wasBrokenPipe() ? RunStatus.FAILED : RunStatus.COMPLETED);
                })
                .epilogue(textOutputEpilogue(ctx, spec.format(), countingOut, outputStage))
                .build());
    }

    /**
     * Engine-backed Parquet run: seeds {@link WorkStealingScan} from the full
     * {@code seeds} list with {@code workerCount} workers, reusing the EXACT
     * output-durability wiring of {@link #runToParquetCheckpointed} — {@link PartListener}
     * advances {@code durable_cursor} per part; on clean {@code pool.close()} calls
     * {@code markOutputComplete} then {@code markRunFinished}; on failure {@code pool.abort()}.
     */
    public ListingStatistics runToParquetWorkStealing(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                      ParquetSpec spec, CheckpointStore store, long runId,
                                                      int workerCount, List<Node> seeds,
                                                      List<PartInfo> existingParts)
            throws SwathException, InterruptedException {
        return runToParquetWorkStealing(ctx, fetcher, outputDir, spec, store, runId, workerCount,
                seeds, existingParts, true);
    }

    public ListingStatistics runToParquetWorkStealing(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                      ParquetSpec spec, CheckpointStore store, long runId,
                                                      int workerCount, List<Node> seeds,
                                                      List<PartInfo> existingParts, boolean ownerSplitEnabled)
            throws SwathException, InterruptedException {
        return runToParquetWorkStealing(ctx, fetcher, outputDir, spec, store, runId, workerCount, seeds,
                existingParts,
                ownerSplitEnabled ? EngineToggles.DEFAULT
                        : EngineToggles.DEFAULT.withOwnerSplit(false));
    }

    /**
     * Full-toggle overload: {@code toggles} is the {@code --engine-toggle} ablation
     * namespace threaded into {@link WorkStealingScan}.
     */
    public ListingStatistics runToParquetWorkStealing(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                      ParquetSpec spec, CheckpointStore store, long runId,
                                                      int workerCount, List<Node> seeds,
                                                      List<PartInfo> existingParts,
                                                      EngineToggles toggles)
            throws SwathException, InterruptedException {
        return runToParquetWorkStealing(ctx, fetcher, outputDir, spec, store, runId, workerCount, seeds,
                existingParts, toggles, TraceSink.NONE);
    }

    /**
     * Full overload: {@code trace} is the opt-in JSONL flight recorder seam
     * threaded into {@link WorkStealingScan}.
     */
    public ListingStatistics runToParquetWorkStealing(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                      ParquetSpec spec, CheckpointStore store, long runId,
                                                      int workerCount, List<Node> seeds,
                                                      List<PartInfo> existingParts,
                                                      EngineToggles toggles, TraceSink trace)
            throws SwathException, InterruptedException {
        return runToParquetWorkStealing(ctx, fetcher, outputDir, spec, store, runId, workerCount, seeds,
                existingParts, toggles, trace, RetryConfig.DEFAULT);
    }

    /**
     * Full overload: {@code retryConfig} threads the transient-retry policy + injectable
     * backoff sleeper into {@link WorkStealingScan}. Every shorter overload defaults it to
     * {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics runToParquetWorkStealing(RunContext ctx, PageFetcher fetcher, Path outputDir,
                                                      ParquetSpec spec, CheckpointStore store, long runId,
                                                      int workerCount, List<Node> seeds,
                                                      List<PartInfo> existingParts,
                                                      EngineToggles toggles, TraceSink trace,
                                                      RetryConfig retryConfig)
            throws SwathException, InterruptedException {

        WorkStealingScan producer = new WorkStealingScan(
                new EngineContext(runId, spec.prefix(), ListingMode.OBJECTS, ctx.metrics(), toggles, trace, retryConfig),
                fetcher, store, workerCount, spec.maxKeys(), seeds, spec.filters());
        ParquetWriterPool pool = new ParquetWriterPool(outputDir, ParquetSchema.canonical(), spec.argsHash(),
                spec.numWriters(), spec.targetBytes(), spec.writerQueueCapacity(),
                new ParquetWriterPoolConfig(spec.bucket(), partFinalizedListener(store, runId), existingParts,
                        spec.rotationIntervalNanos(), spec.rotationMaxRows(), ctx.metrics()));
        ParquetOutputStage stage = new ParquetOutputStage(pool);
        Function<Duration, RunSummary> summary = el -> ctx.metrics().summary(el, "WORK_STEALING",
                pool.committedPartCount(), pool.committedBytes());

        return this.<RuntimeException>runLifecycle(ctx, LifecyclePlan.<RuntimeException>builder()
                .strategy("WORK_STEALING").strategyWhy("checkpointed_resumable_nodes").runId(runId)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info(
                        "list_run_start strategy={} checkpointed={} run_id={} output_format={} worker_count={}",
                        "WORK_STEALING", true, runId, "PARQUET", workerCount))
                .producer(producer).consumerStage(stage)
                .jsonSummaryConfig(spec.jsonSummary()).snapshotSummary(summary).terminalSummary(summary)
                .statistics(stage::statistics)
                .drain(poolDrain(pool))
                .complete(() -> {
                    store.markOutputComplete(runId);
                    store.markRunFinished(runId, RunStatus.COMPLETED);
                })
                .epilogue(() -> ctx.metrics().recordOutput("parquet", "written",
                        pool.committedPartCount(), pool.committedBytes()))
                .build());
    }

    private static void closeQuietly(EntryFormatter formatter, OutputStage stage) throws OutputException {
        try {
            formatter.close();
        } catch (IOException e) {
            if (BrokenPipe.is(e)) {
                // Close/flush-time pipe break: the buffered tail never reached the reader, so
                // the run is truncated even though writeBatch() never saw the failure.
                stage.markBrokenPipe();
            } else if (!stage.wasBrokenPipe()) {
                throw new OutputException("failed to flush output", e);
            }
        }
    }

    /**
     * Null-safe close for the JSON summary sidecar: always run in a {@code finally} so
     * {@code jsonWriter}'s daemon scheduler thread is never leaked, on success,
     * exception, or early return. {@link JsonRunSummaryWriter#close()} already swallows
     * its own failures, so this never masks the run's real exception.
     */
    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            log.warn("json_summary_writer_close_failed message={}", e.getMessage());
        }
    }

    // ---- sorted parquet sink (--sort) -------------------------------

    /**
     * Staging {@code part_file} format namespace, so segment rows never pollute the root manifest.
     * Runs stage page-run segments — this MUST equal {@code PageRunSegmentWriter.FORMAT_NAME}
     * ("page-run"), the value the writer/reader agree on (that constant is package-private to
     * {@code io.varve.swath.sort}, so it is mirrored here rather than referenced). Both the write side
     * ({@code SegmentSink.onSegmentFinalized}) and the select side ({@code sortedSegmentRows}) use this
     * one constant, so new segments are tagged and selected consistently.
     *
     * <p>{@code public} so the CLI resume-safety guard can refuse a resume whose
     * checkpoint carries staging segments tagged with a different, unsupported
     * staging format rather than silently sweep+relist them.
     */
    public static final String SORT_SEGMENT_FORMAT = "page-run";

    /**
     * Engine-backed <b>sorted</b> Parquet run: the listing feeds the single ordered
     * {@link SortLane} (segments finalized in seal order, {@code durable_cursor} advancing per
     * segment via the reused {@code partFinalized} machinery), then — once the listing drains and the
     * encoder is flushed — the merge k-way-merges the durable staging segments into the final sorted
     * output and publishes it (manifest written LAST, the "final output present" commit point).
     *
     * <p>On a clean run: {@code lane.close()} makes every segment durable, {@code markOutputComplete}
     * latches the nodes, the phase advances LISTING → MERGING → PUBLISHED, and the run is marked
     * COMPLETED only after publish. On failure/cancel {@code lane.abort()} drops the in-flight buffer
     * (re-listed on resume); the merge never runs, so the run stays resumable. {@code reattach} is
     * true on a mid-listing resume: it sweeps non-finalized staging segments and re-attaches to the
     * durable ones ({@code SORT.resume_reattached}). {@code reattach} is false on a fresh or
     * {@code --restart} run: the whole owned staging dir content (a DIFFERENT, abandoned prior run's
     * leftover {@code seg-*}/{@code merge-*.parquet}, if any) is swept before listing begins, so the
     * staging dir is always clean before a fresh run's segments land in it.
     *
     * <p>On a reattach resume this method re-lists only the non-durable tail, so as soon as the
     * durable set is read back it back-fills those segments' objects/segment-count into this
     * fresh run's counters ({@link io.varve.swath.observability.RunMetrics#recordRecoveredObjects}
     * + {@link io.varve.swath.observability.RunMetrics#recordRecoveredSortSegments}) — the
     * reattach/partial-relist sibling of {@link #runSortMergeOnly}'s backfill.
     */
    public ListingStatistics runToSortedParquetWorkStealing(
            RunContext ctx, PageFetcher fetcher, Path outputDir, Path stagingDir, ParquetSpec spec,
            CheckpointStore store, long runId, int workerCount, List<Node> seeds,
            SortConfig sortConfig, SortMode mode, EngineToggles toggles,
            TraceSink trace, boolean reattach) throws SwathException, InterruptedException {
        return runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, spec, store, runId,
                workerCount, seeds, sortConfig, mode, toggles, trace, reattach, RetryConfig.DEFAULT);
    }

    /**
     * Full overload: {@code retryConfig} threads the transient-retry policy + injectable
     * backoff sleeper into {@link WorkStealingScan}. The shorter overload defaults it to
     * {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics runToSortedParquetWorkStealing(
            RunContext ctx, PageFetcher fetcher, Path outputDir, Path stagingDir, ParquetSpec spec,
            CheckpointStore store, long runId, int workerCount, List<Node> seeds,
            SortConfig sortConfig, SortMode mode, EngineToggles toggles,
            TraceSink trace, boolean reattach, RetryConfig retryConfig)
            throws SwathException, InterruptedException {

        WorkStealingScan producer = new WorkStealingScan(
                new EngineContext(runId, spec.prefix(), ListingMode.OBJECTS, ctx.metrics(), toggles, trace, retryConfig),
                fetcher, store, workerCount, spec.maxKeys(), seeds, spec.filters());

        Comparator<ListEntry> comparator = new ListEntryComparator();
        // Pack each page on the fetch worker (parallelizes packing off the single sort drain
        // thread; the channel/downstream hold the compact packed page). Uses the SAME comparator + payload
        // codec the drain-thread SortBuffer.admit pack used, so output + durability are behavior-preserving.
        producer.enableSortPacking(new SortPagePacker(comparator, sortConfig));
        SortMetrics sortMetrics = ctx.metrics()::recordStealReason;

        if (reattach) {
            List<PartRef> segmentRows = sortedSegmentRows(store, runId);
            Set<String> keep = segmentRows.stream().map(PartRef::path).collect(Collectors.toSet());
            try {
                ParquetResume.discardNonFinalizedSegments(stagingDir, keep);
            } catch (IOException e) {
                throw new OutputException("failed to sweep sort staging on resume", e);
            }
            if (!segmentRows.isEmpty()) {
                ctx.metrics().recordStealReason("SORT", "resume_reattached");
            }
            // Backfill the pre-crash durable segments' rows/count HERE — the moment they become
            // known, before this process lists anything — rather than once the merge is done. This
            // fresh RunMetrics only ever sees the relisted TAIL in-process, so without the backfill
            // objects/sort.segments under-report the whole run; recording it late additionally cost
            // every live progress event of the listing phase its recovered_objects (they all
            // reported 0, and the tally landed when the phase was already WRITING). The figures are
            // read back from the checkpoint and never change afterwards. Nothing to do on a fresh
            // or --restart run.
            if (!segmentRows.isEmpty()) {
                ctx.metrics().recordRecoveredObjects(segmentRows.stream().mapToLong(PartRef::rows).sum());
                ctx.metrics().recordRecoveredSortSegments(segmentRows.size());
            }
        } else {
            // A FRESH or --restart sorted run
            // starts this run_id at zero finalized segments, so anything already sitting in the
            // staging dir is leftover from a DIFFERENT, abandoned prior run that never published (a
            // crash before merge, or a --restart of a --sort run — --restart only discards checkpoint
            // rows via SqliteCheckpointStore#deleteRun, never touches staging files). Confirmed
            // harmless to correctness even without this sweep: the merge only ever reads paths from
            // THIS run's own finalizedParts rows (sortedSegmentPaths), never a directory listing, so
            // an abandoned run's stale segments were never at risk of being silently merged in — this
            // closes a disk-leak/staging-cleanliness gap, not a data-correctness one. Wipe the whole
            // owned staging content (seg-*.parquet and merge-*.parquet alike) before listing begins.
            try {
                ParquetResume.discardNonFinalizedSegments(stagingDir, Set.of());
            } catch (IOException e) {
                throw new OutputException("failed to sweep abandoned sort staging on fresh run", e);
            }
        }

        SegmentSink sink = result -> {
            List<PartFinalize.DurableAdvance> advances = result.perNodeMaxKeys().entrySet().stream()
                    .map(e -> new PartFinalize.DurableAdvance(e.getKey(), e.getValue())).toList();
            store.partFinalized(new PartFinalize(runId, 0, result.path().getFileName().toString(),
                    SORT_SEGMENT_FORMAT, result.rows(), result.bytes(), advances));
        };
        String segmentPrefix = "seg-" + runId + "-" + Long.toHexString(System.nanoTime());
        SortLane lane = new SortLane(sortConfig, comparator, DuplicateHook.NO_OP, sortMetrics,
                sortLaneMeters(ctx.metrics()), stagingDir, segmentPrefix, sink);
        SortOutputStage stage = new SortOutputStage(lane);

        // The merge result is produced by the completion chain and read by the terminal summary +
        // epilogue, which the template runs strictly after completion returns — so the backfill and the
        // published-file counts land in the summary, never before the merge.
        SortTransformResult[] merged = new SortTransformResult[1];

        return this.<RuntimeException>runLifecycle(ctx, LifecyclePlan.<RuntimeException>builder()
                .strategy("WORK_STEALING").strategyWhy("checkpointed_resumable_nodes").runId(runId)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info(
                        "list_run_start strategy={} checkpointed={} run_id={} output_format={} worker_count={} sort={}",
                        "WORK_STEALING", true, runId, "PARQUET", workerCount, true))
                .producer(producer).consumerStage(stage)
                .jsonSummaryConfig(spec.jsonSummary())
                // During listing the merge has not run, so the periodic snapshot reports zero final files;
                // the terminal summary reads the true published-file count off the completed merge result.
                .snapshotSummary(el -> ctx.metrics().summary(el, "WORK_STEALING", 0L, 0L))
                .terminalSummary(el -> ctx.metrics().summary(el, "WORK_STEALING",
                        merged[0].finalFiles().size(), sortedOutputBytes(merged[0].finalFiles())))
                .statistics(stage::statistics)
                .drain(laneDrain(lane))
                .complete(() -> {
                    // Listing complete + all segments durable. Latch nodes, then merge + publish.
                    store.markOutputComplete(runId);
                    store.setSortPhase(runId, SortPhase.MERGING);
                    ctx.metrics().setPhase(Phase.MERGING);
                    // Normal listing-completion publish: no identity-verified merge-reentry guarantee here,
                    // so the NARROW part-*.parquet stale-finals sweep only (see sortMergeAndPublish javadoc).
                    merged[0] = sortMergeAndPublish(ctx, outputDir, stagingDir,
                            sortedSegmentRows(store, runId), sortConfig, mode, spec.bucket(),
                            spec.argsHash(), runId, spec.progressInterval(), false);
                    store.setSortPhase(runId, SortPhase.PUBLISHED);
                    store.markRunFinished(runId, RunStatus.COMPLETED);
                })
                // The sort path's final published output is itself Parquet (SortedParquetWriterFactory) —
                // labelled "parquet" here, not a distinct "sort" tag, so it lands on the SAME
                // swath.output.{files,bytes}{format=parquet} series as the non-sort Parquet sinks (the
                // sort-specific swath.sort.* meters already distinguish the code path that produced it).
                .epilogue(() -> ctx.metrics().recordOutput("parquet", "written",
                        merged[0].finalFiles().size(), sortedOutputBytes(merged[0].finalFiles())))
                .build());
    }

    /**
     * Merge-pending resume: the listing already completed (no resumable
     * nodes) but the final {@code manifest.json} is absent, so re-run <b>only</b> the merge from the
     * durable staging segments — ZERO new LIST fetches ({@code SORT.merge_redone}) — and publish.
     */
    public ListingStatistics runSortMergeOnly(RunContext ctx, Path outputDir, Path stagingDir,
            CheckpointStore store, long runId, SortConfig sortConfig, SortMode mode, ParquetSpec spec)
            throws SwathException, InterruptedException {

        long startedNs = System.nanoTime();
        ctx.metrics().markRunStarted();
        ctx.metrics().setStrategy("WORK_STEALING");
        ctx.metrics().setStrategyWhy("sort_merge_resume");
        ctx.metrics().setRunId(runId);
        ctx.metrics().setPrefix(spec.prefix());
        ctx.metrics().setPhase(Phase.LISTING);
        ctx.metrics().recordStealReason("SORT", "merge_redone");
        Supplier<RunSummary> snapshot =
                () -> ctx.metrics().summary(elapsedSince(startedNs), "WORK_STEALING", 0L, 0L);
        JsonRunSummaryWriter jsonWriter = startJsonSummary(ctx, spec.jsonSummary(), snapshot);
        log.info("list_sort_merge_resume run_id={} (re-running merge from staging, zero LIST fetches)", runId);
        // The sink is contracted to observe every run exactly once, the unwound ones included: a
        // merge that throws still writes a CRASH partial to the sidecar, and the operator-facing
        // block must not be the one surface that goes silent on it.
        boolean summaryEmitted = false;
        try {
            store.setSortPhase(runId, SortPhase.MERGING);
            ctx.metrics().setPhase(Phase.MERGING);
            List<PartRef> segRows = sortedSegmentRows(store, runId);
            // Merge-only resume: identity-verified merge-reentry (ListCommand#isPublishedByThisRun
            // gated this call), so the WIDE data/*.parquet stale-finals sweep is safe here.
            SortTransformResult result = sortMergeAndPublish(ctx, outputDir, stagingDir,
                    segRows, sortConfig, mode, spec.bucket(),
                    spec.argsHash(), runId, spec.progressInterval(), true);
            store.setSortPhase(runId, SortPhase.PUBLISHED);
            store.markRunFinished(runId, RunStatus.COMPLETED);

            // This process never ran the listing/staging phase that normally drives
            // recordEntriesEmitted/recordSortSegment, so without this backfill the summary would
            // under-report objects:0 / sort.segments:0 despite publishing the full, correct output
            // — see RunMetrics#summary(...,objectsOverride) and #recordRecoveredSortSegments for why
            // this is NOT a blind counter replay (progress.units/entries are deliberately untouched).
            ctx.metrics().recordRecoveredSortSegments(segRows.size());

            // Progress ends before the first terminal record, exactly as in runLifecycle's epilogue:
            // the reporter this method's merge started is closed, but the CLI's session reporter is not.
            ctx.metrics().finishProgress();
            Duration elapsed = elapsedSince(startedNs);
            RunSummary summary = ctx.metrics().summary(elapsed, "WORK_STEALING",
                    result.finalFiles().size(), sortedOutputBytes(result.finalFiles()), result.totalRows());
            ctx.metrics().setPhase(Phase.COMPLETE);
            ctx.metrics().recordRunCompletion(elapsed, summary.keysPerSecond());
            ctx.metrics().recordOutput("parquet", "written",
                    result.finalFiles().size(), sortedOutputBytes(result.finalFiles()));
            logSummary(summary);
            summaryEmitted = true;
            emitQuietly(() -> ctx.metrics().emitSummary(summary, ctx.metrics().diagnostics(elapsed),
                    completionStatus(ctx, null)));
            finish(jsonWriter, summary);
            return new ListingStatistics(result.totalRows(), 0L, 0L, 0L, summary.apiCalls(), elapsed);
        } finally {
            if (!summaryEmitted) {
                emitQuietly(() -> {
                    ctx.metrics().finishProgress();   // before the pinned record, same rule as above
                    Duration unwound = elapsedSince(startedNs);
                    RunSummary partial = snapshot.get();
                    JsonRunSummaryWriter.TerminalStatus status = terminalStatus(ctx, null);
                    pin(jsonWriter, partial, status);
                    ctx.metrics().emitSummary(partial, ctx.metrics().diagnostics(unwound), status);
                });
            }
            closeQuietly(jsonWriter);
        }
    }

    /**
     * Merge the durable staging segments into the published sorted output; manifest written LAST.
     * The final sorted files are written under {@code <root>/data/}, the pure-parquet subdir,
     * so the transform operates on the {@code data/} directory while the consumer manifest + markers
     * land at the dataset root.
     */
    /**
     * @param identityVerifiedWideSweep whether {@link SortTransform}'s WIDE {@code data/*.parquet}
     *         stale-finals sweep may fire (vs. the NARROW {@code part-*.parquet} sweep). Must be
     *         {@code true} ONLY from an identity-verified merge-reentry caller (today:
     *         {@link #runSortMergeOnly}, reached only after {@code ListCommand#isPublishedByThisRun}
     *         confirms this run owns the dataset) — safe there because a fresh run's {@code data/}
     *         was pre-wiped by {@code clearDatasetForFreshRun} and a resume is identity-gated before
     *         reaching this method. The normal listing-completion caller ({@link #run}) passes
     *         {@code false}: it has no such identity guarantee.
     */
    private SortTransformResult sortMergeAndPublish(RunContext ctx, Path outputDir, Path stagingDir,
            List<PartRef> stagedParts, SortConfig config, SortMode mode, String bucket, String argsHash, long runId,
            Duration progressInterval, boolean identityVerifiedWideSweep) throws SwathException {
        // The exact merge denominator, recorded HERE because this is the one point both merge
        // callers pass through with the staged parts in hand: rows merged is measured against the
        // rows those very segments hold (see RunMetrics#recordSortStaged).
        ctx.metrics().recordSortStaged(stagedParts.size(),
                stagedParts.stream().mapToLong(PartRef::rows).sum());
        List<Path> segments = stagedParts.stream().map(p -> stagingDir.resolve(p.path())).toList();
        Comparator<ListEntry> comparator = new ListEntryComparator();
        SortMetrics sortMetrics = ctx.metrics()::recordStealReason;
        // Wrap the final-file writer factory so each row streamed into the merged output marks
        // liveness progress — during the k-way merge NO page completes, so without this the watchdog
        // would false-trip a long final merge. Throttled to every N rows in
        // ProgressMarkingSortedFileWriter (never per key).
        SortedFileWriterFactory writerFactory = progressMarkingFactory(
                new SortedParquetWriterFactory(config, mode), ctx.metrics());
        // The WIDE stale-finals sweep (ALL data/*.parquet, not
        // just this transform's own naming) is safe ONLY on the identity-verified merge-reentry path
        // (ListCommand#isPublishedByThisRun gates whether runSortMergeOnly is ever reached) — see
        // SortTransform's class javadoc / cleanStaleFinals for the full argument, and the
        // identityVerifiedWideSweep javadoc above for which caller passes which value. Every OTHER
        // SortTransform caller (e.g. CaptureSorter's sort-fixture path) has no such guard and must NOT
        // opt in.
        SortTransform transform = new SortTransform(
                new SortRun(config, comparator, DuplicateHook.NO_OP, sortMetrics, writerFactory),
                identityVerifiedWideSweep, ctx.metrics()::recordSortMergeRange);
        Path dataDir = DatasetLayout.of(outputDir).dataDir();
        // Mark a phase-boundary progress tick so the merge/finalize tail starts with a fresh stall
        // window (the LISTING phase just quiesced; the watchdog must not count listing-idle time here).
        ctx.metrics().markProgress();
        Timer.Sample mergeSample = ctx.metrics().startSortMergeTimer();
        // The merge/publish tail must keep reporting: it is genuinely advancing (swath.progress.units,
        // fed by KWayMerge's per-pass callback below) and an external supervisor reading only the log
        // tail would otherwise see silence and kill a healthy, still-merging run. Normally the run's
        // session reporter is already ticking and this start JOINS it (see RunProgressReporter); on
        // the merge-only resume path, where no listing ever ran, this IS the owner. Either way it
        // honors the same configured --progress-interval, and stops on both the success and the
        // exception path.
        try (RunProgressReporter progress = startProgress(ctx, progressInterval)) {
            Files.createDirectories(dataDir);
            SortTransformResult result = transform.transform(segments, dataDir, stagingDir,
                    (finalFiles, totalRows) -> writeSortedManifest(outputDir, bucket, argsHash, runId,
                            finalFiles, ctx.metrics()),
                    ctx.metrics()::recordProgress,
                    // WRITING becomes reachable here — the cascade passes above stay MERGING;
                    // once only the output-writing work + publish remain, the swath.phase gauge
                    // advances instead of folding both into MERGING. The flag is whether that
                    // remaining work has an honest completion denominator (see FinalPassListener).
                    ctx.metrics()::startFinalMergePass);
            ctx.metrics().recordSortMerge(mergeSample);
            ctx.metrics().recordSortMergePasses(result.mergePasses());
            return result;
        } catch (IOException | UncheckedIOException e) {
            // A CLASSIFIED merge failure (today: a staged page-run segment whose
            // page minKeys regress — SegmentCorruptionException, error_class=page_run_min_regression) must
            // be greppable in summary.json, not just in stderr. The run unwinds without complete(), so the
            // sidecar's terminal write comes from JsonRunSummaryWriter#close via terminalStatus() below,
            // which reads this class off RunMetrics; an unclassified merge failure records nothing and
            // keeps the generic error_class:null crash shape. Same typed-exception -> terminal-status ->
            // error_class seam the classified seed-failure path uses.
            //
            // UncheckedIOException is caught HERE, not just IOException: BOTH mergers wrap a read failure
            // in one (StreamingMerger#computeNext, PageAwareMerger#computeNext) because SortedCursor#next
            // cannot throw a checked exception — and a corruption detected mid-drain (the common case: the
            // FIRST page can never regress) therefore arrives as an UncheckedIOException, not an
            // IOException. With only the checked catch this whole classification was INERT on the very
            // path it was written for: recordFatalErrorClass never ran and summary.json said
            // error_class:null. segmentErrorClass walks the cause chain, so the wrapper is transparent.
            ctx.metrics().recordFatalErrorClass(segmentErrorClass(e));
            throw new OutputException("sort merge/publish failed", e);
        }
    }

    /**
     * The classified {@code error_class} fingerprint of a sort merge/publish failure — walks the
     * cause chain (mirroring {@code ListCommand#seedErrorClass} / {@code ExitCodes#forThrowable}) for the
     * first {@link SegmentCorruptionException}. {@code null} for a generic merge failure (disk, Parquet,
     * an unclassified {@link IOException}), which keeps its {@code error_class:null} shape.
     */
    private static String segmentErrorClass(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SegmentCorruptionException sce) {
                return sce.errorClass();
            }
        }
        return null;
    }

    /**
     * Publish commit point (contract §6): the consumer {@code manifest.json} is
     * written over the final files, then the INTERNAL {@code .swath-state.json} identity, then {@code
     * symlink.txt}, then the empty {@code _SUCCESS} marker LAST. {@code runId} is the checkpoint's
     * {@code run_id}, recorded in {@code .swath-state.json} — {@link ListCommand}'s PUBLISHED dispatch
     * requires it to match, together with {@code args_hash}, before trusting an existing dataset as
     * THIS run's publish (never a stale one from a different prior run sharing the output directory).
     * {@code finalFiles} live under {@code <root>/data/}, so their manifest keys are {@code data/}-prefixed.
     */
    private static void writeSortedManifest(Path outputDir, String bucket, String argsHash, long runId,
            List<Path> finalFiles, RunMetrics metrics) throws IOException {
        List<PartInfo> parts = new ArrayList<>(finalFiles.size());
        for (Path f : finalFiles) {
            String relPath = DatasetLayout.key(f.getFileName().toString());
            // The per-file MD5 is the DOMINANT segment of the finalize/publish window — hashing a
            // multi-GB part streams the whole file and emits no progress on
            // its own, so a complete run could be halt()-ed at the finish line. Hash it manually and tick
            // liveness progress in proportion to BYTES actually consumed, keeping the watchdog HONEST: a
            // digest that is slow-but-advancing stays alive, while one whose read truly stalls (no bytes
            // move) emits no tick and still trips.
            String md5 = md5HexWithLivenessProgress(f, metrics);
            // minKey/maxKey/rowCount are the file's ACTUAL first/last key and row count (never
            // footer stats — see SortedFileIndex#bounds), so the manifest's anti-lie invariant
            // (minKey/maxKey/rowCount == the part's real content) holds by construction.
            SortedFileIndex.Bounds bounds = SortedFileIndex.bounds(f);
            String minKey = bounds.firstKey() == null ? null : new String(bounds.firstKey(), StandardCharsets.UTF_8);
            String maxKey = bounds.lastKey() == null ? null : new String(bounds.lastKey(), StandardCharsets.UTF_8);
            parts.add(new PartInfo(relPath, 0, bounds.rowCount(), Files.size(f), md5, minKey, maxKey));
        }
        Manifest.write(outputDir, bucket, ParquetSchema.canonical().toString(), parts,
                true, SortedParquetWriter.ORDER_VALUE);
        Manifest.writeState(outputDir, argsHash, runId);
        Manifest.writeSymlink(outputDir, parts);
        Manifest.writeSuccess(outputDir);
    }

    /** Every this-many bytes hashed/finalized, emit one liveness tick keyed to real work done. */
    private static final long FINALIZE_PROGRESS_BYTE_STRIDE = 64L * 1024 * 1024;   // 64 MiB
    private static final int FINALIZE_READ_BUFFER_BYTES = 1024 * 1024;             // 1 MiB read chunks

    /**
     * Stream {@code f} through an MD5 digest, ticking {@link RunMetrics#markProgress()} once per
     * {@link #FINALIZE_PROGRESS_BYTE_STRIDE} bytes consumed so the liveness watchdog sees the manifest
     * finalize pass advancing on a multi-GB part instead of a silent, minutes-long freeze that trips it
     * ({@code halt}) at the finish line. Byte-keyed (not timer-keyed) so it stays HONEST: a read that
     * genuinely stops moving bytes emits no tick and still trips. Each tick also bumps the {@code
     * SORT/finalize_progress_tick} engagement counter so post-hoc analysis can tell from the metrics
     * alone that the finalize path emitted progress and how often.
     */
    private static String md5HexWithLivenessProgress(Path f, RunMetrics metrics) throws IOException {
        return md5HexWithLivenessProgress(f, metrics, FINALIZE_PROGRESS_BYTE_STRIDE);
    }

    /**
     * Package-private seam (byte stride injectable) so a mechanism test can prove the byte-keyed tick
     * fires over a tiny file with a tiny stride, without writing a multi-GB fixture. Production callers
     * use the {@link #FINALIZE_PROGRESS_BYTE_STRIDE} overload above.
     */
    static String md5HexWithLivenessProgress(Path f, RunMetrics metrics, long byteStride)
            throws IOException {
        MessageDigest md = DigestUtils.getMd5Digest();
        byte[] buf = new byte[FINALIZE_READ_BUFFER_BYTES];
        long sinceTick = 0;
        try (var in = Files.newInputStream(f)) {
            int n;
            while ((n = in.read(buf)) != -1) {
                md.update(buf, 0, n);
                sinceTick += n;
                if (sinceTick >= byteStride) {
                    sinceTick = 0;
                    metrics.markProgress();
                    metrics.recordStealReason("SORT", "finalize_progress_tick");
                }
            }
        }
        return Hex.encodeHexString(md.digest());
    }

    private static long sortedOutputBytes(List<Path> finalFiles) {
        long bytes = 0;
        for (Path f : finalFiles) {
            try {
                bytes += Files.size(f);
            } catch (IOException ignored) {
                // best effort for the summary's compressed-size line
            }
        }
        return bytes;
    }

    private static List<PartRef> sortedSegmentRows(CheckpointStore store, long runId) throws CheckpointException {
        return store.finalizedParts(runId).stream()
                .filter(p -> SORT_SEGMENT_FORMAT.equals(p.format())).toList();
    }

    /**
     * Decorate a {@link SortedFileWriterFactory} so every final file it produces marks liveness
     * progress as rows are written, keeping the watchdog from false-tripping a long sort-merge tail.
     */
    private static SortedFileWriterFactory progressMarkingFactory(SortedFileWriterFactory delegate,
                                                                  RunMetrics metrics) {
        return (path, fileIndex) -> new ProgressMarkingSortedFileWriter(delegate.create(path, fileIndex), metrics);
    }

    /**
     * A {@link SortedFileWriter} that forwards to the real writer but ticks {@link
     * RunMetrics#markProgress()} every {@link #PROGRESS_ROW_STRIDE} rows during the merge, so the
     * liveness watchdog sees the final k-way merge advancing (no page completes there). Cheap: one
     * atomic increment per stride, never per row.
     */
    private static final class ProgressMarkingSortedFileWriter implements SortedFileWriter {

        private static final long PROGRESS_ROW_STRIDE = 10_000L;

        private final SortedFileWriter delegate;
        private final RunMetrics metrics;
        private long sinceMark;

        ProgressMarkingSortedFileWriter(SortedFileWriter delegate, RunMetrics metrics) {
            this.delegate = delegate;
            this.metrics = metrics;
        }

        @Override
        public void write(ListEntry e) throws IOException {
            delegate.write(e);
            if (++sinceMark >= PROGRESS_ROW_STRIDE) {
                sinceMark = 0L;
                metrics.markProgress();
            }
        }

        @Override
        public long rows() {
            return delegate.rows();
        }

        @Override
        public long dataSize() {
            return delegate.dataSize();
        }

        @Override
        public void markFinal() {
            // Tick BEFORE the footer flush + fsync that close() triggers — the last observable
            // forward-progress point before that blocking syscall (which is itself uninstrumentable).
            metrics.markProgress();
            metrics.recordStealReason("SORT", "finalize_progress_tick");
            delegate.markFinal();
        }

        @Override
        public void close() throws IOException {
            // A completed file is unambiguous forward progress, but delegate.close()
            // is a single footer-flush + fsync of a possibly multi-GB part — one blocking syscall that
            // emits no intra-call ticks. This pre-fsync tick resets the stall clock immediately before
            // it; the fsync itself cannot be instrumented (see md5HexWithLivenessProgress javadoc and
            // the operator docs note on raising --stall-timeout for billion-scale --sort).
            metrics.markProgress();
            metrics.recordStealReason("SORT", "finalize_progress_tick");
            delegate.close();
        }
    }

    private static SortLaneMeters sortLaneMeters(RunMetrics metrics) {
        return new SortLaneMeters() {
            @Override
            public void entriesAccepted(long entries) {
                metrics.recordSortEntries(entries);
            }

            @Override
            public void segmentFinalized(long bytes, int pageRuns) {
                metrics.recordSortSegment(bytes, pageRuns);
            }

            @Override
            public void backpressureWaited(long nanos) {
                metrics.recordSortBackpressureWait(nanos);
            }

            @Override
            public void stagingBytesLive(long liveBytes) {
                metrics.recordSortStagingBytesLive(liveBytes);
            }

            @Override
            public void handoffQueueDepth(int depth) {
                metrics.recordSortHandoffQueueDepth(depth);
            }

            @Override
            public void offThreadBuffersLive(int live) {
                metrics.recordSortOffThreadBuffersLive(live);
            }
        };
    }

    /** Drain the sort lane's encoder, mapping its checked failure onto the pipeline's exception types. */
    private static void closeLane(SortLane lane) throws SwathException, InterruptedException {
        try {
            lane.close();
        } catch (SwathException | InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new OutputException("sort segment encode failed", e);
        }
    }

    // ---- parquet sink ---------------------------------------------------------

    public ListingStatistics runToParquet(RunContext ctx, PageFetcher fetcher, Path outputDir, ParquetSpec spec)
            throws SwathException, InterruptedException {
        return runToParquet(ctx, fetcher, outputDir, spec, RetryConfig.DEFAULT);
    }

    /**
     * Full overload: {@code retryConfig} threads the sequential path's retry decorator, as in
     * {@link #run(RunContext, PageFetcher, Writer, Spec, RetryConfig)}. The shorter overload
     * defaults to {@link RetryConfig#DEFAULT}.
     */
    public ListingStatistics runToParquet(RunContext ctx, PageFetcher fetcher, Path outputDir, ParquetSpec spec,
                                          RetryConfig retryConfig)
            throws SwathException, InterruptedException {

        ScanProducer producer = new ScanProducer(observedSequentialFetcher(ctx, fetcher, retryConfig),
                spec.prefix(), spec.maxKeys(), spec.filters());
        ParquetWriterPool pool = new ParquetWriterPool(outputDir, ParquetSchema.canonical(), spec.argsHash(),
                spec.numWriters(), spec.targetBytes(), spec.writerQueueCapacity(),
                new ParquetWriterPoolConfig(spec.bucket(), PartListener.NONE, List.of(),
                        spec.rotationIntervalNanos(), spec.rotationMaxRows(), ctx.metrics()));
        ParquetOutputStage stage = new ParquetOutputStage(pool);
        Function<Duration, RunSummary> summary = el -> ctx.metrics().summary(el, "SEQUENTIAL",
                pool.committedPartCount(), pool.committedBytes());

        return this.<RuntimeException>runLifecycle(ctx, LifecyclePlan.<RuntimeException>builder()
                .strategy("SEQUENTIAL").strategyWhy("checkpoint_none").concurrencyTarget(1L)
                .prefix(spec.prefix()).queueCapacity(spec.queueCapacity())
                .progressInterval(spec.progressInterval())
                .startLog(() -> log.info("list_run_start strategy={} checkpointed={} output_format={}",
                        "SEQUENTIAL", false, "PARQUET"))
                .producer(producer).consumerStage(stage)
                .jsonSummaryConfig(spec.jsonSummary()).snapshotSummary(summary).terminalSummary(summary)
                .statistics(stage::statistics)
                .drain(poolDrain(pool))
                .complete(() -> { })
                .epilogue(() -> ctx.metrics().recordOutput("parquet", "written",
                        pool.committedPartCount(), pool.committedBytes()))
                .build());
    }

    /**
     * Starts the run's progress reporter at the configured {@code --progress-interval}, or the
     * default cadence when unset. Package-private (not {@code private}) so {@code
     * ListRunnerProgressWiringTest} can pin this resolution without reflection. When the CLI has
     * already started the session reporter (it spans the seed step too), this returns a joined
     * handle — see {@link RunProgressReporter}.
     */
    static RunProgressReporter startProgress(RunContext ctx, Duration interval) {
        return RunProgressReporter.start(ctx.metrics(),
                interval == null ? RunProgressReporter.nonTtyInterval() : interval);
    }

    /** No-op (returns {@code null}) unless the CLI configured a JSON run-summary sidecar. */
    private static JsonRunSummaryWriter startJsonSummary(RunContext ctx, JsonRunSummaryWriter.Config config,
                                                          Supplier<RunSummary> snapshotSupplier) {
        return startJsonSummary(ctx, config, snapshotSupplier, null);
    }

    /**
     * As {@link #startJsonSummary(RunContext, JsonRunSummaryWriter.Config, Supplier)} but attributes
     * a terminal partial (the run unwound without {@code complete()}): a {@code --max-duration}/signal
     * cancel carries its {@link StopReason} on the cancellation token; a broken pipe ({@code
     * outputStage.wasBrokenPipe()}) is neither a crash nor a signal, so it leaves {@code stop_reason}
     * unset; anything else is a {@link StopReason#CRASH}.
     */
    private static JsonRunSummaryWriter startJsonSummary(RunContext ctx, JsonRunSummaryWriter.Config config,
                                                          Supplier<RunSummary> snapshotSupplier,
                                                          OutputStage outputStage) {
        if (config == null) {
            return null;
        }
        Supplier<JsonRunSummaryWriter.TerminalStatus> terminalStatus = () -> terminalStatus(ctx, outputStage);
        return JsonRunSummaryWriter.start(
                config, ctx.metrics().registry(), Instant.now(), snapshotSupplier, terminalStatus);
    }

    private static JsonRunSummaryWriter.TerminalStatus terminalStatus(RunContext ctx, OutputStage outputStage) {
        JsonRunSummaryWriter.TerminalStatus attributed = attributedStatus(ctx);
        if (attributed != null) {
            return attributed;
        }
        if (outputStage != null && outputStage.wasBrokenPipe()) {
            return new JsonRunSummaryWriter.TerminalStatus(null);   // clean broken pipe — not a crash/signal
        }
        // A CRASH carries an error_class when — and only when — the unwinding
        // failure classified itself (RunMetrics#recordFatalErrorClass, e.g. a page-run segment whose page
        // minKeys regress). An unclassified crash keeps its error_class:null (and exit_code:null)
        // shape — its terminal throwable is not in hand here, and the exit-code mapping assigns different
        // codes to different fatal types (OutputException/CheckpointException 1, Invalid* 2), so a code
        // must never be guessed. A CLASSIFIED fatal is different: the one class today (page_run_min_regression)
        // is raised as segment corruption and rethrown by sortMergeAndPublish as an OutputException, whose
        // exit code is 1 — so the sidecar can report the true process exit code instead of null, exactly as
        // the classified seed-failure path does (docs/metrics-and-observability.md §3).
        String fatalErrorClass = ctx.metrics().fatalErrorClass();
        return new JsonRunSummaryWriter.TerminalStatus(
                StopReason.CRASH, null, fatalErrorClass,
                fatalErrorClass == null ? null : CLASSIFIED_FATAL_EXIT_CODE);
    }

    /**
     * The attributed stop reason, refined, or {@code null} when nothing ever cancelled this run.
     * Both terminal classifiers consult this FIRST and a broken pipe second, so the two can never
     * disagree about a run that suffered both: a downstream close during an already-cancelling run
     * is a consequence of the cancel, and reporting it as the neutral broken-pipe stop would let
     * the summary block claim a clean early exit while the report recorded {@code
     * stop_reason=signal}.
     */
    private static JsonRunSummaryWriter.TerminalStatus attributedStatus(RunContext ctx) {
        StopReason attributed = ctx.cancellation().stopReason();
        if (attributed == null) {
            return null;
        }
        if (attributed == StopReason.MAX_DURATION && ctx.metrics().sessionObjectsEmitted() == 0L) {
            // The whole timebox burned with zero objects emitted THIS session (a pathological/slow
            // bucket, or a node that only ever attempt-times-out/gets throttled) — distinct from
            // a legit large timeboxed partial that actually made headway. Exit code is unaffected:
            // ListCommand#timeboxExitOrRethrow keys off the CancellationToken's own MAX_DURATION
            // attribution, not this refined summary value.
            return new JsonRunSummaryWriter.TerminalStatus(StopReason.MAX_DURATION_NO_PROGRESS);
        }
        if (attributed == StopReason.STUCK) {
            return stuckTerminalStatus(ctx);
        }
        return new JsonRunSummaryWriter.TerminalStatus(attributed);   // max_duration / signal
    }

    /**
     * The ONE source-routed {@code stop_source}/{@code error_class} derivation for a {@code
     * StopReason.STUCK} terminal — shared by this normal-run summary path AND {@code
     * ListCommand#writeEarlyExitSummary}'s seed-time early-exit summary, so both observability
     * surfaces (and {@code ListCommand}'s {@code list_stuck_stop} marker, which does the same
     * source-tag -> {@code RunMetrics#stuckErrorClass} routing inline) can never disagree about
     * which classification a given {@code stop_source} implies.
     */
    public static JsonRunSummaryWriter.TerminalStatus stuckTerminalStatus(RunContext ctx) {
        CancelSource source = ctx.cancellation().source();
        String stopSource = source == null ? null : source.tag();
        String errorClass = ctx.metrics().stuckErrorClass(stopSource);
        return new JsonRunSummaryWriter.TerminalStatus(StopReason.STUCK, stopSource, errorClass);
    }

    /** Terminal write of the JSON sidecar; a no-op when the sidecar was never configured. */
    private static void finish(JsonRunSummaryWriter jsonWriter, RunSummary summary) {
        if (jsonWriter != null) {
            jsonWriter.complete(summary);
        }
    }

    /**
     * The epilogue's terminal sidecar write, given the disposition the summary block was just
     * rendered from. A broken pipe truncated stdout, so the run is not actually complete and the
     * sidecar must not claim {@code completed:true} (mirroring the {@code
     * store.markRunFinished(..., FAILED)} treatment for the checkpoint DB): the decided pair is
     * pinned instead, and the outer {@code finally}'s {@code close()} writes exactly it — the same
     * numbers and the same disposition the operator was shown, rather than a re-snapshot taken a
     * moment later. Every other epilogue takes the ordinary {@code completed:true} write.
     */
    private static void finishOrPin(JsonRunSummaryWriter jsonWriter, RunSummary summary,
                                     JsonRunSummaryWriter.TerminalStatus status, OutputStage outputStage) {
        if (outputStage != null && outputStage.wasBrokenPipe()) {
            pin(jsonWriter, summary, status);
        } else {
            finish(jsonWriter, summary);
        }
    }

    /** Hand the sidecar the terminal pair the sink was given; a no-op when no sidecar was configured. */
    private static void pin(JsonRunSummaryWriter jsonWriter, RunSummary summary,
                            JsonRunSummaryWriter.TerminalStatus status) {
        if (jsonWriter != null) {
            jsonWriter.pinTerminal(summary, status);
        }
    }

    /**
     * Terminal-summary emit for a run that unwound before its epilogue (a cancel or a fatal): the
     * snapshot and the {@link #terminalStatus} attribution are taken ONCE and pinned into the JSON
     * sidecar before being rendered, so the {@code close()}-time partial record and the
     * operator-facing block are the same terminal record — one {@code duration_ms}, one
     * disposition — and the sink observes exactly the runs the report does, not only the ones that
     * finished.
     *
     * <p>If building that snapshot throws, {@link #emitQuietly} swallows it with nothing pinned,
     * and the sidecar falls back to its own suppliers exactly as before.
     */
    private static <E extends Exception> void emitUnwoundSummary(RunContext ctx, LifecyclePlan<E> plan,
                                                                  JsonRunSummaryWriter jsonWriter, long startedNs) {
        emitQuietly(() -> {
            ctx.metrics().finishProgress();   // before the pinned record, same rule as the epilogue
            Duration elapsed = elapsedSince(startedNs);
            RunSummary summary = plan.snapshotSummary.apply(elapsed);
            JsonRunSummaryWriter.TerminalStatus status = terminalStatus(ctx, plan.outputStage);
            pin(jsonWriter, summary, status);
            ctx.metrics().emitSummary(summary, ctx.metrics().diagnostics(elapsed), status);
        });
    }

    /**
     * Run a terminal-summary emit so it can never end the run: the sink is presentation, and a
     * rendering fault must cost the operator the block, not turn a successful run into a failure
     * (nor a different exit code) on its way out.
     */
    private static void emitQuietly(Runnable emit) {
        try {
            emit.run();
        } catch (RuntimeException e) {
            log.debug("list_run_summary_emit_failed message={}", e.getMessage());
        }
    }

    /**
     * The ONE terminal disposition of a run that reached its epilogue — rendered to the summary
     * sink and, on the broken-pipe branch, pinned into the JSON sidecar, so neither surface can
     * name a disposition the other contradicts.
     *
     * <p>A run that got here drained its pipeline and committed its completion chain, so it is
     * {@link StopReason#COMPLETED} even if a signal or a {@code --max-duration} deadline trips the
     * cancellation token between that point and this one. That is not leniency: {@link
     * #finish}'s {@code complete()} writes {@code completed:true} for exactly this run, and it
     * returns normally, so the process exits 0. A cancel that arrived in time to cost the run
     * anything never reaches here at all — it unwinds as a {@code CancelledException} through
     * {@link #emitUnwoundSummary}, which is where an attributed stop reason genuinely IS the
     * disposition. Consulting the live token here instead would let a cancel landing microseconds
     * after the last object was published print {@code INCOMPLETE} over a report that says
     * completed.
     *
     * <p>A broken pipe is the one epilogue the sidecar does not complete, so this defers to {@link
     * #terminalStatus(RunContext, OutputStage)} — the value {@code close()} would otherwise
     * compute for itself, {@link #attributedStatus} first and then the neutral {@code null} reason
     * that says a downstream close is a clean stop, never a failure.
     */
    private static JsonRunSummaryWriter.TerminalStatus completionStatus(RunContext ctx, OutputStage outputStage) {
        if (outputStage != null && outputStage.wasBrokenPipe()) {
            return terminalStatus(ctx, outputStage);
        }
        return new JsonRunSummaryWriter.TerminalStatus(StopReason.COMPLETED);
    }

    private static void logSummary(RunSummary summary) {
        log.info("list_run_summary run_id={} objects={} duration_ms={} strategy={} api_calls={} cost_usd={} output_files={} compressed_size_bytes={} keys={} pages={} peak_in_flight={} steals={} splits={} errors={} keys_per_sec={} api_calls_per_1k_objects={} peak_rss_bytes={} peak_heap_bytes={} cpu_seconds={} cpu_efficiency={}",
                summary.runId(), summary.objects(), summary.duration().toMillis(), summary.strategy(),
                summary.apiCalls(), summary.costUsd(), summary.outputFiles(), summary.compressedBytes(),
                summary.keys(), summary.pages(), summary.peakInFlight(), summary.steals(), summary.splits(),
                summary.errors(), summary.keysPerSecond(), summary.apiCallsPer1kObjects(),
                summary.peakRssBytes(), summary.peakHeapBytes(), summary.cpuSeconds(), summary.cpuEfficiency());
    }

    private static void logDiagnostics(RunMetrics.RunDiagnostics d) {
        // throttle_events counts only real 503/5xx backpressure, never client
        // attempt-timeouts; transient_events + aimd_votes are surfaced alongside so an operator can
        // read the split without ambiguity (see RunMetrics.RunDiagnostics javadoc).
        log.info("list_run_diagnostics run_id={} duration_ms={} strategy={} strategy_why={} steal_reasons={} probe_fetches={} empty_upper_bisections={} splits_committed={} unsplittable_victims={} split_guard_aborts={} peak_in_flight={} time_to_first_steal_ms={} time_to_peak_in_flight_ms={} pages={} fetched_keys={} mean_keys_per_page={} short_truncated_pages={} throttle_events={} transient_events={} aimd_votes={} aimd_target_reductions={}",
                d.runId(), d.durationMs(), d.strategy(), d.strategyWhy(), d.stealReasons(),
                d.probeFetches(), d.emptyUpperBisections(), d.splitsCommitted(),
                d.unsplittableVictims(), d.splitGuardAborts(), d.peakInFlight(),
                d.timeToFirstStealMs(), d.timeToPeakInFlightMs(), d.pages(), d.totalKeys(),
                d.meanKeysPerPage(), d.shortTruncatedPages(), d.throttleEvents(), d.transientEvents(),
                d.aimdVotes(), d.aimdTargetReductions());
    }

    private static Duration elapsedSince(long startedNs) {
        return Duration.ofNanos(System.nanoTime() - startedNs);
    }

    /** {@code swath.output.{files,bytes}}'s {@code format} tag for a text sink. */
    private static String textFormatTag(OutputFormat format) {
        return format.name().toLowerCase(Locale.ROOT);
    }

    private static PageFetcher observedSequentialFetcher(RunContext ctx, PageFetcher delegate) {
        return observedSequentialFetcher(ctx, delegate, RetryConfig.DEFAULT);
    }

    /**
     * Full overload threading the CLI-resolved {@link RetryConfig} — the same
     * RIDE_OUT/BOUNDED policy + injectable backoff sleeper the seed and engine paths already receive —
     * into the sequential path's transient-retry decorator. Without this, the sequential path always
     * defaults to RIDE_OUT regardless of whether a real {@code LivenessWatchdog} is armed, leaving an
     * infinite-retry hole open whenever BOTH watchdog windows are disabled
     * ({@code --stall-timeout 0 --no-progress-timeout 0}): a permanent storm would ride out forever with
     * no backstop. The shorter overload above defaults to {@link RetryConfig#DEFAULT} (BOUNDED, the
     * safe owner-less semantics, real sleeper) for the checkpointed/legacy call sites that have no CLI wiring.
     */
    private static PageFetcher observedSequentialFetcher(RunContext ctx, PageFetcher delegate,
                                                          RetryConfig retryConfig) {
        // The sequential (non-work-stealing / --checkpoint none) path calls the
        // raw fetcher directly, outside the engine's gauge-wrapped retrying fetcher — so with SDK
        // maxAttempts=1 a single transient 503/attempt-timeout/reset would kill the run. Retry it with
        // the shared bounded, cancellation-aware transient-retry decorator BENEATH the in-flight
        // accounting (the whole retried logical fetch counts as one in-flight unit).
        RetryConfig retry = retryConfig == null ? RetryConfig.DEFAULT : retryConfig;
        PageFetcher retrying = new TransientRetryFetcher(
                delegate, ctx.cancellation(), ctx.metrics(), retry.sleeper(), retry.policy());
        return new PageFetcher() {
            @Override
            public ListPage fetchPage(PageRequest req) throws ListingException, InterruptedException {
                ctx.metrics().incrementInFlight();
                try {
                    return retrying.fetchPage(req);
                } finally {
                    ctx.metrics().decrementInFlight();
                }
            }

            @Override
            public StoreCapabilities capabilities() {
                return delegate.capabilities();
            }
        };
    }

    /**
     * The single lifecycle envelope shared by every {@code run*} entry point that drives a
     * producer/pipeline (all but {@link #runSortMergeOnly}). Each entry point shrinks to wiring: it
     * builds its sink/producer and a {@link LifecyclePlan}, then hands the plan here. The template runs
     * three fixed slots <b>in order</b> — <b>drain → complete → epilogue</b> — around a written-once
     * prologue and teardown:
     *
     * <ul>
     *   <li><b>prologue:</b> start clock, {@code markRunStarted}, the {@code setStrategy/Why/
     *       ConcurrencyTarget/RunId/Prefix} + {@code setPhase(LISTING)} metric sequence, the
     *       {@link Pipeline} + {@link JsonRunSummaryWriter}, and the {@code list_run_start} log;</li>
     *   <li><b>drain (slot 1):</b> the pipeline runs the producer into the consumer stage (wrapped in
     *       {@link #runWithRunId} when the run is checkpointed), then the sink's success verb
     *       ({@link DrainStep#onDrained()}); {@link DrainStep#onFinally} always runs the sink's
     *       teardown (close the text formatter, or abort the parquet pool / sort lane unless drained);</li>
     *   <li><b>complete (slot 2):</b> the path's ordered store-mutation chain
     *       ({@link CompletionCommit}) — whole and unsplit, and always strictly after the parts/segments
     *       are durable and strictly before the terminal summary is computed;</li>
     *   <li><b>epilogue (slot 3):</b> the terminal summary, {@code setPhase(COMPLETE)},
     *       {@code recordRunCompletion} (guarded by the nullable text {@link OutputStage}'s broken-pipe
     *       flag), the sink's {@link Epilogue#recordOutput()}, the summary/diagnostics logs, the JSON
     *       sidecar terminal write, and the outer {@code closeQuietly(jsonWriter)}.</li>
     * </ul>
     *
     * <p>The hooks change only <b>what</b> each slot does, never <b>when</b> relative to the others: a
     * completion chain that must land before the terminal summary (e.g. the sort merge + recovered-segment
     * backfill) lives entirely inside {@code complete}, which the template runs before it reads the
     * summary. {@code E} is the one extra checked exception a completion chain may raise — {@link IOException}
     * for the text work-stealing path's publish, {@link RuntimeException} (i.e. none) elsewhere.
     */
    private <E extends Exception> ListingStatistics runLifecycle(RunContext ctx, LifecyclePlan<E> plan)
            throws SwathException, InterruptedException, E {
        long startedNs = System.nanoTime();
        ctx.metrics().markRunStarted();
        ctx.metrics().setStrategy(plan.strategy);
        ctx.metrics().setStrategyWhy(plan.strategyWhy);
        if (plan.concurrencyTarget != null) {
            ctx.metrics().setConcurrencyTarget(plan.concurrencyTarget);
        }
        if (plan.runId != null) {
            ctx.metrics().setRunId(plan.runId);
        }
        ctx.metrics().setPrefix(plan.prefix);
        ctx.metrics().setPhase(Phase.LISTING);
        Pipeline<PageBatch> pipeline = new Pipeline<>((long) plan.queueCapacity, PageBatch::entryCount);
        JsonRunSummaryWriter jsonWriter = startJsonSummary(ctx, plan.jsonSummaryConfig,
                () -> plan.snapshotSummary.apply(elapsedSince(startedNs)), plan.outputStage);
        plan.startLog.run();
        boolean summaryEmitted = false;
        try {
            // Progress spans BOTH slots, not just the drain: the sort path's whole merge/publish tail
            // runs inside complete(), and scoping the reporter to the pipeline alone is what left a
            // fully-enumerated run reporting nothing for its entire merge. close() only stops daemon
            // schedulers and never throws, so the drained flag below stays accurate.
            try (RunProgressReporter progress = startProgress(ctx, plan.progressInterval)) {
                // Slot 1 — drain: run the pipeline (bound to the run id when checkpointed), then the sink's
                // success verb; teardown always fires in the finally.
                boolean drained = false;
                try {
                    if (plan.runId != null) {
                        runWithRunId(ctx, plan.runId, () -> {
                            pipeline.run(ctx, plan.producer, plan.consumerStage);
                            plan.drain.onDrained();
                        });
                    } else {
                        pipeline.run(ctx, plan.producer, plan.consumerStage);
                        plan.drain.onDrained();
                    }
                    drained = true;
                } finally {
                    plan.drain.onFinally(drained);
                }
                // Slot 2 — complete: the per-path ordered store-mutation chain, before the terminal summary.
                plan.complete.commit();
            }
            // Slot 3 — epilogue. Progress ends HERE, before the first terminal record is written:
            // the reporter closed above is only this method's scope, while the CLI's session
            // reporter spans the whole command, so a scheduled tick would otherwise still be able
            // to land between the list_run_summary line and the emit that ends progress.
            ctx.metrics().finishProgress();
            Duration elapsed = elapsedSince(startedNs);
            RunSummary summary = plan.terminalSummary.apply(elapsed);
            ctx.metrics().setPhase(Phase.COMPLETE);
            if (plan.outputStage == null || !plan.outputStage.wasBrokenPipe()) {
                ctx.metrics().recordRunCompletion(elapsed, summary.keysPerSecond());
            }
            plan.epilogue.recordOutput();
            RunMetrics.RunDiagnostics diagnostics = ctx.metrics().diagnostics(elapsed);
            logSummary(summary);
            logDiagnostics(diagnostics);
            // Claimed BEFORE the emit, and the emit cannot throw: a sink that failed here would
            // otherwise both escape this method (failing a run that succeeded) and be re-emitted
            // by the finally below, attributed CRASH.
            summaryEmitted = true;
            JsonRunSummaryWriter.TerminalStatus terminal = completionStatus(ctx, plan.outputStage);
            emitQuietly(() -> ctx.metrics().emitSummary(summary, diagnostics, terminal));
            finishOrPin(jsonWriter, summary, terminal, plan.outputStage);
            return plan.statistics.compute(summary.apiCalls(), elapsed);
        } finally {
            if (!summaryEmitted) {
                emitUnwoundSummary(ctx, plan, jsonWriter, startedNs);
            }
            closeQuietly(jsonWriter);
        }
    }

    /** Drain teardown for a text sink: close the formatter + stage on every exit (never aborts). */
    private static DrainStep textDrain(EntryFormatter formatter, OutputStage stage) {
        return new DrainStep() {
            @Override
            public void onDrained() {
                // Nothing to finalize before completion — the formatter is flushed/closed in onFinally.
            }

            @Override
            public void onFinally(boolean drained) throws OutputException {
                closeQuietly(formatter, stage);
            }
        };
    }

    /** Drain for a parquet sink: finalize the pool (footer fsync + manifest, I6) on success, abort on failure. */
    private static DrainStep poolDrain(ParquetWriterPool pool) {
        return new DrainStep() {
            @Override
            public void onDrained() throws OutputException {
                pool.close();   // finalize each lane's open part (footer fsync) + commit manifest (I6)
            }

            @Override
            public void onFinally(boolean drained) {
                if (!drained) {
                    pool.abort();   // discard non-finalized parts → resume re-lists them exactly once
                }
            }
        };
    }

    /** Drain for a sort sink: drain the lane's encoder (every segment durable) on success, abort on failure. */
    private static DrainStep laneDrain(SortLane lane) {
        return new DrainStep() {
            @Override
            public void onDrained() throws SwathException, InterruptedException {
                closeLane(lane);   // drain encoder → every segment durable (throws on failure)
            }

            @Override
            public void onFinally(boolean drained) {
                if (!drained) {
                    lane.abort();   // drop the in-flight buffer → re-listed on resume
                }
            }
        };
    }

    /** The parquet {@link PartListener} shared by both checkpointed parquet paths: record + advance {@code durable_cursor}. */
    private static PartListener partFinalizedListener(CheckpointStore store, long runId) {
        return e -> {
            List<PartFinalize.DurableAdvance> advances = e.nodeMaxKeys().entrySet().stream()
                    .map(en -> new PartFinalize.DurableAdvance(en.getKey(), en.getValue()))
                    .toList();
            store.partFinalized(new PartFinalize(runId, e.writerId(), e.fileName(), "parquet",
                    e.rows(), e.bytes(), advances));
        };
    }

    /**
     * The {@code recordOutput} epilogue shared by every text sink: a written/truncated tag keyed off the
     * broken pipe, plus the broken-pipe engagement counter when the stream was truncated.
     */
    private static Epilogue textOutputEpilogue(RunContext ctx, OutputFormat format, CountingWriter countingOut,
                                               OutputStage outputStage) {
        return () -> {
            ctx.metrics().recordOutput(textFormatTag(format),
                    outputStage.wasBrokenPipe() ? "truncated" : "written", 1L, countingOut.bytesWritten());
            if (outputStage.wasBrokenPipe()) {
                ctx.metrics().recordOutputBrokenPipe();
            }
        };
    }

    /** Drain slot: the per-sink success verb and the always-run teardown, with no access to slot sequencing. */
    private interface DrainStep {
        /** The sink's success verb after the pipeline drains cleanly (parquet {@code close}, sort {@code closeLane}); text no-op. */
        void onDrained() throws SwathException, InterruptedException;

        /** Teardown that always runs: {@code drained} is false iff the drain slot did not complete cleanly. */
        void onFinally(boolean drained) throws OutputException;
    }

    /** Completion slot: the per-path ordered store-mutation chain, kept whole and unsplit. */
    @FunctionalInterface
    private interface CompletionCommit<E extends Exception> {
        void commit() throws SwathException, InterruptedException, E;
    }

    /** Epilogue slot: the sink's {@code recordOutput} tag/counts and any broken-pipe flag. */
    @FunctionalInterface
    private interface Epilogue {
        void recordOutput();
    }

    /** The terminal {@link ListingStatistics} builder for a sink, computed from the run's api-call count + elapsed. */
    @FunctionalInterface
    private interface StatisticsFn {
        ListingStatistics compute(long apiCalls, Duration elapsed);
    }

    /**
     * The per-run configuration {@link #runLifecycle} executes: the prologue values, the producer +
     * consumer stage, the two summary views (a periodic-snapshot view and the terminal view), and the
     * three ordered slot hooks. Built once per entry point via {@link #builder()}; {@code concurrencyTarget},
     * {@code runId}, {@code jsonSummaryConfig} and {@code outputStage} are nullable (a null
     * {@code outputStage} marks a non-text sink, which skips the broken-pipe branches).
     */
    private static final class LifecyclePlan<E extends Exception> {

        private final String strategy;
        private final String strategyWhy;
        private final Long concurrencyTarget;
        private final Long runId;
        private final byte[] prefix;
        private final int queueCapacity;
        private final Duration progressInterval;
        private final Runnable startLog;
        private final Pipeline.Producer<PageBatch> producer;
        private final Pipeline.Consumer<PageBatch> consumerStage;
        private final JsonRunSummaryWriter.Config jsonSummaryConfig;
        private final Function<Duration, RunSummary> snapshotSummary;
        private final Function<Duration, RunSummary> terminalSummary;
        private final OutputStage outputStage;
        private final StatisticsFn statistics;
        private final DrainStep drain;
        private final CompletionCommit<E> complete;
        private final Epilogue epilogue;

        private LifecyclePlan(Builder<E> b) {
            this.strategy = b.strategy;
            this.strategyWhy = b.strategyWhy;
            this.concurrencyTarget = b.concurrencyTarget;
            this.runId = b.runId;
            this.prefix = b.prefix;
            this.queueCapacity = b.queueCapacity;
            this.progressInterval = b.progressInterval;
            this.startLog = b.startLog;
            this.producer = b.producer;
            this.consumerStage = b.consumerStage;
            this.jsonSummaryConfig = b.jsonSummaryConfig;
            this.snapshotSummary = b.snapshotSummary;
            this.terminalSummary = b.terminalSummary;
            this.outputStage = b.outputStage;
            this.statistics = b.statistics;
            this.drain = b.drain;
            this.complete = b.complete;
            this.epilogue = b.epilogue;
        }

        static <E extends Exception> Builder<E> builder() {
            return new Builder<>();
        }

        static final class Builder<E extends Exception> {
            private String strategy;
            private String strategyWhy;
            private Long concurrencyTarget;
            private Long runId;
            private byte[] prefix;
            private int queueCapacity;
            private Duration progressInterval;
            private Runnable startLog;
            private Pipeline.Producer<PageBatch> producer;
            private Pipeline.Consumer<PageBatch> consumerStage;
            private JsonRunSummaryWriter.Config jsonSummaryConfig;
            private Function<Duration, RunSummary> snapshotSummary;
            private Function<Duration, RunSummary> terminalSummary;
            private OutputStage outputStage;
            private StatisticsFn statistics;
            private DrainStep drain;
            private CompletionCommit<E> complete;
            private Epilogue epilogue;

            Builder<E> strategy(String v) { this.strategy = v; return this; }
            Builder<E> strategyWhy(String v) { this.strategyWhy = v; return this; }
            Builder<E> concurrencyTarget(long v) { this.concurrencyTarget = v; return this; }
            Builder<E> runId(long v) { this.runId = v; return this; }
            Builder<E> prefix(byte[] v) { this.prefix = v; return this; }
            Builder<E> queueCapacity(int v) { this.queueCapacity = v; return this; }
            Builder<E> progressInterval(Duration v) { this.progressInterval = v; return this; }
            Builder<E> startLog(Runnable v) { this.startLog = v; return this; }
            Builder<E> producer(Pipeline.Producer<PageBatch> v) { this.producer = v; return this; }
            Builder<E> consumerStage(Pipeline.Consumer<PageBatch> v) { this.consumerStage = v; return this; }
            Builder<E> jsonSummaryConfig(JsonRunSummaryWriter.Config v) { this.jsonSummaryConfig = v; return this; }
            Builder<E> snapshotSummary(Function<Duration, RunSummary> v) { this.snapshotSummary = v; return this; }
            Builder<E> terminalSummary(Function<Duration, RunSummary> v) { this.terminalSummary = v; return this; }
            Builder<E> outputStage(OutputStage v) { this.outputStage = v; return this; }
            Builder<E> statistics(StatisticsFn v) { this.statistics = v; return this; }
            Builder<E> drain(DrainStep v) { this.drain = v; return this; }
            Builder<E> complete(CompletionCommit<E> v) { this.complete = v; return this; }
            Builder<E> epilogue(Epilogue v) { this.epilogue = v; return this; }

            LifecyclePlan<E> build() {
                return new LifecyclePlan<>(this);
            }
        }
    }

    private static void runWithRunId(RunContext ctx, long runId, CheckedRun task)
            throws SwathException, InterruptedException {
        try {
            ctx.runWhereBound(runId, () -> {
                task.run();
                return null;
            });
        } catch (SwathException | InterruptedException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface CheckedRun {
        void run() throws SwathException, InterruptedException;
    }
}
