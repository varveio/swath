/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.micrometer.core.instrument.MeterRegistry;
import io.varve.swath.checkpoint.CheckpointStore;
import io.varve.swath.checkpoint.Node;
import io.varve.swath.checkpoint.NodeSpec;
import io.varve.swath.checkpoint.PartRef;
import io.varve.swath.checkpoint.RunKey;
import io.varve.swath.checkpoint.RunMeta;
import io.varve.swath.checkpoint.RunStatus;
import io.varve.swath.checkpoint.SoftRestoreContext;
import io.varve.swath.checkpoint.SortPhase;
import io.varve.swath.checkpoint.SqliteCheckpointStore;
import io.varve.swath.engine.RetryConfig;
import io.varve.swath.engine.RetryPolicy;
import io.varve.swath.engine.SeedMode;
import io.varve.swath.engine.SeedStep;
import io.varve.swath.engine.TransientRetryFetcher;
import io.varve.swath.error.CancelledException;
import io.varve.swath.error.CheckpointException;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.error.InvalidUriException;
import io.varve.swath.error.ListingException;
import io.varve.swath.error.OutputException;
import io.varve.swath.error.ProtocolViolationException;
import io.varve.swath.error.UnsupportedBucketException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.ListingMode;
import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.MeterRegistries;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunProgressReporter;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.SafeInput;
import io.varve.swath.observability.StopReason;
import io.varve.swath.observability.TraceSink;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.parquet.DatasetLayout;
import io.varve.swath.output.parquet.Manifest;
import io.varve.swath.output.parquet.ParquetResume;
import io.varve.swath.output.parquet.PartInfo;
import io.varve.swath.runtime.ArgsHashFields;
import io.varve.swath.runtime.CancelSource;
import io.varve.swath.runtime.CancellationToken;
import io.varve.swath.runtime.DeadlineCanceller;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.LivenessWatchdog;
import io.varve.swath.runtime.OutputPublisher;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.runtime.SignalHandlers;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.SortDiskGuard;
import io.varve.swath.sort.SortMode;
import io.varve.swath.store.FirstRequestMarkerFetcher;
import io.varve.swath.store.PageFetcher;
import io.varve.swath.store.s3.S3ClientFactory;
import io.varve.swath.store.s3.S3Config;
import io.varve.swath.store.s3.S3PageFetcher;
import io.varve.swath.store.s3.S3PageFetcherConfig;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * {@code swath list <s3-uri>} — list a bucket or prefix to a text or Parquet sink, driven by the
 * {@code WorkStealingScan} engine behind {@link ListRunner}. Its options are grouped by concern
 * into picocli {@code @ArgGroup} collaborators ({@link OutputOptions}, {@link ConnectionOptions},
 * {@link FilterOptions}, {@link SortOptions}, {@link CheckpointOptions}, {@link EngineOptions},
 * {@link MetricsOptions}, {@link LivenessOptions}); this command owns only the run orchestration —
 * flag resolution, the checkpoint/resume spine, and the terminal disposition.
 */
@Command(name = "list", description = "List a bucket or prefix.", mixinStandardHelpOptions = true)
public final class ListCommand implements Callable<Integer>, GlobalOptions.Carrier {

    private static final Logger log = LoggerFactory.getLogger(ListCommand.class);

    /** The engine {@code runWithCheckpoint} always drives — the only strategy its early-exit paths see. */
    private static final String STRATEGY_WORK_STEALING = "WORK_STEALING";

    /**
     * The {@code --sort} staging directory, inside the output dir (contract §6). Visible
     * (not a dot-dir) — output-layout redesign: a mid-sort run must be observably distinguishable
     * from a fresh dir with a plain {@code ls}, not just via a hidden {@code .swath-sort-segments}.
     */
    static final String SORT_STAGING_DIR = "_staging";

    /** Per-writer submission-queue depth (in batches) for {@link ListRunner.ParquetSpec#writerQueueCapacity()}. */
    private static final int PARQUET_WRITER_QUEUE_CAPACITY = 64;

    @Spec
    CommandSpec spec;

    @Parameters(index = "0", arity = "0..1", paramLabel = "<s3-uri>",
            description = "Source URI, e.g. s3://bucket/prefix (required unless requesting tune help)")
    String uri;

    @ArgGroup(exclusive = false, validate = false, heading = "%nOutput:%n")
    final OutputOptions output = new OutputOptions();

    @ArgGroup(exclusive = false, validate = false, heading = "%nConnection:%n")
    final ConnectionOptions connection = new ConnectionOptions();

    @ArgGroup(exclusive = false, validate = false, heading = "%nFilters:%n")
    final FilterOptions filters = new FilterOptions();

    SortOptions sorting = new SortOptions();

    LivenessOptions liveness = new LivenessOptions();

    @ArgGroup(exclusive = false, validate = false, heading = "%nRun control:%n")
    final ListOptionGroups.RunControl runControl =
            new ListOptionGroups.RunControl(sorting, liveness);

    @ArgGroup(exclusive = false, validate = false, heading = "%nResume:%n")
    final CheckpointOptions checkpoint = new CheckpointOptions();

    EngineOptions engine = new EngineOptions();

    MetricsOptions otlp = new MetricsOptions();

    TuneOptions tune = new TuneOptions();

    GlobalOptions global = new GlobalOptions();

    @ArgGroup(exclusive = false, validate = false, heading = "%nDiagnostics:%n")
    final ListOptionGroups.Diagnostics diagnostics =
            new ListOptionGroups.Diagnostics(engine, otlp, tune, global);

    @Override
    public GlobalOptions globalOptions() {
        return diagnostics.global;
    }

    /** Rebind facade fields after picocli materializes an {@link ArgGroup} match object. */
    void syncArgGroups() {
        sorting = runControl.sorting;
        liveness = runControl.liveness;
        engine = diagnostics.engine;
        otlp = diagnostics.metrics;
        tune = diagnostics.tune;
        global = diagnostics.global;
    }

    /**
     * Test-only seam (package-private, null in production): when set, {@link #runWithCheckpoint}'s
     * seed probe AND engine use this {@link io.varve.swath.store.PageFetcher} instead of a real
     * {@link S3PageFetcher}, so an end-to-end RES test can inject a throttling/clean fetcher to drive
     * a seed-time STUCK through the full {@code call()} flow without S3. The real S3
     * client is still opened (offline-cheap) so the production path is unchanged.
     */
    PageFetcher fetcherOverride;

    /**
     * Test-only seam (package-private, null in production): when set, {@code call()} uses this
     * instead of a real {@link TerminalCapabilities} — no real controlling terminal is available
     * under a build, so a test injects a fixed stdout/stderr tty pair to exercise {@code --format
     * auto} deterministically (the JDK-25 per-fd isatty fix).
     */
    TerminalCapabilities terminalOverride;

    /** Resolved once at the top of {@link #call()} from {@link #terminalOverride}/{@link
     * TerminalCapabilities} -- threaded into {@link LivenessOptions#startProgressReporter} so the
     * swath-core {@link io.varve.swath.observability.RunProgressReporter} never calls {@code
     * System.console()} itself (core has no ambient terminal knowledge; the CLI is the one layer
     * that does). */
    private boolean stdoutIsTerminal;

    /**
     * Test-only seam (package-private, null in production): when set, {@link
     * #checkSortDiskHeadroom} uses this instead of a real {@code Files.getFileStore(dir)
     * .getUsableSpace()} query — so a test can fake a tiny/generous usable-space value without
     * actually filling a disk. Takes the resolved output dir and returns bytes (negative ⇒ unknown,
     * never trips a refusal — mirrors {@link io.varve.swath.sort.SortDiskGuard}'s own convention).
     */
    ToLongFunction<Path> usableFreeBytesOverride;

    /**
     * Test-only seam (package-private, null in production): when set, the transient-retry
     * loops on BOTH the seed's {@code TransientRetryFetcher} and the engine's {@code GaugedFetcher} use
     * this injected backoff sleeper instead of {@link Thread#sleep(long)}, so a storm-ride-out scenario
     * runs in milliseconds instead of tens of real seconds. Mirrors {@link #fetcherOverride}.
     */
    TransientRetryFetcher.Sleeper backoffSleeperOverride;

    /**
     * Internal {@link ResumeCommand} seam: non-null means checkpoint {@code output_format} parsing
     * is deferred until after the recorded-destination refusal inside {@link #runWithCheckpoint}.
     */
    Path resumeCommandCheckpoint;

    /**
     * Internal {@link ResumeCommand} seam: the {@code <dir>} run handle a {@code swath resume <dir>}
     * was invoked with. That directory IS the output dataset, so an absolute checkpoint-recorded
     * destination must be it (see {@link #recordedDestinationRefusal}) and any other recording is
     * bound to it before the run writes.
     */
    Path resumeCommandRunHandle;

    /** Test-only seam for the recorded {@code .parquet} directory disambiguation probe. */
    DirectoryProbe recordedDestinationDirectoryProbeOverride;

    /**
     * The resolved transient-retry policy, set once in {@link #call()} right after the
     * {@link LivenessWatchdog} is armed — {@link io.varve.swath.engine.RetryPolicy#RIDE_OUT} when a real
     * watchdog is armed (it owns storm death), {@link io.varve.swath.engine.RetryPolicy#BOUNDED} when
     * {@code arm()} returned the no-op (both windows disabled), so cap exhaustion still yields a bounded
     * resumable STUCK rather than an unbounded ride-out with no backstop.
     */
    private RetryPolicy retryPolicy = RetryPolicy.RIDE_OUT;

    /** Bundle the resolved retry policy + (test-only) backoff sleeper for the engine seam. */
    private RetryConfig retryConfig() {
        return new RetryConfig(retryPolicy, backoffSleeperOverride);
    }

    @Override
    public Integer call() throws Exception {
        syncArgGroups();
        PrintWriter commandOut = spec != null
                ? spec.commandLine().getOut() : new PrintWriter(System.out, true);
        if (tune.apply(output, engine, sorting, commandOut)) {
            return ExitCodes.SUCCESS;
        }
        int verbosity = spec != null
                ? GlobalOptions.effectiveVerbosity(spec.commandLine()) : global.verbosity.length;
        if (verbosity > 0) {
            PrintWriter commandErr = spec != null
                    ? spec.commandLine().getErr() : new PrintWriter(System.err, true);
            commandErr.println("tune effective: " + tune.effectiveConfiguration(
                    liveness.resolveEffectiveProgressInterval().toString()));
            commandErr.flush();
        }
        if (uri == null) {
            throw new InvalidArgsException("missing required parameter: '<s3-uri>'");
        }
        if (checkpoint.resume && checkpoint.restart) {
            throw new InvalidArgsException("swath resume and --restart are mutually exclusive");
        }
        if (checkpoint.resume && checkpoint.overwrite) {
            throw new InvalidArgsException("swath resume and --overwrite are mutually exclusive");
        }
        // CheckpointMode.parse is pure (no I/O, doesn't
        // depend on anything resolved below) — validate the --checkpoint none + swath resume/--sort
        // mutual constraints THIS EARLY, before the MeterRegistry is constructed (an eager
        // OtlpMeterRegistry starts a background export thread + registers a JVM shutdown hook) or
        // any watchdog/cancellation machinery is armed, so a doomed combination never pays for any
        // of that setup.
        CheckpointOptions.CheckpointMode mode = checkpoint.mode();
        if (mode.isNone()) {
            if (checkpoint.resume) {
                throw new InvalidArgsException(
                        "swath resume needs a checkpoint; --checkpoint none keeps no durable state");
            }
            if (sorting.sort) {
                throw new InvalidArgsException("--sort needs a checkpoint (--checkpoint auto or a "
                        + "path); --checkpoint none keeps no durable state to track sort segments");
            }
        }
        validateSummaryJsonFlags();
        engine.toggles = engine.resolveToggles();   // exit 2 on a bad/unknown/contradictory toggle
        connection.requestPayerEnabled = connection.resolveRequestPayer();   // exit 2 on a value other than "requester"
        connection.maxParallelListings = ConnectionOptions.resolveMaxParallelListings(connection.maxParallelListings);   // exit 2 on T < 1
        connection.rateLimitApi = ConnectionOptions.resolveRateLimitApi(connection.rateLimitApi);   // exit 2 on a negative rate; 0/unset => no cap
        Duration timebox = liveness.resolveMaxDuration();   // validated early (exit 2 on a bad value)
        Duration stallWindow = liveness.resolveStallTimeout();   // liveness watchdog window (exit 2 on a bad value)
        Duration noProgressWindow = liveness.resolveNoProgressTimeout();   // zero-real-progress backstop (exit 2 on a bad value)
        Duration metricsIntervalDuration = otlp.resolveMetricsInterval();   // null defers to SWATH_OTLP_INTERVAL
        S3Uri s3uri = S3Uri.parse(uri);
        if (s3uri.bucket().endsWith("--x-s3")) {
            throw new UnsupportedBucketException(
                    "S3 directory bucket '" + SafeInput.logText(s3uri.bucket())
                            + "' is not supported: swath requires the lexicographic listing contract of "
                            + "a general-purpose S3 bucket; use a general-purpose bucket or wait for "
                            + "opaque-token sequential listing support");
        }
        // Validate before metrics construction, checkpoint creation, S3 client construction, or
        // any fetch. Re-assign the canonical URI text so args_hash, run_meta.endpoint and
        // identity_spec all persist only a value that passed the central credential-safe policy.
        if (connection.endpointUrl != null) {
            connection.endpointUrl = SafeInput
                    .endpoint("--endpoint-url", connection.endpointUrl).toString();
        }
        // The JDK-25 per-fd isatty fix — System.console() != null conflates
        // stdin AND stdout (null when EITHER is non-tty), so a real terminal on stdout with piped
        // stdin was wrongly classified non-terminal. TerminalCapabilities probes fd 1 independently.
        TerminalCapabilities terminal = terminalOverride != null ? terminalOverride : new TerminalCapabilities();
        stdoutIsTerminal = terminal.stdoutIsTerminal();
        boolean deferredResumeFormat = resumeCommandCheckpoint != null;
        OutputOptions.Resolved resolvedOutput = deferredResumeFormat
                ? output.resolveDeferredResumeOutput(stdoutIsTerminal)
                : output.resolveOutput(stdoutIsTerminal);   // exit 2 on a bad --format/-o combination
        OutputFormat resolved = resolvedOutput.format();
        // Refuse fresh/restart creation before any checkpoint is opened. A genuine swath resume is
        // checked after context restore instead, so hand-made or foreign checkpoints get the same
        // refusal against their recorded final destination.
        if (!checkpoint.resume) {
            String ephemeralSinkCheckpointRefusal =
                    checkpointRefusalForEphemeralSink(resolvedOutput.kind(), mode, resolved);
            if (ephemeralSinkCheckpointRefusal != null) {
                throw new InvalidArgsException(ephemeralSinkCheckpointRefusal);
            }
        }
        // exit 2 on --sort with a non-parquet EFFECTIVE format — for every case except a bare
        // swath resume with no explicit --format, BEFORE any checkpoint is opened.
        validateSortFlags(resolved);
        // Directory-lifecycle safety: before a FRESH (non-resume) directory-dataset run touches
        // the -o dir at all — no checkpoint row, no seed probe, no file write — refuse a FOREIGN
        // non-empty dir or a DAMAGED manifest so we never write into / delete unowned files. A dir
        // that holds a VALID swath dataset falls through to openRun's checkpoint-status gate
        // (unfinished/completed). Only a parquet DIRECTORY destination has this dataset layout.
        if (!checkpoint.resume && resolved == OutputFormat.PARQUET
                && resolvedOutput.kind() == OutputOptions.DestinationKind.DIRECTORY) {
            DatasetDirGuard.guardFreshRunDatasetDir(Path.of(output.destination),
                    checkpoint.overwrite, checkpoint.restart);
        }
        // System.err (not spec.commandLine().getErr()): many unit tests construct ListCommand
        // directly and call call() without ever going through picocli parsing, leaving @Spec spec
        // null -- exactly like openSink()'s raw FileDescriptor.out, this stays independent of picocli.
        // -q consults the MERGED root+leaf quiet: a root-level `swath -q list …`
        // must suppress this too, not just a leaf `swath list -q …` (GlobalOptions.effectiveQuietLevel
        // degrades to the leaf's own mixin when spec is null -- direct-construction unit tests).
        boolean quiet = spec != null
                ? GlobalOptions.effectiveQuietLevel(spec.commandLine()) > 0 : global.quiet.length > 0;
        if (!deferredResumeFormat) {
            output.echoResolvedOutput(resolvedOutput, System.err, quiet);   // never silent
        }

        // On the checkpoint/resume path S3Config (incl. region resolution) is built INSIDE
        // runWithCheckpoint, AFTER the checkpoint is opened and the persisted run-context (region/
        // profile/no-sign/output) is restored — so a bare resume from a DB with a stored region and
        // no --region/--endpoint-url/env region resolves region from the checkpoint.
        // The non-checkpoint path never resumes, so it resolves config eagerly below.
        int pageMax = 1000;   // S3 page cap
        // I11: the queue cap is in ENTRIES — the channel admits a page while in-flight
        // entry count < cap. (The old `queueSizeEntries / pageMax` made it batch slots,
        // so e.g. `--object-listing-queue-size 1` still queued a full 1000-entry page.)
        int channelCapacity = ConnectionOptions.resolveQueueEntries(connection.queueSizeEntries);
        var filterChain = filters.chain();

        String argsHash = ArgsHashFields.forListing(
                "s3", connection.endpointUrl == null ? "" : connection.endpointUrl, s3uri.bucket(), s3uri.prefixAsString()).hash();

        // Per-RUN OTLP resource attributes (one value per process) — intentionally NOT per-meter
        // Micrometer tags; bucket cardinality stays out of the meter dimension (see
        // docs/metrics-and-observability.md).
        var runResourceAttrs = new LinkedHashMap<String, String>();
        runResourceAttrs.put("bucket", s3uri.bucket());
        runResourceAttrs.put("prefix", s3uri.prefixAsString());
        // CLI flags (when passed) override SWATH_OTLP_ENDPOINT/SWATH_OTLP_INTERVAL; unset
        // flags fall back to the exact env-only resolution fromEnv always used (no behavior change
        // for a caller that leaves the endpoint/no-metrics controls unset).
        // Also exit 2, fail-fast, on a malformed explicit endpoint URL.
        MeterRegistry meterRegistry = MeterRegistries
                .fromCliConfig(otlp.resolvedExportMode(), otlp.metricsEndpoint,
                        metricsIntervalDuration, runResourceAttrs);
        RunContext ctx = RunContext.create(meterRegistry);
        // The operator-facing end-of-run block, rendered from the same terminal RunSummary the
        // --report JSON and the -v log line are built from (§ RunSummarySink). System.err for the
        // same reason the destination echo uses it: a directly-constructed ListCommand has no
        // picocli spec. Cost is withheld under --endpoint-url, where the provider's LIST pricing
        // is unknowable.
        ctx.metrics().setSummarySink(new SummaryRenderer(System.err, new SummaryRenderer.Preferences(
                output.stats, quiet, resolvedOutput.kind() != OutputOptions.DestinationKind.STDOUT,
                connection.endpointUrl == null, resumeDestination(resolvedOutput, mode))));

        // A SIGTERM/SIGINT flips the cancellation flag (stop_reason=signal); the --max-duration
        // deadline flips it with stop_reason=max_duration. Either way the run thread observes the
        // cancel, unwinds through the pipeline's try/finally (writing the terminal summary + leaving
        // the checkpoint resumable), then the process exits: a timebox stop is a resumable exit 124,
        // a signal exits 143 (SIGTERM) or 130 (SIGINT).
        SignalHandlers.register(ctx.cancellation());
        // The liveness watchdog is armed as a sibling of the --max-duration deadline: both are
        // cooperative and set the same token (first-writer-wins reason), but the watchdog additionally
        // escalates to interrupt + Runtime.halt if a wedged worker never unwinds. On by default; a
        // --idle-timeout 0/none/off resolves stallWindow to ZERO, which arm() treats as disarmed; the
        // --no-progress-timeout backstop (10m default) is armed alongside it and bounds an
        // active-but-zero-real-progress run (e.g. a permanent 503/5xx endpoint) that --idle-timeout,
        // re-armed by the retry activity itself, would never trip.
        try (DeadlineCanceller ignored = DeadlineCanceller.arm(ctx.cancellation(), timebox);
             LivenessWatchdog watchdog = LivenessWatchdog.arm(
                     ctx.cancellation(), ctx.metrics(), stallWindow, noProgressWindow, ExitCodes.STUCK)) {
            // Pick the transient-retry policy from whether a REAL watchdog poller is
            // armed. If both windows are disabled arm() returned the no-op — nothing could ever end a
            // ride-out, so cap exhaustion must stay BOUNDED (resumable STUCK) instead of looping forever.
            retryPolicy = watchdog.isArmed()
                    ? RetryPolicy.RIDE_OUT
                    : RetryPolicy.BOUNDED;
            // mode + its swath resume/--sort refusals were already validated above, before ctx/the
            // MeterRegistry/the watchdogs were ever constructed.
            return runWithCheckpoint(s3uri, resolved, channelCapacity, pageMax, filterChain, ctx,
                    argsHash, mode, quiet);
        } catch (CancelledException e) {
            return timeboxExitOrRethrow(ctx.cancellation(), ctx.metrics(), e, argsHash);
        } finally {
            SignalHandlers.unregister(ctx.cancellation());
        }
    }

    /**
     * Map a terminal {@link CancelledException} to an exit code. A {@code --max-duration} timebox
     * stop ({@link StopReason#MAX_DURATION}) is an <b>expected, resumable partial</b> → exit 124 (a
     * Batch/orchestrator must not treat it as failure, and 124 keeps it distinct from a clean,
     * complete exit 0; {@code stop_reason=max_duration} in the sidecar records the same fact). A
     * SIGTERM/SIGINT graceful stop ({@link StopReason#SIGNAL}) maps to a distinct code by its
     * attributed {@link io.varve.swath.runtime.CancelSource} — SIGTERM → {@link ExitCodes#SIGTERM}
     * (143), SIGINT (or a legacy source-less signal cancel) → {@link ExitCodes#SIGINT} (130). Only a
     * truly unattributed cancel ({@code stopReason()==null}) re-throws, so
     * {@link CancelledException#exitCode()} (130) stands.
     *
     * <p>A {@link StopReason#STUCK} watchdog abort is neither: it maps to the dedicated
     * {@link ExitCodes#STUCK} (75), NOT the signal 130, so an external runner's classifier reads a
     * {@code stop_reason=stuck} marker + a distinct code rather than mistaking a wedge for a user
     * Ctrl-C. (If the JVM was already {@code Runtime.halt}-ed by the watchdog's terminal ladder step,
     * this branch never runs — halt exits directly; this handles the case where the cooperative cancel
     * DID unwind in time.)
     *
     * <p>The {@code list_stuck_stop} marker also carries an {@code error_class=} token,
     * so the external classifier reads {@code stuck_api_timeouts} (an attempt-timeout storm) /
     * {@code stuck_throttle} / {@code stuck_unknown} instead of collapsing every wedge to
     * {@code unknown}. The source PICKS which classification is honest —
     * {@link RunMetrics#classifyStuckErrorClass()} (the run-wide since-last-real-progress window) for
     * {@code LIVENESS_WATCHDOG} (its own trip condition IS a global freeze, so a global window is
     * sound), vs {@link RunMetrics#transientRetryCapErrorClass()} (one fetch's own local fault
     * history, recorded by the cap-tripping fetch itself) for {@code TRANSIENT_RETRY_CAP} — a single
     * range's cap can exhaust while the rest of the engine stays healthy, so the run-wide window would
     * see an unrelated worker's progress and erase the wedged fetch's own timeout history.
     *
     * <p>The routing above is now the ONE {@link RunMetrics#stuckErrorClass(String)} helper —
     * the JSON run-summary sidecar (via {@code ListRunner}) reuses the exact same method, so the
     * {@code list_stuck_stop} marker here and the summary's {@code error_class} field can never
     * disagree.
     */
    static int timeboxExitOrRethrow(CancellationToken token,
                                    RunMetrics metrics,
                                    CancelledException e, String argsHash) throws CancelledException {
        if (ExitCodes.protocolViolation(e) != null) {
            // Teardown found a violated protocol on the way out (attached to this cancel as a
            // suppressed exception, since the cancel is what ended the scan). Every code below says
            // "resumable partial, come back to it", which the run no longer is, so rethrow and let
            // the exit mapping report the violation's own code.
            throw e;
        }
        if (token.stopReason() == StopReason.MAX_DURATION) {
            log.info("list_max_duration_stop args_hash={} stop_reason=max_duration exit_code=124 "
                    + "(resumable partial)", argsHash);
            return ExitCodes.TIMEBOX;
        }
        if (token.stopReason() == StopReason.STUCK) {
            // Print the HONEST cancel provenance (stop_source) instead of hardcoding the
            // liveness watchdog — a STUCK stop can come from the watchdog, either transient-retry cap,
            // or a bare seed interrupt, and the old parenthetical misattributed all of them.
            CancelSource source = token.source();
            // TRANSIENT_RETRY_CAP is a LOCAL, one-fetch disposition — classify from that
            // fetch's own recorded fault history, not the run-wide window (see this method's javadoc).
            // RunMetrics#stuckErrorClass is the ONE source-routed derivation, also used by the JSON
            // run-summary sidecar (ListRunner), so the marker and the summary can never disagree.
            String errorClass = metrics.stuckErrorClass(source == null ? null : source.tag());
            log.error("list_stuck_stop args_hash={} stop_reason=stuck stop_source={} error_class={} "
                    + "exit_code={} (cooperative cancel unwound to a resumable stuck partial)",
                    argsHash, source != null ? source.tag() : "unknown",
                    errorClass, ExitCodes.STUCK);
            return ExitCodes.STUCK;
        }
        if (token.stopReason() == StopReason.SIGNAL) {
            // Distinguish a supervisor SIGTERM (143) from an interactive SIGINT (130) at the one exit
            // mapping site, off the provenance the handler attributed. A source-less signal cancel
            // (the legacy cancel(SIGNAL) form) defaults to SIGINT/130. The stop_reason wire value is
            // unchanged — the distinction lives only in the exit code + the source() provenance.
            boolean sigterm = token.source() == CancelSource.SIGTERM;
            int code = sigterm ? ExitCodes.SIGTERM : ExitCodes.SIGINT;
            // A structured operator line, symmetric with the max_duration/stuck branches — the run
            // stopped gracefully into a resumable partial, not a crash.
            log.info("list_signal_stop args_hash={} stop_reason=signal stop_source={} exit_code={} "
                    + "(resumable partial)", argsHash, sigterm ? "sigterm" : "sigint", code);
            return code;
        }
        throw e;
    }

    // ---- checkpointed (auto / explicit path): the resume spine ----------------

    private Integer runWithCheckpoint(S3Uri s3uri, OutputFormat resolved,
                                      int channelCapacity, int pageMax, FilterChain filterChain,
                                      RunContext ctx, String argsHash, CheckpointOptions.CheckpointMode mode,
                                      boolean quiet) throws Exception {
        // A run keeps no durable checkpoint (ephemeral, in-memory store) when --checkpoint none is
        // set, or when auto resolves against a non-directory destination (stdout / single-file):
        // resolve() returns null for both. auto with a directory dataset co-locates the checkpoint at
        // <dir>/.swath/checkpoint.sqlite; an explicit --checkpoint <path> is used verbatim. An
        // ephemeral store leaves every other line below (openRun, seeding, the engine dispatch)
        // unchanged, so it drives the exact same WorkStealingScan path.
        // Elapsed-since-start reference for the list_first_request_issued/
        // list_first_page_returned markers below (FirstRequestMarkerFetcher) — a hang on the very
        // first LIST is otherwise invisible, since RunProgressReporter only starts once ListRunner's
        // engine dispatch is entered (well after seeding).
        long runStartedNs = System.nanoTime();
        Path dbPath = mode.resolve(output.destination, output.resolvedKind);
        boolean ephemeral = dbPath == null;
        // The start marker now carries W (max_parallel_listings) and whether every engine
        // toggle is at its supported default, alongside bucket/strategy — closing that gap.
        // The full non-default toggle breakdown still logs separately, right below,
        // exactly as before (logEngineTogglesIfNonDefault stays silent on an all-default run by
        // design — not relitigated here, just echoed as a single boolean on the start line itself).
        log.info("list_command_start bucket={} prefix={} args_hash={} checkpoint_path={} strategy={} "
                        + "max_parallel_listings={} engine_toggles_default={}",
                SafeInput.logText(s3uri.bucket()),
                SafeInput.logText(s3uri.prefixAsString()),
                argsHash, ephemeral ? "none" : dbPath,
                STRATEGY_WORK_STEALING, connection.maxParallelListings, engine.toggles.isDefault());
        // Set the strategy tag BEFORE any api call for this run — including the seed probe
        // below, which fires before WorkStealingScan's constructor would otherwise be the first
        // place to call setStrategy(). Without this, every seed-probe recordApiCall() lands on the
        // Micrometer/OTLP-exported swath.api.calls{strategy="unknown"} default-tag series (the
        // RunMetrics field's constructor default) while the engine's bulk calls land on a SEPARATE
        // swath.api.calls{strategy="WORK_STEALING"} series — two disjoint counter instances/OTLP data
        // points for the one run. summary.json's cost.api_calls stays correct (it sums every
        // strategy-tagged series via RunMetrics.counterTotal), but a consumer reading the raw
        // per-series OTLP export can latch onto just the tiny seed-probe series (typically a handful
        // of probes) and see a near-zero count for a run that made millions of real requests.
        ctx.metrics().setStrategy(STRATEGY_WORK_STEALING);
        logEngineTogglesIfNonDefault();
        if (!ephemeral && dbPath.getParent() != null) {
            Files.createDirectories(dbPath.getParent());
        }
        boolean fileSink = resolved == OutputFormat.PARQUET;
        String filterSpec = filters.spec();

        // On clean completion of a co-located run, the run-handle checkpoint is deleted below (the
        // dataset is the durable artifact; the checkpoint was only the resume ledger). An exception
        // clears the flag so a partial/refused exit never deletes it — a resumable run keeps its
        // checkpoint. Runs after the store closes (SQLite releases the file), gated on the on-disk
        // completion markers so it fires only for a genuinely COMPLETE dataset.
        boolean cleanExit = true;
        try (CheckpointStore store = ephemeral
                     ? SqliteCheckpointStore.openEphemeral(ctx.metrics())
                     : SqliteCheckpointStore.open(dbPath, ctx.metrics())) {
            // Persist the registry-driven IDENTITY fingerprint from the FULLY-RESOLVED
            // command (resolved format, resolved destination kind) so resume can refuse ANY changed
            // IDENTITY option — not just the filter/format/sort triple — naming the changed column.
            // resolvedFormat is what output_format stores (never raw --format auto/null), so the
            // string stored here matches the one recomputed on resume. Only actually persisted on a
            // fresh insert; openRun ignores a RunKey's identitySpec on a resume match.
            output.resolvedFormat = resolved;
            String identitySpec = ResumeRegistry.identitySpec(this);
            RunKey key = new RunKey(
                    "s3", connection.endpointUrl, s3uri.bucket(), s3uri.prefix(), argsHash,
                    "auto", ListingMode.OBJECTS, filterSpec, resolved.name(),
                    new SoftRestoreContext(
                            connection.noSignRequest, connection.profile, connection.region, connection.fetchOwner,
                            output.rawOutput, output.destination, connection.requestPayerEnabled,
                            output.resolvedKind.name(), output.outputType),
                    sorting.sort, identitySpec);
            RunMeta run = store.openRun(
                    key, checkpoint.resume, checkpoint.restart, checkpoint.overwrite);

            if (run.resumed()) {
                // Classify the checkpoint's RECORDED destination before restoreString can honor an
                // invocation-level -o (especially explicit "-"). Destination kind was not stored,
                // so a .parquet path is safe only when the path is presently a real directory.
                // Refuse immediately: malformed format metadata, context restore, destination echo,
                // and config construction are all intentionally downstream of this safety gate.
                String recordedDestinationRefusal = recordedDestinationRefusal(run.context().outputPath());
                if (recordedDestinationRefusal != null) {
                    throw new InvalidArgsException(recordedDestinationRefusal);
                }
                OutputFormat recordedOutputFormat = recordedOutputFormat(run);
                // Only a genuinely bare resume may learn -o from SoftRestoreContext below. Preserve
                // an explicit invocation's already-resolved kind (especially --output-type file):
                // the checkpoint records the path but not that explicit kind override.
                boolean destinationWasExplicit = output.destination != null;
                // A bare `swath resume` must not default (TTY-based) to an output format that
                // differs from what was checkpointed and get refused for a "change" the user
                // never asked for — restore the stored format first (only when the user did
                // not pass --format explicitly), THEN run the real resume-safety check below.
                if (resumeCommandCheckpoint != null) {
                    // ResumeCommand deliberately did not parse this metadata before entry. Mark it
                    // explicit now so its reconstructed invocation has the same semantics as before,
                    // but only after the recorded FILE-origin gate has won the ordering decision.
                    output.format = recordedOutputFormat;
                    resolved = recordedOutputFormat;
                    fileSink = resolved == OutputFormat.PARQUET;
                } else if (!output.formatWasExplicitlySet() && recordedOutputFormat != resolved) {
                    resolved = recordedOutputFormat;
                    fileSink = resolved == OutputFormat.PARQUET;
                }
                // Restore the run's non-args_hash context (auth/region/output) for
                // whatever the caller did not explicitly (re-)supply, BEFORE buildConfig() below —
                // so a bare resume from a DB with a stored region (and no --region/--endpoint-url/env
                // region) resolves region from the checkpoint instead of failing region resolution.
                // Region is deliberately excluded from args_hash.
                restoreRunContext(run);
                OutputOptions.DestinationKind preRestoreKind = output.resolvedKind;
                // A bare swath resume with no explicit -o only learns its destination here. Valid
                // checkpoints restore a directory dataset, including a recognized .parquet path
                // created with --output-type dir. Recognized text FILE checkpoints and ambiguous
                // non-directory .parquet paths were identified from the recorded value above.
                // Explicit destinations retain resolveOutput's kind so an explicit FILE request is
                // refused below.
                if (!destinationWasExplicit) {
                    output.recomputeKindAfterRestore();
                } else if (resumeCommandCheckpoint != null) {
                    // The preliminary deferred-resume pass resolved kind only. Now that the stored
                    // format is known-valid, apply the normal format/extension conflict rules too.
                    OutputOptions.Resolved fullyResolved = output.resolveOutput(stdoutIsTerminal);
                    resolved = fullyResolved.format();
                    fileSink = resolved == OutputFormat.PARQUET;
                }
                if (resumeCommandCheckpoint != null
                        || (!destinationWasExplicit
                                && preRestoreKind == OutputOptions.DestinationKind.STDOUT
                                && output.resolvedKind != OutputOptions.DestinationKind.STDOUT)) {
                    output.echoResolvedOutput(new OutputOptions.Resolved(resolved, output.resolvedKind),
                            System.err, quiet);
                }
            }
            // Build config AFTER any resume-context restore (region/profile/no-sign), and BEFORE the
            // resume-safety summary + the S3Client below are built from it.
            S3Config config = connection.buildConfig();
            if (run.resumed()) {
                // Resume-safety: registry-driven IDENTITY enforcement. restoreRunContext
                // (cli-wins) ran above, so a bare/unpassed IDENTITY value was restored to the stored
                // value and matches; an explicitly-different one survived and mismatches. Comparing the
                // whole identity_spec — not just the filter/format/sort triple — closes two
                // previously-silent-honour gaps: --fetch-owner and the destination path.
                // args_hash's own refusal (openRun) already fired for a changed scheme/endpoint/bucket/
                // prefix; the redundancy is deliberate.
                //
                // Compared HERE, ahead of every refusal below, because the fingerprint must reproduce
                // the RECORDED destination spelling and the binding right below replaces it; the
                // refusal itself keeps its place in the refusal order.
                List<String> changedIdentity = List.of();
                if (run.identitySpec() != null) {
                    output.resolvedFormat = resolved;   // keep the format token on the effective format
                    changedIdentity = ResumeRegistry.changedColumns(
                            run.identitySpec(), ResumeRegistry.identitySpec(this));
                }
                // A RELATIVE recorded destination only ever meant anything against the original run's
                // working directory, which the resuming process need not share, so bind the run handle
                // itself before anything writes -- left as the raw string, the dataset, its markers
                // and the summary would land under whatever that spelling means to the RESUMING shell,
                // outside the directory the operator named. This is also what lets the guard above
                // admit a relative recording without comparing it: the write lands in <dir> either
                // way. Bound before the refusals below and not after them: each of them writes an
                // early-exit summary, and a Parquet run's report defaults to a file INSIDE the
                // destination, so a REFUSED resume would otherwise create or replace one outside the
                // run handle on its way out.
                if (resumeCommandRunHandle != null && output.destination != null
                        && !"-".equals(output.destination)
                        && absoluteRecordedDestination(output.destination) == null) {
                    output.destination = resumeCommandRunHandle.toString();
                }
                // Defensive backstop for hand-made/foreign checkpoints. FILE-kind output is never
                // resumable, regardless of checkpoint status: COMPLETED can precede a failed final
                // publication, so treating it as a harmless no-op would report false success.
                if (output.resolvedKind == OutputOptions.DestinationKind.FILE) {
                    writeEarlyExitSummary(resolved, config, argsHash, ctx, run.id(),
                            false, StopReason.RESUME_REFUSED, STRATEGY_WORK_STEALING);
                    throw new InvalidArgsException("swath resume refused: checkpoint destination -o "
                            + output.destination + " is recorded as FILE kind, which is non-resumable; "
                            + "start a fresh FILE run with --checkpoint none, or use a directory-dataset "
                            + "destination for checkpointed output" + fileKindPhysicalLayoutNote(resolved));
                }
                // A run recorded FAILED with the fatal-error flag set (a deterministic
                // in-process failure — e.g. a fatal ListingException/OutputException/
                // CheckpointException, or a fatal seed-probe failure) must not be blindly
                // re-attempted — it would just re-fail the same way. Refuse (like every other
                // resume-safety refusal below); --restart discards the prior run and starts fresh.
                //
                // Deliberately NARROWER than "status==FAILED": a broken-pipe truncation also marks
                // FAILED directly (ListRunner, WITHOUT this flag) precisely so it stays normally
                // resumable (INT-12) — `swath resume` continuing a truncated-stdout listing is the
                // whole point of that path, not a re-attempt of a deterministic failure. Only a row
                // the CLI's fatal-error guard itself marked (runEngineGuarded / the seed-probe catch
                // below) carries fatalError()==true.
                if (run.status() == RunStatus.FAILED && run.fatalError()) {
                    writeEarlyExitSummary(resolved, config, argsHash, ctx, run.id(),
                            false, StopReason.RESUME_REFUSED, STRATEGY_WORK_STEALING);
                    throw new InvalidArgsException("swath resume refused: run " + run.id() + " for "
                            + s3uri.bucket() + "/" + s3uri.prefixAsString() + " is recorded FAILED "
                            + "(a prior fatal error, not a graceful stop) — investigate before "
                            + "retrying (see its summary.json/logs), or use --restart to discard it "
                            + "and start fresh");
                }
                // The identity refusal, in its settled place after the FILE-kind and fatal-run ones.
                // It names the changed column(s) — a UX gain over the old "the filter, output format,
                // or --sort/--no-sort mode changed", which made the user guess.
                if (!changedIdentity.isEmpty()) {
                    // Even a pre-run refusal writes a summary so a consumer always finds one.
                    writeEarlyExitSummary(resolved, config, argsHash, ctx, run.id(),
                            false, StopReason.RESUME_REFUSED, STRATEGY_WORK_STEALING);
                    throw new InvalidArgsException("swath resume refused: "
                            + String.join(", ", changedIdentity)
                            + " changed since the checkpointed run; use --restart to discard it");
                }
                // A --sort run whose checkpoint carries staging segments tagged with an older
                // (or otherwise-unsupported) staging format must REFUSE, not silently
                // sweep+relist. The reattach path (ListRunner.runToSortedParquetWorkStealing) selects
                // segments by SORT_SEGMENT_FORMAT ("page-run"); a stale "parquet-segment" row is
                // invisible to that filter, so its finalized segment would be swept as "non-finalized"
                // and its data re-listed — wrong/duplicated work, potential loss. Refuse exactly like
                // the --sort/--no-sort mismatch above. (Past the mismatch guard, sort==run.sortEnabled().)
                // TODO(future hardening): the page-run FORMAT_VERSION is NOT recorded in the
                // checkpoint (part_file.format stores only the format string), so a future page-run
                // version bump cannot be refused here on the recorded metadata alone — it is caught
                // later/loudly by PageRunSegmentReader's version check when a segment is opened. Record
                // the format version in the checkpoint to refuse a version mismatch as cleanly as this
                // format-string mismatch.
                if (sorting.sort) {
                    List<String> staleFormats = store.finalizedPartFormats(run.id()).stream()
                            .filter(f -> !ListRunner.SORT_SEGMENT_FORMAT.equals(f))
                            .toList();
                    if (!staleFormats.isEmpty()) {
                        writeEarlyExitSummary(resolved, config, argsHash, ctx, run.id(),
                                false, StopReason.RESUME_REFUSED, STRATEGY_WORK_STEALING);
                        throw new InvalidArgsException("swath resume refused: the sort staging format "
                                + "changed since the checkpointed run (checkpoint records "
                                + staleFormats + ", this build stages '"
                                + ListRunner.SORT_SEGMENT_FORMAT + "') — the old staging segments "
                                + "cannot be safely reused; use --restart to discard the run and "
                                + "start fresh");
                    }
                }
            }

            // Effective-format guard: --sort applies to parquet only. Only a genuine swath resume
            // with no explicit --format can still reach here with a non-parquet resolved format —
            // validateSortFlags already rejected every other case before the checkpoint was even
            // opened; this is the post-restore re-check that case needs, since
            // the effective format wasn't known until the stored format was restored above.
            if (sorting.sort && !fileSink) {
                throw new InvalidArgsException("--sort requires --format parquet (a sorted text sink "
                        + "is deferred); resolved format is " + resolved.name().toLowerCase(Locale.ROOT));
            }

            // Fail fast BEFORE the S3Client below is even constructed —
            // and thus strictly before seedFreshRun's first S3 probe / checkpoint node insert — so
            // its "refused before any listing began" marker (checkSortDiskHeadroom javadoc) is
            // actually true for a FRESH run, not just a resume. checkSortDiskHeadroom depends only
            // on forceSort/output/the physical staging dir on disk, never on the store/run/fetcher,
            // so moving it here (from its old post-seed spot, still guarded by `if (sort)`) is safe.
            if (sorting.sort) {
                checkSortDiskHeadroom();
            }

            // The fetcher is created once and shared by the seed probe and the engine, so a
            // fresh run pays a single S3 client setup. Resume reloads existing nodes (it never
            // re-seeds); a fresh run seeds the worklist before loading the resumable set.
            try (S3Client s3 = S3ClientFactory.create(config, ctx.metrics())) {
                PageFetcher rawFetcher = fetcherOverride != null
                        ? fetcherOverride
                        : connection.maybeRateLimited(new S3PageFetcher(s3, s3uri.bucket(),
                                new S3PageFetcherConfig(connection.fetchOwner, connection.requestPayerEnabled,
                                        ctx.metrics(), config.probeApiCallAttemptTimeout(),
                                        // The SAME value the client above was built with -- the fetcher
                                        // needs it as the escalation-rescale base, not as an override.
                                        config.apiCallAttemptTimeout())),
                                ctx.metrics());
                // Wraps whichever call — seed probe or engine — issues this run's first real
                // request, so a hang there logs list_first_request_issued/list_first_page_returned
                // instead of nothing.
                PageFetcher fetcher =
                        new FirstRequestMarkerFetcher(rawFetcher, runStartedNs);
                // Re-seed a NEVER-SEEDED run on resume. A fresh run seeds; a normal
                // resume does NOT. But a fresh run whose seed aborted STUCK (a seed-time transient
                // storm) committed ZERO nodes (I2, all-or-nothing) and left the run
                // RUNNING (not fatal-FAILED), so a later swath resume passes the fatal-refusal gate above
                // with an EMPTY worklist. Without re-seeding, the non-sort path below would then write
                // a FALSE `completed` (nodes.isEmpty()) and the --sort path would publish an EMPTY
                // sorted dataset — violating the rule that a seed failure must never resume as
                // complete). A zero-node COUNT uniquely identifies that never-seeded state (a
                // genuinely-complete run has nodes, all COMPLETED) and re-seeding is safe (the STUCK
                // seed left nothing to double). This runs BEFORE loadResumable and BOTH output-mode
                // "empty ⇒ done" determinations, so a re-seeded resume proceeds normally in either
                // mode; a normal partial resume (nodes present) is unaffected.
                if (!run.resumed() || store.countNodes(run.id()) == 0) {
                    // A zero-progress heartbeat for the seed window specifically — the gap the
                    // issue evidenced (a hung seed produced zero log lines for hours, since
                    // RunProgressReporter only starts once ListRunner's engine dispatch is entered,
                    // well after seeding). Piggybacks on the EXISTING RunProgressReporter/--progress-
                    // interval cadence rather than new scheduling infra; on the common fast-seed path
                    // it ticks zero times (seeding finishes in milliseconds) and costs nothing beyond
                    // starting/stopping one daemon thread.
                    try (RunProgressReporter seedHeartbeat =
                                 liveness.startProgressReporter(ctx, stdoutIsTerminal)) {
                        seedFreshRun(store, run.id(), fetcher, s3uri, ctx);
                    } catch (Exception e) {
                        // A seed-time InterruptedException is a LIVENESS signal,
                        // NEVER a fatal seed failure. Two ways it arises, both RESUMABLE:
                        // (1) a CANCELLATION-DRIVEN abort — a seed-time transient storm exhausts
                        //     TransientRetryFetcher's retry cap and trips the RUN's cancellation
                        //     (StopReason.STUCK), unwinding via InterruptedException — the same
                        //     resumable exit-75 disposition the engine's GaugedFetcher uses;
                        // (2) defense-in-depth — a NON-cancellation InterruptedException (e.g. a
                        //     fetcher that maps an SDK abort to a bare InterruptedException without
                        //     tripping our cancellation) is still a transient liveness failure, not a
                        //     deterministic fatal fault. Marking EITHER fatal (below) would poison
                        //     swath resume and exit 1 instead of the intended resumable exit 75,
                        //     violating the crash-only recovery contract. So for any InterruptedException:
                        //     if the run is already cancelled, honor its StopReason (STUCK -> 75,
                        //     MAX_DURATION -> 0, SIGNAL -> 130); otherwise trip cancel(STUCK) so the
                        //     disposition is a resumable STUCK. Then convert to a CancelledException
                        //     carrier so ListCommand.call()'s catch flows it through
                        //     timeboxExitOrRethrow. A GENUINE fatal seed error (bad bucket, region
                        //     redirect, auth/config, or any non-Interrupted throwable) still marks
                        //     fatal + exits 1 exactly as below.
                        //
                        // SAFETY (a seed failure must never leave a run resume treats as
                        // complete): a STUCK seed exit is explicitly NOT `completed` (the summary here
                        // is written completed=false, a zero-progress resumable partial). The run row
                        // stays RUNNING (never fatal-FAILED) with ZERO nodes committed (I2), so a later
                        // swath resume RE-SEEDS it (the zero-node gate above) and continues — rather
                        // than the old FALSE-complete/empty-publish that skipping re-seed would cause.
                        // The summary write is best-effort so a summary-write throw can never leave the
                        // run escaping RUNNING-unseeded by an unhandled path instead of the intended
                        // resumable CancelledException disposition.
                        if (e instanceof InterruptedException) {
                            if (!ctx.cancellation().isCancelled()) {
                                // A bare seed interrupt with no prior attributed reason —
                                // attribute the seed as the source (the retry-cap BOUNDED path already
                                // attributed TRANSIENT_RETRY_CAP first, so first-writer-wins keeps that).
                                ctx.cancellation().cancel(StopReason.STUCK,
                                        CancelSource.SEED_INTERRUPT);
                            }
                            StopReason reason = ctx.cancellation().stopReason();
                            writeEarlyExitSummaryBestEffort(resolved, config, argsHash, ctx, run.id(),
                                    false, reason != null ? reason : StopReason.STUCK,
                                    STRATEGY_WORK_STEALING);
                            throw new CancelledException(
                                    "seed aborted: run cancelled (stop_reason=" + reason + ")", e);
                        }
                        // The seed probe's network failure is a real early exit — leave a
                        // summary behind (exit 1 via the rethrown listing exception). Best-effort so a
                        // summary-write throw never masks the real seed exception nor skips the
                        // fatal-mark below (the run must not escape RUNNING-unseeded un-flagged).
                        // Thread the true process exit code (ExitCodes.forThrowable walks the
                        // cause chain — a fatal ListingException maps to 1) so the seed-failure
                        // summary no longer reports exit_code:null, and the classified error_class
                        // (access_denied/unauthorized/no_such_bucket/region_redirect, null for a
                        // generic seed failure) so a supervisor can tell a permissions/config problem
                        // apart from an unclassified crash.
                        writeEarlyExitSummaryBestEffort(resolved, config, argsHash, ctx, run.id(),
                                false, StopReason.SEED_FAILURE, STRATEGY_WORK_STEALING,
                                ExitCodes.forThrowable(e), seedErrorClass(e));
                        // A fatal seed failure inserts ZERO nodes, so without this
                        // the run stays RUNNING and a later bare swath resume would find nodes.isEmpty()
                        // and report a false "nothing to resume, must be done" completed no-op (exit
                        // 0) instead of refusing a run that never actually listed anything. CAS —
                        // never downgrades an already-terminal row. Excludes exit-2 user-correctable
                        // errors (mirrors runEngineGuarded) — none are known to arise from this
                        // network probe today, but a fixable flag/config problem must never be
                        // mistaken for an unrecoverable in-process failure either way.
                        if (!(e instanceof InvalidUriException
                                || e instanceof InvalidConfigException
                                || e instanceof InvalidArgsException)) {
                            markBestEffort(store, run.id(),
                                    CheckpointStore::markRunFatalUnlessFinished);
                        }
                        throw e;
                    }
                }

                List<Node> nodes = store.loadResumable(run.id(), fileSink);

                if (sorting.sort) {
                    // The disk pre-check itself now runs much earlier (before the
                    // S3Client/seedFreshRun above) so its refusal is genuinely pre-listing; this
                    // branch just keeps sort runs out of the generic "nothing to do" early return
                    // below — an empty node set means listing is complete, but the merge/publish
                    // may still be pending (contract §6 state machine); runSortedParquet owns that.
                    try (TraceSink traceSink = engine.openTraceSink()) {
                        return runEngineGuarded(store, run.id(), () -> runSortedParquet(s3uri, channelCapacity,
                                pageMax, filterChain, ctx, argsHash, store, run, nodes, fetcher, config, traceSink));
                    }
                }

                if (nodes.isEmpty()) {
                    // A resume with nothing left genuinely finished — a completed summary.
                    writeEarlyExitSummary(resolved, config, argsHash, ctx, run.id(),
                            true, StopReason.COMPLETED, STRATEGY_WORK_STEALING);
                    return ExitCodes.SUCCESS;   // resume of an already-complete run: nothing to do
                }

                try (TraceSink traceSink = engine.openTraceSink()) {
                    if (fileSink) {
                        return runEngineGuarded(store, run.id(), () -> runEngineParquet(s3uri, channelCapacity,
                                pageMax, filterChain, ctx, argsHash, store, run, nodes, fetcher, config, traceSink));
                    }
                    ListRunner.Spec listSpec = listSpec(s3uri, resolved, channelCapacity, pageMax, filterChain, argsHash, config);
                    // run.resumed() here can only be a STDOUT destination (never FILE-kind -- that
                    // was refused above), so openSink() needs no resumed/append mode to support.
                    // FILE-kind publishes atomically inside the run — after the writer closes and
                    // before the run is recorded complete — so a completed run is never left with its
                    // output unpublished. Stdout has nothing to publish.
                    OutputPublisher publisher =
                            output.resolvedKind == OutputOptions.DestinationKind.FILE
                                    ? output::commitFileSink
                                    : OutputPublisher.NONE;
                    try {
                        try (Writer out = output.openSink()) {
                            runEngineGuarded(store, run.id(), () -> {
                                new ListRunner().runWorkStealing(ctx, fetcher, out, listSpec, store, run.id(),
                                        connection.maxParallelListings, nodes, engine.toggles, traceSink,
                                        retryConfig(), publisher);
                                return null;
                            });
                        }
                    } finally {
                        output.cleanupFileSink();
                    }
                    return ExitCodes.SUCCESS;
                }
            }
        } catch (Exception e) {
            cleanExit = false;   // a partial/refused/failed exit keeps the checkpoint resumable
            throw e;
        } finally {
            if (cleanExit) {
                DatasetDirGuard.deleteColocatedRunHandleCheckpointIfComplete(
                        dbPath, output.resolvedKind, output.destination);
            }
        }
    }

    /**
     * CLI-level fatal-error seam: every real {@code swath list} engine dispatch (text/
     * Parquet/sorted-Parquet) funnels through here so a genuinely unrecoverable listing/output/
     * checkpoint error also marks {@code run_meta.status=FAILED} with the fatal-error flag set
     * ({@link io.varve.swath.checkpoint.CheckpointStore#markRunFatalUnlessFinished}) — distinct from the
     * {@code RUNNING} a SIGKILL/interrupt or a graceful {@code --max-duration}/signal cancel
     * ({@link CancelledException}) leaves behind, and from a broken-pipe FAILED ({@link
     * ListRunner}, flag left unset — INT-12 stays normally resumable) — before rethrowing (the
     * terminal {@code summary.json} write is unaffected: {@link ListRunner}'s existing {@code
     * JsonRunSummaryWriter#close()} partial-fallback already attributes an uncaught fatal
     * exception {@code stop_reason=crash}; this only adds the {@code run_meta} bookkeeping so a
     * subsequent {@code swath resume} can refuse it). That generic mark only applies to a run
     * that has not finished, so it never rewrites a disposition the run already chose: neither a
     * durable {@code COMPLETED} (a post-completion failure inside the guarded region, already past
     * its own {@code markRunFinished(COMPLETED)}) nor the flag-unset {@code FAILED} {@link
     * ListRunner} records for a failed output publish before rethrowing through here — a publish
     * failure is typically an external, transient one (a full disk, a stalled mount) and its run
     * must stay resumable rather than force a whole-bucket {@code --restart}.
     *
     * <p>Deliberately scoped to {@link ListCommand} rather than {@link ListRunner} itself: several
     * engine-level crash-resume tests (e.g. {@code HardCrashResumeExactlyOnceTest}) inject a
     * mid-listing {@code ListingException} as an in-process stand-in for a real {@code kill -9} and
     * drive {@link ListRunner} directly, then resume with {@code openRun(..., true, false)} —
     * exactly like a real SIGKILL (which never runs any of this code) leaves the run resumable as
     * {@code RUNNING}. This seam only fires for a genuine CLI invocation through
     * {@link #runWithCheckpoint}, so those tests are unaffected.
     *
     * <p>The guarded parquet/sort bodies ({@code runEngineParquet}/{@code
     * runSortedParquet}) still run PURE CONFIG VALIDATION inside the guard ({@code
     * openParquetDir()}/{@code parquetSpec()} — e.g. {@code --format parquet} with no {@code -o}).
     * Those are user-correctable exit-2 errors ({@code InvalidUriException}/{@code
     * InvalidConfigException}/{@code InvalidArgsException}), NOT the unrecoverable in-process
     * listing/output/checkpoint failure this guard exists to flag — marking fatal-FAILED for a
     * fixable flag typo would then wrongly refuse a LATER, corrected invocation's resume. Fully
     * hoisting this validation before {@code openRun} was rejected: for a bare {@code swath resume}
     * with no explicit {@code -o}, {@code output} is only known after the post-{@code openRun}
     * checkpoint-context restore ({@code restoreRunContext}), so the check cannot always run
     * before the checkpoint is opened. These three types pass through un-marked exactly like
     * {@code CancelledException}/{@code InterruptedException} above.
     *
     * <p>All five pass through un-marked with one exception: a run that carries a protocol violation
     * is fatal whatever ended it, and is marked with the stronger {@link
     * io.varve.swath.checkpoint.CheckpointStore#markRunUnresumable} — see
     * {@link #markUnresumableIfProtocolViolation}.
     */
    static <T> T runEngineGuarded(CheckpointStore store, long runId,
                                  CheckedSupplier<T> body) throws Exception {
        try {
            return body.get();
        } catch (CancelledException | InterruptedException | InvalidUriException
                | InvalidConfigException | InvalidArgsException e) {
            markUnresumableIfProtocolViolation(store, runId, e);
            throw e;
        } catch (Exception e) {
            if (!markUnresumableIfProtocolViolation(store, runId, e)) {
                markBestEffort(store, runId,
                        CheckpointStore::markRunFatalUnlessFinished);
            }
            throw e;
        }
    }

    /**
     * A protocol violation is the one failure the run must never resume into, so a run carrying one
     * is marked unresumable even when the exception that ended the scan is otherwise resumable (a
     * cancellation/interrupt) or user-correctable (a config/args error) — left unmarked, the very
     * next {@code swath resume} would go straight back to the endpoint that violated the protocol.
     * That is also why this mark, alone, overrides a FAILED-but-resumable row: retrying the
     * violation is worse than the lost resume. Returns whether the run carried a violation, so the
     * generic branch can tell a violation it has already marked from one to mark its own way.
     *
     * <p>A violation discovered during teardown lands on the failure that was already unwinding, as a
     * suppressed exception rather than a replacement (the engine keeps whatever ended the scan as the
     * primary story), so this classification has to read the suppressed exceptions too — hence
     * {@link ExitCodes#protocolViolation}, which the exit-code mapping consults for the same reason.
     */
    private static boolean markUnresumableIfProtocolViolation(CheckpointStore store,
                                                              long runId, Throwable terminal) {
        ProtocolViolationException violation =
                ExitCodes.protocolViolation(terminal);
        if (violation == null) {
            return false;
        }
        log.error("list_protocol_violation_fatal run_id={} error_class={} stopped_by={} "
                        + "(marked FAILED: a violated protocol is never retried)",
                runId, violation.errorClass(), terminal.getClass().getSimpleName());
        markBestEffort(store, runId, CheckpointStore::markRunUnresumable);
        return true;
    }

    /**
     * Best-effort fatal mark: a secondary failure marking the run (e.g. the checkpoint DB itself is
     * what broke) is logged and swallowed, never masking the run's real, already-in-flight fatal
     * exception.
     */
    private static void markBestEffort(CheckpointStore store, long runId,
                                       FatalMark mark) {
        try {
            mark.apply(store, runId);
        } catch (Exception markEx) {
            log.warn("list_run_mark_failed_after_fatal_error_failed run_id={} message={}",
                    runId, markEx.getMessage());
        }
    }

    /** One of the {@link io.varve.swath.checkpoint.CheckpointStore} fatal marks, chosen per call site. */
    @FunctionalInterface
    interface FatalMark {
        void apply(CheckpointStore store, long runId) throws Exception;
    }

    @FunctionalInterface
    interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    interface DirectoryProbe {
        boolean isDirectory(Path path) throws Exception;
    }

    /**
     * Seed the worklist for a fresh run: one bounded {@code delimiter=/} structure probe
     * tiles {@code (⊥, null]} on the top-level common prefixes (default {@code --tune seed.mode=shallow}),
     * or a single root range ({@code --tune seed.mode=none}). Each seed range is inserted as a PENDING
     * node exactly like the old single {@code rootRange}, so claim/steal/split/resume are
     * unchanged.
     */
    private void seedFreshRun(CheckpointStore store, long runId,
                              PageFetcher fetcher, S3Uri s3uri,
                              RunContext ctx) throws Exception {
        RunMetrics metrics = ctx.metrics();
        SeedMode seedMode = engine.resolveSeedMode();
        // With SDK maxAttempts=1 the seed's up-to-~256 sequential probes call the raw
        // fetcher directly (not the engine's gauge-wrapped, retrying fetcher), so a single transient
        // 503/attempt-timeout/reset would kill a fresh run's seed on exactly the throttled buckets these
        // fixes target. Wrap it in the shared bounded, cancellation-aware transient-retry decorator.
        // Thread the resolved retry policy (RIDE_OUT vs BOUNDED) and the test-only
        // backoff-sleeper override into the seed's transient-retry loop, mirroring the engine seam. The
        // RetryConfig resolves a null override to the production Thread::sleep default.
        RetryConfig retry = retryConfig();
        PageFetcher seedFetcher = new TransientRetryFetcher(
                fetcher, ctx.cancellation(), metrics, retry.sleeper(), retry.policy());
        SeedStep seedStep = new SeedStep(
                seedFetcher, s3uri.prefix(), connection.maxParallelListings, metrics, engine.toggles);
        List<NodeSpec> specs = seedStep.seedSpecs(runId, seedMode);
        // ATOMIC: all seed nodes in ONE transaction (I2 — all-or-nothing, no partial partition).
        store.insertNodes(specs);
        log.info("list_seed run_id={} seed={} seed_count={}", runId, seedMode, specs.size());
    }

    private Integer runEngineParquet(S3Uri s3uri, int channelCapacity, int pageMax,
                                     FilterChain filterChain, RunContext ctx, String argsHash,
                                     CheckpointStore store,
                                     RunMeta run,
                                     List<Node> nodes,
                                     PageFetcher fetcher, S3Config config,
                                     TraceSink traceSink) throws Exception {
        Path dir = output.openParquetDir();
        // Sample free space on the volume that actually takes the write load (Parquet parts).
        ctx.metrics().registerDiskFreeGauge(dir);
        if (!run.resumed()) {
            DatasetDirGuard.clearDatasetForFreshRun(dir);   // a fresh run never inherits a stale dataset's markers/parts
        }
        ListRunner.ParquetSpec parquetSpec = parquetSpec(s3uri, channelCapacity, pageMax, filterChain, argsHash, config);

        // Resume reconciliation (I6): finalized parts are carried into the manifest;
        // every other part-*.parquet on disk is discarded (its rows are not durable).
        List<PartRef> finalized = store.finalizedParts(run.id());
        Set<String> finalizedNames = finalized.stream()
                .map(PartRef::path).collect(Collectors.toSet());
        if (run.resumed()) {
            ParquetResume.discardNonFinalized(dir, finalizedNames);
        }
        // The consumer manifest needs an MD5 per part. The checkpoint doesn't persist it, so a
        // resumed run recomputes the MD5 of each carried-over finalized part ONCE here (finalized
        // parts are never rewritten, so the checksum is stable) — the pool then caches it and never
        // recomputes on subsequent manifest rewrites. Parts live under <root>/data/, so the
        // stored data/-prefixed path resolves directly against the output dir.
        List<PartInfo> existing =
                new ArrayList<>(finalized.size());
        for (PartRef p : finalized) {
            String md5;
            try (InputStream in = Files.newInputStream(dir.resolve(p.path()))) {
                md5 = DigestUtils.md5Hex(in);
            }
            existing.add(new PartInfo(
                    p.path(), p.writerId(), p.rows(), p.bytes(), md5));
        }

        new ListRunner().runToParquetWorkStealing(ctx, fetcher, dir, parquetSpec, store, run.id(),
                connection.maxParallelListings, nodes, existing, engine.toggles, traceSink, retryConfig());
        return ExitCodes.SUCCESS;
    }

    /**
     * The one-shot startup half of {@link io.varve.swath.sort.SortDiskGuard} — see its class
     * javadoc for the two-check design and why this one is a plain, user-correctable
     * {@code OutputException} (exit 1) rather than anything routed through the fatal-error
     * machinery. Skipped entirely when {@code --tune sort.ignore-disk-check=on} is set. Uses whatever staging bytes are
     * ALREADY on disk (real for a {@code swath resume} of a run that previously staged segments; zero
     * for a brand-new run — the periodic runtime guard armed inside {@link #runSortedParquet} is
     * what catches a fresh giant bucket once its real trajectory becomes observable).
     *
     * <p><b>Call-site timing:</b> depends ONLY on {@code forceSort}/the output
     * dir/the physical staging directory on disk — NEVER on the checkpoint {@code store}/{@code
     * run}/{@code fetcher} — so {@code runWithCheckpoint} calls this strictly BEFORE the
     * {@code S3Client} is even constructed (and thus before {@code seedFreshRun}'s first S3 probe
     * or any checkpoint node insert), making its "refused before any listing began" marker true for
     * a fresh run too, not just a resume.
     */
    void checkSortDiskHeadroom() throws OutputException {
        if (sorting.forceSort) {
            return;
        }
        Path outputDir;
        try {
            outputDir = output.openParquetDir();
        } catch (Exception e) {
            return;   // a bad -o surfaces properly (InvalidConfigException) from the real call site below
        }
        long usableFreeBytes = usableFreeBytesOverride != null
                ? usableFreeBytesOverride.applyAsLong(outputDir)
                : usableFreeBytesReal(outputDir);
        long stagingBytesOnDisk = stagingBytesOnDisk(outputDir.resolve(SORT_STAGING_DIR));
        Optional<String> reason = SortDiskGuard
                .insufficientDiskReason(usableFreeBytes, stagingBytesOnDisk,
                        SortDiskGuard.DEFAULT_SAFETY_FACTOR,
                        SortDiskGuard.DEFAULT_MIN_FREE_BYTES);
        if (reason.isPresent()) {
            // OutputException carries no error_class field (SwathException only exposes
            // exitCode()), so — mirroring SortDiskGuard's runtime-halt marker — log the same
            // distinct error_class=sort_disk_exhausted / stop_reason=sort_disk_exhausted /
            // resumable=true classification here too, so an external supervisor greps the SAME
            // marker whether the refusal fires at startup or mid-run.
            log.error("sort_disk_precheck_refused reason=\"{}\" error_class=sort_disk_exhausted "
                    + "stop_reason=sort_disk_exhausted resumable=true exit_code=1 (refused before any "
                    + "listing began -- free up space, target a smaller prefix, increase the volume, "
                    + "or pass --tune sort.ignore-disk-check=on to proceed anyway; nothing has run yet so this is trivially "
                    + "resumable, and a swath resume of a run that previously staged real segments remains "
                    + "clean once there is enough room)", reason.get());
            throw new OutputException("--sort disk pre-check refused: " + reason.get()
                    + " (free up space, target a smaller prefix, or pass --tune sort.ignore-disk-check=on to proceed anyway "
                    + "-- see docs/usage.md's --sort sizing guidance)");
        }
    }

    private static long usableFreeBytesReal(Path dir) {
        try {
            return Files.getFileStore(dir).getUsableSpace();
        } catch (IOException | RuntimeException e) {
            return -1L;
        }
    }

    /** Sum of every regular file under {@code stagingDir} (0 if it doesn't exist yet — a fresh run). */
    private static long stagingBytesOnDisk(Path stagingDir) {
        if (!Files.isDirectory(stagingDir)) {
            return 0L;
        }
        try (Stream<Path> walk = Files.walk(stagingDir)) {
            return walk.filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0L;
                        }
                    }).sum();
        } catch (IOException e) {
            return 0L;
        }
    }

    /**
     * The {@code --sort} state-machine dispatcher (contract §6). Empty node set ⇒ listing
     * complete: if {@code manifest.json} exists AND its recorded {@code (args_hash, run_id)} both
     * match THIS checkpoint run, the run is PUBLISHED (idempotent clean-up of leftover staging + stale
     * {@code *.tmp}); otherwise the merge is pending (re-run it from staging with zero LIST fetches —
     * this also covers a manifest that exists but belongs to a DIFFERENT run, below).
     * A non-empty node set ⇒ listing incomplete (fresh or resume-mid-listing) ⇒ run the sort lane,
     * then merge + publish.
     */
    private Integer runSortedParquet(S3Uri s3uri, int channelCapacity, int pageMax,
                                     FilterChain filterChain, RunContext ctx, String argsHash,
                                     CheckpointStore store,
                                     RunMeta run,
                                     List<Node> nodes,
                                     PageFetcher fetcher, S3Config config,
                                     TraceSink traceSink) throws Exception {
        Path outputDir = output.openParquetDir();
        // Sample free space on the volume that takes the write load — sort-staging segments +
        // Parquet parts both live under outputDir (a prior incident crashed on sort-segment disk exhaustion).
        ctx.metrics().registerDiskFreeGauge(outputDir);
        if (!run.resumed()) {
            DatasetDirGuard.clearDatasetForFreshRun(outputDir);   // never inherit a stale dataset's markers/parts
        }
        Path stagingDir = outputDir.resolve(SORT_STAGING_DIR);
        Files.createDirectories(stagingDir);
        ListRunner.ParquetSpec parquetSpec = parquetSpec(s3uri, channelCapacity, pageMax, filterChain, argsHash, config);
        SortConfig sortConfig = SortConfig.fromSystemProperties();
        SortMode sortMode = run.mode() == ListingMode.VERSIONS
                ? SortMode.VERSIONS : SortMode.OBJECTS;

        if (nodes.isEmpty()) {
            if (isPublishedByThisRun(outputDir, argsHash, run.id())) {
                // PUBLISHED: idempotent re-entry cleans leftover staging + stale *.tmp, then exits.
                // markRunFinished is required here too — the merge/publish path
                // (runToSortedParquetWorkStealing / runSortMergeOnly) marks the run COMPLETED itself,
                // but a re-entry that finds a PUBLISHED run (e.g. a fresh process re-invoked after a
                // crash strictly between manifest-write and the terminal-status commit) must still
                // close out run_meta, or it stays RUNNING forever. Idempotent: re-marking an already
                // COMPLETED run is a harmless no-op UPDATE.
                cleanSortStagingAndStaleTmp(outputDir, stagingDir);
                store.setSortPhase(run.id(), SortPhase.PUBLISHED);
                store.markRunFinished(run.id(), RunStatus.COMPLETED);
                writeEarlyExitSummary(OutputFormat.PARQUET, config, argsHash, ctx, run.id(),
                        true, StopReason.COMPLETED, STRATEGY_WORK_STEALING);
                return ExitCodes.SUCCESS;
            }
            // Merge pending (crashed post-listing, pre-publish; or a foreign manifest.json logged
            // above): re-run the merge. SortTransform#cleanStaleFinals (landed alongside the
            // merge-budget fix; widened to ALL data/*.parquet) sweeps any stale finals before
            // this run writes its own, so a foreign manifest's finals never survive into this run's
            // published output.
            new ListRunner().runSortMergeOnly(ctx, outputDir, stagingDir, store, run.id(),
                    sortConfig, sortMode, parquetSpec);
            return ExitCodes.SUCCESS;
        }
        // The periodic half of the disk pre-check — polls the LIVE swath.sort.segment.bytes
        // counter against usable free space while the (possibly multi-hour) listing runs, so a
        // brand-new giant bucket's true disk trajectory trips this well before the merge, not after
        // it. See SortDiskGuard's class javadoc for why it halts directly rather than throwing.
        try (SortDiskGuard diskGuard = SortDiskGuard.arm(
                outputDir, ctx.metrics()::sortSegmentBytesWritten, sorting.forceSort)) {
            new ListRunner().runToSortedParquetWorkStealing(ctx, fetcher, outputDir, stagingDir, parquetSpec,
                    store, run.id(), connection.maxParallelListings, nodes, sortConfig, sortMode, engine.toggles,
                    traceSink, run.resumed(), retryConfig());
        }
        return ExitCodes.SUCCESS;
    }

    /**
     * "Published by THIS run" gate for the sorted merge dispatcher (contract §4.1/6). Two signals
     * must BOTH hold, and the completion marker is authoritative:
     *
     * <ol>
     *   <li>the internal {@code .swath-state.json} identity ({@code args_hash} AND the checkpoint
     *       {@code run_id}) matches THIS run — a stale/foreign state file left by a DIFFERENT prior run
     *       sharing the same {@code -o} dir (a fresh / {@code --restart} run only discards checkpoint
     *       rows, never output files) is logged loudly and treated as merge-pending, never PUBLISHED; and</li>
     *   <li>the empty {@code _SUCCESS} marker exists. It is written LAST (after the manifest, state, and
     *       symlink — {@link ListRunner#writeSortedManifest}), so its presence PROVES the whole publish
     *       committed. Without this gate, a crash AFTER {@code .swath-state.json} but
     *       BEFORE {@code _SUCCESS} would make reentry declare PUBLISHED and short-circuit, marking the
     *       run COMPLETED yet leaving the dataset permanently missing its {@code _SUCCESS} marker.</li>
     * </ol>
     *
     * <p>Identity match but no {@code _SUCCESS} ⇒ our OWN publish crashed mid-flight ⇒ merge-pending
     * (the caller re-runs the merge idempotently, rewriting manifest/state/symlink/_SUCCESS).
     */
    private boolean isPublishedByThisRun(Path outputDir, String argsHash, long runId) {
        Optional<Manifest.Identity> identity =
                Manifest.readIdentity(outputDir);
        if (identity.isEmpty()) {
            return false;   // no state file, or unreadable/corrupt — never trusted either way
        }
        boolean matches = argsHash.equals(identity.get().argsHash())
                && Objects.equals(runId, identity.get().runId());
        if (!matches) {
            log.warn("list_sort_foreign_manifest_detected run_id={} args_hash={} found_run_id={} "
                            + "found_args_hash={} output_dir={} (.swath-state.json does not belong to this "
                            + "checkpoint run; treating as merge-pending — this run's own merge will run "
                            + "and overwrite it, never silently skipped)",
                    runId, argsHash, identity.get().runId(), identity.get().argsHash(), outputDir);
            return false;
        }
        // Identity matches, but PUBLISHED requires the LAST-written marker too. Its absence means
        // our own publish crashed between the state write and _SUCCESS — re-run the merge idempotently.
        Path success = DatasetLayout.of(outputDir).success();
        if (!Files.isRegularFile(success)) {
            log.info("list_sort_publish_incomplete run_id={} output_dir={} (.swath-state.json identity "
                            + "matches but _SUCCESS is absent — publish crashed mid-commit; re-running the "
                            + "merge to complete it)",
                    runId, outputDir);
            return false;
        }
        return true;
    }

    /**
     * PUBLISHED re-entry clean-up (contract §6): a crash between the manifest write and the staging
     * deletion leaves double data — remove the staging segments and any stale {@code part-*.tmp}.
     * The sorter only ever deletes content it owns inside the staging dir. The stale
     * {@code *.tmp} finals live under {@code <root>/data/} (the pure-parquet subdir).
     */
    static void cleanSortStagingAndStaleTmp(Path outputDir,
                                            Path stagingDir) throws IOException {
        if (Files.isDirectory(stagingDir)) {
            try (Stream<Path> entries = Files.list(stagingDir)) {
                for (Path p : entries.toList()) {
                    Files.deleteIfExists(p);
                }
            }
            Files.deleteIfExists(stagingDir);
        }
        Path dataDir = DatasetLayout.of(outputDir).dataDir();
        if (Files.isDirectory(dataDir)) {
            try (DirectoryStream<Path> tmp =
                         Files.newDirectoryStream(dataDir, "part-*.parquet.tmp")) {
                for (Path p : tmp) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }

    ListRunner.Spec listSpec(S3Uri s3uri, OutputFormat resolved, int channelCapacity, int pageMax,
                             FilterChain filterChain, String argsHash, S3Config config)
            throws InvalidConfigException {
        return new ListRunner.Spec(
                s3uri.prefix(), resolved, !output.rawOutput, channelCapacity, pageMax, filterChain,
                liveness.resolveProgressInterval(), buildJsonSummaryConfig(resolved, config, argsHash));
    }

    ListRunner.ParquetSpec parquetSpec(S3Uri s3uri, int channelCapacity, int pageMax,
                                       FilterChain filterChain, String argsHash, S3Config config)
            throws InvalidConfigException, InvalidArgsException {
        boolean singleFile = output.resolvedKind == OutputOptions.DestinationKind.FILE;
        long targetBytes = output.partSizeBytes();
        int writers = OutputOptions.resolveParquetWriters(output.resolvedKind, output.parquetWriters);
        long rotation = singleFile ? Long.MAX_VALUE : targetBytes;
        // A single-file destination (a .parquet extension on -o) must stay exactly
        // one part: the time/row-count cadence triggers are for the multi-part model
        // only, so force them off here.
        long rotationIntervalNanos = singleFile ? 0L : output.resolvePartRotationInterval().toNanos();
        long rotationMaxRows = singleFile ? 0L : output.resolvePartRotationMaxRows();
        return new ListRunner.ParquetSpec(
                s3uri.prefix(), channelCapacity, pageMax, filterChain, writers, rotation,
                PARQUET_WRITER_QUEUE_CAPACITY, argsHash,
                liveness.resolveProgressInterval(), buildJsonSummaryConfig(OutputFormat.PARQUET, config, argsHash),
                rotationIntervalNanos, rotationMaxRows, s3uri.bucket());
    }

    /**
     * Pure run-summary-sidecar validation, hoisted to the top of {@link #call()} ahead of any network/store
     * work: the report/sidecar-suppression mutual-exclusion check and the
     * {@code --tune summary.interval}/{@code --progress-interval} parse used to run inside
     * {@link #buildJsonSummaryConfig}, which on the default {@code --checkpoint auto} path only
     * executes after the SQLite store is opened, the {@code S3Client} is created, and (for a fresh
     * run) the {@code seedFreshRun} network probe — so a bad combo failed late, with side effects
     * already committed. Both checks are pure (no I/O), so running them first is free.
     */
    private void validateSummaryJsonFlags() throws InvalidConfigException {
        if (output.summaryJson != null && output.noSummaryJson) {
            throw new InvalidConfigException("an explicit --report cannot be combined with sidecar suppression");
        }
        if (output.summaryJsonInterval != null) {
            DurationParser.progressInterval(output.summaryJsonInterval);
        }
        liveness.resolveProgressInterval();
    }

    /**
     * Guard: {@code --sort} applies to {@code --format parquet} only (a sorted text sink is
     * deferred to {@code --resume-output}). Rejects as early as possible — before any checkpoint
     * is opened or a run row inserted (a durable mutation must never follow an
     * invalid-args exit) — for every case except a bare {@code swath resume} with no explicit {@code
     * --format}: there the effective format may still be RESTORED from the checkpoint (a stored
     * format can differ from today's TTY-based default), so {@link #runWithCheckpoint} re-checks
     * after that restore, once the effective format is finally known (that re-check is the only one
     * that can ever actually fire for a genuine resume; a fresh/{@code --restart} run's effective
     * format never changes after this point, so it is fully decided here).
     * {@code --sort} additionally requires a directory-dataset destination — a {@code .parquet}
     * single-file {@code -o} (or stdout) is rejected, since a sorted run's merge/publish state
     * machine (contract §6) needs the durable dataset dir as its checkpoint-resumable handle.
     * Mirrors the format check's resume carve-out: {@code -o} can ALSO be soft-restored from the
     * checkpoint on a bare {@code swath resume} ({@link #restoreRunContext}) — that restored value is
     * guaranteed already directory-kind (the original run could not have been created with
     * {@code --sort} otherwise), so skipping the not-yet-restored null case here is safe, not a gap.
     */
    void validateSortFlags(OutputFormat resolved) throws InvalidArgsException {
        if (sorting.sort && resolved != OutputFormat.PARQUET
                && (output.formatWasExplicitlySet() || !checkpoint.resume)) {
            throw new InvalidArgsException("--sort requires --format parquet (a sorted text sink is "
                    + "deferred); " + (output.formatWasExplicitlySet()
                            ? "got --format " + resolved.name().toLowerCase(Locale.ROOT)
                            : "resolved format is " + resolved.name().toLowerCase(Locale.ROOT)));
        }
        if (sorting.sort && output.resolvedKind != OutputOptions.DestinationKind.DIRECTORY
                && (output.destination != null || !checkpoint.resume)) {
            String destinationProblem = output.resolvedKind == OutputOptions.DestinationKind.STDOUT
                    ? "the output destination is stdout -- pass -o <directory>"
                    : "-o " + output.destination + " resolves to FILE kind, which is non-resumable"
                            + fileKindPhysicalLayoutNote(resolved)
                            + " -- drop its recognized extension (or pass --output-type dir)";
            throw new InvalidArgsException("--sort requires a directory dataset destination; "
                    + destinationProblem);
        }
    }

    private static String fileKindPhysicalLayoutNote(OutputFormat resolved) {
        return resolved == OutputFormat.PARQUET
                ? "; note that FILE-kind Parquet currently writes a one-writer dataset directory "
                        + "under the path, but that physical layout is still non-resumable"
                : "";
    }

    /**
     * Refusal message for a fresh/restart checkpoint request against a non-DIRECTORY (structurally
     * ephemeral) output kind, or {@code null} when the combination is safe. FILE-kind refuses ANY
     * non-none mode, including the {@code auto} default: a real {@code -o} path looks durable enough
     * that a caller could plausibly expect resume, so FILE-kind creation requires an explicit
     * {@code --checkpoint none} acknowledgment. stdout carries no such expectation -- {@code auto}
     * already resolves to no durable file at all against stdout ({@link
     * CheckpointOptions.CheckpointMode#resolve}) -- so the default run keeps succeeding unchanged;
     * only an EXPLICIT non-{@code auto}, non-none {@code --checkpoint} against stdout is refused
     * here, since it would otherwise open a real checkpoint file nothing can ever resume (an orphan).
     */
    String checkpointRefusalForEphemeralSink(OutputOptions.DestinationKind kind,
            CheckpointOptions.CheckpointMode mode, OutputFormat resolved) {
        if (mode.isNone()) {
            return null;
        }
        boolean fileKind = kind == OutputOptions.DestinationKind.FILE;
        boolean refuse = fileKind || (kind == OutputOptions.DestinationKind.STDOUT && !mode.isAuto());
        if (!refuse) {
            return null;
        }
        String destinationDescription = fileKind
                ? "FILE-kind destination -o " + output.destination
                : "the stdout destination";
        String safeOptions = fileKind
                ? "or pass --checkpoint none for an ephemeral single-file run"
                : "or pass --checkpoint none (the auto default is already ephemeral there) for an "
                        + "ephemeral stdout run";
        // The physical-layout note only makes sense for a FILE-kind -o path (it describes the
        // one-writer dataset directory Parquet currently writes under that path); stdout has no
        // path to describe, and --format parquet is reachable here (the stdout-vs-parquet sink
        // rejection happens later, in openParquetDir).
        return destinationDescription
                + " is non-resumable: single-file/stdout output cannot carry a checkpoint. Use a "
                + "directory-dataset destination (-o <dir>) for a checkpointed, resumable run, "
                + safeOptions
                + (fileKind ? fileKindPhysicalLayoutNote(resolved) : "");
    }

    /** {@code --tune summary.interval} overrides; default = the resolved progress interval. */
    Duration resolveSummaryJsonInterval() throws InvalidConfigException {
        if (output.summaryJsonInterval != null) {
            return DurationParser.progressInterval(output.summaryJsonInterval);
        }
        return liveness.resolveEffectiveProgressInterval();
    }

    /**
     * Builds the {@link io.varve.swath.observability.JsonRunSummaryWriter.Config} threaded into {@link
     * ListRunner.Spec}/{@link ListRunner.ParquetSpec}, or {@code null} when the sidecar is disabled.
     * {@code config} is the resolved S3 connection config (post-default region).
     */
    JsonRunSummaryWriter.Config buildJsonSummaryConfig(
            OutputFormat resolved, S3Config config, String argsHash) throws InvalidConfigException {
        Path path = output.resolveSummaryJsonPath(resolved);
        if (path == null) {
            return null;
        }
        Duration flushInterval = resolveSummaryJsonInterval();
        Duration resolvedMaxDuration = liveness.resolveMaxDuration();
        Long maxDurationMs = resolvedMaxDuration == null ? null : resolvedMaxDuration.toMillis();
        // Sort observability polish: SortConfig.fromSystemProperties() is a cheap, idempotent
        // re-read of the same -D knobs runSortedParquet() reads independently — echoing
        // effectiveFanIn() here makes the binding merge-pass-width knob self-describing in the
        // summary without threading a SortConfig through the whole call chain.
        Integer sortEffectiveFanIn = sorting.sort
                ? SortConfig.fromSystemProperties().effectiveFanIn() : null;
        JsonRunSummaryWriter.RunConfig runConfig =
                new JsonRunSummaryWriter.RunConfig(
                        uri, config.region() == null ? null : config.region().id(),
                        resolved.name().toLowerCase(Locale.ROOT), connection.maxParallelListings, connection.noSignRequest,
                        connection.rateLimitApi, liveness.resolveEffectiveProgressInterval().toMillis(), filters.descriptions(),
                        engine.toggles, maxDurationMs, sorting.sort, sortEffectiveFanIn, connection.requestPayerEnabled);
        return new JsonRunSummaryWriter.Config(
                path, flushInterval, argsHash, runConfig, resolveArgv());
    }

    /**
     * Safe argv, sourced from picocli's parse result (not threaded as {@code String[]}
     * from {@link App#main}). Endpoint values are replaced by the central fixed redaction marker;
     * other arguments are control-escaped before this representation reaches a durable summary.
     *
     * <p>{@code spec}/its parse result are only populated when this command runs through a real
     * picocli {@code CommandLine} parse/execute; several unit tests construct {@code ListCommand}
     * and set fields directly without going through picocli, so this falls back to an empty list
     * rather than NPE.
     */
    private List<String> resolveArgv() {
        if (spec == null) {
            return List.of();
        }
        ParseResult parseResult = spec.commandLine().getParseResult();
        return parseResult == null ? List.of()
                : SafeInput.argv(parseResult.originalArgs());
    }

    /**
     * The pre-run early-exit paths (no-op resume, seed-probe failure, resume refusal) each
     * leave a JSON run-summary behind so a batch/orchestrator always finds one, with a {@code
     * stop_reason} that names the category. A completed no-op resume writes {@code
     * completed:true}; the failure paths write a {@code completed:false} terminal record whose
     * {@code exit_code} is {@code null} (the writer never learns the process code) while {@code
     * stop_reason} carries {@code seed_failure}/{@code resume_refused}. Respects {@code
     * the internal sidecar-suppression seam and the stdout-no-default-sidecar rule (a {@code null}
     * config no-ops).
     * {@code strategy} is the actual strategy the invocation would have used (the caller knows
     * this — every current early-exit path is inside the checkpointed/WORK_STEALING branch; a
     * hardcoded literal here would mislabel a future non-WORK_STEALING early-exit caller).
     */
    void writeEarlyExitSummary(OutputFormat resolved, S3Config config, String argsHash,
                               RunContext ctx, long runId, boolean completed, StopReason reason,
                               String strategy)
            throws InvalidConfigException {
        writeEarlyExitSummary(resolved, config, argsHash, ctx, runId, completed, reason, strategy,
                null, null);
    }

    /**
     * As the 8-arg {@link #writeEarlyExitSummary}, but the seed-failure path threads the
     * resolved process {@code exitCode} (so the summary reports the true exit 1, not {@code null})
     * and a classified {@code errorClass} (e.g. {@code access_denied}/{@code no_such_bucket}) so a
     * supervisor can tell a permissions/config failure apart from an unclassified crash. Both are
     * {@code null} on the pre-existing early-exit paths (no-op resume, resume refusal), which keep
     * their historical {@code exit_code:null}/{@code error_class:null} shape. A {@code STUCK} reason
     * ignores both and defers to {@link ListRunner#stuckTerminalStatus} as before.
     */
    void writeEarlyExitSummary(OutputFormat resolved, S3Config config, String argsHash,
                               RunContext ctx, long runId, boolean completed, StopReason reason,
                               String strategy, Integer exitCode, String errorClass)
            throws InvalidConfigException {
        JsonRunSummaryWriter.Config summaryConfig =
                buildJsonSummaryConfig(resolved, config, argsHash);
        if (summaryConfig == null) {
            return;   // stdout/single-file text or internal suppression: no sidecar
        }
        ctx.metrics().setRunId(runId);
        RunSummary summary =
                ctx.metrics().summary(Duration.ZERO, strategy, 0L, 0L);
        // A seed-time retry-cap STUCK must carry the same
        // stop_source/error_class the normal-run summary path derives — reuse ListRunner's shared
        // helper rather than duplicating the source-tag -> RunMetrics#stuckErrorClass routing here.
        Supplier<JsonRunSummaryWriter.TerminalStatus> terminalStatus =
                reason == StopReason.STUCK
                        ? () -> ListRunner.stuckTerminalStatus(ctx)
                        : () -> new JsonRunSummaryWriter.TerminalStatus(
                                reason, null, errorClass, exitCode);
        JsonRunSummaryWriter writer =
                JsonRunSummaryWriter.start(summaryConfig,
                        ctx.metrics().registry(), Instant.now(), () -> summary,
                        terminalStatus);
        try {
            if (completed) {
                writer.complete(summary);
            }
            // else: close() emits the completed:false partial with stop_reason=reason.
        } finally {
            writer.close();
        }
    }

    /**
     * Best-effort {@link #writeEarlyExitSummary}: a sidecar-write failure in the seed-abort catch
     * is logged and swallowed, never masking the real seed exception nor diverting the
     * intended resumable disposition (a STUCK seed's {@link CancelledException}, or the fatal-mark +
     * rethrow of a genuine seed failure) into an unhandled summary-write throw that would leave the
     * run escaping RUNNING-unseeded by the wrong path. Mirrors {@link #markBestEffort}.
     */
    private void writeEarlyExitSummaryBestEffort(OutputFormat resolved, S3Config config, String argsHash,
                                                 RunContext ctx, long runId, boolean completed,
                                                 StopReason reason, String strategy) {
        writeEarlyExitSummaryBestEffort(resolved, config, argsHash, ctx, runId, completed, reason,
                strategy, null, null);
    }

    /**
     * Best-effort {@link #writeEarlyExitSummary} threading a resolved {@code exitCode}/{@code
     * errorClass} for the classified seed-failure path.
     */
    private void writeEarlyExitSummaryBestEffort(OutputFormat resolved, S3Config config, String argsHash,
                                                 RunContext ctx, long runId, boolean completed,
                                                 StopReason reason, String strategy,
                                                 Integer exitCode, String errorClass) {
        try {
            writeEarlyExitSummary(resolved, config, argsHash, ctx, runId, completed, reason, strategy,
                    exitCode, errorClass);
        } catch (Exception summaryEx) {
            log.warn("list_seed_early_exit_summary_write_failed run_id={} message={}",
                    runId, summaryEx.getMessage());
        }
    }

    /**
     * The destination {@code swath resume} can pick this run back up from, or {@code null} when a
     * partial leaves nothing to resume — so the summary block only ever offers a resume that will
     * actually work. That is the directory-dataset destination whose co-located run handle {@code
     * swath resume <dir>} opens: {@code --checkpoint none} keeps no durable state, a stdout or
     * single-file destination has no run handle to reopen, and an explicit {@code --checkpoint
     * PATH} is not where the resume verb looks.
     */
    private String resumeDestination(OutputOptions.Resolved resolved, CheckpointOptions.CheckpointMode mode) {
        return mode.isAuto() && resolved.kind() == OutputOptions.DestinationKind.DIRECTORY
                ? output.destination
                : null;
    }

    /**
     * The classified {@code error_class} fingerprint for a seed/fetch failure — walks the
     * cause chain (mirroring {@link ExitCodes#forThrowable}) for the first {@link
     * io.varve.swath.error.ListingException} that carries a non-null {@link
     * io.varve.swath.error.ListingException#errorClass()} (e.g. a typed {@code AccessDenied}/{@code
     * NoSuchBucket}/{@code RegionRedirect}). {@code null} for a generic/unattributed seed failure,
     * which keeps its {@code error_class:null} shape (but still gets a real {@code exit_code}).
     */
    private static String seedErrorClass(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ListingException le && le.errorClass() != null) {
                return le.errorClass();
            }
        }
        return null;
    }

    /**
     * Restore the run's non-{@code args_hash} context — {@code no_sign_request}/{@code
     * profile}/{@code region}/{@code fetch_owner}/{@code raw_output}/output path — for whichever
     * of these the caller did not explicitly (re-)supply, so a bare {@code swath resume}
     * reconstructs the original run's auth/output context rather than silently
     * falling back to CLI defaults (e.g. losing {@code --no-sign-request} → auth failure). This
     * extends to {@code request_payer} ({@code --requester-pays requester}) — the same shape
     * of context field (billing, not "what is listed"), soft-restored identically.
     *
     * <p>Rule: an explicit caller value always wins (never overridden), but is logged as a
     * mismatch when it disagrees with the checkpointed value — distinct from the {@code
     * args_hash} identity fields, which have their own hard refusal in {@link
     * io.varve.swath.checkpoint.SqliteCheckpointStore#openRun}; these context fields are deliberately
     * excluded from {@code args_hash} so a soft restore-or-warn is the right policy. The
     * boolean fields (no-sign-request/fetch-owner/raw-output/request-payer) have no CLI syntax for
     * an explicit "false" (non-negatable flags, or — for {@code --requester-pays} — a value-taking
     * flag whose only accepted value is "on"), so "caller value is false" always means "not supplied".
     */
    /**
     * Non-default-toggle observability (instrumentation discipline, docs/internals/metrics-internals.md
     * §5): a startup INFO line naming the
     * effective state of every toggle, only emitted when at least one deviates from its default (a
     * fully-default run stays silent). The per-mechanism {@code TOGGLE.<name>_off} engagement marks
     * live where each toggle actually bears on the engine ({@code WorkStealingScan}/{@code Thief}/
     * {@code SeedStep} constructors), so an engine-level test can observe them without going through
     * the CLI, and a resumed run (which never re-constructs {@code SeedStep}) still gets the ones
     * that apply to it. Covers all twelve fields: the ten {@link io.varve.swath.engine.EngineToggles#NAMES}
     * ablation toggles plus the two new-mechanism toggles outside that list ({@code readahead}, opt-in;
     * {@code mass_aware_seed}, opt-out) — {@link io.varve.swath.engine.EngineToggles#isDefault()}
     * fires this line on ANY of the twelve deviating, so the printed fields must match, not just the ten
     * ablation ones. Emitted under this facade's own logger identity ({@code ListCommand}) so the
     * {@code %logger{20}} production pattern renders the historical token.
     */
    void logEngineTogglesIfNonDefault() {
        if (engine.toggles.isDefault()) {
            return;
        }
        log.info("engine_toggles_effective owner_split={} density_ewma={} radix_bands={} "
                        + "structure_probes={} far_ahead={} alphabet_pivots={} reflect={} confetti_feedback={} "
                        + "reflect_lift={} fanout_tiling={} mass_aware_seed={} readahead={} "
                        + "(EXPERIMENTAL/DIAGNOSTIC ablation surface — measurement only, not a supported "
                        + "configuration)",
                engine.toggles.ownerSplit(), engine.toggles.densityEwma(), engine.toggles.radixBands(),
                engine.toggles.structureProbes(), engine.toggles.farAhead(), engine.toggles.alphabetPivots(),
                engine.toggles.reflect(), engine.toggles.confettiFeedback(), engine.toggles.reflectLift(),
                engine.toggles.fanoutTiling(), engine.toggles.massAwareSeed(), engine.toggles.readahead());
    }

    private void restoreRunContext(RunMeta run) {
        SoftRestoreContext restored = run.context();
        connection.noSignRequest = restoreBoolean("no_sign_request", connection.noSignRequest, restored.noSignRequest());
        connection.profile = restoreString("profile", connection.profile, restored.profile());
        connection.region = restoreString("region", connection.region, restored.region());
        connection.fetchOwner = restoreBoolean("fetch_owner", connection.fetchOwner, restored.fetchOwner());
        output.rawOutput = restoreBoolean("raw_output", output.rawOutput, restored.rawOutput());
        output.destination = restoreString("output", output.destination, restored.outputPath());
        // --output-type is an IDENTITY option restored=true — a bare resume restores it so its
        // identity_spec reproduces (destination kind is recomputed from the path; this is the raw
        // override behind it). A re-passed differing value survives (cli-wins) and is then refused.
        output.outputType = restoreString("output_type", output.outputType, restored.outputType());
        connection.requestPayerEnabled = restoreBoolean("request_payer", connection.requestPayerEnabled, restored.requestPayer());
    }

    private OutputFormat recordedOutputFormat(RunMeta run)
            throws InvalidArgsException, CheckpointException {
        String stored = run.storedOutputFormat();
        if (stored == null || stored.isBlank()) {
            if (resumeCommandCheckpoint != null) {
                throw new CheckpointException("checkpoint " + resumeCommandCheckpoint
                        + " has invalid output_format: " + stored);
            }
            throw new InvalidArgsException("swath resume refused: checkpoint has no recorded output format, "
                    + "so its destination kind and resume format cannot be reconstructed safely");
        }
        try {
            return OutputFormat.valueOf(stored);
        } catch (IllegalArgumentException e) {
            if (resumeCommandCheckpoint != null) {
                throw new CheckpointException("checkpoint " + resumeCommandCheckpoint
                        + " has invalid output_format: " + stored, e);
            }
            throw new InvalidArgsException("swath resume refused: checkpoint records unknown output format '"
                    + stored + "', so its destination kind and resume format cannot be reconstructed safely");
        }
    }

    /**
     * Refusal message for a non-resumable checkpoint-recorded destination, or {@code null} when the
     * recorded destination is a resumable one for this invocation. This deliberately ignores the
     * current invocation's {@code -o}: changing the resume target cannot make a legacy/foreign
     * FILE-origin checkpoint safe because its committed prefix was never a resumable dataset.
     *
     * <p>A {@code swath resume <dir>} additionally requires an absolute recorded destination to BE that
     * run handle ({@link #runHandleDestinationRefusal}). The FILE-origin clauses are checked first:
     * they name the sharper defect, and their ordering against malformed format metadata is settled.
     */
    private String recordedDestinationRefusal(String recordedDestination) {
        OutputFormat extensionFormat = OutputOptions.formatFromExtension(recordedDestination);
        if (extensionFormat == OutputFormat.TSV || extensionFormat == OutputFormat.JSONL) {
            return "swath resume refused: checkpoint recorded FILE-kind text destination -o "
                    + recordedDestination + ", which is non-resumable; changing the resume invocation's "
                    + "-o cannot recover the already-committed prefix";
        }
        if (extensionFormat == OutputFormat.PARQUET) {
            boolean directory;
            try {
                DirectoryProbe probe = recordedDestinationDirectoryProbeOverride != null
                        ? recordedDestinationDirectoryProbeOverride
                        : Files::isDirectory;
                // Probe the path this resume will actually write to. A run-handle resume writes into
                // <dir> for every recording but an absolute one, so probing the recorded string there
                // would read it against the RESUMING process's working directory and call a perfectly
                // good directory dataset ambiguous. A bare `swath resume` keeps the recorded path,
                // because that is the one it will open.
                Path probeTarget = absoluteRecordedDestination(recordedDestination);
                if (probeTarget == null) {
                    probeTarget = resumeCommandRunHandle != null
                            ? resumeCommandRunHandle
                            : Path.of(recordedDestination);
                }
                directory = probe.isDirectory(probeTarget);
            } catch (Exception e) {
                // SecurityException and every unexpected probe failure must fail closed as the
                // same controlled exit-2 refusal, never escape as an unchecked exit-1 failure.
                directory = false;
            }
            if (!directory) {
                return "swath resume refused: checkpoint recorded ambiguous .parquet destination -o "
                        + recordedDestination + " (it may be a legacy FILE-kind run); a directory dataset "
                        + "must exist at that recorded path to resume safely";
            }
        }
        return runHandleDestinationRefusal(recordedDestination);
    }

    /**
     * Refusal message for a {@code swath resume <dir>} whose checkpoint records an ABSOLUTE output
     * destination other than {@code <dir>}, or {@code null} when the recording cannot name anywhere
     * else (or when this is not a run-handle resume). {@code <dir>} is the whole run handle, so a
     * checkpoint naming another directory would publish the manifest, part files, {@code _SUCCESS} and
     * {@code symlink.txt} outside the directory the operator handed us.
     */
    private String runHandleDestinationRefusal(String recordedDestination) {
        if (resumeCommandRunHandle == null) {
            return null;
        }
        // Anything but an absolute recording names no location this resume could be redirected to: a
        // checkpoint with no destination at all (stdout, or a legacy row written before the
        // destination was stored) says nothing, and a relative one is bound to the run handle before
        // anything writes, so the dataset lands inside <dir> whatever the original shell made that
        // spelling mean. Comparing it would only ever refuse a legitimate resume.
        Path recorded = absoluteRecordedDestination(recordedDestination);
        if (recorded == null
                || canonicalDestination(recorded).equals(canonicalDestination(resumeCommandRunHandle))) {
            return null;
        }
        return "swath resume refused: run handle " + resumeCommandRunHandle + " holds a checkpoint "
                + "recording output destination -o " + recordedDestination + ", so resuming would "
                + "write the dataset there instead of into " + resumeCommandRunHandle + " (the "
                + "checkpoint keeps whatever absolute -o the original run was given, so a moved or "
                + "copied dataset still names its original path); to list into this directory "
                + "instead, run `swath list -o " + resumeCommandRunHandle + " … --restart`";
    }

    /**
     * The recorded destination as an absolute path, or {@code null} when it names no absolute location
     * at all — absent, {@code -}, relative, or unparseable. A RELATIVE recording is stored exactly as
     * it was typed ({@code -o out}) and so means whatever the ORIGINAL run's working directory made it
     * mean, which is not recoverable from the string and which the resuming process need not share.
     * Only an absolute recording names a place that survives the trip, so only that one can be
     * compared with — or refused against — the run handle.
     */
    private static Path absoluteRecordedDestination(String recordedDestination) {
        if (recordedDestination == null || "-".equals(recordedDestination)) {
            return null;
        }
        try {
            Path recorded = Path.of(recordedDestination);
            return recorded.isAbsolute() ? recorded : null;
        } catch (InvalidPathException e) {
            return null;
        }
    }

    /**
     * Canonical form of a destination for comparison: the real path when it resolves (following
     * symlinks), else the absolute normalized form, so a recorded path that no longer exists still
     * compares deterministically instead of throwing.
     */
    private static Path canonicalDestination(Path destination) {
        try {
            return destination.toRealPath();
        } catch (IOException | RuntimeException e) {
            return destination.toAbsolutePath().normalize();
        }
    }

    private boolean restoreBoolean(String field, boolean cliValue, boolean storedValue) {
        if (cliValue) {
            if (!storedValue) {
                log.debug("list_resume_context_mismatch field={} checkpoint={} cli={} (cli wins)",
                        field, storedValue, cliValue);
            }
            return true;
        }
        return storedValue;
    }

    private String restoreString(String field, String cliValue, String storedValue) {
        if (cliValue != null) {
            if (storedValue != null && !storedValue.equals(cliValue)) {
                log.debug("list_resume_context_mismatch field={} checkpoint={} cli={} (cli wins)",
                        field, storedValue, cliValue);
            }
            return cliValue;
        }
        return storedValue;
    }
}
