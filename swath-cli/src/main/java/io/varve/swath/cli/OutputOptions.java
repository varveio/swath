/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import com.github.luben.zstd.ZstdOutputStream;
import io.varve.swath.error.InvalidArgsException;
import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.filter.SizeParser;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.output.dataset.PeriodicDataSync;
import io.varve.swath.output.dataset.SharedDatasetWriterPool;
import io.varve.swath.output.parquet.ParquetWriterMemoryPlan;
import io.varve.swath.output.text.TextCompression;
import io.varve.swath.output.text.TextWriterPoolConfig;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

/**
 * Output destination and shape: the {@code --format}/{@code -o} sink, the Parquet writer pool and
 * part-rotation cadence, and the JSON run-summary sidecar flags. Owns opening the text
 * and Parquet sinks and validating the memory-bounded writer/rotation knobs.
 */
final class OutputOptions {

    @Resume(ResumeClass.FREE)
    @Option(names = "--compression", paramLabel = "none|gzip|zstd", converter = CompressionConverter.class,
            description = "Compress table/TSV/JSONL streams, files, or dataset parts; inferred from "
                    + ".gz/.zst file names when omitted (default: none).")
    void setCompression(TextCompression compression) {
        this.compression = compression;
        this.compressionSpecified = true;
    }

    TextCompression compression = TextCompression.NONE;
    private boolean compressionSpecified;
    TextCompression resolvedCompression = TextCompression.NONE;

    static final class CompressionConverter implements ITypeConverter<TextCompression> {
        @Override
        public TextCompression convert(String value) {
            try {
                return TextCompression.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new TypeConversionException(
                        "'" + value + "' is not one of [none, gzip, zstd]");
            }
        }
    }

    /** The leading {@code _} keeps the sidecar out of a bare {@code *.parquet} glob. */
    static final String DEFAULT_SUMMARY_JSON_NAME = "_swath_summary.json";

    @Resume(value = ResumeClass.IDENTITY, restored = true)
    @Option(names = "--format", paramLabel = "FORMAT", converter = FormatConverter.class,
            description = "Output encoding/sink: auto, table, tsv, jsonl, parquet, or discard "
                    + "(default: auto).")
    void setFormat(OutputFormat format) {
        this.format = format;
        this.formatSpecified = true;
    }

    /** Concrete parsed format, or {@code null} for omitted/explicit {@code auto}. Direct assignment
     * remains a same-package construction seam for tests and {@link ResumeCommand}. */
    OutputFormat format;

    /** Picocli presence bit: unlike {@link #format}, distinguishes omitted from explicit auto. */
    private boolean formatSpecified;

    /** Accepts the explicit {@code auto} spelling alongside the concrete formats. Auto maps
     * to {@code null}; {@link #formatSpecified} preserves its option presence so resume treats it
     * exactly like an explicitly selected concrete format rather than restoring over it. */
    static final class FormatConverter implements ITypeConverter<OutputFormat> {
        @Override
        public OutputFormat convert(String value) {
            if ("auto".equalsIgnoreCase(value)) {
                return null;
            }
            try {
                return OutputFormat.valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new TypeConversionException(
                        "'" + value + "' is not one of [table, tsv, jsonl, parquet, discard, auto]");
            }
        }
    }

    @Resume(value = ResumeClass.IDENTITY, restored = true)
    @Option(names = {"-o", "--output"}, paramLabel = "PATH",
            description = "Write to PATH; known extension selects a file, otherwise a dataset; - is stdout.")
    String destination;

    @Resume(value = ResumeClass.IDENTITY, restored = true)
    @Option(names = "--output-type", paramLabel = "file|dir",
            description = "Override -o file/directory inference.")
    String outputType;

    /** Stored run-context field retained for resume reconstruction; new runs always escape text. */
    boolean rawOutput;

    int parquetWriters = 3;

    @Resume(ResumeClass.FREE)
    @Option(names = "--text-writers", paramLabel = "N",
            description = "Parallel writers for a TSV/JSONL directory dataset (default: 3; range: "
                    + TextWriterPoolConfig.MIN_WRITERS + "-" + TextWriterPoolConfig.MAX_WRITERS + ").")
    int textWriters = 3;

    /** Resolved -o destination kind: set once by {@link #resolveOutput} in {@code call()}. */
    DestinationKind resolvedKind = DestinationKind.STDOUT;

    /** The concrete format {@link #resolveOutput} settled on (post-TTY/-extension resolution), i.e.
     * exactly what {@code run_meta.output_format} stores — never the raw {@code auto}/{@code null}
     * {@link #format}. {@link ResumeRegistry#identitySpec} reads this so the identity string a run
     * persists at creation matches the one recomputed on resume; {@link ListCommand} keeps it in
     * step with its {@code resolved} local across the resume restore. */
    OutputFormat resolvedFormat;

    /** The two axes {@code -o} resolves to: a directory dataset (Parquet, TSV, or JSONL) or an
     * atomically-published single file. {@code STDOUT} is its own kind (no destination given, or
     * {@code -o -}). */
    enum DestinationKind { STDOUT, FILE, DIRECTORY }

    /** A fully resolved {@code --format} x {@code -o} decision. */
    record Resolved(OutputFormat format, DestinationKind kind) {
    }

    /** Known {@code -o} single-file extensions -> format: "One rule, reused from
     * format inference." {@code table} has no file extension -- it is the TTY-only human view. */
    private static final Map<String, OutputFormat> EXTENSION_FORMATS = Map.of(
            ".tsv", OutputFormat.TSV,
            ".jsonl", OutputFormat.JSONL,
            ".parquet", OutputFormat.PARQUET);

    static String token(OutputFormat format) {
        return format.name().toLowerCase(Locale.ROOT);
    }

    /** Whether the caller selected {@code --format}, including explicit {@code auto}. A direct
     * concrete field assignment is also explicit for same-package command-construction seams. */
    boolean formatWasExplicitlySet() {
        return formatSpecified || format != null;
    }

    private static String extensionOf(String destination) {
        Path name = Path.of(stripTrailingSeparators(destination)).getFileName();
        String fileName = name == null ? "" : withoutCompressionExtension(name.toString());
        int dot = fileName.lastIndexOf('.');
        return dot < 0 ? "" : fileName.substring(dot).toLowerCase(Locale.ROOT);
    }

    /** Format implied by a recognized destination extension, or {@code null} when none is known. */
    static OutputFormat formatFromExtension(String destination) {
        if (destination == null || "-".equals(destination)) {
            return null;
        }
        // A trailing separator carries no destination-kind meaning. Strip it before asking Path
        // for the final name component, otherwise a recorded
        // "out.jsonl/" would evade FILE-origin resume refusal. Treat both common separator
        // spellings as metadata may have been written on a different host OS.
        String normalized = stripTrailingSeparators(destination);
        if (normalized.isEmpty()) {
            return null;
        }
        String fileName;
        try {
            Path name = Path.of(normalized).getFileName();
            fileName = name == null ? "" : name.toString();
        } catch (InvalidPathException e) {
            // Foreign/corrupt checkpoint metadata still needs the recorded-destination gate to
            // fail closed. Its filesystem probe will reject the invalid path; retain only the
            // lexical final component here so a recognized extension reaches that probe.
            int slash = Math.max(normalized.lastIndexOf('/'), normalized.lastIndexOf('\\'));
            fileName = normalized.substring(slash + 1);
        }
        if (fileName.isEmpty()) {
            return null;
        }
        fileName = withoutCompressionExtension(fileName);
        int dot = fileName.lastIndexOf('.');
        String extension = dot < 0 ? "" : fileName.substring(dot).toLowerCase(Locale.ROOT);
        return EXTENSION_FORMATS.get(extension);
    }

    private static String withoutCompressionExtension(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".gz")) {
            return fileName.substring(0, fileName.length() - 3);
        }
        if (lower.endsWith(".zst")) {
            return fileName.substring(0, fileName.length() - 4);
        }
        return fileName;
    }

    private TextCompression compressionFromExtension() {
        if (isStdoutDestination()) {
            return TextCompression.NONE;
        }
        String lower = stripTrailingSeparators(destination).toLowerCase(Locale.ROOT);
        return lower.endsWith(".gz") ? TextCompression.GZIP
                : lower.endsWith(".zst") ? TextCompression.ZSTD : TextCompression.NONE;
    }

    private static String stripTrailingSeparators(String value) {
        int end = value.length();
        while (end > 0) {
            char last = value.charAt(end - 1);
            if (last != '/' && last != '\\') {
                break;
            }
            end--;
        }
        return value.substring(0, end);
    }

    private void resolveCompression(OutputFormat resolved) throws InvalidArgsException {
        TextCompression inferred = compressionFromExtension();
        if (compressionSpecified && inferred != TextCompression.NONE && compression != inferred) {
            throw new InvalidArgsException("--compression " + compression.name().toLowerCase(Locale.ROOT)
                    + " conflicts with the compression extension of -o " + destination
                    + " (which implies --compression " + inferred.name().toLowerCase(Locale.ROOT) + ")");
        }
        resolvedCompression = compressionSpecified ? compression : inferred;
        if (resolved == OutputFormat.PARQUET && resolvedCompression != TextCompression.NONE) {
            String cause = compressionSpecified
                    ? "--compression " + resolvedCompression.name().toLowerCase(Locale.ROOT)
                    : "the compression extension of -o " + destination;
            throw new InvalidArgsException(cause + " applies only to text output formats "
                    + "(table, tsv, jsonl); Parquet manages its own compression");
        }
        if (resolved == OutputFormat.DISCARD && resolvedCompression != TextCompression.NONE) {
            throw new InvalidArgsException("--format discard does not serialize output and therefore "
                    + "does not support --compression "
                    + resolvedCompression.name().toLowerCase(Locale.ROOT));
        }
    }

    /**
     * Resolve only destination kind while {@link ResumeCommand} defers parsing checkpoint format
     * metadata until after the recorded-destination refusal. Full format/extension validation is
     * repeated once that gate has passed.
     */
    Resolved resolveDeferredResumeOutput(boolean stdoutIsTerminal) throws InvalidArgsException {
        OutputFormat placeholder = OutputFormat.defaultFor(stdoutIsTerminal);
        if (isStdoutDestination()) {
            if (outputType != null) {
                parseOutputType(outputType);
                throw new InvalidArgsException("--output-type is meaningless with a stdout destination "
                        + "(no -o, or -o -) -- there is no file-vs-directory choice to override; drop "
                        + "--output-type, or pass a real -o PATH");
            }
            resolvedKind = DestinationKind.STDOUT;
            return new Resolved(placeholder, resolvedKind);
        }
        OutputFormat fromExtension = formatFromExtension(destination);
        resolvedKind = resolveOutputTypeOverride(
                fromExtension != null ? DestinationKind.FILE : DestinationKind.DIRECTORY);
        return new Resolved(fromExtension != null ? fromExtension : placeholder, resolvedKind);
    }

    /** {@code true} iff {@code -o} was omitted, or given literally as {@code -} (both
     * mean stdout). Deliberately does NOT mutate {@link #destination} to {@code null}:
     * an explicit {@code -o -} must stay visible to {@code restoreString} in
     * {@link ListCommand#restoreRunContext} (else a bare {@code swath resume} could silently restore
     * the checkpointed destination OVER the user's explicit stdout request) and to {@link
     * ListCommand#validateSortFlags} (else a sorted {@code swath resume -o -} could bypass the
     * stdout-vs-{@code --sort} rejection on the resume-carve-out branch, since that branch only
     * skips the check when {@code destination == null}). */
    boolean isStdoutDestination() {
        return destination == null || "-".equals(destination);
    }

    private static DestinationKind parseOutputType(String outputType) throws InvalidArgsException {
        return switch (outputType.toLowerCase(Locale.ROOT)) {
            case "file" -> DestinationKind.FILE;
            case "dir", "directory" -> DestinationKind.DIRECTORY;
            default -> throw new InvalidArgsException(
                    "--output-type must be 'file' or 'dir' (got '" + outputType + "')");
        };
    }

    private DestinationKind resolveOutputTypeOverride(DestinationKind inferred) throws InvalidArgsException {
        return outputType == null ? inferred : parseOutputType(outputType);
    }

    /**
     * Resolve the {@code --format} x {@code -o} destination axes in one place, so
     * every conflict/precedence rule lives beside its sibling: no {@code -o} (or {@code -o -}) is
     * stdout, format defaults via {@code auto}. A real {@code -o} path with a known
     * extension is a single file (the extension can supply the format when {@code --format} is
     * omitted, and MUST agree with it when both are given, REGARDLESS of any {@code --output-type}
     * override — the extension's implied format is a fact about the path, not something a
     * file-vs-directory override changes); any other path is a directory dataset -- {@code
     * --output-type} overrides the file-vs-directory call for a pathological name. Directory
     * datasets support Parquet, TSV, and JSONL; aligned table output remains a stream/file-only
     * presentation (see the exit-2 guard below).
     */
    Resolved resolveOutput(boolean stdoutIsTerminal) throws InvalidArgsException, InvalidConfigException {
        boolean explicitFormat = formatWasExplicitlySet();
        OutputFormat selected = format != null ? format : OutputFormat.defaultFor(stdoutIsTerminal);
        if (isStdoutDestination()) {
            if (outputType != null) {
                parseOutputType(outputType);   // validate the VALUE first (exit 2 on garbage too)
                throw new InvalidArgsException("--output-type is meaningless with a stdout destination "
                        + "(no -o, or -o -) -- there is no file-vs-directory choice to override; drop "
                        + "--output-type, or pass a real -o PATH");
            }
            resolvedKind = DestinationKind.STDOUT;
            resolvedFormat = selected;
            resolveCompression(selected);
            return new Resolved(selected, resolvedKind);
        }
        if (selected == OutputFormat.DISCARD) {
            throw new InvalidArgsException("--format discard does not accept -o " + destination
                    + "; it drains and tallies the listing without creating an output destination");
        }
        String ext = extensionOf(destination);
        OutputFormat fromExtension = formatFromExtension(destination);
        DestinationKind kind = resolveOutputTypeOverride(
                fromExtension != null ? DestinationKind.FILE : DestinationKind.DIRECTORY);
        OutputFormat resolved;
        if (explicitFormat) {
            if (fromExtension != null && fromExtension != selected) {
                throw new InvalidArgsException("--format " + (format == null ? "auto (resolved to "
                        + token(selected) + ")" : token(selected)) + " conflicts with the '"
                        + ext + "' extension of -o " + destination + " (which implies --format "
                        + token(fromExtension) + "); match the extension, drop --format, or use a "
                        + "path without a recognized extension");
            }
            resolved = selected;
        } else if (fromExtension != null) {
            resolved = fromExtension;
        } else if (outputType != null) {
            // --output-type was already applied above (it forced `kind`); the extension can't
            // supply a format under it, so name what's ACTUALLY missing rather than re-suggesting
            // the flag the caller already passed (the old message's bug: it recommended
            // --output-type file even when --output-type was already set).
            throw new InvalidArgsException("-o " + destination + " has no recognized extension "
                    + "(.tsv/.jsonl/.parquet) and no --format was given; --output-type " + outputType
                    + " chose the destination kind, but only --format can supply the format itself "
                    + "-- pass --format explicitly");
        } else {
            throw new InvalidArgsException("-o " + destination + " has no recognized extension "
                    + "(.tsv/.jsonl/.parquet) and no --format was given; pass --format explicitly, "
                    + "use a file path with a known extension, or --output-type file to force a "
                    + "single-file destination without a matching extension");
        }
        if (kind == DestinationKind.DIRECTORY && resolved == OutputFormat.TABLE) {
            String correction = "pass --output-type file, or choose a recognized-extension path "
                    + "(.tsv/.jsonl/.parquet) with its matching --format";
            throw new InvalidArgsException("directory dataset output (-o " + destination + ") does not support "
                    + "--format table; " + correction);
        }
        resolvedKind = kind;
        resolvedFormat = resolved;
        resolveCompression(resolved);
        return new Resolved(resolved, kind);
    }

    /**
     * Re-derive {@link #resolvedKind} from the (possibly restored) {@link #destination} after
     * resume-context restore ({@link ListCommand#restoreRunContext}) — a bare {@code swath resume}
     * with no explicit {@code -o} restores the checkpointed destination AFTER {@link
     * #resolveOutput} already ran against the pre-restore (often stdout) destination. Valid swath
     * checkpoints can now record only stdout or directory-dataset destinations: FILE-kind runs
     * require {@code --checkpoint none}. A restored {@code .parquet} path is therefore a directory
     * dataset even though fresh path inference would call it FILE: the creating invocation may
     * have used {@code --output-type dir}, and the checkpoint schema does not record that override.
     * The caller first rejects recognized text extensions and ambiguous non-directory
     * {@code .parquet} paths against the checkpoint's recorded value. Anything reaching this method
     * is therefore stdout or a directory dataset, even when the directory has a recognized
     * {@code .parquet} suffix.
     */
    void recomputeKindAfterRestore() {
        if (isStdoutDestination()) {
            resolvedKind = DestinationKind.STDOUT;
            return;
        }
        resolvedKind = DestinationKind.DIRECTORY;
    }

    /**
     * Startup echo of the resolved destination -- the resolved choice is echoed at
     * startup so inference is never silent -- skipped for stdout (the common default has nothing
     * to confirm) and under {@code -q}/{@code --quiet}.
     */
    void echoResolvedOutput(Resolved resolved, PrintStream err, boolean quiet) {
        if (quiet || resolvedKind == DestinationKind.STDOUT) {
            return;
        }
        String what = resolvedKind == DestinationKind.DIRECTORY
                ? token(resolved.format()) + " dataset" : token(resolved.format());
        if (resolvedCompression != TextCompression.NONE) {
            what += " (" + resolvedCompression.name().toLowerCase(Locale.ROOT) + ")";
        }
        err.println("→ writing " + what + " to " + destination);
    }

    @Resume(ResumeClass.FREE)
    @Option(names = "--parquet-part-size", paramLabel = "SIZE", description = "Target Parquet part size (default: 256mb).")
    String partSize;

    @Resume(ResumeClass.FREE)
    @Option(names = "--text-part-size", paramLabel = "SIZE",
            description = "Target uncompressed text part size (default: 256mb).")
    String textPartSize;

    @Resume(ResumeClass.FREE)
    @Option(names = "--writeback-size", paramLabel = "SIZE",
            description = "Shape writeback for open TSV/JSONL/Parquet dataset parts and sorted Parquet finals without rotating "
                    + "(default: off; minimum: 4mb; does not change crash recovery).")
    String writebackSize;

    @Resume(ResumeClass.FREE)
    @Option(names = "--part-rotation-interval", paramLabel = "DURATION",
            description = "Rotate dataset parts by age (default: 30s; 0/none disables).")
    String partRotationInterval;

    @Resume(ResumeClass.FREE)
    @Option(names = "--part-rotation-max-rows", paramLabel = "N",
            description = "Rotate dataset parts by row count (default: 2000000; 0 disables).")
    Long partRotationMaxRows;

    @Resume(ResumeClass.FREE)
    @Option(names = "--report", paramLabel = "PATH",
            description = "Write the machine-readable run report to PATH.")
    String summaryJson;

    /**
     * {@code null} (unset) is the auto rule in {@link SummaryRenderer#shouldRender}; {@code
     * --stats} forces the block on a short run and under {@code -q}, {@code --no-stats} is the
     * true-silence switch. Not a tri-state option: nobody types {@code =auto}, and the negatable
     * pair spells the only two values a user ever needs (the shape {@code rg --stats} uses).
     */
    @Resume(ResumeClass.FREE)
    @Option(names = "--stats", negatable = true,
            description = "Print the end-of-run summary to stderr (default: on for runs over "
                    + "1.5s, runs that produce output, and runs that stop short of finishing; "
                    + "a closed downstream pipe stays silent).")
    Boolean stats;

    /**
     * {@code null} (unset) is the auto rule in {@link ProgressDisplay#shouldDisplay}; {@code
     * --progress} forces the live record on off a terminal and under {@code -q}, {@code
     * --no-progress} suppresses it everywhere. The negatable pair, and the Output heading, are
     * {@code --stats}'s: whether an operator surface prints is an output decision, while
     * {@code --progress-interval} — how often the run samples ITSELF — stays with the run controls.
     */
    @Resume(ResumeClass.FREE)
    @Option(names = "--progress", negatable = true,
            description = "Print live progress records to stderr (default: on when stderr is a "
                    + "terminal and neither -q nor -v was given; --progress-interval implies it).")
    Boolean progress;

    boolean noSummaryJson;

    String summaryJsonInterval;

    /** Contract §4.1 / §7 memory model: bounded, decoupled Parquet writers (default 3). */
    static final int MIN_PARQUET_WRITERS = 2;
    static final int MAX_PARQUET_WRITERS = SharedDatasetWriterPool.MAX_WRITERS;

    /**
     * Rotation cadence defaults: bound the resume {@code durable_cursor} lag to a small
     * fraction of a long run without materially raising peak writer-buffer memory. 30 s keeps the
     * at-risk (non-durable) window short even on a slow/sparse listing; 2M rows caps the same window
     * during bursts fast enough to write 2M small entries well inside 30 s, while staying far above
     * normal per-part row counts so typical runs still rotate on size/time, not rows.
     */
    static final Duration DEFAULT_PART_ROTATION_INTERVAL = Duration.ofSeconds(30);
    static final long DEFAULT_PART_ROTATION_MAX_ROWS = 2_000_000L;

    /**
     * Floor for a positive {@code --part-rotation-interval} (spin-storm defect):
     * the value feeds {@code lane.queue.poll(interval, NANOSECONDS)} in the writer pool's lane loop,
     * so an arbitrarily small positive duration timed out and immediately re-polled — a tight CPU
     * wakeup storm across every lane. {@code 0}/{@code none}/{@code off} still mean "disabled"; only
     * the {@code (0, MIN_PART_ROTATION_INTERVAL)} range is rejected. 100&nbsp;ms bounds an idle lane
     * to at most 10 wakeups/sec (40/sec across the 4-lane max) while staying at or below every
     * rotation-cadence value already exercised as legitimate.
     */
    static final Duration MIN_PART_ROTATION_INTERVAL = Duration.ofMillis(100);

    /** Shared default rotation target for Parquet and partitioned text parts. */
    static final long DEFAULT_PART_SIZE_BYTES = 256L * 1024 * 1024;

    /**
     * Resolve the Parquet writer-pool size, enforcing the contract's bounded memory model
     * (contract §4.1 / §7, I11). A {@code -o path.parquet} single-file destination
     * -- replacing the old {@code --single-file} flag -- collapses to one lane; otherwise
     * the established 2-4 release envelope is always accepted. Expert counts above four are
     * admitted only when the JVM maximum heap covers the conservative {@link
     * ParquetWriterMemoryPlan}; {@code 1} for a directory dataset is the single-file model requested
     * implicitly, which we reject so intent is explicit.
     */
    static int resolveParquetWriters(DestinationKind kind, int parquetWriters) throws InvalidConfigException {
        return resolveParquetWriters(kind, parquetWriters, Runtime.getRuntime().maxMemory());
    }

    static int resolveParquetWriters(DestinationKind kind, int parquetWriters, long maxHeapBytes)
            throws InvalidConfigException {
        if (kind == DestinationKind.FILE) {
            return 1;
        }
        if (parquetWriters < MIN_PARQUET_WRITERS || parquetWriters > MAX_PARQUET_WRITERS) {
            throw new InvalidConfigException("--tune parquet.writers must be between " + MIN_PARQUET_WRITERS
                    + " and " + MAX_PARQUET_WRITERS + " (got " + parquetWriters
                    + "); use -o <path>.parquet for a single output part");
        }
        int heapLimit = ParquetWriterMemoryPlan.maxWritersForHeap(maxHeapBytes);
        if (parquetWriters > heapLimit) {
            long planned = ParquetWriterMemoryPlan.plannedHeapBytes(parquetWriters);
            throw new InvalidConfigException("--tune parquet.writers=" + parquetWriters
                    + " needs a conservative heap plan of " + planned + " bytes, but this JVM's maximum heap is "
                    + maxHeapBytes + " bytes; use at most " + heapLimit + " writers, increase the JVM/container "
                    + "memory limit, or use --text-writers for a text dataset");
        }
        return parquetWriters;
    }

    int resolveTextWriters() throws InvalidConfigException {
        if (textWriters < TextWriterPoolConfig.MIN_WRITERS
                || textWriters > TextWriterPoolConfig.MAX_WRITERS) {
            throw new InvalidConfigException("--text-writers must be between "
                    + TextWriterPoolConfig.MIN_WRITERS + " and " + TextWriterPoolConfig.MAX_WRITERS
                    + " (got " + textWriters + ")");
        }
        return textWriters;
    }

    /** The Parquet part rotation target in bytes: {@code --parquet-part-size} if set, else 256 MB. */
    long partSizeBytes() throws InvalidConfigException, InvalidArgsException {
        return positivePartSize("--parquet-part-size",
                partSize != null ? SizeParser.parse(partSize) : DEFAULT_PART_SIZE_BYTES);
    }

    long textPartSizeBytes() throws InvalidConfigException, InvalidArgsException {
        return positivePartSize("--text-part-size",
                textPartSize != null ? SizeParser.parse(textPartSize) : DEFAULT_PART_SIZE_BYTES);
    }

    long writebackSizeBytes() throws InvalidConfigException, InvalidArgsException {
        if (writebackSize == null) {
            return 0L;
        }
        long bytes = SizeParser.parse(writebackSize);
        if (bytes == 0L) {
            return 0L;
        }
        if (bytes < PeriodicDataSync.MIN_INTERVAL_BYTES) {
            throw new InvalidConfigException("--writeback-size must be at least 4mb when enabled"
                    + " (got " + writebackSize + "); use 0 to disable it");
        }
        return bytes;
    }

    void validateWritebackTarget(OutputFormat format, boolean sorted)
            throws InvalidConfigException, InvalidArgsException {
        if (writebackSizeBytes() == 0L) {
            return;
        }
        boolean supportedDataset = resolvedKind == DestinationKind.DIRECTORY
                && (format == OutputFormat.TSV || format == OutputFormat.JSONL
                || format == OutputFormat.PARQUET);
        if (!supportedDataset || (sorted && format != OutputFormat.PARQUET)) {
            throw new InvalidConfigException("--writeback-size supports TSV/JSONL/Parquet directory "
                    + "datasets, including sorted Parquet final files");
        }
    }

    private static long positivePartSize(String option, long bytes) throws InvalidConfigException {
        if (bytes <= 0) {
            throw new InvalidConfigException(option + " must be greater than zero");
        }
        return bytes;
    }

    Duration resolvePartRotationInterval() throws InvalidConfigException {
        return partRotationInterval == null ? DEFAULT_PART_ROTATION_INTERVAL
                : parsePartRotationInterval(partRotationInterval);
    }

    static Duration parsePartRotationInterval(String raw) throws InvalidConfigException {
        Duration parsed = DurationParser.parse(raw, "part-rotation-interval", true);
        if (!parsed.isZero() && parsed.compareTo(MIN_PART_ROTATION_INTERVAL) < 0) {
            throw new InvalidConfigException("--part-rotation-interval must be >= "
                    + MIN_PART_ROTATION_INTERVAL + " (got " + raw
                    + "); use 0/none/off to disable the trigger instead of a near-zero value");
        }
        return parsed;
    }

    long resolvePartRotationMaxRows() throws InvalidConfigException {
        if (partRotationMaxRows == null) {
            return DEFAULT_PART_ROTATION_MAX_ROWS;
        }
        if (partRotationMaxRows < 0) {
            throw new InvalidConfigException(
                    "--part-rotation-max-rows must be >= 0 (got " + partRotationMaxRows + "); 0 disables it");
        }
        return partRotationMaxRows;
    }

    Path openParquetDir() throws InvalidConfigException, IOException {
        if (isStdoutDestination()) {
            throw new InvalidConfigException("Parquet output requires -o <dir> (a directory for the part files)");
        }
        Path dir = Path.of(destination);
        Files.createDirectories(dir);
        return dir;
    }

    Path openDatasetDir() throws InvalidConfigException, IOException {
        if (isStdoutDestination()) {
            throw new InvalidConfigException("dataset output requires -o <dir>");
        }
        Path dir = Path.of(destination);
        Files.createDirectories(dir);
        return dir;
    }

    /** The temp-sibling path {@link #openSink} stages a FILE-kind destination through; not committed
     * until {@link #commitFileSink()} runs, so a crash mid-run never leaves a partial file at the
     * REAL destination path -- single files are published atomically. */
    private Path pendingTempPath;
    private Path pendingRealPath;
    private Writer pendingWriter;

    @FunctionalInterface
    interface EncodedWriterFactory {
        Writer open(OutputStream stream) throws IOException;
    }

    EncodedWriterFactory encodedWriterFactoryOverride;

    /**
     * Open a streaming text output sink. Stdout is opened directly (fd 1). A FILE-kind {@code -o}
     * destination is staged through a hidden temp sibling — {@link #commitFileSink()}
     * atomically renames it into place, and MUST be called by the caller only after a successful
     * run (never on a crash/exception path), so an interrupted run leaves no partial file at the
     * real destination. Single-file destinations are non-resumable (refused in {@link
     * ListCommand#runWithCheckpoint}), so there is no append mode to support here.
     */
    Writer openSink() throws IOException {
        if (isStdoutDestination()) {
            return encode(new FileOutputStream(FileDescriptor.out));   // raw fd → broken pipe throws (INT-12)
        }
        Path real = Path.of(destination);
        Path name = real.getFileName();
        String prefix = "." + (name == null ? "swath-out" : name.toString()) + ".swath.";
        Path parent = real.getParent();
        Path temp = Files.createTempFile(parent == null ? Path.of(".") : parent, prefix, ".tmp");
        pendingTempPath = temp;
        pendingRealPath = real;
        OutputStream os = null;
        try {
            os = Files.newOutputStream(temp, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            Writer writer = encode(os);
            pendingWriter = writer;
            return writer;
        } catch (IOException | RuntimeException | Error e) {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            // createTempFile succeeded, so both raw-open and compression-wrapper failures obey
            // the zero-litter contract without leaking the descriptor or masking the primary error.
            cleanupFileSink();
            throw e;
        }
    }

    private Writer encode(OutputStream os) throws IOException {
        if (encodedWriterFactoryOverride != null) {
            return encodedWriterFactoryOverride.open(os);
        }
        OutputStream buffered = new BufferedOutputStream(os);
        OutputStream encoded = switch (resolvedCompression) {
            case NONE -> buffered;
            case GZIP -> new GZIPOutputStream(buffered);
            case ZSTD -> new ZstdOutputStream(buffered);
        };
        return new BufferedWriter(new OutputStreamWriter(encoded, StandardCharsets.UTF_8));
    }

    /**
     * Publish a FILE-kind single-file sink opened by {@link #openSink} — atomically renames the temp
     * sibling into place. Call ONLY after the writer has been closed following a run that completed
     * without throwing; a mid-run exception must skip this call so the real destination path is
     * simply never created. No-op for stdout or when no FILE-kind sink was opened.
     */
    void commitFileSink() throws IOException {
        if (pendingTempPath == null) {
            return;
        }
        try {
            // Closing finishes gzip/zstd frames before the staged file becomes visible.
            pendingWriter.close();
            pendingWriter = null;
            try {
                Files.move(pendingTempPath, pendingRealPath,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // Same-directory rename: the temp is fully closed before publication, so readers
                // never observe the partially-written staging contents.
                Files.move(pendingTempPath, pendingRealPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            cleanupFileSink();
        }
    }

    /** Best-effort cleanup for every failed FILE-kind attempt. Never masks the run's real failure. */
    void cleanupFileSink() {
        Path temp = pendingTempPath;
        pendingTempPath = null;
        pendingRealPath = null;
        pendingWriter = null;
        if (temp != null) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // Preserve the primary listing/output exception, matching other atomic writers.
            }
        }
    }

    /**
     * An explicit report path always wins; the internal suppression seam
     * disables the default sidecar; otherwise a directory output (parquet {@code -o <dir>}) gets one
     * next to the part files, and a stdout/single-file text run gets none.
     */
    Path resolveSummaryJsonPath(OutputFormat resolved) throws InvalidConfigException {
        if (summaryJson != null && noSummaryJson) {
            throw new InvalidConfigException("an explicit --report cannot be combined with sidecar suppression");
        }
        if (noSummaryJson) {
            return null;
        }
        if (summaryJson != null) {
            return Path.of(summaryJson);
        }
        if (resolved == OutputFormat.PARQUET && !isStdoutDestination()) {
            return Path.of(destination, DEFAULT_SUMMARY_JSON_NAME);
        }
        return null;
    }
}
