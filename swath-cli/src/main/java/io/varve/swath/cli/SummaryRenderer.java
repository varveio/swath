/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.RunSummarySink;
import io.varve.swath.observability.StopReason;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator-facing end-of-run summary block on stderr — what the person who ran the command
 * gets to see, rendered from the very {@link RunSummary} the {@code --report} JSON and the {@code
 * -v} {@code list_run_summary} line are written from (installed as the run's {@link
 * RunSummarySink}). It computes no figure of its own beyond formatting.
 *
 * <p>Whether the block prints is decided by {@link #shouldRender}, never by terminal detection:
 * {@code isatty} answers "can I colorize and redraw?", not "does this operator deserve to know what
 * happened?". A run whose stderr is a file, a pipe, or a CI log gets exactly the same content —
 * which is the case that matters most, since for an overnight or fleet run the captured log IS the
 * artifact. This follows {@code dd}/{@code wget}/{@code rsync}, whose completion stats reach a
 * redirected stderr unconditionally, rather than {@code rg}/{@code fd}, whose peer group is
 * sub-second search.
 *
 * <p>Cost is withheld entirely when the provider is unknown ({@code --endpoint-url}), and labeled
 * with the rate it assumed when shown — see {@link RunMetrics#LIST_COST_PER_1K_USD}.
 */
final class SummaryRenderer implements RunSummarySink {

    private static final Logger log = LoggerFactory.getLogger(SummaryRenderer.class);

    /**
     * How long a run must take before it earns an unrequested summary. Short enough that any run
     * an operator actually waits on qualifies, long enough that scripted sub-second listings stay
     * silent (git's delayed-progress threshold is the nearest anchor at 2 s).
     */
    static final Duration AUTO_MIN_ELAPSED = Duration.ofMillis(1_500);

    /** Two-space indent, so the block reads as one unit distinct from any log or echo line. */
    private static final String INDENT = "  ";

    /** The field separator, matching the progress/summary idiom of modern CLIs. */
    private static final String SEP = " · ";

    /**
     * What the CLI resolved before the run started, and the renderer cannot work out for itself.
     *
     * @param stats the {@code --stats}/{@code --no-stats} flag: {@code null} = auto (§ {@link
     *         #shouldRender}), {@code TRUE} = always (bypassing every gate), {@code FALSE} = never
     * @param quiet whether {@code -q}/{@code --quiet} was given — suppresses the auto summary only
     * @param durableDestination whether the run writes to a file or dataset rather than stdout, so
     *         producing output is itself a reason to report
     * @param costKnown whether the provider's LIST pricing is knowable — false under {@code
     *         --endpoint-url} (MinIO/R2/LocalStack/self-hosted), where any {@code $} would be a guess
     * @param resumeDestination the destination a partial run can be resumed from, or {@code null}
     *         when this run left nothing resumable
     */
    record Preferences(Boolean stats, boolean quiet, boolean durableDestination, boolean costKnown,
                       String resumeDestination) {
    }

    private final PrintStream err;
    private final Preferences prefs;

    SummaryRenderer(PrintStream err, Preferences prefs) {
        this.err = err;
        this.prefs = prefs;
    }

    @Override
    public void accept(RunSummary summary, RunMetrics.RunDiagnostics diagnostics,
            JsonRunSummaryWriter.TerminalStatus status) {
        if (!shouldRender(prefs, summary, status)) {
            return;
        }
        try {
            for (String line : lines(summary, diagnostics, status)) {
                err.println(INDENT + line);
            }
            err.flush();
        } catch (RuntimeException e) {
            // The sink runs on the run's terminal path: a formatting fault must cost the operator
            // the block, never the run's disposition or exit code.
            log.debug("run_summary_render_failed message={}", e.getMessage());
        }
    }

    /**
     * The gate. An explicit flag decides on its own — {@code --stats} forces the block past every
     * gate (terminal-ness, duration and {@code -q} alike), {@code --no-stats} suppresses it
     * everywhere. Otherwise a run earns a summary automatically when it ran long enough to be
     * waited on, produced durable output, or stopped for any reason other than finishing:
     *
     * <pre>auto = (elapsed &gt; 1.5s OR durable dataset produced OR stopped abnormally) AND NOT quiet</pre>
     *
     * <p>A broken pipe is excluded: {@code swath list | head} is the most ordinary interactive
     * workflow there is, it exits 0, and it must not be dressed up as an incident.
     */
    static boolean shouldRender(Preferences prefs, RunSummary summary,
            JsonRunSummaryWriter.TerminalStatus status) {
        if (prefs.stats() != null) {
            return prefs.stats();
        }
        if (prefs.quiet() || isBrokenPipe(status)) {
            return false;
        }
        return summary.duration().compareTo(AUTO_MIN_ELAPSED) > 0
                || (prefs.durableDestination() && summary.outputFiles() > 0)
                || status.reason() != StopReason.COMPLETED;
    }

    /** The block's lines, without the indent — the shape the tests pin. */
    List<String> lines(RunSummary summary, RunMetrics.RunDiagnostics diagnostics,
            JsonRunSummaryWriter.TerminalStatus status) {
        List<String> lines = new ArrayList<>();
        String disposition = disposition(status);
        if (disposition != null) {
            lines.add(disposition);
        }
        lines.add(count(summary.objects()) + " objects in " + elapsed(summary.duration())
                + SEP + count(Math.round(summary.keysPerSecond())) + " keys/s");
        lines.add(count(summary.apiCalls()) + " API calls"
                + SEP + rate(summary.apiCallsPer1kObjects()) + " per 1k objects"
                + SEP + "in flight avg " + rate(summary.avgInFlight())
                + SEP + "peak " + count(summary.peakInFlight()));
        // Only when something actually went wrong: a clean run keeps a short block, and the line's
        // presence is the signal. This is where the retried transients that used to reach stderr as
        // WARNs now surface -- counted, in one place, instead of line by line.
        long throttled = diagnostics.throttleEvents();
        long retried = diagnostics.transientEvents();
        if (throttled > 0 || retried > 0 || summary.errors() > 0) {
            lines.add("throttled " + count(throttled) + SEP + "retried " + count(retried)
                    + SEP + "errors " + count(summary.errors()));
        }
        if (prefs.costKnown()) {
            lines.add(cost(summary.costUsd()));
        }
        String output = output(summary);
        if (output != null) {
            lines.add(output);
        }
        return lines;
    }

    /**
     * The leading disposition line: the {@code INCOMPLETE} marker (with the reason the JSON report
     * records, and a resume invitation when this run left something to resume) for a run that
     * stopped short, neutral wording for a broken pipe, and nothing at all for a clean run.
     */
    private String disposition(JsonRunSummaryWriter.TerminalStatus status) {
        if (isBrokenPipe(status)) {
            return "stopped early — downstream closed";
        }
        if (status.reason() == StopReason.COMPLETED) {
            return null;
        }
        String marker = "INCOMPLETE (" + status.reason().wireName() + ")";
        return prefs.resumeDestination() == null
                ? marker
                : marker + " — resume: swath resume " + prefs.resumeDestination();
    }

    /** Output volume and the run's memory high-water mark, each omitted when it has no value. */
    private static String output(RunSummary summary) {
        List<String> parts = new ArrayList<>();
        if (summary.outputFiles() > 0) {
            parts.add(count(summary.outputFiles()) + (summary.outputFiles() == 1 ? " file" : " files"));
        }
        if (summary.compressedBytes() > 0) {
            parts.add(bytes(summary.compressedBytes()) + " written");
        }
        if (summary.peakRssBytes() > 0) {
            parts.add("peak RSS " + bytes(summary.peakRssBytes()));
        }
        return parts.isEmpty() ? null : String.join(SEP, parts);
    }

    /**
     * The estimated spend WITH the rate it assumed. Stating the assumption is the honesty: LIST
     * pricing varies by region, so a labeled reference rate lets a reader rescale, where a bare
     * figure would over-claim (and a region→rate table in an OSS repo would silently go stale).
     */
    private static String cost(double usd) {
        String amount = usd > 0.0 && usd < 0.001
                ? "<$0.001"
                : String.format(Locale.ROOT, "~$%.3f", usd);
        return amount + " (est. @ $" + RunMetrics.LIST_COST_PER_1K_USD + "/1k LIST)";
    }

    /** A broken pipe carries no stop reason — the downstream closed, which is not a failure. */
    private static boolean isBrokenPipe(JsonRunSummaryWriter.TerminalStatus status) {
        return status.reason() == null;
    }

    private static String count(long value) {
        return String.format(Locale.ROOT, "%,d", value);
    }

    private static String rate(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }

    /** {@code 4.2s} under a minute, then {@code 4m12s}, then {@code 1h04m}. */
    private static String elapsed(Duration duration) {
        long seconds = duration.toSeconds();
        if (seconds < 60) {
            return String.format(Locale.ROOT, "%.1fs", duration.toMillis() / 1000.0);
        }
        if (seconds < 3600) {
            return String.format(Locale.ROOT, "%dm%02ds", seconds / 60, seconds % 60);
        }
        return String.format(Locale.ROOT, "%dh%02dm", seconds / 3600, (seconds % 3600) / 60);
    }

    /** Decimal (1000-based) units, the same convention S3 itself bills and reports in. */
    private static String bytes(long value) {
        String[] units = {"B", "KB", "MB", "GB", "TB", "PB"};
        double scaled = value;
        int unit = 0;
        while (scaled >= 1000.0 && unit < units.length - 1) {
            scaled /= 1000.0;
            unit++;
        }
        return unit == 0
                ? count(value) + " B"
                : String.format(Locale.ROOT, "%.1f %s", scaled, units[unit]);
    }
}
