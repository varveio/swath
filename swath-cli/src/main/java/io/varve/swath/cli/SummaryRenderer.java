/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static io.varve.swath.cli.OperatorText.bytes;
import static io.varve.swath.cli.OperatorText.cost;
import static io.varve.swath.cli.OperatorText.count;
import static io.varve.swath.cli.OperatorText.elapsed;
import static io.varve.swath.cli.OperatorText.rate;

import io.varve.swath.observability.JsonRunSummaryWriter;
import io.varve.swath.observability.RunMetrics;
import io.varve.swath.observability.RunSummary;
import io.varve.swath.observability.RunSummarySink;
import io.varve.swath.observability.StopReason;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
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
 * with the rate it assumed when shown — see {@link OperatorText#cost}, the one formatter this block
 * and the live {@link ProgressDisplay} share so the two can never disagree.
 */
final class SummaryRenderer implements RunSummarySink {

    private static final Logger log = LoggerFactory.getLogger(SummaryRenderer.class);

    /**
     * How long a run must take before it earns an unrequested summary. Measured against {@link
     * RunSummary#sessionDuration()} — the operator's whole wall clock, seeding included — never
     * {@link RunSummary#duration()}, which starts only after a fresh run's seed step and so would
     * judge a long seed followed by a short listing as sub-threshold even though the operator sat
     * through the whole thing. Short enough that any run an operator actually waits on qualifies,
     * long enough that scripted sub-second listings stay silent (git's delayed-progress threshold
     * is the nearest anchor at 2 s).
     */
    static final Duration AUTO_MIN_ELAPSED = Duration.ofMillis(1_500);

    /**
     * How far {@link RunSummary#sessionDuration()} must exceed {@link RunSummary#duration()}
     * before the headline earns a second figure. {@code duration} is the listing clock (a fresh
     * run's zero point is set AFTER seeding); {@code sessionDuration} is the whole CLI invocation,
     * seeding included. A negligible seed (or a resumed run, where the two are always equal) keeps
     * the one-figure line rather than printing two near-identical numbers a second apart.
     */
    static final Duration SESSION_DELTA_MIN = Duration.ofSeconds(1);

    /**
     * The stops a {@code swath resume} genuinely picks up from — interruptions of a run that was
     * otherwise going fine. Deliberately an allow-list: a stop reason added later carries no resume
     * invitation until someone has confirmed a resume actually works for it.
     */
    private static final Set<StopReason> RESUMABLE_STOPS = EnumSet.of(
            StopReason.SIGNAL, StopReason.MAX_DURATION, StopReason.MAX_DURATION_NO_PROGRESS,
            StopReason.STUCK);

    /** The block's shared field separator — see {@link OperatorText}. */
    private static final String SEP = OperatorText.SEP;

    /**
     * What the CLI resolved, and the renderer cannot work out for itself. Read through a supplier
     * at emit time rather than captured when the sink is installed: a {@code swath resume <dir>}
     * only learns its destination — and therefore whether it is durable and what it is resumable
     * from — after the checkpoint's run context is restored, which happens long after the sink is
     * in place.
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
     * @param colorEnabled the resolved {@code --color}/{@code NO_COLOR}/{@code TERM}/{@code
     *         CLICOLOR_FORCE}/stderr-tty decision (§4.9, {@link AnsiPalette#resolveEnabled}) —
     *         resolved by the CLI, since only it knows the fd's terminal-ness
     */
    record Preferences(Boolean stats, boolean quiet, boolean durableDestination, boolean costKnown,
                       String resumeDestination, boolean colorEnabled) {
    }

    private final StderrCoordinator stderr;
    private final Supplier<Preferences> preferences;

    SummaryRenderer(StderrCoordinator stderr, Supplier<Preferences> preferences) {
        this.stderr = stderr;
        this.preferences = preferences;
    }

    @Override
    public void accept(RunSummary summary, RunMetrics.RunDiagnostics diagnostics,
            JsonRunSummaryWriter.TerminalStatus status) {
        // The block is the run's last word on this fd, so live progress ends PERMANENTLY here --
        // not merely interleaves safely. A tick already formatting when the run ended has its frame
        // dropped when it reaches the coordinator, so nothing lands after the summary.
        stderr.finishProgress();
        Preferences prefs = preferences.get();
        if (!shouldRender(prefs, summary, status)) {
            return;
        }
        try {
            List<String> content = lines(prefs, summary, diagnostics, status);
            AnsiPalette ansi = new AnsiPalette(prefs.colorEnabled());
            boolean hasDisposition = disposition(prefs, status) != null;
            stderr.record(err -> {
                for (int i = 0; i < content.size(); i++) {
                    err.println(OperatorText.INDENT + colorize(content.get(i), i, hasDisposition, ansi));
                }
            });
        } catch (RuntimeException e) {
            // The sink runs on the run's terminal path: a formatting fault must cost the operator
            // the block, never the run's disposition or exit code.
            log.debug("run_summary_render_failed message={}", e.getMessage());
        }
    }

    /**
     * The palette (§4.9), applied to one already-{@link #lines}-composed line by its role rather
     * than by restructuring how that method builds the text — {@code lines} stays plain, so its
     * own tests keep pinning exact, uncolored content. The disposition line (when {@link
     * #disposition} added one) is red only for the actual {@code INCOMPLETE} marker, never the
     * neutral broken-pipe wording; the first content line after it — objects/elapsed/rate, the
     * run's headline — gets the palette's one accent; every line after that (API calls, faults,
     * cost, output) is dimmed so the headline reads first.
     */
    private static String colorize(String line, int index, boolean hasDisposition, AnsiPalette ansi) {
        if (hasDisposition && index == 0) {
            return line.startsWith("INCOMPLETE") ? ansi.red(line) : ansi.dim(line);
        }
        boolean headline = hasDisposition ? index == 1 : index == 0;
        return headline ? ansi.accent(line) : ansi.dim(line);
    }

    /**
     * The gate. An explicit flag decides on its own — {@code --stats} forces the block past every
     * gate (terminal-ness, duration and {@code -q} alike), {@code --no-stats} suppresses it
     * everywhere. Otherwise a run earns a summary automatically when it ran long enough to be
     * waited on, produced durable output, or stopped for any reason other than finishing:
     *
     * <pre>auto = (session elapsed &gt; 1.5s OR durable dataset produced OR stopped abnormally) AND NOT quiet</pre>
     *
     * <p>The elapsed clause is keyed to {@link RunSummary#sessionDuration()}, not {@link
     * RunSummary#duration()}: this gate's question is "did a human wait on this?", and it is the
     * operator's whole wall clock — seeding included — that answers it, not the listing-only span
     * a fresh run's seed step excludes. {@code sessionDuration} falls back to {@code duration} when
     * no session-wide reporter ever claimed the run (a pre-seed early exit), so that case is
     * unaffected.
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
        return summary.sessionDuration().compareTo(AUTO_MIN_ELAPSED) > 0
                || (prefs.durableDestination() && summary.outputFiles() > 0)
                || status.reason() != StopReason.COMPLETED;
    }

    /** The block's lines, without the indent — the shape the tests pin. */
    static List<String> lines(Preferences prefs, RunSummary summary,
            RunMetrics.RunDiagnostics diagnostics, JsonRunSummaryWriter.TerminalStatus status) {
        List<String> lines = new ArrayList<>();
        String disposition = disposition(prefs, status);
        if (disposition != null) {
            lines.add(disposition);
        }
        if (status.reason() == StopReason.RESUME_REFUSED) {
            // A refused resume never started the engine, so the disposition IS the whole record: a
            // statistics body would be a block of zeros claiming to describe a run that never
            // happened. This is the only stop reason with nothing behind it -- every other early
            // exit at least got as far as seeding.
            return lines;
        }
        lines.add(headline(summary));
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
        // No LIST call, no bill: a merge-only resume republishes staged output without fetching a
        // single page, and "~$0.000" would be noise dressed up as an estimate.
        if (prefs.costKnown() && summary.apiCalls() > 0) {
            lines.add(cost(summary.costUsd()));
        }
        String output = output(summary);
        if (output != null) {
            lines.add(output);
        }
        return lines;
    }

    /**
     * The block's headline: objects, the LISTING clock {@code keys_per_sec} is keyed to, and the
     * rate itself — plus, when a fresh run's seed step made the whole session materially longer than
     * that (§ {@link #SESSION_DELTA_MIN}), the session total too, so the two adjacent numbers an
     * operator sees (this line and the live progress line, which is session-scoped) never disagree
     * without explanation. {@code keys/s} sits directly after the {@code listing} figure — not the
     * {@code total} one — so which of the two the rate divides by is never left for the reader to
     * guess.
     */
    private static String headline(RunSummary summary) {
        Duration listing = summary.duration();
        Duration session = summary.sessionDuration();
        boolean materialSeed = session.minus(listing).compareTo(SESSION_DELTA_MIN) > 0;
        String line = count(summary.objects()) + " objects in " + elapsed(listing)
                + (materialSeed ? " listing" : "")
                + SEP + count(Math.round(summary.keysPerSecond())) + " keys/s";
        return materialSeed ? line + SEP + elapsed(session) + " total" : line;
    }

    /**
     * The leading disposition line: the {@code INCOMPLETE} marker (with the reason the JSON report
     * records, and a resume invitation when this run left something to resume) for a run that
     * stopped short, neutral wording for a broken pipe, and nothing at all for a clean run.
     *
     * <p>A crash and a seed failure get the marker but never the invitation. A signal, a timebox
     * and a stuck run — including a seed-time {@code STUCK}, which commits no nodes and is
     * re-seeded by the next resume — are all interruptions of a run that was otherwise going fine,
     * so resuming picks up where it left off. A crash is a deterministic in-process failure (a
     * corrupt segment) that a resume will simply hit again, and a seed failure (a denied or missing
     * bucket) marks the run fatal, so a resume would be REFUSED outright; inviting either is advice
     * that wastes the operator's time.
     */
    private static String disposition(Preferences prefs, JsonRunSummaryWriter.TerminalStatus status) {
        if (isBrokenPipe(status)) {
            return "stopped early — downstream closed";
        }
        if (status.reason() == StopReason.COMPLETED) {
            return null;
        }
        String marker = "INCOMPLETE (" + status.reason().wireName() + ")";
        return prefs.resumeDestination() == null || !RESUMABLE_STOPS.contains(status.reason())
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

    /** A broken pipe carries no stop reason — the downstream closed, which is not a failure. */
    private static boolean isBrokenPipe(JsonRunSummaryWriter.TerminalStatus status) {
        return status.reason() == null;
    }

}
