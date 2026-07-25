/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import io.varve.swath.observability.ProgressEvent;
import io.varve.swath.observability.ProgressSink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator-facing live-progress record on stderr — what the person watching a long run sees
 * while it is still going, rendered from the {@link ProgressEvent} the engine builds once per tick
 * (installed as the run's {@link ProgressSink}). It computes no figure of its own beyond
 * formatting, and shares every number format with the end-of-run block through {@link
 * OperatorText}, so the two surfaces cannot spell the same figure two ways.
 *
 * <p><b>Two forms, one content.</b> On a terminal wide enough to say so, a frame overwrites its
 * predecessor in place — carriage return, the text, clear-to-end-of-line, no newline — so a long
 * run occupies one line rather than scrolling the session away. Everywhere else the same text is a
 * plain newline-terminated record: a redirected stderr must never receive a control sequence, and a
 * captured log of carriage returns is not a log. The two differ in framing alone; no field appears
 * in one and not the other, so a run's captured output says exactly what its terminal did. Both are
 * written and flushed under the {@link StderrCoordinator}'s lock, which for the redrawing form also
 * erases and repaints around every other writer of the fd.
 *
 * <p><b>Shaped by the phase.</b> Seeding shows probes against the seed's bounded budget and the age
 * of the last completed probe — the pair that tells a healthy seed from a hung one, and the case
 * that motivated this display: a 22-second seed used to look exactly like a hang. Listing shows
 * counters and rates and deliberately no bar, percentage or ETA (an unsorted scan has no honest
 * denominator). A merge shows the rows it has moved, and a percentage only for its final pass —
 * the one pass whose denominator is exactly the staged row total.
 *
 * <p><b>It renders; it does not decide.</b> Whether progress runs, when it stops and whether a cost
 * figure exists are all settled before an event reaches here (see {@link ProgressSink}), so this
 * class cannot be the reason a run that asked for silence got a line.
 *
 * <p><b>No key text.</b> Nothing an S3 bucket controls reaches this line: keys can carry arbitrary
 * bytes, and a display that echoed them could be made to forge a terminal record. Every field here
 * is a number, a duration or a phase name. Any future key sample must go through {@code
 * RunMetrics}'s {@code display()}/{@code ControlCharEscaper} path and stay off the default line.
 */
final class ProgressDisplay implements ProgressSink {

    private static final Logger log = LoggerFactory.getLogger(ProgressDisplay.class);

    /**
     * What the CLI resolved about whether this run should show live progress. Unlike the end-of-run
     * block's preferences these are all known when the sink is installed: nothing here changes when
     * a resume restores its run context.
     *
     * @param progress the {@code --progress}/{@code --no-progress} flag: {@code null} = auto (§
     *         {@link #shouldDisplay}), {@code TRUE} = always, {@code FALSE} = never
     * @param quiet whether {@code -q}/{@code --quiet} was given — suppresses auto progress only
     * @param verbose whether {@code -v} or higher was given, which turns INFO logging on and makes
     *         the structured {@code progress} log record the run's progress surface
     * @param intervalExplicit whether {@code --progress-interval} was passed, which is a request
     *         for progress at that cadence and therefore opts in on its own
     * @param stderrIsTerminal whether fd 2 is a terminal — the fd this display writes to, never
     *         stdout's terminal-ness
     */
    record Preferences(Boolean progress, boolean quiet, boolean verbose, boolean intervalExplicit,
                       boolean stderrIsTerminal) {
    }

    private final StderrCoordinator.ProgressChannel channel;

    /**
     * Where the terminal's width comes from, per frame. Injected so the redraw path is testable
     * without a pty: production passes {@link TerminalGeometry#stderrWidth}, a test passes a fixed
     * width or {@link TerminalGeometry#UNKNOWN}.
     */
    private final IntSupplier width;

    /** Whether frames overwrite in place. False makes this exactly the plain-record display. */
    private final boolean redraw;

    ProgressDisplay(StderrCoordinator stderr, boolean redraw, IntSupplier width) {
        this.redraw = redraw;
        this.width = width;
        this.channel = stderr.openProgress(redraw);
    }

    /**
     * The run's progress sink — ONE decision, so no surface is governed by accident. {@code
     * --no-progress} means {@link ProgressSink#NONE}: not "no display", which would leave the
     * structured {@code progress} log record running and hand an INFO-enabled run the very output
     * it asked to be spared. Otherwise the display renders when {@link #shouldDisplay} says so, and
     * every other run keeps the structured record — the surface a supervisor tailing the log reads,
     * and the only one a redirected run has.
     */
    static ProgressSink sinkFor(Preferences prefs, StderrCoordinator stderr) {
        return sinkFor(prefs, stderr, TerminalGeometry::stderrWidth);
    }

    static ProgressSink sinkFor(Preferences prefs, StderrCoordinator stderr, IntSupplier width) {
        if (Boolean.FALSE.equals(prefs.progress())) {
            return ProgressSink.NONE;
        }
        if (!shouldDisplay(prefs)) {
            return ProgressSink.LOG;
        }
        return new ProgressDisplay(stderr, shouldRedraw(prefs, width), width);
    }

    /**
     * Whether frames overwrite in place rather than each taking a line. A narrower gate than {@link
     * #shouldDisplay} on purpose: {@code --progress} forces progress to <em>appear</em> off a
     * terminal, but nothing forces control sequences onto a stream that cannot act on them, so a
     * redirected stderr keeps the plain records whatever the flag says. {@code TERM=dumb} is
     * excluded on the same reasoning the colour resolution uses — a terminal that disclaims
     * capability is taken at its word.
     *
     * <p>Width is the last condition because it is the one that can fail at runtime: no provider, a
     * terminal that will not report, or one too narrow to say anything useful in
     * ({@link TerminalGeometry#MIN_USABLE_WIDTH}) all fall back rather than emit an erase sequence
     * whose effect cannot be predicted.
     */
    static boolean shouldRedraw(Preferences prefs, IntSupplier width) {
        if (!prefs.stderrIsTerminal() || "dumb".equals(System.getenv("TERM"))) {
            return false;
        }
        return width.getAsInt() >= TerminalGeometry.MIN_USABLE_WIDTH;
    }

    /**
     * The gate. An explicit flag decides on its own — {@code --progress} forces the display past
     * every gate (terminal-ness and {@code -q} alike, exactly as {@code --stats} does),
     * {@code --no-progress} suppresses it everywhere. An explicit {@code --progress-interval} is
     * itself an opt-in: asking for a cadence asks for the thing that has a cadence.
     *
     * <p>Otherwise:
     *
     * <pre>auto = stderr is a TTY AND NOT quiet AND NOT verbose</pre>
     *
     * <p>The summary block's "form, not whether" rule deliberately does NOT transfer here: a
     * summary prints once and belongs in a captured log, while progress repeats, so off a terminal
     * it appears only when it was actually asked for. Verbosity is in the ladder because {@code -v}
     * turns on the structured {@code progress} log record, which IS this run's progress surface for
     * whoever is tailing that log; installing a display replaces that record rather than adding to
     * it, so one tick always renders exactly once either way.
     */
    static boolean shouldDisplay(Preferences prefs) {
        if (prefs.progress() != null) {
            return prefs.progress();
        }
        if (prefs.intervalExplicit()) {
            return true;
        }
        return prefs.stderrIsTerminal() && !prefs.quiet() && !prefs.verbose();
    }

    @Override
    public void accept(ProgressEvent event) {
        try {
            channel.frame(redraw
                    ? fit(parts(event), width.getAsInt())
                    : OperatorText.INDENT + line(event));
        } catch (RuntimeException e) {
            // The sink runs on the run's progress thread: a formatting fault must cost the operator
            // a frame, never the run's disposition or exit code.
            log.debug("progress_render_failed message={}", e.getMessage());
        }
    }

    /**
     * Bound a redrawing frame to the terminal's width, by dropping whole fields off the end rather
     * than cutting through one. A frame that wraps occupies two physical rows while the erase that
     * follows it reaches only one, so the remainder is stranded on screen and the display walks
     * down it — the single failure an in-place redraw has to prevent. But {@code 8 API ca} is a
     * frame that looks broken, and the fields are ordered most-important-first precisely so the
     * ones that fall off the end are the ones worth losing.
     *
     * <p>Width is re-read every frame rather than tracked through a {@code WINCH} handler, so a
     * terminal resized mid-run is honoured by the next frame and swath installs no signal handler
     * for a display. {@link TerminalGeometry#UNKNOWN} bounds nothing: a width that stopped being
     * knowable mid-run leaves one frame that may wrap, which beats a frame cut to nothing.
     *
     * <p>Measured in {@code String.length()} — UTF-16 units, not display columns. That is exact
     * here and only here: every field is a number, a duration or an ASCII phase name, by the
     * class's no-key-text rule. A field carrying arbitrary text would need real column measurement,
     * and would be the reason to reach for one.
     */
    static String fit(List<String> parts, int width) {
        if (width == TerminalGeometry.UNKNOWN) {
            return OperatorText.INDENT + String.join(OperatorText.SEP, parts);
        }
        StringBuilder frame = new StringBuilder(OperatorText.INDENT);
        for (String part : parts) {
            int separator = frame.length() == OperatorText.INDENT.length() ? 0 : OperatorText.SEP.length();
            if (frame.length() + separator + part.length() > width) {
                break;
            }
            if (separator > 0) {
                frame.append(OperatorText.SEP);
            }
            frame.append(part);
        }
        if (frame.length() > OperatorText.INDENT.length()) {
            return frame.toString();
        }
        // A terminal too narrow for even the phase name. Nothing here is worth showing, but a
        // wrapping frame would still strand rows, so this one case does cut mid-field.
        return truncate(OperatorText.INDENT + parts.getFirst(), width);
    }

    /** Hard cut, for the one case {@link #fit} cannot solve by dropping a field. */
    static String truncate(String frame, int width) {
        if (width == TerminalGeometry.UNKNOWN || frame.length() <= width) {
            return frame;
        }
        return frame.substring(0, width);
    }

    /** Whether a frame would be written at all — the gate that skips building the event. */
    @Override
    public boolean isEnabled() {
        return channel.isActive();
    }

    /**
     * Whether this display overwrites its frames in place. Read by the CLI to settle the run's tick
     * cadence, so that decision is taken from the display actually installed rather than derived a
     * second time from the same inputs and free to disagree with it.
     */
    boolean redraws() {
        return redraw;
    }

    /** One frame's text, without the indent — the shape the tests pin. */
    static String line(ProgressEvent event) {
        return String.join(OperatorText.SEP, parts(event));
    }

    /**
     * A frame's fields, most-important-first: the phase and its own counters, then the figures
     * every phase shares. The order is what {@link #fit} drops from, so it is a display decision
     * and not merely an assembly one — elapsed and API calls go last because a narrow terminal can
     * spare them, and the phase goes first because a frame that cannot say what the run is doing
     * says nothing at all.
     */
    static List<String> parts(ProgressEvent event) {
        List<String> parts = new ArrayList<>();
        parts.add(event.phase().name().toLowerCase(Locale.ROOT));
        if (event.seeding() != null) {
            parts.addAll(seeding(event));
        } else if (event.listing() != null) {
            parts.addAll(listing(event.listing()));
        } else if (event.merging() != null) {
            parts.addAll(merging(event));
        }
        parts.add(OperatorText.elapsed(event.sessionElapsed()) + " elapsed");
        parts.add(OperatorText.count(event.apiCalls()) + " API calls");
        // The same rule the end-of-run block applies: no LIST call, no bill — and no figure exists
        // at all when the provider is unknown, so the live line and the final block never disagree.
        if (event.estimatedCostUsd() != null && event.apiCalls() > 0) {
            parts.add(OperatorText.cost(event.estimatedCostUsd()));
        }
        return parts;
    }

    /**
     * Probes against the seed's budget, plus the age of the most recently completed one. The age is
     * the liveness signal — every other seed-time counter reads zero whether the seed is healthy or
     * wedged.
     */
    private static List<String> seeding(ProgressEvent event) {
        ProgressEvent.Seeding seeding = event.seeding();
        String probes = event.completion() != null
                ? completion(event.completion(), "probes")
                : OperatorText.count(seeding.probesCompleted()) + " probes";
        return List.of(probes, "last probe " + age(seeding.sinceLastProbe()));
    }

    /** Counters and rates only: no bar, no percentage, no ETA — see the class javadoc. */
    private static List<String> listing(ProgressEvent.Listing listing) {
        // Session vs recovered work, never silently added together: a merge-only or reattach resume
        // carries rows this process did not list, and folding them in would show a jump the run
        // never made.
        String objects = OperatorText.count(listing.sessionObjects()) + " objects";
        if (listing.recoveredObjects() > 0) {
            objects += " (+" + OperatorText.count(listing.recoveredObjects()) + " recovered)";
        }
        return List.of(objects,
                OperatorText.count(Math.round(listing.liveObjectsPerSecond())) + " keys/s (avg "
                        + OperatorText.count(Math.round(listing.averageObjectsPerSecond())) + ")",
                OperatorText.count(listing.pages()) + " pages",
                OperatorText.count(listing.inFlight()) + " in flight");
    }

    /**
     * The final pass's rows against the staged total — an exact denominator, so that pass gets a
     * figure. A cascade pass rewrites every staged row again, so it reports the work it has done
     * and no percentage rather than a fraction that would run past 100%.
     */
    private static List<String> merging(ProgressEvent event) {
        ProgressEvent.Merging merging = event.merging();
        String rows = event.completion() != null
                ? completion(event.completion(), "rows")
                : OperatorText.count(merging.sessionRowsMerged()) + " rows merged";
        return List.of(rows, OperatorText.count(merging.segments()) + " segments");
    }

    /** {@code 12/64 probes (19%)} — only ever built where the denominator is exact. */
    private static String completion(ProgressEvent.Completion completion, String unit) {
        return OperatorText.count(completion.done()) + "/" + OperatorText.count(completion.total())
                + " " + unit + " (" + Math.round(completion.fraction() * 100) + "%)";
    }

    private static String age(Duration duration) {
        return OperatorText.elapsed(duration) + " ago";
    }
}
