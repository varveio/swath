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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The operator-facing live-progress record on stderr — what the person watching a long run sees
 * while it is still going, rendered from the {@link ProgressEvent} the engine builds once per tick
 * (installed as the run's {@link ProgressSink}). It computes no figure of its own beyond
 * formatting, and shares every number format with the end-of-run block through {@link
 * OperatorText}, so the two surfaces cannot spell the same figure two ways.
 *
 * <p><b>Plain, newline-terminated records.</b> No carriage return, no clear-to-end-of-line, no
 * terminal-width measurement: an in-place redraw is a separate, deliberately deferred piece of
 * work, and a redirected stderr must never receive control sequences. One frame is one complete
 * line, written and flushed under the {@link StderrCoordinator}'s lock.
 *
 * <p><b>Shaped by the phase.</b> Seeding shows probes against the seed's bounded budget and the age
 * of the last completed probe — the pair that tells a healthy seed from a hung one, and the case
 * that motivated this display: a 22-second seed used to look exactly like a hang. Listing shows
 * counters and rates and deliberately no bar, percentage or ETA (an unsorted scan has no honest
 * denominator). Merging shows rows merged against the exact staged row total.
 *
 * <p><b>No key text.</b> Nothing an S3 bucket controls reaches this line: keys can carry arbitrary
 * bytes, and a display that echoed them could be made to forge a terminal record. Every field here
 * is a number, a duration or a phase name. Any future key sample must go through {@code
 * RunMetrics}'s {@code display()}/{@code ControlCharEscaper} path and stay off the default line.
 */
final class ProgressDisplay implements ProgressSink, AutoCloseable {

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
    private final boolean costKnown;

    /**
     * @param costKnown whether the provider's LIST pricing is knowable — false under {@code
     *         --endpoint-url}, where any {@code $} would be a guess, exactly as the end-of-run
     *         block treats it
     */
    ProgressDisplay(StderrCoordinator stderr, boolean costKnown) {
        this.channel = stderr.openProgress();
        this.costKnown = costKnown;
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
            channel.frame(OperatorText.INDENT + line(event, costKnown));
        } catch (RuntimeException e) {
            // The sink runs on the run's progress thread: a formatting fault must cost the operator
            // a frame, never the run's disposition or exit code.
            log.debug("progress_render_failed message={}", e.getMessage());
        }
    }

    /** Whether a frame would be written at all — the gate that skips building the event. */
    @Override
    public boolean isEnabled() {
        return channel.isActive();
    }

    /** Ends this display's progress generation. Idempotent; nothing writes after it. */
    @Override
    public void close() {
        channel.close();
    }

    /** One frame's text, without the indent — the shape the tests pin. */
    static String line(ProgressEvent event, boolean costKnown) {
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
        // The same rule the end-of-run block applies: no LIST call, no bill, and no figure at all
        // when the provider is unknown -- so the live line and the final block never disagree.
        if (costKnown && event.apiCalls() > 0) {
            parts.add(OperatorText.cost(event.estimatedCostUsd()));
        }
        return String.join(OperatorText.SEP, parts);
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

    /** Rows merged against the staged total — an exact denominator, so this phase gets a figure. */
    private static List<String> merging(ProgressEvent event) {
        ProgressEvent.Merging merging = event.merging();
        String rows = event.completion() != null
                ? completion(event.completion(), "rows")
                : OperatorText.count(merging.sessionRowsMerged()) + " rows";
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
