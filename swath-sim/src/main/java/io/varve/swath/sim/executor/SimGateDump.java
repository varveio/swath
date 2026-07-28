/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.executor;

import io.varve.swath.engine.policy.OwnerSplitGateInputs;
import io.varve.swath.engine.policy.VictimScan;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;

/**
 * A write-only dump of the readings a run's two gate chains actually read: one TSV row per
 * {@code OwnerSplitGovernor.decide} and one per {@code ThiefPolicy.selectVictim} pass. The columns
 * are the engine's own {@link OwnerSplitGateInputs} and {@link VictimScan} payloads — the same
 * numbers the engine emits as its {@code owner_split_decision} and {@code victim_scan} trace events
 * (docs/internals/metrics-internals.md §7) — so a simulated run's inputs can be diffed row for row
 * against a replay-server trace of the same listing, which is the only way to localise a sim-vs-engine
 * divergence to the gate that produced it.
 *
 * <p><b>An observer, never a participant.</b> Nothing here is read back by the executor and nothing
 * here mutates anything a decision sees: a run with the dump on takes exactly the decisions it takes
 * with the dump off. It is off unless {@value #DUMP_PATH_PROPERTY} names a path, and off means
 * <em>absent</em> rather than disabled — {@link #fromSystemProperties()} returns {@code null} and the
 * executor's call sites are guarded on it, so a run that is not dumping does not format a row, render
 * a key, or read the clock for one.
 *
 * <p><b>Time is the run's own.</b> Every row is stamped with the virtual instant the deciding actor
 * was at, taken from {@code SimContext.nowNanos()} and handed in — this class never reaches for a
 * clock, which is the module-wide rule {@code SimAmbientSourceGuardTest} enforces.
 *
 * <p><b>It fails loudly or not at all.</b> The files are opened {@link StandardOpenOption#CREATE_NEW},
 * so a dump can never truncate the one before it, and every IO failure — on open, on write, on close —
 * is rethrown as an {@link UncheckedIOException} that fails the run. A diagnosis artifact that
 * silently stops halfway is worse than none: the analysis it feeds would read the truncation as a
 * finding.
 */
public final class SimGateDump implements AutoCloseable {

    /**
     * The system property naming where the owner-decision TSV is written; absent or blank means no
     * dump. Follows the {@code swath.sim.*} property idiom the store configuration already uses —
     * these tools sit outside swath's CLI config system.
     */
    public static final String DUMP_PATH_PROPERTY = "swath.sim.gate-dump";

    /** What the victim-scan TSV's path adds to {@value #DUMP_PATH_PROPERTY}'s. */
    public static final String SCAN_PATH_SUFFIX = ".scans.tsv";

    /** The {@code chosen_node_id} of a scan that refused, matching the engine's own trace value (§7). */
    public static final long NO_CHOSEN_VICTIM = -1L;

    static final String DECISION_HEADER = "virtual_time_ns\tnode_id\treason\test"
            + "\tpages_since_last_self_split\toutstanding\tfar_ahead_fraction\tdensity_ratio"
            + "\tkeys_emitted\tlo\tcursor_to\thi";

    static final String SCAN_HEADER = "virtual_time_ns\tseen\tskipped_paced\tskipped_unsplittable"
            + "\tskipped_no_span\tchosen_node_id\tbest_est\treason"
            + "\tchosen_lo\tchosen_cursor\tchosen_hi";

    private final BufferedWriter decisions;
    private final BufferedWriter scans;

    private SimGateDump(BufferedWriter decisions, BufferedWriter scans) {
        this.decisions = decisions;
        this.scans = scans;
    }

    /**
     * The dump {@value #DUMP_PATH_PROPERTY} asks for, or {@code null} when it names nothing. Both
     * files are created and given their headers here, so a run that cannot write its dump fails
     * before it has produced a single number.
     */
    static SimGateDump fromSystemProperties() {
        String path = System.getProperty(DUMP_PATH_PROPERTY);
        if (path == null || path.isBlank()) {
            return null;
        }
        Path decisionFile = Path.of(path.trim());
        Path scanFile = Path.of(path.trim() + SCAN_PATH_SUFFIX);
        BufferedWriter decisions = open(decisionFile, DECISION_HEADER);
        try {
            return new SimGateDump(decisions, open(scanFile, SCAN_HEADER));
        } catch (RuntimeException opening) {
            try {
                decisions.close();
            } catch (IOException closing) {
                opening.addSuppressed(closing);
            }
            throw opening;
        }
    }

    /**
     * One owner-split gate chain outcome, blocked or carved, against the range it decided on.
     *
     * <p>Writes nothing when {@code inputs} is {@code null}, which is exactly the open-frontier
     * early-out: it reads no gate and the engine emits no {@code owner_split_decision} event for it
     * either, so a row here would be a row the trace it is diffed against does not have.
     *
     * @param virtualTimeNanos the deciding worker's own virtual instant
     * @param nodeId           the ledger's id for the decided range, so tail-range rows can be isolated
     * @param inputs           what the chain read on its way to its terminal gate
     * @param lo               the decided range's lower bound
     * @param cursorTo         the cursor the commit left, which is where a carve would divide
     * @param hi               its upper bound, {@code null} on an open frontier
     */
    void ownerDecision(long virtualTimeNanos, long nodeId, OwnerSplitGateInputs inputs,
                       byte[] lo, byte[] cursorTo, byte[] hi) {
        if (inputs == null) {
            return;
        }
        StringBuilder row = new StringBuilder(160);
        row.append(virtualTimeNanos).append('\t')
                .append(nodeId).append('\t')
                .append(inputs.reason()).append('\t')
                .append(inputs.est()).append('\t')
                .append(inputs.pagesSinceLastSelfSplit()).append('\t')
                .append(inputs.outstanding()).append('\t')
                .append(inputs.farAheadFraction()).append('\t')
                .append(inputs.densityRatio()).append('\t')
                .append(inputs.keysEmitted());
        appendKey(row, lo);
        appendKey(row, cursorTo);
        appendKey(row, hi);
        write(decisions, row);
    }

    /**
     * One victim-selection pass over the live pool, with the bounds of whichever candidate it chose.
     *
     * @param virtualTimeNanos the scanning thief's own virtual instant
     * @param scan             the aggregate tallies the pass produced
     * @param chosenNodeId     the winner's ledger id, or {@link #NO_CHOSEN_VICTIM} when it refused
     * @param reason           the {@code NoVictimReason} code on a refusal, {@code null} on a hit
     * @param chosenLo         the winner's lower bound, {@code null} on a refusal
     * @param chosenCursor     the winner's cursor as the scan left it, {@code null} on a refusal
     * @param chosenHi         the winner's upper bound, {@code null} on a refusal or an open frontier
     */
    void victimScan(long virtualTimeNanos, VictimScan scan, long chosenNodeId, String reason,
                    byte[] chosenLo, byte[] chosenCursor, byte[] chosenHi) {
        StringBuilder row = new StringBuilder(120);
        row.append(virtualTimeNanos).append('\t')
                .append(scan.seen()).append('\t')
                .append(scan.skippedPaced()).append('\t')
                .append(scan.skippedUnsplittable()).append('\t')
                .append(scan.skippedNoSpan()).append('\t')
                .append(chosenNodeId).append('\t')
                .append(scan.bestEst()).append('\t')
                .append(reason == null ? "" : reason);
        appendKey(row, chosenLo);
        appendKey(row, chosenCursor);
        appendKey(row, chosenHi);
        write(scans, row);
    }

    /** Flushes and closes both files; a failure to flush is a truncated dump and is thrown, not logged. */
    @Override
    public void close() {
        try (BufferedWriter closingDecisions = decisions; BufferedWriter closingScans = scans) {
            closingDecisions.flush();
            closingScans.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("the gate dump could not be flushed and closed, so what it "
                    + "holds is a prefix of the run rather than the run", e);
        }
    }

    /**
     * Appends one key column verbatim. Keys are object keys, and the dump is a private diagnosis
     * artifact, so they are written as text rather than encoded — but a key carrying a column or row
     * separator would shift every field after it with nothing to show for it, so that is refused
     * rather than written.
     */
    private static void appendKey(StringBuilder row, byte[] key) {
        row.append('\t');
        if (key == null) {
            return;
        }
        String text = new String(key, StandardCharsets.UTF_8);
        if (text.indexOf('\t') >= 0 || text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) {
            throw new IllegalStateException("a key carrying a tab or a newline cannot be a TSV column "
                    + "without silently shifting every column after it: " + HexFormat.of().formatHex(key));
        }
        row.append(text);
    }

    private static BufferedWriter open(Path file, String header) {
        try {
            BufferedWriter out = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            out.write(header);
            out.newLine();
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("the gate dump could not open " + file
                    + " (it must not already exist: a dump never truncates the one before it)", e);
        }
    }

    private static void write(BufferedWriter out, StringBuilder row) {
        try {
            out.append(row);
            out.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("the gate dump could not write a row, so the analysis it "
                    + "feeds would read the missing rows as a finding", e);
        }
    }
}
