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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;

/**
 * Opt-in, write-only TSV dump of owner decisions and victim scans using engine policy payloads and
 * caller-supplied virtual instants. It supports local and future simulator/engine parity diagnosis;
 * no end-to-end real-fixture row-for-row comparison currently validates that parity.
 *
 * <p>Without {@value #DUMP_PATH_PROPERTY}, {@link #fromSystemProperties()} returns {@code null} and
 * guarded call sites perform no dump work. Enabled dumps create both files and headers with
 * {@link StandardOpenOption#CREATE_NEW}; open, write, flush, and close failures are reported as
 * {@link UncheckedIOException}. If the second file cannot be opened, the first writer is closed.
 */
public final class SimGateDump implements AutoCloseable {

    /** Owner-decision TSV path; absent or blank disables dumping. */
    public static final String DUMP_PATH_PROPERTY = "swath.sim.gate-dump";

    /** Suffix appended to the configured path for the victim-scan TSV. */
    public static final String SCAN_PATH_SUFFIX = ".scans.tsv";

    /** Refusal sentinel for {@code chosen_node_id}. */
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

    /** Creates both dump files and headers, or returns {@code null} when dumping is disabled. */
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
     * Writes one owner-split outcome. A null {@code inputs} value intentionally emits no row.
     *
     * @param virtualTimeNanos the deciding worker's own virtual instant
     * @param nodeId           the ledger id for the decided range
     * @param inputs           gate inputs, or {@code null} for an omitted early-out
     * @param lo               the decided range's lower bound
     * @param cursorTo         the post-commit cursor
     * @param hi               the upper bound, or {@code null} for an open frontier
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
     * Writes one victim scan. Refusals use {@link #NO_CHOSEN_VICTIM}, a non-null reason, and null
     * chosen-key fields; a chosen open-frontier victim may also have a null upper bound.
     *
     * @param virtualTimeNanos the scanning thief's own virtual instant
     * @param scan             aggregate scan tallies
     * @param chosenNodeId     winner id or the refusal sentinel
     * @param reason           refusal reason, or {@code null} on a hit
     * @param chosenLo         winner lower bound, or {@code null} on refusal
     * @param chosenCursor     winner cursor, or {@code null} on refusal
     * @param chosenHi         winner upper bound, or {@code null} on refusal/open frontier
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

    /** Flushes and closes both files, throwing on any I/O failure. */
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

    /** Appends a UTF-8 key column, rejecting invalid text and TSV tab/newline separators. */
    private static void appendKey(StringBuilder row, byte[] key) {
        row.append('\t');
        if (key == null) {
            return;
        }
        String text;
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            text = decoder.decode(ByteBuffer.wrap(key)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalStateException("a key that is not valid UTF-8 cannot be written as a TSV "
                    + "text column without corrupting it: " + HexFormat.of().formatHex(key), e);
        }
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
