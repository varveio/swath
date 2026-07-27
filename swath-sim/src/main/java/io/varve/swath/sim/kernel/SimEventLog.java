/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sim.kernel;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The ordered trace of everything that happened in a run, and the artifact the determinism claim is
 * made against: two runs of one scenario at one seed must produce <b>byte-identical</b>
 * {@link #canonicalBytes()}. Comparing bytes rather than a summary is deliberate — a wall time and a
 * call count can agree while the interleaving that produced them differs, and it is the interleaving
 * that decides which racing actor wins.
 *
 * <p><b>Recording is opt-in</b> ({@link #recording()} / {@link #disabled()}). A run over a large
 * fixture processes events in the tens of millions, so a sweep leg keeps the log off and asserts on
 * counters; an invariant or determinism test turns it on. A disabled log accepts and drops every
 * entry, so no caller needs a null check or a conditional around a {@code record} call.
 *
 * <p>Every field of an entry is a number or a caller-supplied label. Nothing derived from the host —
 * no wall time, no thread name, no identity hash — may enter one, or the byte comparison would fail
 * for reasons that have nothing to do with the model.
 */
public final class SimEventLog {

    private static final char FIELD_SEPARATOR = '\t';
    private static final char RECORD_SEPARATOR = '\n';

    /**
     * One line of the trace.
     *
     * @param atNanos the virtual instant it happened at
     * @param ordinal its position in the trace, from zero — the tie-break that makes two entries at
     *                one instant totally ordered
     * @param actorId the actor it is attributed to
     * @param kind    the event kind (a dotted label, e.g. {@code list.response})
     * @param detail  kind-specific payload, empty when there is none
     */
    public record Entry(long atNanos, long ordinal, int actorId, String kind, String detail) {
    }

    private final List<Entry> entries;
    private long ordinal;

    private SimEventLog(List<Entry> entries) {
        this.entries = entries;
    }

    /** A log that retains every entry. */
    public static SimEventLog recording() {
        return new SimEventLog(new ArrayList<>());
    }

    /** A log that retains nothing — the default for a sweep leg, where the trace is the dominant cost. */
    public static SimEventLog disabled() {
        return new SimEventLog(null);
    }

    /** Whether entries are retained; {@code false} makes {@link #entries()} permanently empty. */
    public boolean isRecording() {
        return entries != null;
    }

    /** Appends one entry, assigning it the next ordinal. A disabled log drops it. */
    void append(long atNanos, int actorId, String kind, String detail) {
        if (entries == null) {
            return;
        }
        entries.add(new Entry(atNanos, ordinal++, actorId, kind, detail));
    }

    /** The trace so far, in dispatch order; empty for a disabled log. */
    public List<Entry> entries() {
        return entries == null ? List.of() : List.copyOf(entries);
    }

    /**
     * The canonical serialization: one UTF-8 line per entry,
     * {@code atNanos \t ordinal \t actorId \t kind \t detail}, in trace order. A field's own
     * separators are rejected at write time rather than escaped — a kind or detail containing a tab
     * or a newline is a caller bug, and silently escaping it would let two different traces
     * serialize to the same bytes.
     */
    public byte[] canonicalBytes() {
        StringBuilder out = new StringBuilder();
        for (Entry e : entries()) {
            requireSeparatorFree(e.kind());
            requireSeparatorFree(e.detail());
            out.append(e.atNanos()).append(FIELD_SEPARATOR)
                    .append(e.ordinal()).append(FIELD_SEPARATOR)
                    .append(e.actorId()).append(FIELD_SEPARATOR)
                    .append(e.kind()).append(FIELD_SEPARATOR)
                    .append(e.detail()).append(RECORD_SEPARATOR);
        }
        return out.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void requireSeparatorFree(String field) {
        if (field.indexOf(FIELD_SEPARATOR) >= 0 || field.indexOf(RECORD_SEPARATOR) >= 0) {
            throw new IllegalStateException("event-log field contains a separator, so the trace could "
                    + "not be serialized unambiguously: " + field);
        }
    }
}
