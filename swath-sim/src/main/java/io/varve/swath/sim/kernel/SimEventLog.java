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
 * An optional ordered trace whose canonical UTF-8 bytes are the simulator's determinism artifact.
 * Ordinals totally order entries at the same virtual instant. Host-derived fields are forbidden,
 * and disabling recording must not alter simulated results.
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

    /** Appends an entry, rejecting ambiguous separators immediately. A disabled log drops it. */
    void append(long atNanos, int actorId, String kind, String detail) {
        if (entries == null) {
            return;
        }
        requireSeparatorFree(kind);
        requireSeparatorFree(detail);
        entries.add(new Entry(atNanos, ordinal++, actorId, kind, detail));
    }

    /** The trace so far, in dispatch order; empty for a disabled log. */
    public List<Entry> entries() {
        return entries == null ? List.of() : List.copyOf(entries);
    }

    /**
     * Serializes entries as UTF-8 lines of
     * {@code atNanos \t ordinal \t actorId \t kind \t detail}. Tabs and newlines in fields are
     * rejected rather than escaped so the encoding remains unambiguous.
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
