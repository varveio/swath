/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import java.util.LinkedHashMap;
import java.util.SequencedMap;

/**
 * Collision-free encoding of an ordered list of named string fields as
 * {@code name=<len>:<value>} entries joined by {@code ;}. The decimal length prefix makes
 * value content irrelevant to field boundaries: a value that happens to contain {@code ;},
 * {@code =} or {@code :} cannot shift a boundary, so two field lists that differ in any
 * value always produce different specs.
 *
 * <p>This is the single source of the length-prefix escaping shared by the two persisted
 * {@code run_meta} identity strings: {@link FilterSpecCodec} ({@code filter_spec}) and
 * {@link ResumeRegistry} ({@code identity_spec}). Both persist the result in SQLite and
 * compare it on resume to detect a changed run, so the grammar must not change across
 * releases.
 *
 * <p>A {@code null} value is encoded with length {@code -1} (an ABSENT marker with no value
 * bytes) and an empty string with length {@code 0}; the two decode back distinctly because
 * they are behaviorally different (e.g. {@code --exclude ""} versus no {@code --exclude}).
 * Field names are assumed delimiter-free ({@code =}/{@code ;}/{@code :} never appear in a
 * name); only values are adversarial, which is exactly what the length prefix guards.
 */
final class LengthPrefixedFields {

    private LengthPrefixedFields() {}

    /** Encode an ordered name→value map into {@code name=<len>:<value>;...} (null value → absent marker). */
    static String encode(SequencedMap<String, String> fields) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (var e : fields.entrySet()) {
            if (!first) {
                sb.append(';');
            }
            first = false;
            String value = e.getValue();
            if (value == null) {
                sb.append(e.getKey()).append('=').append(-1).append(':');
            } else {
                sb.append(e.getKey()).append('=').append(value.length()).append(':').append(value);
            }
        }
        return sb.toString();
    }

    /**
     * Decode a spec produced by {@link #encode} back into an ordered map, restoring {@code null}
     * for the absent marker. A {@code null} or empty spec decodes to an empty map. Throws
     * {@link Malformed} on any structural error (bad length, missing delimiter, or a duplicate
     * name); field-name validity is the caller's concern.
     */
    static LinkedHashMap<String, String> decode(String spec) throws Malformed {
        LinkedHashMap<String, String> out = new LinkedHashMap<>();
        if (spec == null || spec.isEmpty()) {
            return out;
        }
        int pos = 0;
        while (pos < spec.length()) {
            int eq = spec.indexOf('=', pos);
            if (eq < 0) {
                throw new Malformed("missing '=' at offset " + pos);
            }
            String name = spec.substring(pos, eq);
            pos = eq + 1;
            int colon = spec.indexOf(':', pos);
            if (colon < 0) {
                throw new Malformed("missing ':' after field '" + name + "'");
            }
            int len;
            try {
                len = Integer.parseInt(spec.substring(pos, colon));
            } catch (NumberFormatException e) {
                throw new Malformed("invalid length for field '" + name + "'");
            }
            if (len < -1) {
                throw new Malformed("invalid negative length " + len + " for field '" + name + "'");
            }
            pos = colon + 1;
            String value;
            if (len < 0) {
                value = null;   // absent marker: no value bytes to consume
            } else {
                if (pos + len > spec.length()) {
                    throw new Malformed("field '" + name + "' length " + len + " exceeds remaining input");
                }
                value = spec.substring(pos, pos + len);
                pos += len;
            }
            if (out.containsKey(name)) {
                throw new Malformed("duplicate field '" + name + "'");
            }
            out.put(name, value);
            // skip the ';' separator between fields
            if (pos < spec.length()) {
                if (spec.charAt(pos) == ';') {
                    pos++;
                } else {
                    throw new Malformed("missing ';' after field '" + name + "'");
                }
            }
        }
        return out;
    }

    /** A structurally invalid spec (bad length prefix, missing delimiter, or duplicate field). */
    static final class Malformed extends Exception {
        Malformed(String message) {
            super(message);
        }
    }
}
