/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.util.Comparator;

/**
 * THE total preorder over {@link ListEntry}, used <b>identically</b> in-memory and in the k-way
 * merge. The order is:
 *
 * <ol>
 *   <li><b>key bytes</b> — unsigned lexicographic ({@link KeyBytes#compareUnsigned}, I10;
 *       <b>never</b> {@code String.compareTo}, whose UTF-16 order diverges from S3's UTF-8
 *       byte order);</li>
 *   <li><b>version_id</b> — an absent/null version sorts <b>first</b>, then present versions
 *       in unsigned UTF-8 byte order (keeps the {@code (key, version_id)} primary
 *       tiebreak intact, so a key's versions stay grouped);</li>
 *   <li><b>row_type rank</b> — {@code OBJECT < COMMON_PREFIX < DELETE_MARKER}, the final
 *       deterministic tail that only breaks a genuine key+version-equal cross-type tie.</li>
 * </ol>
 *
 * <p>{@link CommonPrefixEntry} has no version, so it is treated as version-absent.
 *
 * <p><b>Total preorder, not consistent-with-equals</b>: two entries
 * identical in (key, version, row_type) but differing in payload (e.g. two objects with the same
 * key and version but a different size) compare equal here. S3's key+version uniqueness precludes
 * that case in healthy data, and a stable sort keeps such rows in input order.
 *
 * <p><b>Allocation-free version compare.</b> Version ids are compared code point by code point
 * ({@link #compareUtf8}): UTF-8 encodes so that byte-wise unsigned order equals Unicode code-point
 * order, so this yields exactly the same result as comparing the UTF-8 byte arrays without the
 * per-compare {@code String#getBytes} allocation (I1).
 */
public final class ListEntryComparator implements Comparator<ListEntry> {

    @Override
    public int compare(ListEntry a, ListEntry b) {
        int c = KeyBytes.compareUnsigned(a.key().rawUnsafe(), b.key().rawUnsafe());
        if (c != 0) {
            return c;
        }
        c = compareVersion(versionOf(a), versionOf(b));
        if (c != 0) {
            return c;
        }
        return Integer.compare(rank(a), rank(b));
    }

    /** The version string of an entry, or {@code null} when it has none (common prefixes). */
    private static String versionOf(ListEntry e) {
        return switch (e) {
            case ObjectEntry o -> o.versionId();
            case DeleteMarkerEntry d -> d.versionId();
            case CommonPrefixEntry ignored -> null;
        };
    }

    /** row_type rank tail: OBJECT (0) &lt; COMMON_PREFIX (1) &lt; DELETE_MARKER (2). */
    private static int rank(ListEntry e) {
        return switch (e) {
            case ObjectEntry ignored -> 0;
            case CommonPrefixEntry ignored -> 1;
            case DeleteMarkerEntry ignored -> 2;
        };
    }

    /** Absent (null) sorts first; otherwise unsigned UTF-8 byte order (via code-point compare). */
    private static int compareVersion(String a, String b) {
        if (a == null) {
            return b == null ? 0 : -1;
        }
        if (b == null) {
            return 1;
        }
        return compareUtf8(a, b);
    }

    /**
     * Compare two strings in UTF-8 unsigned byte order without allocating: iterate by Unicode code
     * point (UTF-8 byte order == code-point order), so the result matches
     * {@code Arrays.compareUnsigned(a.getBytes(UTF_8), b.getBytes(UTF_8))} exactly.
     */
    private static int compareUtf8(String a, String b) {
        int la = a.length();
        int lb = b.length();
        int i = 0;
        int j = 0;
        while (i < la && j < lb) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) {
                return Integer.compare(ca, cb);
            }
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        // One string is a prefix of the other (or both exhausted): the shorter sorts first.
        return Integer.compare(la - i, lb - j);
    }
}
