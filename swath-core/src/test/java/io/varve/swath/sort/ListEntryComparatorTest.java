/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import org.junit.jupiter.api.Test;

/**
 * The one total preorder: key bytes unsigned → version_id
 * (null first, then UTF-8 bytes) → row_type rank (OBJECT &lt; COMMON_PREFIX &lt; DELETE_MARKER).
 * The version compare is allocation-free (I1) — verified by result equivalence to
 * the byte-array compare on multibyte input.
 */
class ListEntryComparatorTest {

    private final ListEntryComparator cmp = new ListEntryComparator();

    private static ObjectEntry object(String key, String versionId) {
        return new ObjectEntry(KeyBytes.ofUtf8(key), 1L, 0L, "etag", "STANDARD",
                versionId, true, null, null, null, null);
    }

    private static ObjectEntry objectRaw(byte[] key) {
        return new ObjectEntry(KeyBytes.of(key), 1L, 0L, null, null, null, true, null, null, null, null);
    }

    private static DeleteMarkerEntry deleteMarker(String key, String versionId) {
        return new DeleteMarkerEntry(KeyBytes.ofUtf8(key), versionId, true, 0L, null);
    }

    private static CommonPrefixEntry commonPrefix(String key) {
        return new CommonPrefixEntry(KeyBytes.ofUtf8(key));
    }

    @Test
    void keyBytesOrderIsUnsignedNotSigned() {
        ListEntry low = objectRaw(new byte[] {0x7f});
        ListEntry high = objectRaw(new byte[] {(byte) 0x80});   // signed-negative but sorts AFTER 0x7f
        assertThat(cmp.compare(low, high)).isNegative();
        assertThat(cmp.compare(high, low)).isPositive();
    }

    @Test
    void keyIsPrimaryBeforeVersionAndType() {
        assertThat(cmp.compare(object("a", "v9"), deleteMarker("b", null))).isNegative();
        assertThat(cmp.compare(commonPrefix("z"), object("a", null))).isPositive();
    }

    @Test
    void nullVersionSortsBeforeAnyPresentVersion() {
        assertThat(cmp.compare(object("k", null), object("k", "v1"))).isNegative();
        assertThat(cmp.compare(object("k", "v1"), object("k", null))).isPositive();
        assertThat(cmp.compare(object("k", null), object("k", null))).isZero();
    }

    @Test
    void presentVersionsOrderByUtf8Bytes() {
        assertThat(cmp.compare(object("k", "v1"), object("k", "v2"))).isNegative();
        assertThat(cmp.compare(object("k", "v2"), object("k", "v10"))).isPositive();   // '2' > '1'
    }

    @Test
    void versionCompareMatchesUtf8ByteOrderForMultibyte() {
        // U+07FF (DF BF) vs U+0800 (E0 A0 80): code-point order == UTF-8 byte order (DF < E0). The
        // allocation-free code-point compare must agree with the raw byte compare.
        String a = "v߿";
        String b = "vࠀ";
        assertThat(cmp.compare(object("k", a), object("k", b))).isNegative();
        assertThat(cmp.compare(object("k", b), object("k", a))).isPositive();
        // Supplementary plane (surrogate pair) sorts after the BMP char — where String.compareTo would diverge.
        String bmpHigh = "v￿";                 // U+FFFF
        String supp = "v" + new String(Character.toChars(0x10000));  // U+10000
        assertThat(cmp.compare(object("k", bmpHigh), object("k", supp))).isNegative();
    }

    @Test
    void rowTypeRankBreaksKeyPlusVersionTies() {
        ObjectEntry o = object("same", null);
        CommonPrefixEntry cp = commonPrefix("same");
        DeleteMarkerEntry dm = deleteMarker("same", null);
        assertThat(cmp.compare(o, cp)).isNegative();
        assertThat(cmp.compare(cp, dm)).isNegative();
        assertThat(cmp.compare(o, dm)).isNegative();
        assertThat(cmp.compare(dm, o)).isPositive();
    }

    @Test
    void versionBreaksTypeTieBeforeRowType() {
        // version is ranked BEFORE row_type: a null-version delete-marker precedes a
        // present-version object at the same key even though OBJECT out-ranks DELETE_MARKER.
        assertThat(cmp.compare(deleteMarker("k", null), object("k", "v1"))).isNegative();
    }

    @Test
    void identicalKeyVersionTypeComparePreorderEqual() {
        // Total preorder, not consistent-with-equals: same (key,version,type), different payload → equal.
        ObjectEntry big = new ObjectEntry(KeyBytes.ofUtf8("k"), 999L, 0L, "e", "S", "v", true, null, null, null, null);
        ObjectEntry small = new ObjectEntry(KeyBytes.ofUtf8("k"), 1L, 0L, "z", "G", "v", true, null, null, null, null);
        assertThat(cmp.compare(big, small)).isZero();
        assertThat(cmp.compare(commonPrefix("k"), commonPrefix("k"))).isZero();
    }
}
