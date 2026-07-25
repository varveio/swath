/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import java.util.Arrays;
import java.util.Base64;

/**
 * Opaque S3 continuation cursor: a boundary key plus whether the boundary itself is inclusive.
 *
 * <p><b>Wire format v2</b> — {@code "v2:" + base64url(flag || boundaryBytes)}, unpadded, where
 * {@code flag} is a single leading byte: {@code 0x00} = exclusive (resume at {@code key > boundary}),
 * {@code 0x01} = inclusive (resume at {@code key >= boundary}). The inclusive form supports the
 * sorted-serving delimiter seek-to-successor path, where the resume point is a real key that
 * may equal a rolled-up prefix's successor.
 *
 * <p><b>{@code v1} tokens</b> carry only the boundary bytes (no flag byte) and decode as
 * <em>exclusive</em>. Any other shape — unknown prefix, non-base64
 * body, or a v2 body missing/carrying an out-of-range flag byte — is rejected with a 400
 * {@code InvalidArgument}.
 *
 * <p><b>Structural validation only, not integrity protection.</b> A
 * token is {@code decode}d by shape (known prefix, valid base64, in-range flag byte) — it carries no
 * signature, MAC, or any other tamper-evidence. "Forging" one is not a privilege escalation: the
 * result is simply choosing an arbitrary resume boundary, exactly the same power a caller already has
 * legitimately via {@code start-after} — there is no privilege boundary a hand-crafted token could
 * cross. {@code decode} rejecting a malformed shape is therefore an input-validation guard (a clear
 * 400 instead of an internal decode exception), not a security control; do not read "reject bad
 * tokens" as "tokens are integrity-protected". No crypto belongs here.
 */
public record ContinuationToken(byte[] boundary, boolean inclusive) {

    private static final String V1_PREFIX = "v1:";
    private static final String V2_PREFIX = "v2:";
    private static final byte FLAG_EXCLUSIVE = 0x00;
    private static final byte FLAG_INCLUSIVE = 0x01;

    /** Defensive copy crossing the public seam (matches {@code ByteKey} / {@code RowGroupKey}). */
    public ContinuationToken {
        boundary = boundary.clone();
    }

    /** Defensive copy — callers may mutate the returned array without corrupting this token. */
    @Override
    public byte[] boundary() {
        return boundary.clone();
    }

    /**
     * Content equality — two tokens with byte-identical boundaries and the same inclusive flag are
     * equal, regardless of whether they share the same underlying array (records generate
     * reference-identity equals/hashCode for array components by default, which is wrong here).
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof ContinuationToken other
                && inclusive == other.inclusive
                && Arrays.equals(boundary, other.boundary);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(boundary) * 31 + Boolean.hashCode(inclusive);
    }

    @Override
    public String toString() {
        return "ContinuationToken[boundary=" + boundary.length + " bytes, inclusive=" + inclusive + "]";
    }

    /** Encodes an exclusive-boundary cursor (the default resume semantics). */
    public static String encode(byte[] boundary) {
        return encode(boundary, false);
    }

    /**
     * Encodes a cursor, choosing the inclusive/exclusive resume semantics.
     *
     * <p>The pager emits {@code inclusive=true} for a delimiter page that ends
     * on a rolled-up common prefix {@code P} — the resume boundary is {@code successor(P)} inclusive,
     * so a range-only store seeks past {@code P}'s whole subtree in one hop with no store-side dedup
     * guard to lean on. Object-terminated pages still resume exclusive-at-the-last-key. The flag is
     * part of the v2 payload regardless of which semantics is chosen, so switching between them is a
     * token-format no-op, not a new wire version.
     */
    public static String encode(byte[] boundary, boolean inclusive) {
        byte[] payload = new byte[boundary.length + 1];
        payload[0] = inclusive ? FLAG_INCLUSIVE : FLAG_EXCLUSIVE;
        System.arraycopy(boundary, 0, payload, 1, boundary.length);
        return V2_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(payload);
    }

    /**
     * Decodes a token, or returns {@code null} for a blank/absent token. Rejects any structurally
     * invalid or unknown-format token (unknown prefix, non-base64 body, out-of-range flag byte) with
     * a 400 {@code InvalidArgument} — a shape check, not an integrity check (see the class javadoc).
     */
    public static ContinuationToken decode(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        if (token.startsWith(V2_PREFIX)) {
            byte[] payload = base64(token.substring(V2_PREFIX.length()));
            if (payload.length == 0) {
                throw reject();
            }
            boolean inclusive = switch (payload[0]) {
                case FLAG_EXCLUSIVE -> false;
                case FLAG_INCLUSIVE -> true;
                default -> throw reject();
            };
            return new ContinuationToken(Arrays.copyOfRange(payload, 1, payload.length), inclusive);
        }
        if (token.startsWith(V1_PREFIX)) {
            return new ContinuationToken(base64(token.substring(V1_PREFIX.length())), false);
        }
        throw reject();
    }

    private static byte[] base64(String body) {
        try {
            return Base64.getUrlDecoder().decode(body);
        } catch (IllegalArgumentException e) {
            throw reject();
        }
    }

    private static S3Error reject() {
        return new S3Error(400, "InvalidArgument", "invalid continuation-token");
    }
}
