/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

/**
 * The single shared test-side mirror of {@code ByteMidpoint.isSafe}'s excluded set {@code E}
 * ({@code docs/internals/contracts.md} §0 I12, {@code docs/internals/algorithms.md} §3.1): the
 * code points a SYNTHESIZED pivot must never carry. {@code E = {U+0000..U+001F} ∪ {U+007F} ∪
 * {U+0080..U+009F} ∪ {U+FDD0..U+FDEF} ∪ {every plane's trailing xFFFE/xFFFF pair} ∪ {U+0025}}
 * — the C1 control block and every supplementary noncharacter were added (a
 * synthesized {@code start-after} cursor landing on either can 400 on real S3, even though both
 * are XML-1.0-legal). {@code U+0025} ({@code '%'}) was added for a DIFFERENT
 * reason: real S3 is fine with it, but LocalStack/MinIO echo it back verbatim (not
 * re-percent-encoded), and the AWS SDK's own response interceptor strict-decodes that echo with
 * {@code URLDecoder}, which crashes on a lone/trailing {@code '%'} — see
 * {@code docs/internals/s3-implementation-compatibility.md}.
 *
 * <p>{@code io.varve.swath.model.ByteMidpoint#isSafe} is the source of truth; this predicate mirrors
 * it independently (kept deliberately in ONE place, not copy-pasted per test class) so every
 * test that guards a synthesized-pivot-safety property — PROP-2 and its engine-package callers —
 * checks against the SAME excluded set as the contract and can't silently drift out of sync with
 * it the way the per-file copies once did.
 */
public final class ScalarSafety {

    private ScalarSafety() {
    }

    /** True iff {@code cp} is in the excluded set {@code E} — never a valid synthesized scalar. */
    public static boolean isExcludedScalar(int cp) {
        return cp < 0x20                            // C0 control block
                || cp == 0x7F                        // DEL
                || (cp >= 0x80 && cp <= 0x9F)         // C1 control block
                || (cp >= 0xFDD0 && cp <= 0xFDEF)     // standalone BMP noncharacter block
                || (cp & 0xFFFE) == 0xFFFE            // trailing xFFFE/xFFFF pair, every plane
                || cp == 0x25;                        // '%' — verbatim-echo endpoint URLDecoder crash
    }
}
