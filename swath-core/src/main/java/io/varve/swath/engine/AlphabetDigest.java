/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.model.ByteMidpoint;
import io.varve.swath.observability.RunMetrics;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Per-worker <b>observed-alphabet digest</b> for rank-space pivot interpolation. A tiny, zero-API
 * structure that learns, per key-<b>code-point</b> position, the set of
 * printable-ASCII scalar values a worker has actually seen among the keys already fetched/emitted in
 * its range. It exists to fix the sparse-alphabet <b>dead-zone pivot</b> pathology of raw code-point
 * pivot math: hex/UUID/base64 keys use only a handful of the 256 byte values per position, so a
 * code-point-space midpoint between two such keys can land in an <i>empty</i> byte range (e.g. between
 * {@code 9}=0x39 and {@code a}=0x61 — no keys there). Feeding this digest as a
 * {@link ByteMidpoint.ScalarChooser} makes the synthesized pivot land on a <b>populated</b> value at
 * rank-fraction {@code f} of the observed alphabet instead.
 *
 * <h3>Bounded, zero-API, cold-start-safe</h3>
 * Only the first {@value #MAX_POSITIONS} code-point positions <i>past the range's divergence point</i>
 * (the common-prefix length of the worker's {@code (lo, hi]}) are tracked — memory is a fixed
 * {@code 2 longs} (a 128-bit printable-ASCII mask) per position. A position that ever sees a
 * non-printable or multi-byte scalar is marked not-clean and yields no signal there (fall back to the
 * default synthesis). With no signal at all (a fresh worker, or a non-ASCII keyspace) the chooser
 * returns {@link ByteMidpoint.ScalarChooser#NO_SCALAR} and rank-space estimation returns {@code NaN},
 * so every consumer degrades to the exact prior code-point behavior.
 *
 * <h3>Concurrency</h3>
 * {@link #observe} is called by the owner <b>under {@code WorkerState.lock()}</b> (alongside the
 * density digest); a {@link Thief} reads via {@link #chooseScalar} <b>without</b> the lock. A
 * trailing {@code volatile} {@link #generation} publishes the mask/clean
 * writes; a torn read is benign — the chooser's result is independently re-validated for safety and
 * strict betweenness by {@link ByteMidpoint}, so it can only shift the pivot's balance within valid
 * bounds, never break tiling correctness (mirrors the lock-free density read).
 */
final class AlphabetDigest implements ByteMidpoint.ScalarChooser {

    /** Number of code-point positions tracked past the range divergence point (K, small). */
    static final int MAX_POSITIONS = 8;
    /** Smallest tracked printable-ASCII scalar (space). Everything below is C0 control — not tracked. */
    private static final int LO_SCALAR = 0x20;
    /** Largest tracked printable-ASCII scalar (tilde). 0x7F (DEL) and up are not tracked. */
    private static final int HI_SCALAR = 0x7E;

    /** Code-point index at which tracking begins: the common-prefix length of the range {@code (lo, hi]}. */
    private final int base;
    /** 128-bit printable-ASCII presence mask per tracked position ({@code mask[pos][scalar >>> 6]}). */
    private final long[][] mask = new long[MAX_POSITIONS][2];
    /** Whether a tracked position has only ever seen printable-ASCII scalars (else no signal there). */
    private final boolean[] clean = new boolean[MAX_POSITIONS];
    /** Publication fence: bumped (last) after every mutating {@link #observe}; read first by readers. */
    private volatile int generation;
    /**
     * Optional sink for the per-consult {@code ALPHABET.*} fallback engagement counters
     * ({@code fallback_out_of_window}/{@code fallback_no_room}/{@code window_gap}) — {@code null} on the
     * unit-test constructor, wired only through the production {@link WorkerState} path.
     */
    private final RunMetrics metrics;

    AlphabetDigest(byte[] lo, byte[] hi) {
        this(lo, hi, null);
    }

    AlphabetDigest(byte[] lo, byte[] hi, RunMetrics metrics) {
        Arrays.fill(clean, true);
        this.base = commonPrefixCodePoints(decode(lo), decode(hi));
        this.metrics = metrics;
    }

    /**
     * Fold one key already in hand into the per-position alphabet. Called under {@code WorkerState.lock()}
     * (no extra I/O — the key is already fetched). A non-printable / multi-byte scalar at a position
     * permanently marks that position not-clean (no alphabet signal there).
     */
    void observe(byte[] key) {
        if (key == null) {
            return;
        }
        int[] cps = decode(key);
        boolean changed = false;
        int limit = Math.min(cps.length, base + MAX_POSITIONS);
        for (int p = base; p < limit; p++) {
            int pos = p - base;
            if (!clean[pos]) {
                continue;
            }
            int s = cps[p];
            if (s < LO_SCALAR || s > HI_SCALAR) {
                clean[pos] = false;            // non-printable / multi-byte — position no longer alphabet-clean
                mask[pos][0] = 0L;
                mask[pos][1] = 0L;
                changed = true;                // publish the clean→dirty flip / mask clear too — a lock-free
                                               // reader must see the position go dark (else it keeps choosing
                                               // a now-stale ASCII scalar instead of NO_SCALAR)
                continue;
            }
            int w = s >>> 6;
            long bit = 1L << (s & 63);
            if ((mask[pos][w] & bit) == 0L) {
                mask[pos][w] |= bit;
                changed = true;
            }
        }
        if (changed) {
            generation++;                       // publish the mask writes above (release)
        }
    }

    // -------------------------------------------------------------------------
    // ScalarChooser — alphabet-aware pivot synthesis.
    // -------------------------------------------------------------------------

    @Override
    public int chooseScalar(int cpIndex, int loCp, int hiCp, double fraction) {
        int g = generation;                     // acquire: see the mask/clean writes published before it
        if (g < 0) {                            // (never; silences "unused" — the read is the fence)
            return NO_SCALAR;
        }
        int pos = cpIndex - base;
        if (pos < 0 || pos >= MAX_POSITIONS || !clean[pos]) {
            recordFallback("fallback_out_of_window");   // consult fell outside the tracked/clean window
            return NO_SCALAR;
        }
        int from = Math.max(LO_SCALAR, (loCp < 0) ? LO_SCALAR : loCp + 1);   // strictly > loCp
        int to = Math.min(HI_SCALAR, hiCp - 1);                              // strictly < hiCp
        if (from > to) {
            recordFallback("fallback_no_room");         // no room for any scalar strictly in (loCp, hiCp)
            return NO_SCALAR;
        }
        int n = countBits(pos, from, to);
        if (n == 0) {
            recordFallback("window_gap");               // no observed value populates the (loCp, hiCp) gap
            return NO_SCALAR;
        }
        int target = (int) Math.floor(fraction * n);
        if (target < 0) {
            target = 0;
        } else if (target >= n) {
            target = n - 1;
        }
        return kthSetScalar(pos, from, to, target);
    }

    // -------------------------------------------------------------------------
    // Private helpers.
    // -------------------------------------------------------------------------

    /** Count set presence bits in the inclusive scalar range {@code [from, to]} at {@code pos}. */
    private int countBits(int pos, int from, int to) {
        int count = 0;
        for (int s = from; s <= to; s++) {
            if ((mask[pos][s >>> 6] & (1L << (s & 63))) != 0L) {
                count++;
            }
        }
        return count;
    }

    /** The {@code k}-th (0-based) set scalar in {@code [from, to]} at {@code pos}. */
    private int kthSetScalar(int pos, int from, int to, int k) {
        int seen = 0;
        for (int s = from; s <= to; s++) {
            if ((mask[pos][s >>> 6] & (1L << (s & 63))) != 0L) {
                if (seen == k) {
                    return s;
                }
                seen++;
            }
        }
        return NO_SCALAR;   // unreachable: k < countBits(pos, from, to)
    }

    /** Record a per-consult {@code ALPHABET.<reason>} fallback engagement counter (§5). */
    private void recordFallback(String reason) {
        if (metrics != null) {
            metrics.recordStealReason("ALPHABET", reason);
        }
    }

    /**
     * A snapshot of the per-position printable-ASCII presence masks (two {@code long}
     * words per tracked position, {@code out[2*pos]}/{@code out[2*pos+1]}), for the run-level alphabet
     * cardinality AGGREGATE. A dirty (non-alphabet) or never-observed position reads back {@code 0}
     * (its mask was cleared on the clean→dirty flip), so OR-ing these across every completed node
     * yields the union of scalars observed at each relative position <i>past its range's divergence
     * point</i> — the aggregation washes out per-range divergence-depth drift (a v1 corpus signal).
     */
    long[] maskWords() {
        int g = generation;                     // acquire: see the mask/clean writes published before it
        if (g < 0) {
            return new long[MAX_POSITIONS * 2];
        }
        long[] out = new long[MAX_POSITIONS * 2];
        for (int pos = 0; pos < MAX_POSITIONS; pos++) {
            out[2 * pos] = mask[pos][0];
            out[2 * pos + 1] = mask[pos][1];
        }
        return out;
    }

    private static int commonPrefixCodePoints(int[] a, int[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /** Lenient UTF-8 → code points ({@code null} = ⊥ = empty). Real keys/bounds are valid UTF-8. */
    private static int[] decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new int[0];
        }
        return new String(bytes, StandardCharsets.UTF_8).codePoints().toArray();
    }
}
