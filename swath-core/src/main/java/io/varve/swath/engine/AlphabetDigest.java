/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.engine;

import io.varve.swath.engine.policy.AlphabetFallback;
import io.varve.swath.engine.policy.Engagement;
import io.varve.swath.model.ByteMidpoint;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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
 *
 * <p>Public only so {@code io.varve.swath.engine.policy}'s {@code StealAttemptView} can carry a
 * victim's digest through to the pivot cascade (policy-domain, no S3/protocol dependency — the
 * same status as {@code StealMath}/{@code ByteMidpoint}); construction and mutation
 * ({@link #observe}) stay package-private to {@code io.varve.swath.engine} — only
 * {@link WorkerState} builds and feeds one.
 */
public final class AlphabetDigest implements ByteMidpoint.ScalarChooser {

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

    AlphabetDigest(byte[] lo, byte[] hi) {
        Arrays.fill(clean, true);
        this.base = commonPrefixCodePoints(decode(lo), decode(hi));
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
        return chooseScalar(cpIndex, loCp, hiCp, fraction, null);
    }

    /**
     * The alphabet-aware consult, reporting any {@code ALPHABET.*} fallback reason into {@code
     * collector} instead of a {@code RunMetrics} field of this class's own (issue #19's fix): a
     * {@code decide()}-reachable consult must never hold or touch {@code RunMetrics} itself, so the
     * caller — {@code StealMath#interpolate(byte[], byte[], double, Snapshot, List)}, threaded
     * from {@code ThiefPolicy}'s pivot cascade or {@code OwnerSplitGovernor}'s carve — supplies its
     * own pending-{@link Engagement} list and hands it to the executor exactly like every other
     * engagement (contracts.md §2.1). A {@code null} collector (the {@link
     * ByteMidpoint.ScalarChooser} override above, and every existing unit test that drives this
     * method directly) records nothing — behavior-preserving by construction, since a consult fires
     * at most one fallback either way.
     *
     * <p>This is the LIVE read, kept for the direct pure-math unit tests and for
     * {@link ByteMidpoint.ScalarChooser} conformance. <b>No decision path reaches it</b> — a policy
     * consults a {@link #snapshot()} instead (issue #30).
     */
    int chooseScalar(int cpIndex, int loCp, int hiCp, double fraction, List<Engagement> collector) {
        int g = generation;                     // acquire: see the mask/clean writes published before it
        if (g < 0) {                            // (never; silences "unused" — the read is the fence)
            return NO_SCALAR;
        }
        int pos = cpIndex - base;
        boolean inWindow = pos >= 0 && pos < MAX_POSITIONS;
        return choose(inWindow ? mask[pos][0] : 0L,
                inWindow ? mask[pos][1] : 0L,
                inWindow && clean[pos],
                loCp, hiCp, fraction, collector);
    }

    /**
     * Freeze this digest's current per-position alphabet into an immutable value a policy can decide
     * over — <b>issue #30's fix</b>.
     *
     * <p>A {@code StealAttemptView}/{@code OwnerSplitView} used to carry the victim's LIVE digest, so
     * a concurrent {@code observe} on the owner could change what the pivot cascade read <i>after</i>
     * the view was built: the decision was not a function of the recorded view, which is exactly the
     * property the policy seam exists to provide (contracts.md §2.1) and the premise offline replay
     * needs. The whole digest is a fixed 8 positions × 2 words plus 8 clean flags, so freezing it is
     * one {@code long[16]} allocation and an {@code int} of packed flags — cheap enough to do per
     * steal attempt and per owner-split consult.
     *
     * <p>Semantics are unchanged: a snapshot answers every consult byte-for-byte as the live digest
     * would have at the instant it was taken, including <i>which</i> {@code ALPHABET.*} fallback
     * fires. That is why {@code clean} is carried explicitly rather than inferred from an all-zero
     * mask — a dirty position and a merely-unobserved one both have a zero mask but report
     * {@code fallback_out_of_window} and {@code window_gap} respectively.
     */
    public Snapshot snapshot() {
        int g = generation;                     // acquire: see the mask/clean writes published before it
        if (g < 0) {                            // (never; silences "unused" — the read is the fence)
            return new Snapshot(base, new long[MAX_POSITIONS * 2], 0);
        }
        long[] words = new long[MAX_POSITIONS * 2];
        int cleanBits = 0;
        for (int pos = 0; pos < MAX_POSITIONS; pos++) {
            words[2 * pos] = mask[pos][0];
            words[2 * pos + 1] = mask[pos][1];
            if (clean[pos]) {
                cleanBits |= 1 << pos;
            }
        }
        return new Snapshot(base, words, cleanBits);
    }

    /**
     * An immutable, point-in-time {@link AlphabetDigest} — the alphabet signal a <b>policy</b>
     * decides over (issue #30). Constructed only by {@link AlphabetDigest#snapshot()}, which hands it
     * a private array no one else retains, and it exposes no accessor for that array: a decision over
     * a {@code Snapshot} is reproducible from the recorded view forever, whatever the owner thread
     * does next.
     *
     * <p>A torn read is still possible <i>while snapshotting</i> (the owner may {@code observe}
     * concurrently — the digest is deliberately read without the worker lock, as documented on the
     * enclosing class). That is benign and unchanged: {@link ByteMidpoint} independently re-validates
     * any chosen scalar for safety and strict betweenness, so a torn word can only shift the pivot's
     * balance inside valid bounds. What the snapshot fixes is not tearing but MUTATION AFTER THE
     * FACT — the decision now sees exactly one alphabet, the one recorded in the view.
     */
    public static final class Snapshot implements ByteMidpoint.ScalarChooser {

        private final int base;
        /** Flat presence masks, {@code words[2*pos]}/{@code words[2*pos+1]} — never handed out. */
        private final long[] words;
        /** Bit {@code pos} set iff that tracked position is alphabet-clean. */
        private final int cleanBits;

        private Snapshot(int base, long[] words, int cleanBits) {
            this.base = base;
            this.words = words;
            this.cleanBits = cleanBits;
        }

        @Override
        public int chooseScalar(int cpIndex, int loCp, int hiCp, double fraction) {
            return chooseScalar(cpIndex, loCp, hiCp, fraction, null);
        }

        /** The frozen counterpart of {@link AlphabetDigest#chooseScalar(int, int, int, double, List)}. */
        int chooseScalar(int cpIndex, int loCp, int hiCp, double fraction, List<Engagement> collector) {
            int pos = cpIndex - base;
            boolean inWindow = pos >= 0 && pos < MAX_POSITIONS;
            return choose(inWindow ? words[2 * pos] : 0L,
                    inWindow ? words[2 * pos + 1] : 0L,
                    inWindow && (cleanBits >>> pos & 1) != 0,
                    loCp, hiCp, fraction, collector);
        }
    }

    /**
     * The consult itself, over one position's two presence words — the SINGLE implementation shared
     * by the live digest and its {@link Snapshot}, so the two can never answer differently. The
     * caller has already resolved the tracked position and folded "outside the tracked window" into
     * {@code clean == false} (both report {@code fallback_out_of_window}, as before).
     */
    private static int choose(long w0, long w1, boolean clean, int loCp, int hiCp, double fraction,
            List<Engagement> collector) {
        if (!clean) {
            // consult fell outside the tracked/clean window
            if (collector != null) {
                collector.add(new Engagement("ALPHABET", AlphabetFallback.FALLBACK_OUT_OF_WINDOW.code()));
            }
            return NO_SCALAR;
        }
        int from = Math.max(LO_SCALAR, (loCp < 0) ? LO_SCALAR : loCp + 1);   // strictly > loCp
        int to = Math.min(HI_SCALAR, hiCp - 1);                              // strictly < hiCp
        if (from > to) {
            // no room for any scalar strictly in (loCp, hiCp)
            if (collector != null) {
                collector.add(new Engagement("ALPHABET", AlphabetFallback.FALLBACK_NO_ROOM.code()));
            }
            return NO_SCALAR;
        }
        int n = countBits(w0, w1, from, to);
        if (n == 0) {
            // no observed value populates the (loCp, hiCp) gap
            if (collector != null) {
                collector.add(new Engagement("ALPHABET", AlphabetFallback.WINDOW_GAP.code()));
            }
            return NO_SCALAR;
        }
        int target = (int) Math.floor(fraction * n);
        if (target < 0) {
            target = 0;
        } else if (target >= n) {
            target = n - 1;
        }
        return kthSetScalar(w0, w1, from, to, target);
    }

    // -------------------------------------------------------------------------
    // Private helpers.
    // -------------------------------------------------------------------------

    /** Is scalar {@code s} present, given one position's two presence words? */
    private static boolean present(long w0, long w1, int s) {
        return (((s >>> 6) == 0 ? w0 : w1) & (1L << (s & 63))) != 0L;
    }

    /** Count set presence bits in the inclusive scalar range {@code [from, to]}. */
    private static int countBits(long w0, long w1, int from, int to) {
        int count = 0;
        for (int s = from; s <= to; s++) {
            if (present(w0, w1, s)) {
                count++;
            }
        }
        return count;
    }

    /** The {@code k}-th (0-based) set scalar in {@code [from, to]}. */
    private static int kthSetScalar(long w0, long w1, int from, int to, int k) {
        int seen = 0;
        for (int s = from; s <= to; s++) {
            if (present(w0, w1, s)) {
                if (seen == k) {
                    return s;
                }
                seen++;
            }
        }
        return NO_SCALAR;   // unreachable: k < countBits(w0, w1, from, to)
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
