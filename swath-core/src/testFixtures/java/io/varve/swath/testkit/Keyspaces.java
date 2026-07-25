/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.testkit;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Deterministic, seedable generators for the bucket-shape matrix:
 * deep tree, flat random, badly skewed, tiny, exactly-N, empty, and adversarial
 * non-UTF-8 bytes. All return raw {@code byte[]} keys; the
 * {@link MockPageFetcher.Builder} sorts/dedups them into the source-of-truth
 * keyspace.
 */
public final class Keyspaces {

    private Keyspaces() {
    }

    /** N flat random keys (hex of random 64-bit values). */
    public static List<byte[]> flatRandom(long seed, int n) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("obj/%016x", r.nextLong()).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /** A Hive/Spark-style deep tree: {@code crawl=<c>/pid=<hex>/part-<n>.parquet}. */
    public static List<byte[]> deepTree(long seed, int crawls, int pidsPerCrawl) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(crawls * pidsPerCrawl);
        for (int c = 0; c < crawls; c++) {
            for (int p = 0; p < pidsPerCrawl; p++) {
                String k = String.format("crawl=%03d/pid=%08x/part-%05d.parquet", c, r.nextInt(), p);
                keys.add(k.getBytes(StandardCharsets.UTF_8));
            }
        }
        return keys;
    }

    /**
     * Badly skewed: {@code fraction} of N under one narrow prefix {@code hot/},
     * the rest spread thinly across {@code cold/}. The pathological case the
     * stealer must rebalance.
     */
    public static List<byte[]> badlySkewed(long seed, int n, double hotFraction) {
        Random r = new Random(seed);
        int hot = (int) (n * hotFraction);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < hot; i++) {
            keys.add(String.format("hot/%010d", i).getBytes(StandardCharsets.UTF_8));
        }
        for (int i = hot; i < n; i++) {
            keys.add(String.format("cold/%08x/%06d", r.nextInt(), i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /** Exactly N sequential keys (e.g. the 1000/1001 boundary fixtures). */
    public static List<byte[]> exactly(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("key/%09d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    public static List<byte[]> empty() {
        return List.of();
    }

    /** A single key — the floor case for the partition (no split is ever possible). */
    public static List<byte[]> oneKey() {
        return List.of("only/the/one".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Every key under one narrow prefix, densely numbered ({@code flat/<00000000..>}):
     * the single-prefix-flat case where {@code delimiter=/} seeding yields nothing and
     * the stealer must bisect a flat run (PROP-1).
     */
    public static List<byte[]> singlePrefixFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(String.format("flat/%08d", i).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Keys sharing a long common prefix and differing <b>only in the last byte</b>
     * ({@code last/byte/key/<b>}, {@code b} a single ASCII byte {@code 0x21..}). Forces
     * {@link io.varve.swath.model.ByteMidpoint} to split at adjacent single-byte boundaries.
     * Capped at 94 keys (the printable-ASCII span) so every key stays valid UTF-8.
     */
    public static List<byte[]> lastByteDiffer(int n) {
        byte[] base = "last/byte/key/".getBytes(StandardCharsets.UTF_8);
        int count = Math.min(n, 94);   // 0x21..0x7e
        List<byte[]> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] k = Arrays.copyOf(base, base.length + 1);
            k[base.length] = (byte) (0x21 + i);
            keys.add(k);
        }
        return keys;
    }

    /**
     * The valid-UTF-8 high-byte extreme: nested keys ending in runs of the max scalar
     * {@code U+10FFFF} (encodes to {@code F4 8F BF BF}). This is the contract-legal analog
     * of "0xFF-run keys" — real S3 keys are always valid UTF-8 (algorithms.md §3.1), so the
     * byte-order extreme a split must tolerate is a max-scalar run, not a literal {@code 0xFF}
     * (which is never a valid UTF-8 byte). Each key is a code-point prefix of the next, so
     * splitting exercises the prefix / max-scalar branches of {@code byteMidpoint}.
     */
    public static List<byte[]> maxScalarRun(int n) {
        String maxScalar = new String(Character.toChars(0x10FFFF));
        List<byte[]> keys = new ArrayList<>(n);
        StringBuilder sb = new StringBuilder("ms/");
        for (int i = 0; i < n; i++) {
            sb.append(maxScalar);
            keys.add(sb.toString().getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    /**
     * Keys near S3's 1024-byte cap, sharing a ~1020-byte common prefix and differing in a
     * short numeric tail — exercises {@code byteMidpoint}'s long-key / cap-fallback path.
     */
    public static List<byte[]> longKeys1024(int n) {
        int padLen = 1020;
        byte[] pad = new byte[padLen];
        Arrays.fill(pad, (byte) 'a');
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            byte[] tail = String.format("/%05d", i).getBytes(StandardCharsets.UTF_8);
            byte[] k = Arrays.copyOf(pad, padLen + tail.length);
            System.arraycopy(tail, 0, k, padLen, tail.length);
            keys.add(k);   // 1026 bytes total — long, multi-page-prefix, valid ASCII
        }
        return keys;
    }

    /**
     * Keys containing supplementary-plane code points (≥ U+10000, here the emoji block):
     * multi-byte keys where {@code String.compareTo} (UTF-16) <b>diverges</b> from UTF-8
     * byte order (I10) — the engine must split/stop byte-exact, never via {@code String}.
     */
    public static List<byte[]> supplementaryPlane(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String emoji = new String(Character.toChars(0x1F600 + (i % 80)));
            keys.add(("sp/" + emoji + "/" + String.format("%05d", i)).getBytes(StandardCharsets.UTF_8));
        }
        return keys;
    }

    // ------------------------------------------------------------------------
    // Generality stress-test matrix: each generator targets a
    // named keyspace pathology so the efficiency harness can prove whether the
    // engine's fan-out/over-fetch behavior generalizes or is overfit. Every
    // generator is deterministic and returns raw byte[] keys. Counts are the
    // requested target n (occasional dedup by MockPageFetcher only shrinks it).
    // ------------------------------------------------------------------------

    private static final char[] HEX_LOWER = "0123456789abcdef".toCharArray();
    private static final char[] HEX_UPPER = "0123456789ABCDEF".toCharArray();
    /** Bitcoin base58 alphabet (skips 0 O I l) — the multi-island shape. */
    private static final char[] BASE58 =
            "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".toCharArray();

    private static String hex(Random r, int nibbles, char[] alphabet) {
        StringBuilder sb = new StringBuilder(nibbles);
        for (int i = 0; i < nibbles; i++) {
            sb.append(alphabet[r.nextInt(16)]);
        }
        return sb.toString();
    }

    private static String base58(long v) {
        if (v == 0) {
            return "1";
        }
        StringBuilder sb = new StringBuilder();
        long u = v;
        while (u != 0) {
            int d = (int) Long.remainderUnsigned(u, 58);
            sb.append(BASE58[d]);
            u = Long.divideUnsigned(u, 58);
        }
        return sb.reverse().toString();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * CAS layout (the embarrassment shape): {@code blobs/sha256/<64-lowercase-hex>},
     * uniform. Lowercase-hex leading bytes straddle the {@code 0x3A–0x60} DEAD ZONE, so
     * naive-midpoint pivots land in the zero-mass gap (zero-transfer splits).
     */
    public static List<byte[]> casSha256(long seed, int n) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(utf8("blobs/sha256/" + hex(r, 64, HEX_LOWER)));
        }
        return keys;
    }

    /** Flat UPPERCASE-hex 64-char keys (no prefix): a second dead-zone alphabet variant. */
    public static List<byte[]> flatUpperHex(long seed, int n) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(utf8(hex(r, 64, HEX_UPPER)));
        }
        return keys;
    }

    /**
     * Hot needle: {@code hotFraction} of N under one ~24-char prefix, the rest a
     * uniform sprinkle across the byte space. The stealer must converge onto the needle
     * without stalling at a fan-out floor.
     */
    public static List<byte[]> hotNeedle(long seed, int n, double hotFraction) {
        Random r = new Random(seed);
        int hot = (int) (n * hotFraction);
        String needle = "tenant/acct-00042/dataset-";   // ~24 chars, one narrow prefix
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < hot; i++) {
            keys.add(utf8(needle + String.format("%010d", i)));
        }
        for (int i = hot; i < n; i++) {
            keys.add(utf8("z-sprinkle/" + hex(r, 16, HEX_LOWER)));
        }
        return keys;
    }

    /**
     * Time-decayed {@code YYYY/MM/DD/HH/<uuid>}, 2019→2026 exponential growth. When
     * {@code ascendingMass} is true the later years carry the mass (heavy tail high);
     * false puts the mass in the early years (heavy tail low), which strands the far end of
     * the keyspace nearly empty — the "far-ahead empty-tail" pathology.
     */
    public static List<byte[]> timeDecayed(long seed, int n, boolean ascendingMass) {
        Random r = new Random(seed);
        int years = 8;                       // 2019..2026
        double[] weight = new double[years];
        double total = 0;
        for (int y = 0; y < years; y++) {
            double w = Math.pow(3.0, ascendingMass ? y : (years - 1 - y));
            weight[y] = w;
            total += w;
        }
        List<byte[]> keys = new ArrayList<>(n);
        for (int y = 0; y < years; y++) {
            int count = (int) Math.round(n * weight[y] / total);
            int year = 2019 + y;
            for (int i = 0; i < count; i++) {
                int mm = 1 + r.nextInt(12);
                int dd = 1 + r.nextInt(28);
                int hh = r.nextInt(24);
                keys.add(utf8(String.format("%04d/%02d/%02d/%02d/%s",
                        year, mm, dd, hh, hex(r, 32, HEX_LOWER))));
            }
        }
        return keys;
    }

    /** Flat dashed-UUID keys ({@code 8-4-4-4-12} lowercase hex), no prefix. */
    public static List<byte[]> flatDashedUuid(long seed, int n) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(utf8(hex(r, 8, HEX_LOWER) + "-" + hex(r, 4, HEX_LOWER) + "-"
                    + hex(r, 4, HEX_LOWER) + "-" + hex(r, 4, HEX_LOWER) + "-" + hex(r, 12, HEX_LOWER)));
        }
        return keys;
    }

    /**
     * Wide-huge: {@code tenants} top-level prefixes × {@code perTenant} keys each
     * ({@code tenant%05d/obj%06d}). A {@code delimiter=/} probe rolls the top level up to
     * thousands of tiny common prefixes — the tiny-leaf misbranch risk.
     */
    public static List<byte[]> wideHuge(int tenants, int perTenant) {
        List<byte[]> keys = new ArrayList<>(tenants * perTenant);
        for (int t = 0; t < tenants; t++) {
            for (int o = 0; o < perTenant; o++) {
                keys.add(utf8(String.format("tenant%05d/obj%06d", t, o)));
            }
        }
        return keys;
    }

    /**
     * Sparse-alphabet flat, variable-length: pure-numeric NON-padded root keys
     * ({@code 1,10,100,…}), no delimiter. Lexical order diverges wildly from numeric order,
     * and every divergence turns on the 10-symbol digit sub-alphabet.
     */
    public static List<byte[]> sparseNumericFlat(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            keys.add(utf8(Integer.toString(i)));
        }
        return keys;
    }

    /** Multi-island alphabet: flat base58 (digits+upper+lower, skipping 0/O/I/l), uniform. */
    public static List<byte[]> base58Flat(long seed, int n) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(utf8(base58(r.nextLong())));
        }
        return keys;
    }

    /**
     * Alphabet shift along the keyspace: the first {@code headFraction} of key-order is
     * one alphabet/structure ({@code a/<lowercase-hex>}), the rest another
     * ({@code b/<UPPERCASE-hex>}). {@code a < b} keeps the blocks globally ordered, so the
     * effective alphabet changes partway through the sorted keyspace.
     */
    public static List<byte[]> alphabetShift(long seed, int n, double headFraction) {
        Random r = new Random(seed);
        int head = (int) (n * headFraction);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < head; i++) {
            keys.add(utf8("a/" + hex(r, 32, HEX_LOWER)));
        }
        for (int i = head; i < n; i++) {
            keys.add(utf8("b/" + hex(r, 32, HEX_UPPER)));
        }
        return keys;
    }

    /**
     * Degenerate positions: zero-padded counters ({@code data/00000<n>}) so successive
     * keys share long cardinality-1 runs before the divergence byte — the radix-1 positions a
     * byte-midpoint pivot must skip past to find a splitting byte.
     */
    public static List<byte[]> zeroPaddedCounter(int n) {
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(utf8(String.format("data/%012d", i)));
        }
        return keys;
    }

    /**
     * Tiny × many-prefix: {@code prefixes} common prefixes × {@code perPrefix} keys each
     * ({@code p%05d/o%d}), a total that fits in a handful of pages. Exercises the
     * many-CommonPrefixes-but-tiny-total shape (INT-8 budget, no real fan-out headroom).
     */
    public static List<byte[]> tinyManyPrefix(int prefixes, int perPrefix) {
        List<byte[]> keys = new ArrayList<>(prefixes * perPrefix);
        for (int p = 0; p < prefixes; p++) {
            for (int o = 0; o < perPrefix; o++) {
                keys.add(utf8(String.format("p%05d/o%d", p, o)));
            }
        }
        return keys;
    }

    /**
     * Scout-hostile CDF: all mass under one ~30-char shared prefix, densely numbered,
     * so the keyspace occupies a single narrow lexical interval. Blind alphabet-uniform bands
     * (and spread scatter probes) mostly land in the empty space to either side of the cluster.
     */
    public static List<byte[]> scoutHostileCluster(int n) {
        String shared = "logs/2024/06/30/region-eu-west-1a/";   // ~34-char shared prefix
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            keys.add(utf8(shared + String.format("%08d", i)));
        }
        return keys;
    }

    /**
     * Adversarial keys including bytes &lt; 0x20, 0x7f, and high bytes — where
     * unsigned byte order diverges from UTF-16. Deterministic.
     */
    public static List<byte[]> nonUtf8(long seed, int n) {
        Random r = new Random(seed);
        List<byte[]> keys = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int len = 1 + r.nextInt(8);
            byte[] k = new byte[len];
            for (int j = 0; j < len; j++) {
                k[j] = (byte) r.nextInt(256);
            }
            keys.add(k);
        }
        return keys;
    }
}
