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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Compact per-page in-memory codec. A page's entries are packed <b>at admission</b>
 * into one byte payload and decoded <b>sequentially</b> at seal — the block is never randomly
 * accessed, so it stores no per-row index and stays close to ~1× the logical byte size instead of
 * the 2–3× of parsed {@link ListEntry} objects. Parsed entries can then die young rather than
 * tenuring into old gen.
 *
 * <p><b>Layout.</b> Keys are <b>front-coded</b> against the previous key written to the block —
 * {@code varint(sharedPrefixLen)} + {@code varint(suffixLen)} + suffix bytes — see {@link
 * Writer#key}; {@code size}/{@code last_modified} are fixed 8-byte longs; low-cardinality string
 * columns ({@code storage_class}, {@code checksum_algorithm}, {@code checksum_type},
 * {@code owner_id}, {@code owner_display_name}) are each a per-block dictionary index, decided once
 * per column per block from a distinct-value probe taken before encoding — a column whose distinct
 * count exceeds {@link #DICT_CAP} falls back to raw for the WHOLE block (never switches mid-block,
 * so decode needs no per-row marker), which matters because {@code owner_id} can legitimately be
 * high-cardinality in cross-account buckets; the {@code etag} of a plain 32-char lowercase-hex md5 is
 * packed into 16 bytes (multipart {@code hex-N} and any non-standard etag fall back to a raw UTF-8
 * string, so the round-trip is always byte-exact); every nullable string carries an explicit presence
 * flag so {@code null} and empty are distinguished. All three {@link ListEntry} subtypes round-trip,
 * including null fields, unicode keys, and 1&nbsp;KB keys.
 *
 * <p><b>Ordering metadata.</b> {@link #pack} also records, in the same single pass over
 * {@code entries}, whether the page is <b>internally strictly ascending under the full {@code
 * ListEntryComparator}</b> (key, then version_id, then row_type — not key bytes alone), plus the
 * page's first and last {@link ListEntry} (not just their key bytes). {@link SealedBuffer}'s
 * defensive pre-sortedness check needs the full comparator because versioned listings
 * (`--all-versions`) deliver a key's versions in S3's last-modified-descending order, not §0.3
 * order — a key-bytes-only check sees those as "equal" (a tie) and would wrongly take the
 * zero-sort happy path on a mis-ordered version tiebreak.
 *
 * <p><b>Front-coding never assumes sortedness</b>: {@link #commonPrefixLength} is a pure byte
 * comparison against whatever key was written immediately before it — correct for entries in ANY
 * order (a shared-prefix length of 0 is always a valid encoding). Pages arriving sorted (the common
 * case, S3 pagination) profit with a large shared-prefix win; an out-of-order or hash-like page still
 * round-trips exactly, just without the compression win. {@link #estimatedBytes()} stays the
 * LOGICAL Σ(key.len + {@link #ENTRY_OVERHEAD_BYTES}) regardless of front-coding or dictionary
 * savings, so the §5 byte gate remains conservative and key-size-independent; {@link #packedBytes()}
 * reflects the real (smaller) footprint after both optimizations.
 */
final class PageBlock {

    /** Per-entry fixed overhead used by the §5 byte gate estimate (Σ key.len + this). */
    static final int ENTRY_OVERHEAD_BYTES = 64;

    /**
     * Per-block, per-column dictionary cap: a column whose distinct-value count would exceed
     * this falls back to raw encoding for the whole block. 64 keeps a dictionary index's varint at a
     * single byte (indices are 1-based, so index+1 must stay &le; 127) and comfortably covers
     * realistic low-cardinality domains (storage classes, checksum algorithms/types, and the common
     * single- or few-owner case) while still catching the explosion case — {@code owner_id} and
     * {@code owner_display_name} can legitimately be high-cardinality in cross-account buckets.
     */
    static final int DICT_CAP = 64;

    /** Hard allocation ceiling for one decoded S3 page payload read from an internal segment. */
    static final int MAX_RAW_PAYLOAD_BYTES = 256 * 1024 * 1024;

    private static final byte[] EMPTY_KEY = new byte[0];

    private static final byte TAG_OBJECT = 0;
    private static final byte TAG_COMMON_PREFIX = 1;
    private static final byte TAG_DELETE_MARKER = 2;

    private static final byte ETAG_NULL = 0;
    private static final byte ETAG_PACKED_MD5 = 1;
    private static final byte ETAG_RAW = 2;

    /** Low-cardinality string columns eligible for per-block dictionary encoding. */
    private enum DictColumn { STORAGE_CLASS, CHECKSUM_ALGORITHM, CHECKSUM_TYPE, OWNER_ID, OWNER_DISPLAY_NAME }

    private static final int DICT_COLUMN_COUNT = DictColumn.values().length;

    // Compress-at-pack: storedPayload is codec's compressed bytes (verbatim raw bytes for
    // PageCodec.NONE) — the actual in-memory/on-disk footprint. decodedPayloadCache is the
    // decompressed front-coded payload Cursor decodes rows from; lazily computed ONCE (decompress the
    // FULL payload, never per-row) and cached — a benign race (idempotent decode) if two threads race
    // to populate it, same pattern as firstEntry/lastEntry below.
    private final byte[] storedPayload;
    private final int rawPayloadLength;
    private final PageCodec codec;
    private byte[] decodedPayloadCache;
    private final String[][] dicts;     // per-DictColumn dictionary (index → value); empty if unused
    private final boolean[] useDict;    // per-DictColumn: dict-mode (true) vs raw fallback (false)
    private final int count;
    private final byte[] firstKeyBytes;
    private final byte[] lastKeyBytes;
    // Full-fidelity first/last entries: eager (non-null) for a pack()-produced block; lazily decoded
    // from the payload for a deserialize()-produced block, whose record carries only the min/max KEY
    // bytes above (see class javadoc "Self-contained record"). The lazy assignment is a benign race
    // (idempotent decode) — same pattern as KeyBytes#cachedString.
    private ListEntry firstEntry;
    private ListEntry lastEntry;
    private final long estimatedBytes;
    private final boolean orderedUnderFullComparator;
    /** Non-null only for a persisted page, so cursor-time decode faults retain typed path context. */
    private final Path sourcePath;

    private PageBlock(byte[] storedPayload, int rawPayloadLength, PageCodec codec, String[][] dicts,
                      boolean[] useDict, int count, byte[] firstKeyBytes, byte[] lastKeyBytes,
                      ListEntry firstEntry, ListEntry lastEntry, long estimatedBytes,
                      boolean orderedUnderFullComparator, Path sourcePath) {
        this.storedPayload = storedPayload;
        this.rawPayloadLength = rawPayloadLength;
        this.codec = codec;
        this.dicts = dicts;
        this.useDict = useDict;
        this.count = count;
        this.firstKeyBytes = firstKeyBytes;
        this.lastKeyBytes = lastKeyBytes;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.estimatedBytes = estimatedBytes;
        this.orderedUnderFullComparator = orderedUnderFullComparator;
        this.sourcePath = sourcePath;
    }

    /**
     * Pack a page's entries with {@link PageCodec#NONE} (no payload compression) — the default
     * overload used by call sites that don't thread a configured codec through (re-pack, the
     * CASCADE backstop). See {@link #pack(List, Comparator, PageCodec)} for the compress-at-pack path.
     */
    static PageBlock pack(List<ListEntry> entries, Comparator<ListEntry> comparator) {
        return pack(entries, comparator, PageCodec.NONE);
    }

    /**
     * Pack a page's entries, compressing the front-coded PAYLOAD per {@code codec} — the
     * record HEADER (min/max, dict tables, {@code useDict}, count, ordered-bit) is never compressed.
     * The list must be non-empty (an empty page is never admitted). {@code comparator} is the §0.3
     * total preorder ({@link ListEntryComparator}) — used only to compute
     * {@link #orderedUnderFullComparator()} in the same pass, not for packing itself.
     */
    static PageBlock pack(List<ListEntry> entries, Comparator<ListEntry> comparator, PageCodec codec) {
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("cannot pack an empty page");
        }

        // Pass 1: the ordering check, plus a per-column distinct-value probe that decides
        // ONCE, before any byte is written, whether each dict-eligible column dict-encodes or falls
        // back to raw for the whole block — so decode never needs a per-row mode marker.
        boolean ordered = true;
        ListEntry prev = null;
        DictProbe[] probes = new DictProbe[DICT_COLUMN_COUNT];
        for (int i = 0; i < probes.length; i++) {
            probes[i] = new DictProbe();
        }
        for (ListEntry e : entries) {
            if (prev != null && comparator.compare(prev, e) >= 0) {
                ordered = false;   // includes exact adjacent ties — never assumed pre-sorted
            }
            prev = e;
            switch (e) {
                case ObjectEntry o -> {
                    probes[DictColumn.STORAGE_CLASS.ordinal()].offer(o.storageClass());
                    probes[DictColumn.CHECKSUM_ALGORITHM.ordinal()].offer(o.checksumAlgorithm());
                    probes[DictColumn.CHECKSUM_TYPE.ordinal()].offer(o.checksumType());
                    probes[DictColumn.OWNER_ID.ordinal()].offer(o.ownerId());
                    probes[DictColumn.OWNER_DISPLAY_NAME.ordinal()].offer(o.ownerDisplayName());
                }
                case DeleteMarkerEntry d -> probes[DictColumn.OWNER_ID.ordinal()].offer(d.ownerId());
                case CommonPrefixEntry ignored -> { }
            }
        }
        boolean[] useDict = new boolean[DICT_COLUMN_COUNT];
        for (int i = 0; i < probes.length; i++) {
            useDict[i] = probes[i].useDict();
        }

        // Pass 2: the actual encode, front-coding keys and dict-or-raw-encoding the five
        // low-cardinality columns per the mode decided above.
        Writer w = new Writer(entries.size(), useDict);
        long estimate = 0;
        for (ListEntry e : entries) {
            estimate += e.key().length() + ENTRY_OVERHEAD_BYTES;
            switch (e) {
                case ObjectEntry o -> {
                    w.tag(TAG_OBJECT);
                    w.key(o.key());
                    w.fixedLong(o.size());
                    w.fixedLong(o.lastModifiedEpochMicros());
                    w.etag(o.etag());
                    w.dictOrRaw(DictColumn.STORAGE_CLASS, o.storageClass());
                    w.nullableString(o.versionId());
                    w.bool(o.isLatest());
                    w.dictOrRaw(DictColumn.OWNER_ID, o.ownerId());
                    w.dictOrRaw(DictColumn.OWNER_DISPLAY_NAME, o.ownerDisplayName());
                    w.dictOrRaw(DictColumn.CHECKSUM_ALGORITHM, o.checksumAlgorithm());
                    w.dictOrRaw(DictColumn.CHECKSUM_TYPE, o.checksumType());
                }
                case CommonPrefixEntry c -> {
                    w.tag(TAG_COMMON_PREFIX);
                    w.key(c.key());
                }
                case DeleteMarkerEntry d -> {
                    w.tag(TAG_DELETE_MARKER);
                    w.key(d.key());
                    w.nullableString(d.versionId());
                    w.bool(d.isLatest());
                    w.fixedLong(d.lastModifiedEpochMicros());
                    w.dictOrRaw(DictColumn.OWNER_ID, d.ownerId());
                }
            }
        }
        ListEntry first = entries.get(0);
        ListEntry last = entries.get(entries.size() - 1);
        byte[] raw = w.toBytes();
        byte[] stored = codec.compress(raw);
        return new PageBlock(stored, raw.length, codec, w.dictArrays(), useDict, entries.size(),
                first.key().rawUnsafe(), last.key().rawUnsafe(), first, last, estimate, ordered, null);
    }

    /** The first entry's key (defensive copy). */
    byte[] firstKey() {
        return firstKeyBytes.clone();
    }

    /** The last entry's key (defensive copy). */
    byte[] lastKey() {
        return lastKeyBytes.clone();
    }

    /**
     * The page's first entry, full-fidelity (key + version + row_type) — for the defensive order
     * check. Eager for a {@link #pack}-produced block; for a {@link #deserialize}-produced block,
     * lazily decoded from the payload's first row on first call (the record itself carries only the
     * min key bytes, not the full entry).
     */
    ListEntry firstEntry() {
        ListEntry e = firstEntry;
        if (e == null) {
            e = cursor().next();
            firstEntry = e;
        }
        return e;
    }

    /**
     * The page's last entry, full-fidelity (key + version + row_type) — for the defensive order
     * check. Eager for a {@link #pack}-produced block; for a {@link #deserialize}-produced block,
     * lazily decoded on first call by scanning every row (front-coding gives no random access to the
     * last row — this is the only correct way to recover it).
     */
    ListEntry lastEntry() {
        ListEntry e = lastEntry;
        if (e == null) {
            Cursor c = cursor();
            while (c.hasNext()) {
                e = c.next();
            }
            lastEntry = e;
        }
        return e;
    }

    /**
     * True iff this page's entries are strictly ascending under the full §0.3 {@code
     * ListEntryComparator} (key, then version_id, then row_type) — computed once, at admission, in
     * {@link #pack}. False for any adjacent tie (including two versions of the same key that are
     * present but not ascending) as well as any out-of-order pair.
     */
    boolean orderedUnderFullComparator() {
        return orderedUnderFullComparator;
    }

    int count() {
        return count;
    }

    /** The payload codec of this page (used by the {@code dump-run} debug inspector). */
    PageCodec codec() {
        return codec;
    }

    /**
     * Estimated pre-encode LOGICAL bytes (Σ key.len + {@link #ENTRY_OVERHEAD_BYTES}) — the §5 gate
     * input. Deliberately independent of front-coding and dictionary savings, so the
     * byte gate stays conservative and key-size-independent regardless of how compressible a page's
     * keys/columns turn out to be. Not part of the {@link #serialize()} record (it is a pre-seal,
     * in-memory buffering concern only) — a {@link #deserialize}-produced block reports its packed
     * size here instead, since nothing on the deserialize path re-runs the §5 gate.
     */
    long estimatedBytes() {
        return estimatedBytes;
    }

    /**
     * Actual packed footprint in bytes — reflects the real, front-coded/dictionary-encoded, and
     * codec-compressed size: the bytes actually held in memory / written to disk for the
     * payload region (excludes the plain header — min/max, dict tables, {@code useDict}, count).
     */
    long packedBytes() {
        return storedPayload.length;
    }

    /** A fresh sequential decoder over this block. Decompresses the payload once, lazily, on demand. */
    Cursor cursor() {
        try {
            return new Cursor(decodedPayload());
        } catch (RuntimeException e) {
            throw decodedCorruption(e);
        }
    }

    /**
     * The decompressed, front-coded payload {@link Cursor} decodes rows from — computed once (the
     * FULL payload, never per-row) and cached; see the field comment above for the benign-race note.
     */
    private byte[] decodedPayload() {
        byte[] cached = decodedPayloadCache;
        if (cached == null) {
            cached = codec.decompress(storedPayload, rawPayloadLength);
            decodedPayloadCache = cached;
        }
        return cached;
    }

    /**
     * Serialize this block to a self-contained record — everything that lives outside the
     * front-coded payload today (dict tables, {@link #useDict} flags, min/max keys, {@link #count},
     * the ordered-bit) folds into a plain (uncompressed) header so a reader can reconstruct an
     * equivalent {@code PageBlock} from these bytes alone via {@link #deserialize} — and, critically,
     * {@link PageFrontierReader} can parse the header's leading min/max/count fields WITHOUT
     * decompressing anything. The payload region alone is codec-compressed ({@link
     * PageCodec}) — {@code storedPayloadLen} bytes on disk decompress back to {@code rawPayloadLen}
     * bytes of front-coded rows; {@code storedPayloadLen == rawPayloadLen} for {@link PageCodec#NONE}.
     * Big-endian throughout:
     * <pre>
     * [minKeyLen u16][minKey bytes][maxKeyLen u16][maxKey bytes]
     * [count u32][ordered-bit u8]
     * [dictTables: for each of the 5 DictColumns in enum order:
     *     [n u16] [n entries: [valueLen u16][value UTF-8 bytes]]]
     * [useDict: 5 bits packed into 1 byte, bit i = useDict[i]]
     * [codec u8]   // {@link PageCodec#code()}: NONE=0, LZ4=1, ZSTD1=2
     * [rawPayloadLen u32][storedPayloadLen u32][storedPayload bytes, codec-compressed]
     * </pre>
     * This is the record BODY only — a separate framing layer wraps it with an outer
     * {@code [len][crc32c]} and a file trailer; this method does not add that framing.
     */
    byte[] serialize() {
        byte[][][] dictValueBytes = new byte[DICT_COLUMN_COUNT][][];
        int dictTablesSize = 0;
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            String[] values = dicts[i];
            byte[][] valueBytes = new byte[values.length][];
            int size = 2;   // [n u16]
            for (int j = 0; j < values.length; j++) {
                valueBytes[j] = values[j].getBytes(StandardCharsets.UTF_8);
                size += 2 + valueBytes[j].length;
            }
            dictValueBytes[i] = valueBytes;
            dictTablesSize += size;
        }

        int total = 2 + firstKeyBytes.length + 2 + lastKeyBytes.length
                + 4 + 1
                + dictTablesSize
                + 1
                + 1
                + 4 + 4 + storedPayload.length;

        ByteBuffer buf = ByteBuffer.allocate(total);
        putLenBytes(buf, firstKeyBytes);
        putLenBytes(buf, lastKeyBytes);
        buf.putInt(count);
        buf.put((byte) (orderedUnderFullComparator ? 1 : 0));
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            byte[][] valueBytes = dictValueBytes[i];
            buf.putShort((short) valueBytes.length);
            for (byte[] v : valueBytes) {
                putLenBytes(buf, v);
            }
        }
        int packedUseDict = 0;
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            if (useDict[i]) {
                packedUseDict |= 1 << i;
            }
        }
        buf.put((byte) packedUseDict);
        buf.put(codec.code());
        buf.putInt(rawPayloadLength);
        buf.putInt(storedPayload.length);
        buf.put(storedPayload);
        return buf.array();
    }

    /**
     * Reconstruct a {@code PageBlock} from a record produced by {@link #serialize()}. The result
     * exposes the identical {@link #firstKey()}, {@link #lastKey()}, {@link #count()}, {@link
     * #orderedUnderFullComparator()}, and a {@link #cursor()} that decodes the same entry sequence
     * as the original — {@link #firstEntry()}/{@link #lastEntry()} are lazily re-derived from the
     * payload on first use (see the field comment above) since the record carries only the min/max
     * key bytes, not full {@link ListEntry} objects. The payload stays codec-compressed until
     * something actually needs the rows ({@link #cursor()}/{@link #firstEntry()}/{@link #lastEntry()})
     * — {@link #decodedPayload()} decompresses it once, lazily, and verifies the decompressed length
     * matches the record's declared {@code rawPayloadLen} (fails fast on a corrupt/truncated payload).
     */
    static PageBlock deserialize(byte[] record) {
        return deserialize(record, null);
    }

    static PageBlock deserialize(byte[] record, Path sourcePath) {
        SerializedFields fields = parseSerializedFields(record);
        return new PageBlock(fields.storedPayload(), fields.rawPayloadLength(), fields.codec(),
                fields.dicts(), fields.useDict(), fields.count(), fields.minKey(), fields.maxKey(),
                null, null, fields.storedPayload().length, fields.ordered(), sourcePath);
    }

    /** Structurally validated fields needed by the decode-free frontier and full deserializer. */
    record SerializedFields(byte[] minKey, byte[] maxKey, int count, boolean ordered,
                            String[][] dicts, boolean[] useDict, PageCodec codec,
                            int rawPayloadLength, byte[] storedPayload) {
    }

    /**
     * Validate every length/count/mode field in a serialized page before allocating from it. This
     * deliberately does not decompress or materialize rows; the source-aware {@link Cursor}
     * performs the row/header cross-check when a frontier actually selects the page for decoding.
     */
    static SerializedFields parseSerializedFields(byte[] record) {
        ByteBuffer buf = ByteBuffer.wrap(record);
        byte[] minKey = getBoundedLenBytes(buf, "minKey");
        byte[] maxKey = getBoundedLenBytes(buf, "maxKey");
        requireRemaining(buf, 5, "count and ordered flag");
        int count = buf.getInt();
        if (count <= 0) {
            throw malformed("count must be positive, got " + count);
        }
        byte orderedByte = buf.get();
        if (orderedByte != 0 && orderedByte != 1) {
            throw malformed("ordered flag must be 0 or 1, got " + (orderedByte & 0xFF));
        }

        String[][] dicts = new String[DICT_COLUMN_COUNT][];
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            requireRemaining(buf, 2, "dictionary count");
            int n = buf.getShort() & 0xFFFF;
            if (n > DICT_CAP) {
                throw malformed("dictionary " + i + " count " + n + " exceeds " + DICT_CAP);
            }
            String[] values = new String[n];
            for (int j = 0; j < n; j++) {
                values[j] = new String(getBoundedLenBytes(buf, "dictionary value"),
                        StandardCharsets.UTF_8);
            }
            dicts[i] = values;
        }

        requireRemaining(buf, 10, "page modes and payload lengths");
        int packedUseDict = buf.get() & 0xFF;
        int validUseDictBits = (1 << DICT_COLUMN_COUNT) - 1;
        if ((packedUseDict & ~validUseDictBits) != 0) {
            throw malformed("useDict contains unknown bits: 0x"
                    + Integer.toHexString(packedUseDict));
        }
        boolean[] useDict = new boolean[DICT_COLUMN_COUNT];
        for (int i = 0; i < DICT_COLUMN_COUNT; i++) {
            useDict[i] = (packedUseDict & (1 << i)) != 0;
        }

        PageCodec codec;
        byte codecCode = buf.get();
        try {
            codec = PageCodec.fromCode(codecCode);
        } catch (IllegalStateException e) {
            throw malformed("unsupported codec " + (codecCode & 0xFF), e);
        }
        int rawPayloadLength = buf.getInt();
        int storedPayloadLength = buf.getInt();
        if (rawPayloadLength <= 0 || rawPayloadLength > MAX_RAW_PAYLOAD_BYTES) {
            throw malformed("raw payload length " + rawPayloadLength + " is outside 1.."
                    + MAX_RAW_PAYLOAD_BYTES);
        }
        if (storedPayloadLength <= 0 || storedPayloadLength != buf.remaining()) {
            throw malformed("stored payload length " + storedPayloadLength
                    + " does not equal remaining body bytes " + buf.remaining());
        }
        byte[] storedPayload = new byte[storedPayloadLength];
        buf.get(storedPayload);
        return new SerializedFields(minKey, maxKey, count, orderedByte == 1, dicts, useDict,
                codec, rawPayloadLength, storedPayload);
    }

    private static void putLenBytes(ByteBuffer buf, byte[] bytes) {
        buf.putShort((short) bytes.length);
        buf.put(bytes);
    }

    private static byte[] getBoundedLenBytes(ByteBuffer buf, String field) {
        requireRemaining(buf, 2, field + " length");
        int len = buf.getShort() & 0xFFFF;
        requireRemaining(buf, len, field);
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }

    private static void requireRemaining(ByteBuffer buf, int needed, String field) {
        if (needed < 0 || buf.remaining() < needed) {
            throw malformed(field + " exceeds record body (needed " + needed
                    + ", remaining " + buf.remaining() + ")");
        }
    }

    private static IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException("malformed PageBlock: " + message);
    }

    private static IllegalArgumentException malformed(String message, Throwable cause) {
        return new IllegalArgumentException("malformed PageBlock: " + message, cause);
    }

    /**
     * Forward-only sequential decoder. No random access — matches the write order exactly. Holds the
     * per-cursor front-coding state ({@code prevKey}) so independent traversals of the same
     * block (multiple {@link #cursor()} calls) never interfere with each other.
     */
    final class Cursor {

        private final byte[] payload;
        private int pos;
        private int emitted;
        private byte[] prevKey = EMPTY_KEY;
        private byte[] decodedFirstKey;

        private Cursor(byte[] payload) {
            this.payload = payload;
        }

        boolean hasNext() {
            return emitted < count;
        }

        ListEntry next() {
            if (emitted >= count) {
                throw new NoSuchElementException();
            }
            try {
                emitted++;
                byte tag = payload[pos++];
                ListEntry entry = switch (tag) {
                case TAG_OBJECT -> {
                    KeyBytes key = key();
                    long size = fixedLong();
                    long lastModified = fixedLong();
                    String etag = etag();
                    String storageClass = dictOrRaw(DictColumn.STORAGE_CLASS);
                    String versionId = nullableString();
                    boolean isLatest = bool();
                    String ownerId = dictOrRaw(DictColumn.OWNER_ID);
                    String ownerDisplayName = dictOrRaw(DictColumn.OWNER_DISPLAY_NAME);
                    String checksumAlgorithm = dictOrRaw(DictColumn.CHECKSUM_ALGORITHM);
                    String checksumType = dictOrRaw(DictColumn.CHECKSUM_TYPE);
                    yield new ObjectEntry(key, size, lastModified, etag, storageClass, versionId,
                            isLatest, ownerId, ownerDisplayName, checksumAlgorithm, checksumType);
                }
                case TAG_COMMON_PREFIX -> new CommonPrefixEntry(key());
                case TAG_DELETE_MARKER -> {
                    KeyBytes key = key();
                    String versionId = nullableString();
                    boolean isLatest = bool();
                    long lastModified = fixedLong();
                    String ownerId = dictOrRaw(DictColumn.OWNER_ID);
                    yield new DeleteMarkerEntry(key, versionId, isLatest, lastModified, ownerId);
                }
                default -> throw new IllegalStateException("bad PageBlock tag: " + tag);
                };
                if (emitted == 1) {
                    decodedFirstKey = entry.key().rawUnsafe();
                }
                if (emitted == count) {
                    if (pos != payload.length) {
                        throw malformed("decoded " + count + " rows with "
                                + (payload.length - pos) + " trailing payload bytes");
                    }
                    if (!Arrays.equals(decodedFirstKey, firstKeyBytes)
                            || !Arrays.equals(entry.key().rawUnsafe(), lastKeyBytes)) {
                        throw malformed(
                                "decoded first/last raw keys do not match persisted page bounds");
                    }
                }
                return entry;
            } catch (RuntimeException e) {
                throw decodedCorruption(e);
            }
        }

        /**
         * Decode a front-coded key: {@code shared} bytes copied from the previous key,
         * {@code suffixLen} fresh bytes read from the payload. Correct regardless of write order —
         * {@code shared} is whatever the encoder actually measured, never assumed. One allocation per
         * row (the exact-sized reconstructed key, handed to {@link KeyBytes#of} which takes ownership
         * with no defensive copy) — the SAME allocation cost a non-front-coded decode already paid;
         * only the amount of NEW payload read per row (the suffix) shrinks with front-coding.
         */
        private KeyBytes key() {
            int shared = varint();
            int suffixLen = varint();
            byte[] full = new byte[shared + suffixLen];
            System.arraycopy(prevKey, 0, full, 0, shared);
            System.arraycopy(payload, pos, full, shared, suffixLen);
            pos += suffixLen;
            prevKey = full;
            return KeyBytes.of(full);
        }

        private long fixedLong() {
            long v = ((long) (payload[pos] & 0xFF) << 56)
                    | ((long) (payload[pos + 1] & 0xFF) << 48)
                    | ((long) (payload[pos + 2] & 0xFF) << 40)
                    | ((long) (payload[pos + 3] & 0xFF) << 32)
                    | ((long) (payload[pos + 4] & 0xFF) << 24)
                    | ((long) (payload[pos + 5] & 0xFF) << 16)
                    | ((long) (payload[pos + 6] & 0xFF) << 8)
                    | (payload[pos + 7] & 0xFF);
            pos += 8;
            return v;
        }

        private boolean bool() {
            return payload[pos++] != 0;
        }

        private String nullableString() {
            int v = varint();
            if (v == 0) {
                return null;
            }
            int len = v - 1;
            String s = new String(payload, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        private String etag() {
            byte marker = payload[pos++];
            return switch (marker) {
                case ETAG_NULL -> null;
                case ETAG_PACKED_MD5 -> {
                    String s = unpackMd5(payload, pos);
                    pos += 16;
                    yield s;
                }
                case ETAG_RAW -> {
                    int len = varint();
                    String s = new String(payload, pos, len, StandardCharsets.UTF_8);
                    pos += len;
                    yield s;
                }
                default -> throw new IllegalStateException("bad etag marker: " + marker);
            };
        }

        /** Decode a dict-or-raw column per this block's fixed per-column mode. */
        private String dictOrRaw(DictColumn col) {
            int v = varint();
            if (v == 0) {
                return null;
            }
            if (useDict[col.ordinal()]) {
                return dicts[col.ordinal()][v - 1];
            }
            int len = v - 1;
            String s = new String(payload, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        private int varint() {
            int result = 0;
            int shift = 0;
            while (true) {
                byte b = payload[pos++];
                result |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
            }
        }
    }

    private RuntimeException decodedCorruption(RuntimeException cause) {
        if (sourcePath == null || cause instanceof UncheckedIOException) {
            return cause;
        }
        return new UncheckedIOException(new SegmentCorruptionException(sourcePath,
                SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                "decoded page does not match its structural metadata", cause));
    }

    /**
     * The length of the common byte prefix of {@code a} and {@code b} — a pure comparison with no
     * ordering assumption: correct and safe (0 is always valid) whether or not {@code b}
     * follows {@code a} in sort order.
     */
    private static int commonPrefixLength(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        int i = 0;
        while (i < n && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /**
     * Per-block distinct-value probe for one dictionary-eligible column: tracks up to
     * {@link #DICT_CAP} distinct non-null values, then flags itself "capped" (raw fallback) the
     * moment a {@code DICT_CAP + 1}-th distinct value appears — dropping its working set immediately
     * since the verdict is already decided and no further tracking is needed.
     */
    private static final class DictProbe {
        private Set<String> seen = new HashSet<>();
        private boolean capped;

        void offer(String value) {
            if (value == null || capped) {
                return;
            }
            seen.add(value);
            if (seen.size() > DICT_CAP) {
                capped = true;
                seen = null;
            }
        }

        boolean useDict() {
            return !capped;
        }
    }

    // --- packing ---

    private static final class Writer {

        private byte[] buf = new byte[256];
        private int len;
        private final Dict[] dicts;
        private final boolean[] useDict;
        private byte[] prevKey = EMPTY_KEY;

        Writer(int hint, boolean[] useDict) {
            if (hint > 8) {
                buf = new byte[Math.min(hint * 48, 1 << 20)];
            }
            this.useDict = useDict;
            this.dicts = new Dict[DICT_COLUMN_COUNT];
            for (int i = 0; i < dicts.length; i++) {
                dicts[i] = new Dict();
            }
        }

        void tag(byte t) {
            put(t);
        }

        /**
         * Front-coded key: {@code varint(sharedPrefixLen)} + {@code varint(suffixLen)} +
         * suffix bytes, against the previous key written to this block (the empty key for the first
         * row). {@link #commonPrefixLength} makes no ordering assumption — always correct, and a
         * shared length of 0 (e.g. an out-of-order or hash-like page) is simply the "no win" case.
         */
        void key(KeyBytes k) {
            byte[] raw = k.rawUnsafe();
            int shared = commonPrefixLength(prevKey, raw);
            int suffixLen = raw.length - shared;
            varint(shared);
            varint(suffixLen);
            write(raw, shared, suffixLen);
            prevKey = raw;
        }

        void fixedLong(long v) {
            ensure(8);
            buf[len++] = (byte) (v >>> 56);
            buf[len++] = (byte) (v >>> 48);
            buf[len++] = (byte) (v >>> 40);
            buf[len++] = (byte) (v >>> 32);
            buf[len++] = (byte) (v >>> 24);
            buf[len++] = (byte) (v >>> 16);
            buf[len++] = (byte) (v >>> 8);
            buf[len++] = (byte) v;
        }

        void bool(boolean b) {
            put((byte) (b ? 1 : 0));
        }

        void nullableString(String s) {
            if (s == null) {
                varint(0);
                return;
            }
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            varint(bytes.length + 1);
            write(bytes);
        }

        void etag(String etag) {
            if (etag == null) {
                put(ETAG_NULL);
                return;
            }
            byte[] packed = tryPackMd5(etag);
            if (packed != null) {
                put(ETAG_PACKED_MD5);
                write(packed);
            } else {
                put(ETAG_RAW);
                byte[] bytes = etag.getBytes(StandardCharsets.UTF_8);
                varint(bytes.length);
                write(bytes);
            }
        }

        /**
         * Dict-or-raw encode for a low-cardinality-eligible column: {@code col}'s mode was
         * decided ONCE for the whole block (the pre-pass probe in {@link #pack}), so this never
         * switches mid-block and decode needs no per-row marker.
         */
        void dictOrRaw(DictColumn col, String value) {
            if (value == null) {
                varint(0);
                return;
            }
            if (useDict[col.ordinal()]) {
                varint(dicts[col.ordinal()].indexOf(value) + 1);
            } else {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                varint(bytes.length + 1);
                write(bytes);
            }
        }

        String[][] dictArrays() {
            String[][] out = new String[dicts.length][];
            for (int i = 0; i < dicts.length; i++) {
                out[i] = dicts[i].toArray();
            }
            return out;
        }

        byte[] toBytes() {
            byte[] out = new byte[len];
            System.arraycopy(buf, 0, out, 0, len);
            return out;
        }

        private void varint(int v) {
            int x = v;
            while ((x & ~0x7F) != 0) {
                put((byte) ((x & 0x7F) | 0x80));
                x >>>= 7;
            }
            put((byte) x);
        }

        private void put(byte b) {
            ensure(1);
            buf[len++] = b;
        }

        private void write(byte[] bytes) {
            write(bytes, 0, bytes.length);
        }

        private void write(byte[] bytes, int off, int length) {
            ensure(length);
            System.arraycopy(bytes, off, buf, len, length);
            len += length;
        }

        private void ensure(int extra) {
            if (len + extra > buf.length) {
                int cap = buf.length;
                while (cap < len + extra) {
                    cap <<= 1;
                }
                byte[] grown = new byte[cap];
                System.arraycopy(buf, 0, grown, 0, len);
                buf = grown;
            }
        }
    }

    /** Per-column dictionary being built at write time: values in first-seen order, plus a reverse index. */
    private static final class Dict {
        private final List<String> values = new ArrayList<>();
        private final Map<String, Integer> index = new HashMap<>();

        int indexOf(String value) {
            Integer idx = index.get(value);
            if (idx == null) {
                idx = values.size();
                values.add(value);
                index.put(value, idx);
            }
            return idx;
        }

        String[] toArray() {
            return values.toArray(new String[0]);
        }
    }

    // --- etag md5 packing ---

    /** 16 packed bytes iff {@code etag} is exactly 32 lowercase-hex chars, else {@code null}. */
    private static byte[] tryPackMd5(String etag) {
        if (etag.length() != 32) {
            return null;
        }
        byte[] out = new byte[16];
        for (int i = 0; i < 16; i++) {
            int hi = hexVal(etag.charAt(i * 2));
            int lo = hexVal(etag.charAt(i * 2 + 1));
            if (hi < 0 || lo < 0) {
                return null;   // not lowercase hex — fall back to raw so case round-trips exactly
            }
            out[i] = (byte) ((hi << 4) | lo);
        }
        return out;
    }

    private static String unpackMd5(byte[] buf, int off) {
        char[] hex = new char[32];
        for (int i = 0; i < 16; i++) {
            int b = buf[off + i] & 0xFF;
            hex[i * 2] = HEX[b >>> 4];
            hex[i * 2 + 1] = HEX[b & 0x0F];
        }
        return new String(hex);
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /** Value of a lowercase-hex char, or -1 if it is not one (uppercase falls back to raw). */
    private static int hexVal(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        return -1;
    }
}
