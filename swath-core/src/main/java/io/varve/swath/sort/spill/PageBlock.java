/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

/**
 * Compact immutable per-page model. Entries are packed at admission into one front-coded payload;
 * {@link PageBlockCodec} owns its byte layout and {@link PageBlockCursor} decodes it sequentially.
 * The block stores no per-row index and stays close to the logical byte size rather than retaining
 * parsed {@link ListEntry} objects.
 *
 * <p>Low-cardinality string columns use per-block dictionaries when their distinct count does not
 * exceed {@link #DICT_CAP}. {@code owner_id} and {@code owner_display_name} may legitimately exceed
 * that cap, in which case the entire column uses raw values. Payload compression is selected once
 * at pack time; record headers remain uncompressed for decode-free frontier reads.
 * A persisted block retains the one CRC-validated record-body array read from disk and addresses
 * its stored payload by offset/length. Header parsing never copies that payload. For {@link
 * PageCodec#NONE}, cursors read the slice in place; compressed codecs allocate only the required
 * decompressed payload.
 *
 * <p>{@link #pack} also records whether entries are non-decreasing under the full comparator and
 * retains the first and last entries. Persisted blocks lazily reconstruct those entries from their
 * payload. The lazy caches are benign races: decoding is deterministic and blocks are otherwise
 * immutable.
 */
final class PageBlock {

    /** Per-entry fixed overhead used by the staging byte estimate. */
    static final int ENTRY_OVERHEAD_BYTES = 64;

    /** Maximum distinct values retained by one dictionary-eligible column. */
    static final int DICT_CAP = 64;

    /** Hard allocation ceiling for one decoded S3 page payload read from an internal segment. */
    static final int MAX_RAW_PAYLOAD_BYTES = 256 * 1024 * 1024;

    /** Packed payload array, or the complete owned record body for a persisted block. */
    private final byte[] payloadOwner;
    private final int payloadOffset;
    private final int payloadLength;
    private final int rawPayloadLength;
    private final PageCodec codec;
    private byte[] decodedPayloadCache;
    private final PageBlockDictionaries dictionaries;
    private final boolean[] useDict;
    private final int count;
    private final byte[] firstKeyBytes;
    private final byte[] lastKeyBytes;
    private ListEntry firstEntry;
    private ListEntry lastEntry;
    private final long stagingEstimatedBytes;
    private final boolean orderedUnderFullComparator;
    /** Non-null only for a persisted page, so cursor-time decode faults retain typed path context. */
    private final Path sourcePath;
    /** The one header parse that produced a persisted block; null for admission-packed blocks. */
    private final PageBlockCodec.Header parsedHeader;

    PageBlock(byte[] storedPayload, int rawPayloadLength, PageCodec codec, String[][] dicts,
              boolean[] useDict, int count, byte[] firstKeyBytes, byte[] lastKeyBytes,
              ListEntry firstEntry, ListEntry lastEntry, long estimatedBytes,
              boolean orderedUnderFullComparator, Path sourcePath) {
        this.payloadOwner = storedPayload;
        this.payloadOffset = 0;
        this.payloadLength = storedPayload.length;
        this.rawPayloadLength = rawPayloadLength;
        this.codec = codec;
        this.dictionaries = PageBlockDictionaries.packed(dicts);
        this.useDict = useDict;
        this.count = count;
        this.firstKeyBytes = firstKeyBytes;
        this.lastKeyBytes = lastKeyBytes;
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.stagingEstimatedBytes = estimatedBytes;
        this.orderedUnderFullComparator = orderedUnderFullComparator;
        this.sourcePath = sourcePath;
        this.parsedHeader = null;
    }

    /** Persisted-page constructor: retains the one owned record body and its parsed payload slice. */
    PageBlock(PageBlockCodec.Header header, byte[] recordBody,
              ListEntry firstEntry, ListEntry lastEntry, Path sourcePath) {
        this.payloadOwner = recordBody;
        this.payloadOffset = header.payloadOffset();
        this.payloadLength = header.payloadLength();
        this.rawPayloadLength = header.rawPayloadLength();
        this.codec = header.codec();
        this.dictionaries = header.dictionaries();
        this.useDict = header.useDict();
        this.count = header.count();
        this.firstKeyBytes = header.minKey();
        this.lastKeyBytes = header.maxKey();
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.stagingEstimatedBytes = -1;
        this.orderedUnderFullComparator = header.ordered();
        this.sourcePath = sourcePath;
        this.parsedHeader = header;
    }

    /** Pack a page without payload compression. */
    static PageBlock pack(List<ListEntry> entries, Comparator<ListEntry> comparator) {
        return pack(entries, comparator, PageCodec.NONE);
    }

    /**
     * Pack a non-empty page, retaining its input order and recording full-comparator orderedness.
     * Front coding compares each key only with its immediate predecessor and therefore makes no
     * sorted-input assumption.
     */
    static PageBlock pack(List<ListEntry> entries, Comparator<ListEntry> comparator, PageCodec codec) {
        return pack(entries, comparator, codec, MAX_RAW_PAYLOAD_BYTES);
    }

    /** Pack while refusing to grow the raw payload beyond a merge-planned page residency limit. */
    static PageBlock pack(List<ListEntry> entries, Comparator<ListEntry> comparator, PageCodec codec,
                          int maxRawPayloadBytes) {
        return PageBlockPacker.pack(entries, comparator, codec, maxRawPayloadBytes);
    }

    /** Internal control signal used to split a cascade batch before it exceeds planned residency. */
    static final class RawPayloadLimitException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        RawPayloadLimitException(int limit) {
            super("raw page payload exceeds the planned " + limit + " byte limit");
        }
    }

    /** The first entry's key as a defensive copy. */
    byte[] firstKey() {
        return firstKeyBytes.clone();
    }

    /** Read-only internal first-key view for allocation-sensitive package hot paths. */
    byte[] firstKeyUnsafe() {
        return firstKeyBytes;
    }

    /** The last entry's key as a defensive copy. */
    byte[] lastKey() {
        return lastKeyBytes.clone();
    }

    /** Full first entry, lazily decoded for persisted blocks. */
    ListEntry firstEntry() {
        ListEntry entry = firstEntry;
        if (entry == null) {
            entry = cursor().next();
            firstEntry = entry;
        }
        return entry;
    }

    /** Full last entry, lazily decoded by a sequential scan for persisted blocks. */
    ListEntry lastEntry() {
        ListEntry entry = lastEntry;
        if (entry == null) {
            PageBlockCursor cursor = cursor();
            while (cursor.hasNext()) {
                entry = cursor.next();
            }
            lastEntry = entry;
        }
        return entry;
    }

    /** True when entries are non-decreasing under the full list-entry comparator. */
    boolean orderedUnderFullComparator() {
        return orderedUnderFullComparator;
    }

    int count() {
        return count;
    }

    PageCodec codec() {
        return codec;
    }

    /** Entry-shape estimate used only while an admission-packed block enters the staging gate. */
    long stagingEstimatedBytes() {
        if (stagingEstimatedBytes < 0) {
            throw new IllegalStateException("persisted pages do not carry a staging estimate");
        }
        return stagingEstimatedBytes;
    }

    /** The shared logical-byte estimate used by every staging-segment gate. */
    static long estimatedBytes(ListEntry entry) {
        return entry.key().length() + ENTRY_OVERHEAD_BYTES;
    }

    /** Compressed payload footprint, excluding the plain record header. */
    long packedBytes() {
        return payloadLength;
    }

    /** A fresh sequential decoder, lazily decompressing this block's payload once. */
    PageBlockCursor cursor() {
        try {
            if (codec == PageCodec.NONE) {
                if (payloadLength != rawPayloadLength) {
                    throw new IllegalStateException(
                            "PageBlock NONE codec length mismatch: expected " + rawPayloadLength
                                    + " but stored is " + payloadLength);
                }
                return new PageBlockCursor(this, payloadOwner, payloadOffset, payloadLength);
            }
            byte[] decoded = decodedPayload();
            return new PageBlockCursor(this, decoded, 0, decoded.length);
        } catch (RuntimeException e) {
            throw decodedCorruption(e);
        }
    }

    byte[] serialize() {
        return PageBlockCodec.serialize(this);
    }

    /**
     * Parse a persisted body and transfer its immutable ownership to the returned block. Package
     * callers must not mutate {@code record} afterwards.
     */
    static PageBlock deserialize(byte[] record) {
        return PageBlockCodec.deserialize(record, null);
    }

    /** As {@link #deserialize(byte[])}, retaining typed corruption path context. */
    static PageBlock deserialize(byte[] record, Path sourcePath) {
        return PageBlockCodec.deserialize(record, sourcePath);
    }

    byte[] payloadOwnerUnsafe() {
        return payloadOwner;
    }

    int payloadOffset() {
        return payloadOffset;
    }

    int payloadLength() {
        return payloadLength;
    }

    PageBlockCodec.Header parsedHeaderUnsafe() {
        return parsedHeader;
    }

    /** True for a deserialized page, where stored row order is format truth. */
    boolean validatesPersistedOrder() {
        return parsedHeader != null;
    }

    int rawPayloadLength() {
        return rawPayloadLength;
    }

    /** Bytes of the retained CRC-verified record body backing a persisted page. */
    int retainedRecordBytes() {
        return payloadOwner.length;
    }

    long dictionaryCacheBudgetBytes() {
        return dictionaries.decodedCacheBudgetBytes();
    }

    int dictionaryCoordinateBytes() {
        return dictionaries.coordinateBytes();
    }

    PageBlockDictionaries dictionariesUnsafe() {
        return dictionaries;
    }

    boolean[] useDictUnsafe() {
        return useDict;
    }

    byte[] lastKeyUnsafe() {
        return lastKeyBytes;
    }

    private byte[] decodedPayload() {
        byte[] cached = decodedPayloadCache;
        if (cached == null) {
            cached = codec.decompress(payloadOwner, payloadOffset, payloadLength,
                    rawPayloadLength);
            decodedPayloadCache = cached;
        }
        return cached;
    }

    RuntimeException decodedCorruption(RuntimeException cause) {
        if (sourcePath == null || cause instanceof UncheckedIOException) {
            return cause;
        }
        return new UncheckedIOException(new SegmentCorruptionException(sourcePath,
                SegmentCorruptionException.PAGE_RUN_BODY_CORRUPTION,
                "decoded page does not match its structural metadata", cause));
    }

}
