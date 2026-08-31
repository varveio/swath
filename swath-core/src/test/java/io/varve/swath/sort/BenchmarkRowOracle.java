/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

/** Constant-memory source multiset oracle and physically ordered output fingerprint. */
final class BenchmarkRowOracle {

    record SourceSegment(Path path, long expectedRows, long expectedBytes) {
    }

    record InputOracle(long rows, long trailerEntries, long trailerRecords, String multisetDigest) {
    }

    record OutputValidation(long rows, String multisetDigest, String orderedFingerprint) {
    }

    private BenchmarkRowOracle() {
    }

    static InputOracle readInputs(List<SourceSegment> inputs) throws IOException {
        CommutativeDigest multiset = new CommutativeDigest();
        long rows = 0;
        long trailerEntries = 0;
        long trailerRecords = 0;
        for (SourceSegment input : inputs) {
            if (java.nio.file.Files.size(input.path()) != input.expectedBytes()) {
                throw new IOException("catalog bytes disagree with page-run segment: " + input.path());
            }
            PageRunTrailer.Trailer trailer;
            try (PageRunSegmentIo io = PageRunSegmentIo.open(input.path(), SortMetrics.NO_OP)) {
                trailer = PageRunTrailer.read(io);
            }
            if (trailer.totalEntries() != input.expectedRows()) {
                throw new IOException("checkpoint rows disagree with page-run trailer: " + input.path());
            }
            long segmentRows = 0;
            try (PageRunSegmentIo io = PageRunSegmentIo.open(input.path(), SortMetrics.NO_OP)) {
                PageRunSegmentIo.Page page;
                while ((page = io.nextPage()) != null) {
                    PageBlockCursor cursor = page.decode(input.path()).cursor();
                    while (cursor.hasNext()) {
                        multiset.add(cursor.next());
                        segmentRows++;
                    }
                    cursor.drainAndValidate();
                }
            }
            if (segmentRows != trailer.totalEntries()) {
                throw new IOException("decoded rows disagree with page-run trailer: " + input.path());
            }
            rows += segmentRows;
            trailerEntries += trailer.totalEntries();
            trailerRecords += trailer.totalRecords();
        }
        if (rows != trailerEntries) {
            throw new IOException("input oracle rows disagree with aggregate trailer entries");
        }
        return new InputOracle(rows, trailerEntries, trailerRecords, multiset.hex());
    }

    static OutputValidation validateOutput(List<Path> files, InputOracle input,
                                           Comparator<ListEntry> comparator) throws IOException {
        MessageDigest ordered = sha256();
        CommutativeDigest multiset = new CommutativeDigest();
        ListEntry previous = null;
        long rows = 0;
        for (Path file : files) {
            try (SegmentReader reader = new SegmentReader(file)) {
                while (reader.hasNext()) {
                    ListEntry entry = reader.next();
                    if (previous != null && comparator.compare(previous, entry) > 0) {
                        throw new IOException("benchmark output is not physically sorted at row " + rows);
                    }
                    updateEntry(ordered, entry);
                    multiset.add(entry);
                    previous = entry;
                    rows++;
                }
            }
        }
        String multisetHex = multiset.hex();
        if (rows != input.rows() || rows != input.trailerEntries()
                || !multisetHex.equals(input.multisetDigest())) {
            throw new IOException("benchmark output does not match input oracle: rows=" + rows
                    + " expected_rows=" + input.rows() + " multiset=" + multisetHex
                    + " expected_multiset=" + input.multisetDigest());
        }
        return new OutputValidation(rows, multisetHex, HexFormat.of().formatHex(ordered.digest()));
    }

    static OutputValidation validateEntriesForTesting(List<ListEntry> entries, InputOracle input,
                                                       Comparator<ListEntry> comparator) throws IOException {
        MessageDigest ordered = sha256();
        CommutativeDigest multiset = new CommutativeDigest();
        ListEntry previous = null;
        long rows = 0;
        for (ListEntry entry : entries) {
            if (previous != null && comparator.compare(previous, entry) > 0) {
                throw new IOException("benchmark output is not physically sorted at row " + rows);
            }
            updateEntry(ordered, entry);
            multiset.add(entry);
            previous = entry;
            rows++;
        }
        String digest = multiset.hex();
        if (rows != input.rows() || !digest.equals(input.multisetDigest())) {
            throw new IOException("benchmark output does not match input oracle");
        }
        return new OutputValidation(rows, digest, HexFormat.of().formatHex(ordered.digest()));
    }

    static InputOracle inputForTesting(List<ListEntry> entries) {
        CommutativeDigest digest = new CommutativeDigest();
        entries.forEach(digest::add);
        return new InputOracle(entries.size(), entries.size(), entries.size(), digest.hex());
    }

    /**
     * Hash the canonical Parquet row, not incidental {@link ListEntry} source representation.
     * Sorted spill and Parquet both store last-modified as epoch microseconds. Parquet also omits
     * {@code is_latest} for a versionless object, so its decoded value is false even though the S3
     * OBJECTS mapper marks the live entry latest. Those are writer-schema normalizations, not row
     * loss; every representable field remains in the digest.
     */
    private static void updateEntry(MessageDigest digest, ListEntry entry) {
        switch (entry) {
            case ObjectEntry object -> {
                digest.update((byte) 1);
                updateBytes(digest, object.key().rawUnsafe());
                updateLong(digest, object.size());
                updateLong(digest, object.lastModifiedEpochMicros());
                updateString(digest, object.etag());
                updateString(digest, object.storageClass());
                updateString(digest, object.versionId());
                digest.update((byte) (object.versionId() != null && object.isLatest() ? 1 : 0));
                updateString(digest, object.ownerId());
                updateString(digest, object.ownerDisplayName());
                updateString(digest, object.checksumAlgorithm());
                updateString(digest, object.checksumType());
            }
            case CommonPrefixEntry prefix -> {
                digest.update((byte) 2);
                updateBytes(digest, prefix.key().rawUnsafe());
            }
            case DeleteMarkerEntry marker -> {
                digest.update((byte) 3);
                updateBytes(digest, marker.key().rawUnsafe());
                updateString(digest, marker.versionId());
                digest.update((byte) (marker.isLatest() ? 1 : 0));
                updateLong(digest, marker.lastModifiedEpochMicros());
                updateString(digest, marker.ownerId());
            }
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 is required by the JDK", e);
        }
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static void updateString(MessageDigest digest, String value) {
        if (value == null) {
            updateLong(digest, -1L);
        } else {
            updateBytes(digest, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void updateBytes(MessageDigest digest, byte[] bytes) {
        updateLong(digest, bytes.length);
        digest.update(bytes);
    }

    /** Two independent 256-bit modular sums retain multiplicity without depending on row order. */
    private static final class CommutativeDigest {
        private final byte[] first = new byte[32];
        private final byte[] second = new byte[32];

        void add(ListEntry entry) {
            MessageDigest a = sha256();
            a.update((byte) 0x51);
            updateEntry(a, entry);
            addUnsigned(first, a.digest());
            MessageDigest b = sha256();
            b.update((byte) 0xA7);
            updateEntry(b, entry);
            addUnsigned(second, b.digest());
        }

        String hex() {
            return HexFormat.of().formatHex(first) + HexFormat.of().formatHex(second);
        }

        private static void addUnsigned(byte[] accumulator, byte[] value) {
            int carry = 0;
            for (int i = accumulator.length - 1; i >= 0; i--) {
                int sum = (accumulator[i] & 0xFF) + (value[i] & 0xFF) + carry;
                accumulator[i] = (byte) sum;
                carry = sum >>> 8;
            }
        }
    }
}
