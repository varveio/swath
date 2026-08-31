/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import io.varve.swath.sort.SortMode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

/**
 * Detects and reads the sortedness stamp ({@link SortedParquetWriter}) from a Parquet file's
 * footer key-value metadata. {@link #read} returns {@link Optional#empty()} unless the file carries
 * all three required stamp keys with valid values: a recognized {@link SortedParquetWriter#ORDER_VALUE}
 * (a forward-compatibility guard: a future writer's order string must never be silently trusted by an
 * older reader), an understood {@link SortMode}, and a parseable format version. A missing or malformed
 * value for any of those three — as well as no stamp at all — yields empty; a malformed
 * {@link SortedParquetWriter#FILE_INDEX_KEY}, by contrast, is tolerated (see {@code fileIndex}) and never
 * invalidates an otherwise-recognized stamp. Used by
 * the replay server ({@code io.varve.swath.replay.fixture}) and the sort-mismatch-on-resume
 * check.
 *
 * @param fileIndex this file's 1-based position in the output's roll sequence;
 *                  defaults to {@code 1} when {@link SortedParquetWriter#FILE_INDEX_KEY} is absent or its
 *                  value is unparseable as an int — additive key, so neither absence nor a malformed value
 *                  invalidates an otherwise-recognized stamp, and {@code 1} is the correct reading for a
 *                  single-file output (the only shape a stamp predating this key could ever have come from)
 * @param fileFinal whether this file is stamped as the LAST file of the output
 *                  ({@link SortedParquetWriter#FILE_FINAL_KEY} present and equal to {@link
 *                  SortedParquetWriter#FILE_FINAL_VALUE}); {@code false} when absent (the key is never
 *                  written {@code "false"} — absence IS the negative case)
 */
public record SortedParquetStamp(String order, SortMode mode, int formatVersion, int fileIndex, boolean fileFinal) {

    private static final int SUPPORTED_FORMAT_VERSION = Integer.parseInt(SortedParquetWriter.FORMAT_VERSION_VALUE);

    public static Optional<SortedParquetStamp> read(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return fromKeyValueMetaData(reader.getFooter().getFileMetaData().getKeyValueMetaData());
        }
    }

    /**
     * Whether {@link #formatVersion()} is one this reader actually understands (currently only
     * {@code 1}) — distinct from merely being <em>parseable</em> as an int. A stamp with a future,
     * unrecognized format version must never be silently trusted by an older reader: the two
     * additive keys ({@link #fileIndex()}/{@link #fileFinal()}) are safe to add
     * within version 1, but a hypothetical version 2 could mean anything, so an older reader that
     * only understands 1 must decline rather than guess.
     */
    public boolean isKnownFormatVersion() {
        return formatVersion == SUPPORTED_FORMAT_VERSION;
    }

    /** Package-visible for unit tests that synthesize footer KV maps directly. */
    static Optional<SortedParquetStamp> fromKeyValueMetaData(Map<String, String> kv) {
        String order = kv.get(SortedParquetWriter.ORDER_KEY);
        if (!SortedParquetWriter.ORDER_VALUE.equals(order)) {
            return Optional.empty();
        }
        Optional<SortMode> mode = SortMode.fromValue(kv.get(SortedParquetWriter.MODE_KEY));
        if (mode.isEmpty()) {
            return Optional.empty();
        }
        Integer formatVersion = parseInt(kv.get(SortedParquetWriter.FORMAT_VERSION_KEY));
        if (formatVersion == null) {
            return Optional.empty();
        }
        Integer fileIndex = parseInt(kv.get(SortedParquetWriter.FILE_INDEX_KEY));
        boolean fileFinal = SortedParquetWriter.FILE_FINAL_VALUE.equals(kv.get(SortedParquetWriter.FILE_FINAL_KEY));
        return Optional.of(new SortedParquetStamp(order, mode.get(), formatVersion, fileIndex == null ? 1 : fileIndex, fileFinal));
    }

    private static Integer parseInt(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
