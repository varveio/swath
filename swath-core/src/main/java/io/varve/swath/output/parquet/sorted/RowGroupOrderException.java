/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import io.varve.swath.sort.spill.SegmentCorruptionException;
import java.nio.file.Path;

/**
 * One row group's rows are not in strictly ascending unsigned key order, raised where they are read.
 * Typed rather than a bare {@link IllegalStateException} for the same reason
 * {@link SegmentCorruptionException} is: the caller that hits this is a sweep over a corpus of
 * fixtures, and it must be able to classify the exclusion — "this fixture is internally disordered",
 * as opposed to any other read failure — from {@link #reason()} and a counter, never by matching
 * substrings of a message.
 *
 * <p>The failure is raised in two shapes, because the two structures that see it know different
 * amounts. {@link #at} is for a reader that knows which file and row group it is decoding
 * ({@code SortedParquetRowGroupReader.KeyCursor}); {@link #atRow} is for a key structure built out of one
 * row group's rows, which knows its own row ordinal and nothing more, and whose caller adds the rest
 * with {@link #locatedIn}.
 *
 * <p>{@link #redactedMessage()} is the same report with the fixture reduced to its file name, for a
 * surface that must not publish a server's filesystem layout (the replay server's HTTP error body);
 * the full path stays in {@link #getMessage()}, which is what a server logs.
 */
public final class RowGroupOrderException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /** The one reason in play: a row group's own rows are not in strictly ascending unsigned order. */
    public static final String ROW_GROUP_DISORDER = "row_group_disorder";

    private final String reason;
    private final Path file;
    private final int rowGroup;
    private final long row;
    private final String detail;

    private RowGroupOrderException(String reason, Path file, int rowGroup, long row, String detail,
                                   Throwable cause) {
        super(message(reason, file, rowGroup, detail), cause);
        this.reason = reason;
        this.file = file;
        this.rowGroup = rowGroup;
        this.row = row;
        this.detail = detail;
    }

    /**
     * The disorder as a reader that knows the fixture it is reading reports it.
     *
     * @param row    the 0-based row within {@code rowGroup} that is at or below its predecessor
     * @param detail what was wrong, in the reader's own terms
     */
    public static RowGroupOrderException at(Path file, int rowGroup, long row, String detail) {
        return new RowGroupOrderException(ROW_GROUP_DISORDER, file, rowGroup, row, detail, null);
    }

    /**
     * The disorder as a structure that does not know its fixture reports it — see {@link #locatedIn},
     * which re-raises it with the file and row group its rows came from.
     */
    public static RowGroupOrderException atRow(long row, String detail) {
        return new RowGroupOrderException(ROW_GROUP_DISORDER, null, -1, row, detail, null);
    }

    /** This failure re-raised naming the file and row group its rows came from. */
    public RowGroupOrderException locatedIn(Path file, int rowGroup) {
        return new RowGroupOrderException(reason, file, rowGroup, row, detail, this);
    }

    /** The machine-readable classification: currently always {@link #ROW_GROUP_DISORDER}. */
    public String reason() {
        return reason;
    }

    /** The fixture file, or {@code null} when this failure has not been {@linkplain #locatedIn located}. */
    public Path file() {
        return file;
    }

    /** The physical row-group block index, or {@code -1} when this failure has not been located. */
    public int rowGroup() {
        return rowGroup;
    }

    /** The 0-based row within the row group that broke the ascent. */
    public long row() {
        return row;
    }

    /** {@link #getMessage()} with the fixture reduced to its file name — see the class javadoc. */
    public String redactedMessage() {
        return message(reason, file == null ? null : file.getFileName(), rowGroup, detail);
    }

    private static String message(String reason, Path file, int rowGroup, String detail) {
        String where = file == null ? "a row group" : "row group " + rowGroup + " of " + file;
        return where + " is not sorted (reason=" + reason + "): " + detail;
    }
}
