/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.checkpoint;

import io.varve.swath.sort.PageRunFormat;
import java.util.List;

/**
 * One finalized (footer-fsynced) output part and the {@code durable_cursor}
 * advances it makes durable (algorithms.md §4.5, I6). Recorded in <b>one</b>
 * transaction: insert the {@code part_file} row and, for each node whose pages
 * the part held, advance its {@code durable_cursor} to the highest such key.
 *
 * <p>This — not the on-disk footer — is the exactly-once <b>commit point</b>: a
 * crash after the footer fsync but before this commit leaves the part un-recorded,
 * so resume discards it and re-lists its tail (no finalized row is ever lost or
 * duplicated).
 *
 * <p>{@code formatVersion}/{@code extensionType} are both nullable for ordinary output formats.
 * New page-run staging events must instead carry an explicit, supported {@link PageRunFormat};
 * legacy pre-column {@code NULL}/{@code NULL} metadata exists only when reading old SQLite rows and
 * is never produced by this write-side value.
 */
public record PartFinalize(
        long runId,
        int writerId,
        String path,
        String format,
        Integer formatVersion,
        Integer extensionType,
        long rows,
        long bytes,
        List<DurableAdvance> advances) {

    /** Output formats without versioned internal staging metadata retain the established shape. */
    public PartFinalize(long runId, int writerId, String path, String format, long rows, long bytes,
                        List<DurableAdvance> advances) {
        this(runId, writerId, path, format, null, null, rows, bytes, advances);
    }

    /** A page-run staging part, carrying the actual header/extension identity its encoder emitted. */
    public PartFinalize(long runId, int writerId, String path, PageRunFormat pageRunFormat,
                        long rows, long bytes, List<DurableAdvance> advances) {
        this(runId, writerId, path, PageRunFormat.NAME, pageRunFormat.formatVersion(),
                pageRunFormat.extensionType(), rows, bytes, advances);
    }

    public PartFinalize {
        if ((formatVersion == null) != (extensionType == null)) {
            throw new IllegalArgumentException(
                    "format_version and extension_type must both be recorded or both be absent");
        }
        if ((formatVersion != null && formatVersion < 0)
                || (extensionType != null && extensionType < 0)) {
            throw new IllegalArgumentException("part format metadata must be non-negative");
        }
        boolean pageRun = PageRunFormat.NAME.equals(format);
        if (!pageRun && (formatVersion != null || extensionType != null)) {
            throw new IllegalArgumentException(
                    "only page-run staging parts may carry format_version and extension_type");
        }
        if (pageRun) {
            PageRunFormat.Compatibility compatibility =
                    PageRunFormat.compatibility(formatVersion, extensionType);
            if (compatibility != PageRunFormat.Compatibility.SUPPORTED) {
                throw new IllegalArgumentException("page-run staging parts require complete supported "
                        + "format metadata, got format_version=" + formatVersion
                        + ", extension_type=" + extensionType + " (" + compatibility + ")");
            }
        }
    }

    /** A node's highest key whose pages this part made durable. */
    public record DurableAdvance(long nodeId, byte[] maxKey) {
    }
}
