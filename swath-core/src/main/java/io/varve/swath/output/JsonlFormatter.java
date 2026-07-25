/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.RowType;
import java.io.IOException;
import java.io.Writer;

/**
 * One JSON object per line. Keys are emitted as JSON strings
 * (always validly escaped — JSON validity is not affected by {@code
 * --raw-output}); the summary is not written to the data stream so {@code
 * jq -s length} equals the row count (INT-2).
 */
public final class JsonlFormatter implements EntryFormatter {

    private final Writer out;
    private final StringBuilder sb = new StringBuilder(256);

    public JsonlFormatter(Writer out) {
        this.out = out;
    }

    @Override
    public void writeHeader() {
        // JSONL has no header.
    }

    @Override
    public void write(ListEntry e) throws IOException {
        sb.setLength(0);
        sb.append("{\"key\":");
        Json.quote(sb, e.key().asString());   // first field, no leading comma
        switch (e) {
            case ObjectEntry o -> {
                num("size", o.size());
                time(o.lastModifiedEpochMicros());
                optStr("etag", o.etag());
                optStr("storage_class", o.storageClass());
                optStr("version_id", o.versionId());
                if (o.versionId() != null) {
                    bool("is_latest", o.isLatest());
                }
                optStr("owner_id", o.ownerId());
                optStr("owner_display_name", o.ownerDisplayName());
                optStr("checksum_algorithm", o.checksumAlgorithm());
                optStr("checksum_type", o.checksumType());
            }
            case DeleteMarkerEntry d -> {
                optStr("version_id", d.versionId());
                bool("is_latest", d.isLatest());
                time(d.lastModifiedEpochMicros());
                optStr("owner_id", d.ownerId());
            }
            case CommonPrefixEntry ignored -> {
                // key + row_type only
            }
        }
        sb.append(",\"row_type\":\"").append(RowType.of(e)).append("\"}\n");
        out.write(sb.toString());
    }

    /** Every field after {@code key} is comma-prefixed. */
    private void str(String name, String value) {
        sb.append(",\"").append(name).append("\":");
        Json.quote(sb, value);
    }

    private void num(String name, long value) {
        sb.append(",\"").append(name).append("\":").append(value);
    }

    private void bool(String name, boolean value) {
        sb.append(",\"").append(name).append("\":").append(value);
    }

    private void time(long epochMicros) {
        String iso = Fields.isoMicros(epochMicros);
        if (!iso.isEmpty()) {
            str("last_modified", iso);
        }
    }

    private void optStr(String name, String value) {
        if (value != null) {
            str(name, value);
        }
    }

    @Override
    public void close() throws IOException {
        out.flush();
    }
}
