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
import java.io.IOException;
import java.io.Writer;

/**
 * Human-readable aligned columns (the TTY default): right-justified size, a
 * fixed-width timestamp, then the (escaped) key. Streams per row with fixed
 * column widths — no full-result buffering, so it holds at any scale.
 */
public final class AlignedFormatter implements EntryFormatter {

    private static final int SIZE_WIDTH = 14;
    private static final int TIME_WIDTH = 24;

    private final Writer out;
    private final boolean escape;
    private final StringBuilder sb = new StringBuilder(256);

    public AlignedFormatter(Writer out, boolean escape) {
        this.out = out;
        this.escape = escape;
    }

    @Override
    public void writeHeader() {
        // No header for the aligned view.
    }

    @Override
    public void write(ListEntry e) throws IOException {
        sb.setLength(0);
        String size;
        String time;
        switch (e) {
            case ObjectEntry o -> {
                size = Long.toString(o.size());
                time = escaped(o.lastModifiedText());
            }
            case DeleteMarkerEntry d -> {
                size = "-";
                time = escaped(d.lastModifiedText());
            }
            case CommonPrefixEntry ignored -> {
                size = "PRE";
                time = "";
            }
        }
        pad(size, SIZE_WIDTH, true);
        sb.append("  ");
        pad(time, TIME_WIDTH, false);
        sb.append("  ");
        sb.append(escaped(e.key().asString()));
        sb.append('\n');
        out.write(sb.toString());
    }

    private String escaped(String value) {
        return escape ? ControlCharEscaper.escape(value) : value;
    }

    private void pad(String value, int width, boolean right) {
        int gap = width - value.length();
        if (gap <= 0) {
            sb.append(value);
            return;
        }
        if (right) {
            sb.append(" ".repeat(gap)).append(value);
        } else {
            sb.append(value).append(" ".repeat(gap));
        }
    }

    @Override
    public void close() throws IOException {
        out.flush();
    }
}
