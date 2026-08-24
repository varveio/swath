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
 * Tab-separated rows: {@code key  size  last_modified  etag  storage_class
 * row_type}. Control-char escaping (on by default) renders embedded tabs/newlines
 * as {@code \xHH}, so a key can never break the column layout.
 */
public final class TsvFormatter implements EntryFormatter {

    private static final String HEADER = "key\tsize\tlast_modified\tetag\tstorage_class\trow_type\n";

    private final Writer out;
    private final boolean escape;
    private final StringBuilder sb = new StringBuilder(256);

    public TsvFormatter(Writer out, boolean escape) {
        this.out = out;
        this.escape = escape;
    }

    @Override
    public void writeHeader() throws IOException {
        out.write(HEADER);
    }

    @Override
    public void write(ListEntry e) throws IOException {
        sb.setLength(0);
        sb.append(text(e.key().asString())).append('\t');
        switch (e) {
            case ObjectEntry o -> sb.append(o.size()).append('\t')
                    .append(text(o.lastModifiedText())).append('\t')
                    .append(text(Fields.orEmpty(o.etag()))).append('\t')
                    .append(text(Fields.orEmpty(o.storageClass())));
            case DeleteMarkerEntry d -> sb.append('\t')
                    .append(text(d.lastModifiedText())).append('\t')
                    .append('\t');
            case CommonPrefixEntry ignored -> sb.append('\t').append('\t').append('\t');
        }
        sb.append('\t').append(RowType.of(e)).append('\n');
        out.write(sb.toString());
    }

    private String text(String s) {
        return escape ? ControlCharEscaper.escape(s) : s;
    }

    @Override
    public void close() throws IOException {
        out.flush();
    }
}
