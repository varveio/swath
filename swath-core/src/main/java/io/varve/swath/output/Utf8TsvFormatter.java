/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Byte-oriented equivalent of {@link TsvFormatter} for partitioned dataset parts.
 *
 * <p>S3 keys arrive in {@link KeyBytes} as UTF-8. Valid key bytes are copied directly into one
 * reusable row buffer; malformed internal/test keys fall back through {@link KeyBytes#asString()}
 * so this remains byte-for-byte equivalent to the character formatter's replacement behavior.
 */
public final class Utf8TsvFormatter {
    private static final byte[] HEADER =
            "key\tsize\tlast_modified\tetag\tstorage_class\trow_type\n"
                    .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OBJECT = "OBJECT".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] DELETE_MARKER = "DELETE_MARKER".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COMMON_PREFIX = "COMMON_PREFIX".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] LOWER_HEX = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    private final OutputStream out;
    private final boolean escape;
    private byte[] row = new byte[2048];
    private int length;
    private long bytesWritten;

    public Utf8TsvFormatter(OutputStream out, boolean escape) {
        this.out = out;
        this.escape = escape;
    }

    public void writeHeader() throws IOException {
        out.write(HEADER);
        bytesWritten += HEADER.length;
    }

    public void write(ListEntry entry) throws IOException {
        length = 0;
        appendKey(entry.key());
        appendByte('\t');
        switch (entry) {
            case ObjectEntry object -> {
                appendNonNegativeLong(object.size());
                appendByte('\t');
                appendText(object.lastModifiedText());
                appendByte('\t');
                appendText(Fields.orEmpty(object.etag()));
                appendByte('\t');
                appendText(Fields.orEmpty(object.storageClass()));
                appendByte('\t');
                appendBytes(OBJECT);
            }
            case DeleteMarkerEntry marker -> {
                appendByte('\t');
                appendText(marker.lastModifiedText());
                appendByte('\t');
                appendByte('\t');
                appendByte('\t');
                appendBytes(DELETE_MARKER);
            }
            case CommonPrefixEntry ignored -> {
                appendByte('\t');
                appendByte('\t');
                appendByte('\t');
                appendByte('\t');
                appendBytes(COMMON_PREFIX);
            }
        }
        appendByte('\n');
        out.write(row, 0, length);
        bytesWritten += length;
    }

    public long bytesWritten() {
        return bytesWritten;
    }

    public void flush() throws IOException {
        out.flush();
    }

    public void close() throws IOException {
        out.close();
    }

    private void appendKey(KeyBytes key) {
        byte[] bytes = key.rawUnsafe();
        if (KeyBytes.isValidUtf8(bytes)) {
            appendEscapedBytes(bytes);
        } else {
            appendText(key.asString());
        }
    }

    private void appendText(String value) {
        int firstNonAscii = -1;
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) > 0x7f) {
                firstNonAscii = i;
                break;
            }
        }
        if (firstNonAscii < 0) {
            ensureCapacity(value.length() * (escape ? 4 : 1));
            for (int i = 0; i < value.length(); i++) {
                appendEscapedByte((byte) value.charAt(i));
            }
            return;
        }
        appendEscapedBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    private void appendAscii(String value) {
        ensureCapacity(value.length());
        for (int i = 0; i < value.length(); i++) {
            row[length++] = (byte) value.charAt(i);
        }
    }

    private void appendNonNegativeLong(long value) {
        if (value < 0) {
            appendAscii(Long.toString(value));
            return;
        }
        int digits = 1;
        for (long remaining = value; remaining >= 10; remaining /= 10) {
            digits++;
        }
        ensureCapacity(digits);
        int end = length + digits;
        int cursor = end;
        do {
            long quotient = value / 10;
            row[--cursor] = (byte) ('0' + (value - quotient * 10));
            value = quotient;
        } while (value != 0);
        length = end;
    }

    private void appendEscapedBytes(byte[] bytes) {
        ensureCapacity(bytes.length * (escape ? 4 : 1));
        for (byte value : bytes) {
            appendEscapedByte(value);
        }
    }

    private void appendEscapedByte(byte value) {
        int unsigned = value & 0xff;
        if (escape && (unsigned < 0x20 || unsigned == 0x7f)) {
            row[length++] = '\\';
            row[length++] = 'x';
            row[length++] = LOWER_HEX[unsigned >>> 4];
            row[length++] = LOWER_HEX[unsigned & 0x0f];
        } else {
            row[length++] = value;
        }
    }

    private void appendByte(int value) {
        ensureCapacity(1);
        row[length++] = (byte) value;
    }

    private void appendBytes(byte[] bytes) {
        ensureCapacity(bytes.length);
        System.arraycopy(bytes, 0, row, length, bytes.length);
        length += bytes.length;
    }

    private void ensureCapacity(int additional) {
        int required = length + additional;
        if (required > row.length) {
            row = Arrays.copyOf(row, Math.max(required, row.length << 1));
        }
    }

}
