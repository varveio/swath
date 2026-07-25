/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Text-formatter unit checks (INT-2 line integrity). */
class FormatterTest {

    private static ObjectEntry obj(String key, long size) {
        return ObjectEntry.withoutOwnerDisplayNameAndChecksumType(KeyBytes.ofUtf8(key), size, 1_700_000_000_000_000L,
                "abc123", "STANDARD", null, true, null, null);
    }

    @Test
    void jsonlEmitsOneObjectPerLineWithExpectedFields() throws IOException {
        StringWriter sw = new StringWriter();
        try (JsonlFormatter f = new JsonlFormatter(sw)) {
            f.writeHeader();
            f.write(obj("a/1", 10));
            f.write(new CommonPrefixEntry(KeyBytes.ofUtf8("b/")));
        }
        String[] lines = sw.toString().split("\n");
        assertThat(lines).hasSize(2);
        assertThat(lines[0]).startsWith("{").endsWith("}")
                .contains("\"key\":\"a/1\"")
                .contains("\"size\":10")
                .contains("\"row_type\":\"OBJECT\"")
                .contains("\"last_modified\":");
        assertThat(lines[1]).contains("\"key\":\"b/\"").contains("\"row_type\":\"COMMON_PREFIX\"");
        // common prefix carries no size
        assertThat(lines[1]).doesNotContain("\"size\"");
    }

    @Test
    void jsonlEmitsOwnerDisplayNameAndChecksumTypeWhenPresent() throws IOException {
        ObjectEntry entry = new ObjectEntry(KeyBytes.ofUtf8("a/1"), 10, 1_700_000_000_000_000L,
                "abc123", "STANDARD", null, true, "owner1", "Alice", "SHA256", "FULL_OBJECT");
        StringWriter sw = new StringWriter();
        try (JsonlFormatter f = new JsonlFormatter(sw)) {
            f.write(entry);
        }
        assertThat(sw.toString())
                .contains("\"owner_display_name\":\"Alice\"")
                .contains("\"checksum_type\":\"FULL_OBJECT\"");
    }

    @Test
    void jsonlKeepsControlCharsInKeyOnOneLine() throws IOException {
        StringWriter sw = new StringWriter();
        String key = "a" + (char) 0x0a + "b";   // embedded newline
        try (JsonlFormatter f = new JsonlFormatter(sw)) {
            f.write(obj(key, 1));
        }
        // Exactly one data line (the newline in the key is JSON-escaped, not literal).
        assertThat(sw.toString().split("\n")).hasSize(1);
        assertThat(sw.toString()).contains("\\n");
    }

    @Test
    void tsvEscapesEmbeddedTabsAndNewlinesByDefault() throws IOException {
        StringWriter sw = new StringWriter();
        String key = "a" + (char) 0x09 + "b";   // embedded tab would break columns
        try (TsvFormatter f = new TsvFormatter(sw, true)) {
            f.writeHeader();
            f.write(obj(key, 5));
        }
        String[] lines = sw.toString().split("\n");
        assertThat(lines[0]).isEqualTo("key\tsize\tlast_modified\tetag\tstorage_class\trow_type");
        assertThat(lines[1]).startsWith("a\\x09b\t");          // tab escaped, columns intact
        assertThat(lines[1].split("\t")).hasSize(6);
    }

    @Test
    void tsvRawOutputKeepsBytesUnescaped() throws IOException {
        StringWriter sw = new StringWriter();
        try (TsvFormatter f = new TsvFormatter(sw, false)) {
            f.write(obj("plain", 5));
        }
        assertThat(sw.toString()).startsWith("plain\t");
    }

    @Test
    void alignedRightJustifiesSizeAndShowsKey() throws IOException {
        StringWriter sw = new StringWriter();
        try (AlignedFormatter f = new AlignedFormatter(sw, true)) {
            f.write(obj("dir/file", 42));
        }
        String line = sw.toString().stripTrailing();
        assertThat(line).contains("42").endsWith("dir/file");
    }
}
