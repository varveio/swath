/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.sort.PageRunFixtures;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * {@code swath dump-run}: the hidden debug subcommand dumps a real page-run staging
 * segment's header/records/trailer with a CRC verdict, and reports {@code crc=FAIL} + a non-zero exit
 * on a corrupted segment. Uses {@link PageRunFixtures} to produce a genuine segment file.
 */
class DumpRunCommandTest {

    private static final int PAGE_RUN_V3_HEADER_BYTES = 21;

    private static String run(int[] exit, String... args) {
        StringWriter out = new StringWriter();
        CommandLine cmd = App.commandLine();
        cmd.setOut(new PrintWriter(out));
        cmd.setErr(new PrintWriter(out));
        exit[0] = cmd.execute(args);
        return out.toString();
    }

    private static String hex(String s) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    @Test
    void dumpsHeaderRecordTrailerWithCrcOk(@TempDir Path dir) throws Exception {
        Path seg = dir.resolve("seg.pageseg");
        PageRunFixtures.writeSinglePageSegment(seg, List.of("alpha", "bravo"));

        int[] exit = new int[1];
        String text = run(exit, "dump-run", seg.toString());

        assertThat(exit[0]).isZero();
        assertThat(text).contains("header: magic=0x53504752 version=3");
        assertThat(text).contains("records: 1");
        // One page → one record: min=hex(alpha), max=hex(bravo), count=2, CRC verifies.
        assertThat(text).contains("min=" + hex("alpha"));
        assertThat(text).contains("max=" + hex("bravo"));
        assertThat(text).contains("count=2");
        assertThat(text).contains("crc=OK");
        assertThat(text).doesNotContain("crc=FAIL");
        assertThat(text).contains("totalRecords=1");
        assertThat(text).contains("totalEntries=2");
    }

    @Test
    void corruptedSegmentReportsCrcFailAndExitsNonZero(@TempDir Path dir) throws Exception {
        Path seg = dir.resolve("seg.pageseg");
        PageRunFixtures.writeSinglePageSegment(seg, List.of("alpha", "bravo"));

        // Flip a byte inside the record body after the v2 header and 8-byte [len][crc] frame.
        byte[] raw = Files.readAllBytes(seg);
        raw[PAGE_RUN_V3_HEADER_BYTES + 8] ^= 0x7F;
        Files.write(seg, raw);

        int[] exit = new int[1];
        String text = run(exit, "dump-run", seg.toString());

        assertThat(exit[0]).isEqualTo(1);
        assertThat(text).contains("crc=FAIL");
    }
}
