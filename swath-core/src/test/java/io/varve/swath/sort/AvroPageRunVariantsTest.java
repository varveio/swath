/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * PR 6b deep dive: does the spike's integrity verdict survive the production-shaped read paths?
 *
 * <p>The spike only exercised its {@code GenericRecord} reader. If raw-block iteration or the
 * routing-first schema changed the integrity story, the recommendation would have to move; these
 * tests pin that they do not.
 */
class AvroPageRunVariantsTest {

    private static final ListEntryComparator CMP = new ListEntryComparator();

    @Test
    void rawAndReuseReadersRoundTripIdenticallyToTheSpikeReader(@TempDir Path dir)
            throws IOException {
        List<PageBlock> pages = pages(3, 4, PageCodec.LZ4);
        Path spike = dir.resolve("spike.avro");
        writeSpike(spike, pages);
        Path reordered = dir.resolve("reordered.avro");
        writeVariant(reordered, pages, true);

        List<String> expected = keysOfSpikeReader(spike);
        assertThat(expected).hasSize(12);
        assertThat(drainReuse(spike)).isEqualTo(expected);
        assertThat(drainRaw(spike, false)).isEqualTo(expected);
        assertThat(drainRaw(reordered, true)).isEqualTo(expected);
        assertThat(AvroPageRunVariants.scanHeadersSeeking(reordered))
                .isEqualTo(new AvroPageRunContainer.HeaderSummary(3, 12));
    }

    /**
     * Claim 1 of the spike: a one-bit flip in a packed payload passes silently. Raw-block iteration
     * does strictly less per-record work than the spike reader, so it cannot notice more; this pins
     * that it notices exactly as little, in both field orders.
     */
    @Test
    void oneBitPayloadFlipIsSilentOnEveryAvroReadShape(@TempDir Path dir) throws IOException {
        List<PageBlock> pages = pages(1, 20, PageCodec.LZ4);
        byte[] body = pages.getFirst().serialize();
        PageBlockCodec.Header header = PageBlockCodec.parseHeader(body);
        List<String> original = describe(drainPage(pages.getFirst()));
        SilentMutation mutation = findSilentMutation(body, header);

        Path spike = flip(dir.resolve("flip-spike.avro"), pages, false, body, mutation);
        Path reordered = flip(dir.resolve("flip-reordered.avro"), pages, true, body, mutation);

        List<String> viaSpike = keysOfSpikeReader(spike);
        List<String> viaReuse = drainReuse(spike);
        List<String> viaRaw = drainRaw(spike, false);
        List<String> viaRawReordered = drainRaw(reordered, true);

        assertThat(viaSpike).isNotEqualTo(original);
        assertThat(viaReuse).isEqualTo(viaSpike);
        assertThat(viaRaw).isEqualTo(viaSpike);
        assertThat(viaRawReordered).isEqualTo(viaSpike);
        // The seeking header pass never touches the payload at all, so it is silent by construction.
        assertThat(AvroPageRunVariants.scanHeadersSeeking(reordered))
                .isEqualTo(new AvroPageRunContainer.HeaderSummary(1, 20));
    }

    /**
     * Claim 2 of the spike: truncation exactly at a block boundary is caught only by the swath seal
     * convention, never by OCF framing. Verified on the raw and seeking shapes too.
     */
    @Test
    void boundaryTruncationIsCaughtOnlyBySealOnEveryAvroReadShape(@TempDir Path dir)
            throws IOException {
        List<PageBlock> pages = pages(3, 5, PageCodec.NONE);
        Path spike = dir.resolve("trunc-spike.avro");
        AvroPageRunContainer.WriteResult layout = writeSpike(spike, pages);
        Path reordered = dir.resolve("trunc-reordered.avro");
        List<Long> reorderedBoundaries = writeVariant(reordered, pages, true);

        long boundary = layout.pageBoundaries().getFirst();
        byte[] spikeBytes = Files.readAllBytes(spike);
        Path spikeCut = Files.write(dir.resolve("cut-spike.avro"),
                Arrays.copyOf(spikeBytes, Math.toIntExact(boundary)));

        // The reordered file's first block boundary: same convention, one block per page + sync.
        byte[] reorderedBytes = Files.readAllBytes(reordered);
        long reorderedBoundary = reorderedBoundaries.getFirst();
        Path reorderedCut = Files.write(dir.resolve("cut-reordered.avro"),
                Arrays.copyOf(reorderedBytes, Math.toIntExact(reorderedBoundary)));

        // OCF framing itself reaches a clean EOF: iterating blocks raises nothing on its own.
        assertThat(blockCount(spikeCut)).isEqualTo(1);

        assertThatThrownBy(() -> drainReuse(spikeCut))
                .isInstanceOf(IOException.class).hasMessageContaining("missing final SEAL");
        assertThatThrownBy(() -> drainRaw(spikeCut, false))
                .isInstanceOf(IOException.class).hasMessageContaining("missing final SEAL");
        assertThatThrownBy(() -> drainRaw(reorderedCut, true))
                .isInstanceOf(IOException.class).hasMessageContaining("missing final SEAL");
        assertThatThrownBy(() -> AvroPageRunVariants.scanHeadersSeeking(reorderedCut))
                .isInstanceOf(IOException.class).hasMessageContaining("missing final SEAL");
    }

    // ------------------------------------------------------------------ helpers

    private static int blockCount(Path path) throws IOException {
        int blocks = 0;
        try (org.apache.avro.file.DataFileReader<org.apache.avro.generic.GenericRecord> reader =
                new org.apache.avro.file.DataFileReader<>(path.toFile(),
                        new org.apache.avro.generic.GenericDatumReader<>(
                                AvroPageRunContainer.SCHEMA))) {
            while (reader.hasNext()) {
                reader.nextBlock();
                blocks++;
            }
        }
        return blocks;
    }

    private static Path flip(Path target, List<PageBlock> pages, boolean reordered, byte[] body,
            SilentMutation mutation) throws IOException {
        if (reordered) {
            writeVariant(target, pages, true);
        } else {
            writeSpike(target, pages);
        }
        byte[] bytes = Files.readAllBytes(target);
        int offset = indexOf(bytes, body);
        assertThat(offset).isPositive();
        bytes[offset + mutation.offset()] ^= mutation.mask();
        Files.write(target, bytes);
        return target;
    }

    private static AvroPageRunContainer.WriteResult writeSpike(Path path, List<PageBlock> pages)
            throws IOException {
        try (AvroPageRunContainer.Writer writer =
                AvroPageRunContainer.openWriter(path, PageCodec.LZ4)) {
            for (PageBlock page : pages) {
                writer.append(page);
            }
            return writer.seal();
        }
    }

    private static List<Long> writeVariant(Path path, List<PageBlock> pages, boolean reordered)
            throws IOException {
        try (AvroPageRunVariants.Writer writer =
                new AvroPageRunVariants.Writer(path, PageCodec.LZ4, reordered, 64)) {
            for (PageBlock page : pages) {
                writer.append(page);
            }
            writer.seal();
            return writer.pageBoundaries();
        }
    }

    private static List<String> keysOfSpikeReader(Path path) throws IOException {
        List<ListEntry> entries = new ArrayList<>();
        try (AvroPageRunContainer.Reader reader = AvroPageRunContainer.openReader(path)) {
            PageBlock page;
            while ((page = reader.nextPage()) != null) {
                entries.addAll(drainPage(page));
            }
        }
        return keysOf(entries);
    }

    private static List<String> drainReuse(Path path) throws IOException {
        List<ListEntry> entries = new ArrayList<>();
        AvroPageRunVariants.Frame frame = new AvroPageRunVariants.Frame();
        try (AvroPageRunVariants.ReuseReader reader =
                new AvroPageRunVariants.ReuseReader(path, false)) {
            while (reader.next(frame, true)) {
                entries.addAll(drainPage(PageBlock.deserialize(frame.page.clone())));
            }
        }
        return keysOf(entries);
    }

    private static List<String> drainRaw(Path path, boolean reordered) throws IOException {
        List<ListEntry> entries = new ArrayList<>();
        AvroPageRunVariants.Frame frame = new AvroPageRunVariants.Frame();
        try (AvroPageRunVariants.RawReader reader =
                new AvroPageRunVariants.RawReader(path, reordered)) {
            while (reader.next(frame, true)) {
                entries.addAll(drainPage(PageBlock.deserialize(frame.page.clone())));
            }
        }
        return keysOf(entries);
    }

    private static List<String> keysOf(List<ListEntry> entries) {
        return describe(entries);
    }

    /** Full-row rendering: a silent payload flip may change a non-key column. */
    private static List<String> describe(List<ListEntry> entries) {
        List<String> rendered = new ArrayList<>(entries.size());
        for (ListEntry entry : entries) {
            rendered.add(entry.toString());
        }
        return rendered;
    }

    private static List<ListEntry> drainPage(PageBlock page) {
        List<ListEntry> entries = new ArrayList<>();
        PageBlockCursor cursor = page.cursor();
        while (cursor.hasNext()) {
            entries.add(cursor.next());
        }
        return entries;
    }

    private static List<PageBlock> pages(int pageCount, int rowsPerPage, PageCodec codec) {
        List<PageBlock> pages = new ArrayList<>();
        for (int page = 0; page < pageCount; page++) {
            List<ListEntry> rows = new ArrayList<>();
            for (int row = 0; row < rowsPerPage; row++) {
                rows.add(object(page * rowsPerPage + row));
            }
            pages.add(PageBlock.pack(rows, CMP, codec));
        }
        return pages;
    }

    private static ObjectEntry object(int ordinal) {
        return new ObjectEntry(KeyBytes.ofUtf8(key(ordinal)), 4096L + ordinal,
                1_788_131_200_000_000L + ordinal, String.format("%032x", ordinal),
                "STANDARD", null, false, "owner-0001", null, "SHA256", "FULL_OBJECT");
    }

    private static String key(int ordinal) {
        return String.format("tenant=0042/region=us-east-1/year=2026/month=08/day=31/"
                + "hour=12/partition=0017/object-%012d.dat", ordinal);
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static SilentMutation findSilentMutation(byte[] original,
            PageBlockCodec.Header header) {
        List<ListEntry> expected = drainPage(PageBlock.deserialize(original.clone()));
        int end = header.payloadOffset() + header.payloadLength();
        for (int offset = header.payloadOffset(); offset < end; offset++) {
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                byte mask = (byte) (1 << bit);
                byte[] candidate = original.clone();
                candidate[offset] ^= mask;
                try {
                    if (!drainPage(PageBlock.deserialize(candidate)).equals(expected)) {
                        return new SilentMutation(offset, mask);
                    }
                } catch (RuntimeException rejected) {
                    // structurally noticed; keep searching for a silent one
                }
            }
        }
        throw new AssertionError("no silently accepted LZ4 payload mutation found");
    }

    private record SilentMutation(int offset, byte mask) {
    }
}
