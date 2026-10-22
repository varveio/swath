/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output.parquet.sorted;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.SplittableRandom;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link BufferedLocalInputFile} serves every read shape parquet-java issues against the bytes at the
 * stream's position: single bytes and short arrays (Thrift page-index parsing, through the buffer),
 * long reads (page data, straight to the channel), and seeks that land inside, before, and past the
 * buffered span.
 */
class BufferedLocalInputFileTest {

    @Test
    void everyReadShapeReturnsTheBytesAtThePosition(@TempDir Path dir) throws IOException {
        byte[] data = new byte[100_000];
        new SplittableRandom(7).nextBytes(data);
        Path file = Files.write(dir.resolve("data.bin"), data);

        BufferedLocalInputFile input = new BufferedLocalInputFile(file);
        assertThat(input.getLength()).isEqualTo(data.length);
        try (SeekableInputStream stream = input.newStream()) {
            assertThat(stream.read()).isEqualTo(data[0] & 0xFF);
            assertThat(stream.getPos()).isEqualTo(1);

            byte[] small = new byte[100];
            stream.readFully(small);
            assertThat(small).isEqualTo(Arrays.copyOfRange(data, 1, 101));

            // Inside the buffered span, then before it, then past it.
            stream.seek(5_000);
            assertThat(stream.read()).isEqualTo(data[5_000] & 0xFF);
            stream.seek(3);
            assertThat(stream.read()).isEqualTo(data[3] & 0xFF);
            stream.seek(90_000);
            assertThat(stream.read()).isEqualTo(data[90_000] & 0xFF);

            // A long read starting inside the buffer continues past its end.
            stream.seek(89_990);
            byte[] large = new byte[8_000];
            stream.readFully(large);
            assertThat(large).isEqualTo(Arrays.copyOfRange(data, 89_990, 97_990));
            assertThat(stream.getPos()).isEqualTo(97_990);

            // A long read with nothing buffered goes to the channel.
            stream.seek(20_000);
            ByteBuffer direct = ByteBuffer.allocate(30_000);
            stream.readFully(direct);
            assertThat(direct.array()).isEqualTo(Arrays.copyOfRange(data, 20_000, 50_000));
        }
    }

    @Test
    void theEndOfTheFileIsReportedNotPadded(@TempDir Path dir) throws IOException {
        byte[] data = {1, 2, 3, 4, 5};
        Path file = Files.write(dir.resolve("data.bin"), data);

        try (SeekableInputStream stream = new BufferedLocalInputFile(file).newStream()) {
            stream.seek(3);
            byte[] tail = new byte[10];
            assertThat(stream.read(tail, 0, tail.length)).isEqualTo(2);
            assertThat(stream.read()).isEqualTo(-1);

            stream.seek(4);
            assertThatThrownBy(() -> stream.readFully(new byte[2])).isInstanceOf(EOFException.class);
        }
    }
}
