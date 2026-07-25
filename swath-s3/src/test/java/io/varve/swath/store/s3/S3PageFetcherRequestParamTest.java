/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@link S3PageFetcher#toRequestParam} converts raw key bytes to the {@code String} sent as
 * {@code prefix}/{@code start-after}/{@code delimiter} — the inverse of the response-decode
 * direction, pinning the "raw bytes -> request String" contract that a corrupted
 * {@code start_after} cursor would run through.
 *
 * <p>{@code toRequestParam(raw).getBytes(UTF_8)} must equal {@code raw} byte-for-byte for
 * every raw key swath ever passes here (algorithms.md §1.1) — including keys whose literal
 * bytes look like percent-encoding, real space/{@code +} (which the SDK's own URL-encoder
 * treats specially on the wire but which round-trip fine through this plain UTF-8 String
 * conversion), control bytes, and multi-byte UTF-8 sequences.
 */
class S3PageFetcherRequestParamTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("rawKeys")
    void toRequestParamIsByteExactUtf8Identity(String caseName, byte[] raw) {
        assertThat(S3PageFetcher.toRequestParam(raw).getBytes(StandardCharsets.UTF_8)).isEqualTo(raw);
    }

    private static Stream<Arguments> rawKeys() {
        return Stream.of(
                Arguments.of("literal %20", bytes("dir/site_name=Coffs%20Harbour/a")),
                Arguments.of("literal %3D", bytes("dir/x%3Dy")),
                Arguments.of("literal %25", bytes("dir/z%25raw")),
                Arguments.of("trailing %", bytes("dir/trailing%")),
                Arguments.of("incomplete %2", bytes("dir/incomplete%2")),
                Arguments.of("real space (0x20)", bytes("dir/real space")),
                Arguments.of("real + (0x2B)", bytes("dir/real+plus")),
                Arguments.of("control bytes {0x01,0x1F}", new byte[]{0x01, 0x1F}),
                Arguments.of("3-byte UTF-8 (world)", bytes("dir/世")),
                Arguments.of("4-byte UTF-8 (supplementary plane)", bytes("dir/𝔘"))
        );
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
