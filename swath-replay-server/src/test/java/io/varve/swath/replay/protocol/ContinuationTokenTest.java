/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class ContinuationTokenTest {

    @Test
    void v2ExclusiveRoundTrips() {
        String token = ContinuationToken.encode(bytes("a/dir/"));
        assertThat(token).startsWith("v2:");
        ContinuationToken decoded = ContinuationToken.decode(token);
        assertThat(decoded.boundary()).isEqualTo(bytes("a/dir/"));
        assertThat(decoded.inclusive()).isFalse();
    }

    @Test
    void v2InclusiveRoundTrips() {
        String token = ContinuationToken.encode(bytes("photos0"), true);
        assertThat(token).startsWith("v2:");
        ContinuationToken decoded = ContinuationToken.decode(token);
        assertThat(decoded.boundary()).isEqualTo(bytes("photos0"));
        assertThat(decoded.inclusive()).isTrue();
    }

    @Test
    void legacyV1TokenDecodesAsExclusive() {
        String v1 = "v1:" + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes("a/2"));
        ContinuationToken decoded = ContinuationToken.decode(v1);
        assertThat(decoded.boundary()).isEqualTo(bytes("a/2"));
        assertThat(decoded.inclusive()).isFalse();
    }

    @Test
    void blankOrAbsentTokenDecodesToNull() {
        assertThat(ContinuationToken.decode(null)).isNull();
        assertThat(ContinuationToken.decode("")).isNull();
        assertThat(ContinuationToken.decode("   ")).isNull();
    }

    @Test
    void rejectsUnknownPrefix() {
        assertThatThrownBy(() -> ContinuationToken.decode("not-a-token"))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    @Test
    void rejectsNonBase64Body() {
        assertThatThrownBy(() -> ContinuationToken.decode("v2:*** not base64 ***"))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    @Test
    void rejectsV2TokenWithOutOfRangeFlagByte() {
        String tampered = "v2:" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(new byte[]{0x7F, 'a', '/'});
        assertThatThrownBy(() -> ContinuationToken.decode(tampered))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    @Test
    void rejectsV2TokenWithEmptyPayload() {
        assertThatThrownBy(() -> ContinuationToken.decode("v2:"))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
