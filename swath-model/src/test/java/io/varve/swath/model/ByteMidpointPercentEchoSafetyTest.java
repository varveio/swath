/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.testkit.CodePointKeys;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;

/**
 * Hermetic (no container, no network) guard for the actual verbatim-echo crash mechanism.
 *
 * <p>The mechanism ({@code ByteMidpoint#isSafe} javadoc, {@code
 * docs/internals/s3-implementation-compatibility.md}): a verbatim-echo S3-compatible endpoint
 * (LocalStack, MinIO) echoes the request's {@code start-after}/{@code prefix} parameter back into
 * the listing response's {@code StartAfter}/{@code Marker} fields WITHOUT re-percent-encoding it.
 * The AWS SDK's own always-on response interceptor then strict-decodes those fields with
 * {@link java.net.URLDecoder}, which throws {@code IllegalArgumentException} on a lone or
 * trailing {@code '%'} ("Incomplete trailing escape (%) pattern") and aborts the whole listing.
 * Real S3 re-encodes its echo and is unaffected.
 *
 * <p>{@code swath-model} cannot exercise the SDK's response interceptor directly — that requires
 * a live/LocalStack HTTP round trip and belongs in {@code swath-s3} (an IT) or {@code
 * swath-core}, NOT here. See {@link #trueEndToEndGuardSpec()} below for the exact
 * spec of that test. What THIS class proves hermetically, using the EXACT JDK class ({@link
 * java.net.URLDecoder}) the SDK's interceptor calls, is the root guarantee that makes the crash
 * unreachable in the first place: {@code ByteMidpoint} never emits a synthesized pivot that a
 * verbatim-echo endpoint could bounce back into a decode step that throws on it.
 */
class ByteMidpointPercentEchoSafetyTest {

    /**
     * Positive control — proves the guard below is not a tautology. Without {@code
     * ByteMidpoint}'s '%' exclusion, a lone/trailing '%' echoed verbatim into exactly this decode
     * step genuinely throws: this is the precise exception class and message a real run hit
     * against LocalStack/MinIO.
     */
    @Test
    void positiveControlLoneOrTrailingPercentBreaksUrlDecoder() {
        assertThatThrownBy(() -> URLDecoder.decode("A%", StandardCharsets.UTF_8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Incomplete trailing escape");
        assertThatThrownBy(() -> URLDecoder.decode("A%Z", StandardCharsets.UTF_8))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** THE known regression trigger, run through the actual decode step a verbatim-echo endpoint feeds. */
    @Test
    void knownTriggerNeverProducesADecoderCrashingCursor() {
        byte[] m = ByteMidpoint.between(new byte[]{0x24}, new byte[]{0x26});
        if (m != null) {
            assertDecodesCleanly(m);
        }
    }

    @Property(tries = 3000)
    void everyBetweenPivotDecodesCleanlyAsAnEchoedField(
            @ForAll("safeKeys") byte[] a, @ForAll("utf8Keys") byte[] b) {
        Assume.that(Arrays.compareUnsigned(a, b) < 0);
        byte[] m = ByteMidpoint.between(a, b);
        if (m != null) {
            assertDecodesCleanly(m);
        }
    }

    @Property(tries = 3000)
    void everyForwardReflectPivotDecodesCleanlyAsAnEchoedField(
            @ForAll("utf8Keys") byte[] lo, @ForAll("safeKeys") byte[] c) {
        Assume.that(c.length > 0);
        byte[] m = ByteMidpoint.forwardReflect(lo, c);
        if (m != null) {
            assertDecodesCleanly(m);
        }
    }

    /** Narrow band bracketing '%' — the input space most likely to provoke the crash. */
    @Property(tries = 3000)
    void narrowBandPivotsDecodeCleanly(
            @ForAll("narrowBandSafeKeys") byte[] a, @ForAll("narrowBandKeys") byte[] b) {
        Assume.that(Arrays.compareUnsigned(a, b) < 0);
        byte[] m = ByteMidpoint.between(a, b);
        if (m != null) {
            assertDecodesCleanly(m);
        }
    }

    /**
     * Simulates the verbatim-echo endpoint's response field: {@code m} decoded as UTF-8 text is
     * exactly what such an endpoint echoes back unescaped, so running it through the SDK's own
     * strict {@link java.net.URLDecoder} decode step must never throw.
     */
    private static void assertDecodesCleanly(byte[] m) {
        String echoed = new String(m, StandardCharsets.UTF_8);
        try {
            String decoded = URLDecoder.decode(echoed, StandardCharsets.UTF_8);
            assertThat(decoded).isNotNull();
        } catch (IllegalArgumentException e) {
            throw new AssertionError(
                    "synthesized pivot would crash a verbatim-echo endpoint's URLDecoder: "
                            + Arrays.toString(m) + " (\"" + echoed + "\")", e);
        }
    }

    @Provide
    Arbitrary<byte[]> utf8Keys() {
        return CodePointKeys.scalars().list().ofMinSize(0).ofMaxSize(32).map(CodePointKeys::encode);
    }

    @Provide
    Arbitrary<byte[]> safeKeys() {
        return CodePointKeys.safeScalars().list().ofMinSize(0).ofMaxSize(16).map(CodePointKeys::encode);
    }

    @Provide
    Arbitrary<byte[]> narrowBandKeys() {
        return CodePointKeys.narrowBandKeys();
    }

    @Provide
    Arbitrary<byte[]> narrowBandSafeKeys() {
        return CodePointKeys.narrowBandSafeKeys();
    }

    /**
     * NOT a runnable test — the precise spec of the end-to-end guard this class cannot host (it
     * needs a real HTTP round trip through the AWS SDK's response interceptor), left here for
     * the {@code swath-s3}/{@code swath-core} author who picks it up:
     *
     * <ol>
     *   <li>Stand up (or mock at the SDK-HTTP-client seam, e.g. via a Testcontainers LocalStack
     *       fixture or {@code swath-s3}'s existing {@code LocalStackSupport} testFixture) an S3
     *       ListObjectsV2-shaped endpoint that echoes {@code start-after} back into the response's
     *       {@code StartAfter}/{@code Marker} field VERBATIM (byte-identical to the request
     *       parameter), i.e. WITHOUT re-percent-encoding it — the LocalStack/MinIO shape.
     *   <li>Drive a listing whose worker synthesizes a pivot ending in a raw {@code 0x25} byte —
     *       reachable ONLY by temporarily reverting the {@code isSafe}/{@code isExcludedScalar}
     *       '%' exclusion under test, OR by directly constructing/injecting a {@code start-after}
     *       value containing a lone/trailing '%' at the fetch layer (bypassing {@code
     *       ByteMidpoint} entirely) so the test exercises the SDK response path independent of the
     *       (already-fixed) synthesis path.
     *   <li>Assert PRE-mitigation (i.e. with the exclusion bypassed as above) that the SDK's
     *       response interceptor throws {@code IllegalArgumentException} from {@code
     *       java.net.URLDecoder} ("Incomplete trailing escape") and the run aborts — the positive
     *       control that the wire-level crash is real, at the actual SDK/HTTP seam (not just the
     *       hermetic proxy in this class).
     *   <li>Assert that with {@code ByteMidpoint}'s fix in place (the normal, non-bypassed path),
     *       an equivalent listing against the SAME verbatim-echo fixture completes without ever
     *       hitting that exception — end-to-end confirmation that swath's own synthesized cursors
     *       cannot trigger it against a real verbatim-echo endpoint.
     * </ol>
     *
     * This method exists only to carry that spec in one place near the hermetic guard it
     * complements; it intentionally asserts nothing and is not annotated as a test.
     */
    private void trueEndToEndGuardSpec() {
        // See javadoc above. Deliberately not a @Test.
    }
}
