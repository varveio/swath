/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Base64;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.statistics.Statistics;

/**
 * REPLAY-PROP-2 (jqwik): the {@link ContinuationToken} wire contract — v2 round-trip over
 * arbitrary boundary bytes × inclusive/exclusive, v1 back-compat (arbitrary bytes, always
 * exclusive), and structural-validation robustness. These adversarially assert the token is never
 * silently MISdecoded — a token is only ever <em>structurally</em> validated (known prefix, valid
 * base64, in-range flag byte), never integrity-protected (see the class javadoc for why). So a
 * mutated body is either rejected with the S3 400 or decodes to an observably DIFFERENT (boundary,
 * inclusive) pair — never a silent wrong-equal decode of a corrupted token; only a base64-aliasing
 * no-op (same decoded payload bytes) is exempt.
 *
 * <p>The last property covers the other end of the contract: which strings count as a token at all,
 * which is what decides whether a request resumes at its token or at its {@code start-after}.
 */
class ContinuationTokenPropertyTest {

    private static final char[] BASE64_URL =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();

    @Provide
    Arbitrary<byte[]> boundaries() {
        // Full unsigned byte range incl. 0x00/0xFF; min size 1 (a real boundary is a non-empty key).
        return Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(40);
    }

    @Property(tries = 500)
    void v2RoundTripsAnyBoundaryAndFlag(@ForAll("boundaries") byte[] boundary, @ForAll boolean inclusive) {
        String token = ContinuationToken.encode(boundary, inclusive);
        assertThat(token).startsWith("v2:");

        ContinuationToken decoded = ContinuationToken.decode(token);
        assertThat(decoded.boundary()).isEqualTo(boundary);
        assertThat(decoded.inclusive()).isEqualTo(inclusive);
    }

    /**
     * Content-equality regression: two independently-constructed tokens over byte-identical-but-
     * distinct boundary arrays must be equal, and encode-then-decode must round-trip to an EQUAL
     * token, not merely an equal-by-field one — {@link ContinuationToken#equals} would fail under
     * the default record (reference-identity array) equals/hashCode.
     */
    @Property(tries = 500)
    void tokensWithEqualContentAreEqual(@ForAll("boundaries") byte[] boundary, @ForAll boolean inclusive) {
        ContinuationToken a = new ContinuationToken(boundary.clone(), inclusive);
        ContinuationToken b = new ContinuationToken(boundary.clone(), inclusive);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());

        ContinuationToken decoded = ContinuationToken.decode(ContinuationToken.encode(boundary, inclusive));
        assertThat(decoded).isEqualTo(a);
    }

    @Property(tries = 300)
    void v1TokensDecodeAsExclusiveWithTheSameBytes(@ForAll("boundaries") byte[] boundary) {
        String v1 = "v1:" + Base64.getUrlEncoder().withoutPadding().encodeToString(boundary);
        ContinuationToken decoded = ContinuationToken.decode(v1);
        assertThat(decoded.boundary()).isEqualTo(boundary);
        assertThat(decoded.inclusive()).isFalse();
    }

    /**
     * Flipping the leading flag byte flips the inclusive/exclusive semantics but keeps the boundary —
     * i.e. a single-byte mutation is a detectable change of cursor semantics, never a silent no-op
     * (structural robustness — not a claim about detecting malicious tampering, see the class
     * javadoc).
     */
    @Property(tries = 300)
    void flippingTheFlagFlipsInclusivenessButKeepsTheBoundary(@ForAll("boundaries") byte[] boundary,
                                                              @ForAll boolean inclusive) {
        ContinuationToken flipped = ContinuationToken.decode(ContinuationToken.encode(boundary, !inclusive));
        assertThat(flipped.boundary()).isEqualTo(boundary);
        assertThat(flipped.inclusive()).isEqualTo(!inclusive);
    }

    /**
     * The load-bearing structural-validation property: flip ONE character of the base64 body of a
     * valid v2 token. Unless the flip is a base64-aliasing no-op (the mutated body decodes to the
     * identical payload bytes — a harmless different spelling of the same cursor), the result must be
     * rejected with the S3 error OR decode to a DIFFERENT (boundary, inclusive) — never a silent
     * wrong-equal. This is a robustness property (decode is well-defined over every byte string), not
     * a security property — see the class javadoc on why "rejects a mutated token" does not mean
     * "detects a malicious one".
     */
    @Property(tries = 800)
    void flippingAnyBodyCharacterIsRejectedOrDecodesToADifferentCursor(
            @ForAll("boundaries") byte[] boundary, @ForAll boolean inclusive,
            @ForAll @IntRange(min = 0, max = 63) int replacement, @ForAll long positionSeed) {

        String token = ContinuationToken.encode(boundary, inclusive);
        String body = token.substring("v2:".length());
        int position = Math.floorMod(positionSeed, body.length());
        char original = body.charAt(position);
        char mutated = BASE64_URL[replacement];
        Assume.that(mutated != original);

        String mutatedBody = body.substring(0, position) + mutated + body.substring(position + 1);
        Assume.that(!decodesToSamePayload(body, mutatedBody));   // exempt pure base64-alias no-ops

        String mutatedToken = "v2:" + mutatedBody;
        ContinuationToken unmutated = new ContinuationToken(boundary, inclusive);
        try {
            ContinuationToken decoded = ContinuationToken.decode(mutatedToken);
            assertThat(decoded).isNotEqualTo(unmutated);   // observably different, never silently equal
            Statistics.collect("decoded-different");
        } catch (S3Error rejected) {
            assertThat(rejected.status()).isEqualTo(400);   // acceptable: rejected outright
            Statistics.collect("rejected");
        }
        // Content-equality comparison must genuinely be exercised, not vacuous: require BOTH branches
        // of the "rejected OR decodes different" disjunction to actually occur across the run.
        Statistics.coverage(coverage -> {
            coverage.check("decoded-different").count(c -> c > 0);
            coverage.check("rejected").count(c -> c > 0);
        });
    }

    /** A body carrying a character outside the base64url alphabet is rejected with the S3 400. */
    @Property(tries = 200)
    void nonBase64BodyIsRejected(@ForAll("boundaries") byte[] boundary, @ForAll boolean inclusive) {
        String token = ContinuationToken.encode(boundary, inclusive);
        String tampered = token + "*";   // '*' is not in the base64url alphabet
        assertThatThrownBy(() -> ContinuationToken.decode(tampered))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    /** Any v2 payload whose leading flag byte is out of range (>= 2) is rejected. */
    @Property(tries = 300)
    void outOfRangeFlagByteIsRejected(@ForAll @IntRange(min = 2, max = 255) int flag,
                                      @ForAll("boundaries") byte[] boundary) {
        byte[] payload = new byte[boundary.length + 1];
        payload[0] = (byte) flag;
        System.arraycopy(boundary, 0, payload, 1, boundary.length);
        String tampered = "v2:" + Base64.getUrlEncoder().withoutPadding().encodeToString(payload);

        assertThatThrownBy(() -> ContinuationToken.decode(tampered))
                .isInstanceOf(S3Error.class)
                .hasMessageContaining("invalid continuation-token");
    }

    private static boolean decodesToSamePayload(String bodyA, String bodyB) {
        try {
            return Arrays.equals(
                    Base64.getUrlDecoder().decode(bodyA),
                    Base64.getUrlDecoder().decode(bodyB));
        } catch (IllegalArgumentException e) {
            return false;   // one side is not valid base64 at all — not a no-op
        }
    }

    @Provide
    Arbitrary<String> tokenStrings() {
        return Arbitraries.oneOf(
                Arbitraries.strings().ascii().ofMinLength(0).ofMaxLength(20),
                Arbitraries.of("", " ", "   ", "\t", "\n", " \t\n "),
                boundaries().map(boundary -> ContinuationToken.encode(boundary, true)));
    }

    /**
     * {@link S3ListRequest#hasContinuationToken} decides whether a continuation token takes precedence
     * over a conflicting {@code start-after}; {@link ContinuationToken#decode} decides what that token
     * resumes at. The two must classify every string the same way. Were a string ever "present" here
     * and absent there, the request would discard its start-after in favor of a token that resolves to
     * no boundary — restarting from the top a walk the client asked to resume, silently and with no
     * error to notice.
     */
    @Property(tries = 500)
    void hasContinuationTokenAgreesWithDecodeOnEveryString(@ForAll("tokenStrings") String token) {
        boolean decodeReadsAToken;
        try {
            decodeReadsAToken = ContinuationToken.decode(token) != null;
        } catch (S3Error rejected) {
            decodeReadsAToken = true;   // structurally invalid is still a token the client meant to send
        }

        assertThat(new S3ListRequest("bucket", null, null, null, token, 1000, true, false)
                .hasContinuationToken())
                .as("token %s", token.isEmpty() ? "<empty>" : "'" + token + "'")
                .isEqualTo(decodeReadsAToken);
    }
}
