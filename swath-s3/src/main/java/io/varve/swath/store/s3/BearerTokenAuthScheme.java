/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;

/**
 * Replaces AWS SigV4 signing with a plain OAuth {@code Authorization: Bearer <token>} header —
 * GCS's XML API accepts this directly (docs.cloud.google.com/storage/docs/authentication), so a
 * GCS bucket can be listed through the same S3-compatible client path without HMAC interoperability
 * keys.
 *
 * <p>Registered under {@link AwsV4AuthScheme#SCHEME_ID} (see {@link S3ClientFactory}): the S3
 * client's built-in auth-scheme resolver still selects "sigv4" for every request (that selection
 * logic is unrelated to which {@link AuthScheme} implementation answers for that scheme id), but
 * this implementation is installed in its place, so its {@link #identityProvider} and {@link
 * #signer} run instead of the SDK's real SigV4 ones. This is the current (non-deprecated) SDK
 * auth-scheme SPI — the legacy {@code Signer}/{@code SdkAdvancedClientOption.SIGNER} override point
 * still exists but is deprecated in the pinned SDK version.
 */
final class BearerTokenAuthScheme implements AuthScheme<BearerTokenIdentity> {

    private final BearerTokenSupplier tokenSupplier;

    BearerTokenAuthScheme(BearerTokenSupplier tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public String schemeId() {
        return AwsV4AuthScheme.SCHEME_ID;
    }

    @Override
    public IdentityProvider<BearerTokenIdentity> identityProvider(IdentityProviders providers) {
        return new IdentityProvider<>() {
            @Override
            public Class<BearerTokenIdentity> identityType() {
                return BearerTokenIdentity.class;
            }

            @Override
            public CompletableFuture<BearerTokenIdentity> resolveIdentity(ResolveIdentityRequest request) {
                return CompletableFuture.completedFuture(new BearerTokenIdentity(tokenSupplier.token()));
            }
        };
    }

    @Override
    public HttpSigner<BearerTokenIdentity> signer() {
        return new HttpSigner<>() {
            @Override
            public SignedRequest sign(SignRequest<? extends BearerTokenIdentity> request) {
                return SignedRequest.builder()
                        .request(withBearerHeader(request.request(), request.identity()))
                        .payload(request.payload().orElse(null))
                        .build();
            }

            @Override
            public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends BearerTokenIdentity> request) {
                return CompletableFuture.completedFuture(AsyncSignedRequest.builder()
                        .request(withBearerHeader(request.request(), request.identity()))
                        .payload(request.payload().orElse(null))
                        .build());
            }
        };
    }

    private static SdkHttpRequest withBearerHeader(SdkHttpRequest request, BearerTokenIdentity identity) {
        return request.toBuilder()
                .putHeader("Authorization", "Bearer " + identity.token())
                .build();
    }
}
