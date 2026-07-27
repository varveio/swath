/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.scheme.AwsV4AuthScheme;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;

/**
 * {@link BearerTokenAuthScheme} replaces SigV4 signing with a plain OAuth bearer header.
 * Registered under {@link AwsV4AuthScheme#SCHEME_ID} so the S3 client's built-in "use sigv4"
 * resolution keeps selecting it (see the class javadoc); these tests exercise the identity
 * resolution and signing behavior directly, without a network call.
 */
class BearerTokenAuthSchemeTest {

    private static final SdkHttpRequest BASE_REQUEST = SdkHttpRequest.builder()
            .method(SdkHttpMethod.GET)
            .uri(URI.create("https://storage.googleapis.com/some-bucket?list-type=2"))
            .putHeader("x-existing", "kept")
            .build();

    @Test
    void schemeIdMatchesSigV4SoTheBuiltInResolverStillSelectsIt() {
        BearerTokenAuthScheme scheme = new BearerTokenAuthScheme(() -> "tok");
        assertThat(scheme.schemeId()).isEqualTo(AwsV4AuthScheme.SCHEME_ID);
    }

    @Test
    void identityProviderWrapsTheSuppliersCurrentToken() {
        BearerTokenAuthScheme scheme = new BearerTokenAuthScheme(() -> "fresh-token");
        IdentityProvider<BearerTokenIdentity> provider = scheme.identityProvider(noOpProviders());

        assertThat(provider.identityType()).isEqualTo(BearerTokenIdentity.class);
        BearerTokenIdentity identity = provider.resolveIdentity(ResolveIdentityRequest.builder().build()).join();
        assertThat(identity.token()).isEqualTo("fresh-token");
    }

    @Test
    void signerAddsBearerHeaderAndPreservesTheRequestOtherwise() {
        BearerTokenAuthScheme scheme = new BearerTokenAuthScheme(() -> "secret-token");
        BearerTokenIdentity identity = new BearerTokenIdentity("secret-token");
        SignRequest<BearerTokenIdentity> request = SignRequest.builder(identity)
                .request(BASE_REQUEST)
                .build();

        SignedRequest signed = scheme.signer().sign(request);

        assertThat(signed.request().firstMatchingHeader("Authorization"))
                .contains("Bearer secret-token");
        assertThat(signed.request().firstMatchingHeader("x-existing")).contains("kept");
        assertThat(signed.request().method()).isEqualTo(SdkHttpMethod.GET);
        assertThat(signed.request().getUri()).isEqualTo(BASE_REQUEST.getUri());
    }

    @Test
    void signerNeverFallsBackToSigV4EvenWhenIdentityCarriesNoCredentialFields() {
        // The whole point: a BearerTokenIdentity has no access-key/secret-key at all, so if the
        // signer ever silently delegated to real SigV4 machinery instead of just stamping the
        // header itself, this would fail loudly (NPE/ClassCast) rather than producing a wrong
        // signature that only fails against a live endpoint.
        BearerTokenAuthScheme scheme = new BearerTokenAuthScheme(() -> "t");
        SignRequest<BearerTokenIdentity> request = SignRequest.builder(new BearerTokenIdentity("t"))
                .request(BASE_REQUEST)
                .build();

        assertThat(scheme.signer().sign(request).request().firstMatchingHeader("Authorization"))
                .contains("Bearer t");
    }

    @Test
    void asyncSignerAddsTheSameBearerHeader() {
        BearerTokenAuthScheme scheme = new BearerTokenAuthScheme(() -> "async-token");
        BearerTokenIdentity identity = new BearerTokenIdentity("async-token");
        AsyncSignRequest<BearerTokenIdentity> request = AsyncSignRequest.builder(identity)
                .request(BASE_REQUEST)
                .build();

        CompletableFuture<AsyncSignedRequest> future = scheme.signer().signAsync(request);

        assertThat(future).isCompleted();
        assertThat(future.join().request().firstMatchingHeader("Authorization"))
                .contains("Bearer async-token");
    }

    private static IdentityProviders noOpProviders() {
        return IdentityProviders.builder().build();
    }
}
