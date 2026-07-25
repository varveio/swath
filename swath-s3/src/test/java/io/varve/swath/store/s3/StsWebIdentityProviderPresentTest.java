/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.jupiter.api.Test;

/**
 * The AWS SDK v2 default credential chain loads its
 * {@code WebIdentityTokenFileCredentialsProvider} REFLECTIVELY, so if the
 * {@code software.amazon.awssdk:sts} module is absent from the runtime classpath the
 * chain silently skips web-identity (OIDC / GKE/EKS workload identity) and a signed
 * private-bucket listing fails with a no-credentials error. This asserts the STS
 * web-identity provider class is present on the runtime/test classpath — it fails
 * before the {@code runtimeOnly(libs.awssdk.sts)} dependency and passes after.
 *
 * <p>Uses reflection (no compile-time dependency on the sts module, which is
 * {@code runtimeOnly}). Fast unit test — deliberately untagged so CI's {@code
 * fast-tests} job ({@code ./gradlew build -PnoIntegration}) runs it.
 */
final class StsWebIdentityProviderPresentTest {

    @Test
    void stsWebIdentityProviderIsOnTheRuntimeClasspath() {
        assertThatCode(() -> Class.forName(
                "software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider"))
                .as("aws-sdk `sts` must be bundled so the default credential chain resolves "
                        + "AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE")
                .doesNotThrowAnyException();
    }
}
