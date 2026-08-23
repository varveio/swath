/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.error;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.cli.ExitCodes;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * UNIT-exit: every {@link SwathException} subtype maps to its documented exit
 * code; a broken pipe maps to a clean 0; an unexpected throwable maps to 1.
 */
class ExitCodeMappingTest {

    @Test
    void everySwathExceptionMapsToItsDocumentedCode() {
        assertThat(new CancelledException("x").exitCode()).isEqualTo(130);
        assertThat(new InvalidUriException("x").exitCode()).isEqualTo(2);
        assertThat(new InvalidConfigException("x").exitCode()).isEqualTo(2);
        assertThat(new InvalidArgsException("x").exitCode()).isEqualTo(2);
        assertThat(new ListingException("x").exitCode()).isEqualTo(1);
        assertThat(ThrottleException.slowDown("x").exitCode()).isEqualTo(1);
        assertThat(new OutputException("x").exitCode()).isEqualTo(1);
        assertThat(new PublicationPendingException("x", new IOException("x")).exitCode()).isEqualTo(1);
        assertThat(new CheckpointException("x").exitCode()).isEqualTo(1);
    }

    @Test
    void exitCodesForThrowableRoutesThroughTheSealedHierarchy() {
        List<SwathException> all = List.of(
                new CancelledException("x"),
                new InvalidUriException("x"),
                new InvalidConfigException("x"),
                new InvalidArgsException("x"),
                new ListingException("x"),
                ThrottleException.slowDown("x"),
                new OutputException("x"),
                new PublicationPendingException("x", new IOException("x")),
                new CheckpointException("x"));
        for (SwathException e : all) {
            assertThat(ExitCodes.forThrowable(e)).isEqualTo(e.exitCode());
        }
    }

    @Test
    void swathExceptionWrappedInARuntimeStillMapsCorrectly() {
        Throwable wrapped = new RuntimeException("boom", new CheckpointException("db"));
        assertThat(ExitCodes.forThrowable(wrapped)).isEqualTo(1);
    }

    @Test
    void brokenPipeMapsToCleanZero() {
        assertThat(ExitCodes.forThrowable(new IOException("Broken pipe"))).isEqualTo(0);
        assertThat(ExitCodes.forThrowable(new IOException("Stream closed"))).isEqualTo(0);
    }

    @Test
    void unexpectedThrowableMapsToOne() {
        assertThat(ExitCodes.forThrowable(new IllegalStateException("nope"))).isEqualTo(1);
        assertThat(ExitCodes.forThrowable(new IOException("disk full"))).isEqualTo(1);
    }
}
