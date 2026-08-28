/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class ListObjectsV2RequestParserTest {

    @Test
    void decodesFormStyleSpaceWithoutCollapsingAnEncodedPlus() {
        S3ListRequest request = ListObjectsV2RequestParser.parse(
                "bucket", "list-type=2&prefix=American+Samoa%2Bextra");

        assertThat(request.prefix())
                .containsExactly("American Samoa+extra".getBytes(StandardCharsets.UTF_8));
    }
}
