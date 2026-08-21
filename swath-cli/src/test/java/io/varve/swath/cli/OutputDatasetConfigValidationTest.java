/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.output.text.TextWriterPoolConfig;
import org.junit.jupiter.api.Test;

final class OutputDatasetConfigValidationTest {

    @Test
    void parquetAndTextUseOnePartSizeDefault() throws Exception {
        OutputOptions options = new OutputOptions();

        assertThat(options.partSizeBytes()).isEqualTo(OutputOptions.DEFAULT_PART_SIZE_BYTES);
        assertThat(options.textPartSizeBytes()).isEqualTo(OutputOptions.DEFAULT_PART_SIZE_BYTES);
    }

    @Test
    void zeroPartSizesAreUserConfigurationErrors() {
        OutputOptions parquet = new OutputOptions();
        parquet.partSize = "0";
        assertThatThrownBy(parquet::partSizeBytes)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--parquet-part-size")
                .hasMessageContaining("greater than zero");

        OutputOptions text = new OutputOptions();
        text.textPartSize = "0";
        assertThatThrownBy(text::textPartSizeBytes)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("--text-part-size")
                .hasMessageContaining("greater than zero");
    }

    @Test
    void textWriterRangeComesFromTheCorePoolContract() {
        OutputOptions below = new OutputOptions();
        below.textWriters = TextWriterPoolConfig.MIN_WRITERS - 1;
        assertThatThrownBy(below::resolveTextWriters)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MIN_WRITERS))
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MAX_WRITERS));

        OutputOptions above = new OutputOptions();
        above.textWriters = TextWriterPoolConfig.MAX_WRITERS + 1;
        assertThatThrownBy(above::resolveTextWriters)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MIN_WRITERS))
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MAX_WRITERS));
    }
}
