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
    void textWriterRangeComesFromTheCorePoolContract() throws Exception {
        OutputOptions below = new OutputOptions();
        below.textWriters = TextWriterPoolConfig.MIN_WRITERS - 1;
        assertThatThrownBy(below::resolveTextWriters)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MIN_WRITERS))
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MAX_WRITERS));

        OutputOptions maximum = new OutputOptions();
        maximum.textWriters = TextWriterPoolConfig.MAX_WRITERS;
        assertThat(maximum.resolveTextWriters()).isEqualTo(TextWriterPoolConfig.MAX_WRITERS);

        OutputOptions above = new OutputOptions();
        above.textWriters = TextWriterPoolConfig.MAX_WRITERS + 1;
        assertThatThrownBy(above::resolveTextWriters)
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MIN_WRITERS))
                .hasMessageContaining(String.valueOf(TextWriterPoolConfig.MAX_WRITERS));
    }

    @Test
    void productionWriterQueuesShareOneFixedPoolBudget() {
        assertThat(ListCommand.datasetWriterQueueCapacityPerLane(3)).isEqualTo(64);
        assertThat(ListCommand.datasetWriterQueueCapacityPerLane(4)).isEqualTo(64);
        assertThat(ListCommand.datasetWriterQueueCapacityPerLane(8)).isEqualTo(32);
        assertThat(ListCommand.datasetWriterQueueCapacityPerLane(64)).isEqualTo(4);

        for (int writers = 1; writers <= 64; writers++) {
            assertThat((long) writers * ListCommand.datasetWriterQueueCapacityPerLane(writers))
                    .isLessThanOrEqualTo(ListCommand.DATASET_WRITER_TOTAL_QUEUE_CAPACITY);
        }
    }
}
