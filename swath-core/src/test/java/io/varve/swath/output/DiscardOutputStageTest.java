/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.output;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.pipeline.Channel;
import io.varve.swath.pipeline.End;
import io.varve.swath.pipeline.Failure;
import io.varve.swath.pipeline.Item;
import io.varve.swath.runtime.RunContext;
import java.util.List;
import org.junit.jupiter.api.Test;

final class DiscardOutputStageTest {

    @Test
    void drainsAndTalliesWithoutAnOutputWriter() throws Exception {
        RunContext ctx = RunContext.create();
        DiscardOutputStage stage = new DiscardOutputStage();
        Channel<PageBatch> channel = new Channel<>(10, PageBatch::entryCount);
        List<ListEntry> entries = List.of(
                new ObjectEntry(KeyBytes.ofUtf8("a"), 7L, 0L,
                        null, "STANDARD", null, false, null, null, null, null),
                new CommonPrefixEntry(KeyBytes.ofUtf8("dir/")),
                new DeleteMarkerEntry(KeyBytes.ofUtf8("gone"), "v1", true, 0L, null));
        channel.send(new Item<>(new PageBatch(1L, 0L, entries)));
        channel.send(new End<>());

        stage.consume(ctx, channel);
        ListingStatistics statistics = stage.statistics(3L, java.time.Duration.ofSeconds(1));

        assertThat(statistics.objects()).isEqualTo(1L);
        assertThat(statistics.commonPrefixes()).isEqualTo(1L);
        assertThat(statistics.deleteMarkers()).isEqualTo(1L);
        assertThat(statistics.estimatedBytes()).isEqualTo(7L);
        assertThat(ctx.meterRegistry().get("swath.entries.emitted").counter().count()).isEqualTo(3.0);
        assertThat(ctx.meterRegistry().get("swath.bytes.estimated").counter().count()).isEqualTo(7.0);
        assertThat(ctx.meterRegistry().get("swath.emit.latency").timer().count()).isEqualTo(1L);
    }

    @Test
    void propagatesTheTypedUpstreamFailure() throws Exception {
        RunContext ctx = RunContext.create();
        DiscardOutputStage stage = new DiscardOutputStage();
        Channel<PageBatch> channel = new Channel<>(1, PageBatch::entryCount);
        ListingException failure = new ListingException("injected");
        channel.send(new Failure<>(failure));

        assertThatThrownBy(() -> stage.consume(ctx, channel)).isSameAs(failure);
    }
}
