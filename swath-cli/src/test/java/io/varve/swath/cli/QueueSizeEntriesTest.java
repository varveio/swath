/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.varve.swath.error.InvalidConfigException;
import io.varve.swath.filter.FilterChain;
import io.varve.swath.model.KeyBytes;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PageBatch;
import io.varve.swath.output.OutputFormat;
import io.varve.swath.runtime.ListRunner;
import io.varve.swath.runtime.RunContext;
import io.varve.swath.testkit.Keyspaces;
import io.varve.swath.testkit.MockPageFetcher;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/**
 * Pins {@code --object-listing-queue-size} as an <b>entry</b> budget (I11),
 * the way the contract documents it. Converting the knob to batch slots by integer division
 * ({@code queueSizeEntries / 1000}) would let {@code 1} still admit a full {@code 1000}-entry
 * page, making the memory knob materially different from the contract.
 *
 * <p>{@link ChannelWeightedBoundTest} proves the weight gate's admit/block edges
 * exactly; here we pin (1) the CLI resolution and (2) that a sub-page cap drives
 * the real pipeline to completion (the weighted channel admits one page at a time
 * and never deadlocks), delivering every object.
 */
class QueueSizeEntriesTest {

    @Test
    void resolvesAnEntryCountNotABatchSlotCount() throws Exception {
        // Returned verbatim as entries — NOT divided by the 1000-key page size.
        assertThat(ConnectionOptions.resolveQueueEntries(1)).isEqualTo(1);
        assertThat(ConnectionOptions.resolveQueueEntries(999)).isEqualTo(999);     // the boundary value from the class doc's batch-slot counterfactual
        assertThat(ConnectionOptions.resolveQueueEntries(50_000)).isEqualTo(50_000);
    }

    @Test
    void rejectsNonPositiveCaps() {
        for (int bad : new int[]{0, -1, -50_000}) {
            assertThatThrownBy(() -> ConnectionOptions.resolveQueueEntries(bad))
                    .as("--object-listing-queue-size=%d must be rejected", bad)
                    .isInstanceOf(InvalidConfigException.class)
                    .hasMessageContaining("--object-listing-queue-size");
        }
    }

    @Test
    void entryCountIsThePipelineWeight() {
        PageBatch batch = new PageBatch(0, 0,
                Keyspaces.exactly(7).stream().<ListEntry>map(
                        k -> ObjectEntry.withoutOwnerDisplayNameAndChecksumType(
                                KeyBytes.of(k), 1,
                                0, "e", "STANDARD", null, true, null, null)).toList());
        assertThat(batch.entryCount()).isEqualTo(7);
    }

    @Test
    void subPageEntryCapStillDeliversEveryObject() throws Exception {
        // cap = 1 entry: pages (1000 entries) are admitted one at a time. The weighted
        // channel must make progress (no deadlock) and deliver all 2500 objects.
        MockPageFetcher fetcher = MockPageFetcher.builder().keys(Keyspaces.exactly(2500)).build();
        StringWriter out = new StringWriter();

        var stats = new ListRunner().run(RunContext.create(), fetcher, out,
                new ListRunner.Spec(new byte[0], OutputFormat.JSONL, true, 1, 1000, FilterChain.EMPTY, null, null));

        assertThat(stats.objects()).isEqualTo(2500);
        assertThat(out.toString().lines().count()).isEqualTo(2500);
    }
}
