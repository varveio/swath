/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

final class ReferenceRoutingPropertyTest {
    private final ListEntryComparator comparator = new ListEntryComparator();

    @Property(tries = 40)
    void everyReferenceAppearsInExactlyOnePlan(@ForAll long seed) throws IOException {
        Random random = new Random(seed);
        Path root = Files.createTempDirectory("reference-routing-");
        List<PageRunSegmentIo> channels = new ArrayList<>();
        try {
            int segmentCount = 1 + random.nextInt(6);
            long expectedRefs = 0;
            for (int segment = 0; segment < segmentCount; segment++) {
                int pages = 1 + random.nextInt(12);
                int offset = random.nextInt(7);
                List<List<ListEntry>> contents = new ArrayList<>();
                for (int page = 0; page < pages; page++) {
                    int low = page * 10 + offset;
                    int high = low + random.nextInt(9);
                    List<ListEntry> rows = new ArrayList<>();
                    rows.add(SortTestSupport.object(String.format("k%04d", low)));
                    if (high != low) {
                        rows.add(SortTestSupport.object(String.format("k%04d", high)));
                    }
                    contents.add(rows);
                }
                Path path = SortTestSupport.writeIndexedPages(
                        root.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), contents);
                channels.add(PageRunSegmentIo.open(path, SortMetrics.NO_OP));
                expectedRefs += pages;
            }
            PipelineFailure failure = new PipelineFailure();
            List<PartPlan> plans = new ArrayList<>();
            PipelinePartSizer sizer = new PipelinePartSizer(
                    PipelinePartSizer.Target.fixedRows(1 + random.nextInt(20)), Long.MAX_VALUE);
            try (SegmentHeaderCursors cursors = new SegmentHeaderCursors(channels,
                    SegmentHeaderCursors.planned(channels.size()), SortMetrics.NO_OP, failure)) {
                MergeRouter.Result result = new MergeRouter(
                        cursors, plans::add, sizer, SortMetrics.NO_OP, failure)
                        .route(channels.size());

                List<PageRef> routed = plans.stream().flatMap(plan -> plan.items().stream())
                        .flatMap(item -> item.refs().stream()).toList();
                HashSet<String> identities = new HashSet<>();
                for (PageRef ref : routed) {
                    assertThat(identities.add(ref.segmentId() + ":" + ref.ordinal())).isTrue();
                }
                assertThat(routed).hasSize(Math.toIntExact(expectedRefs));
                assertThat(result.refs()).isEqualTo(expectedRefs);
                assertThat(plans).extracting(PartPlan::ordinal)
                        .containsExactlyElementsOf(java.util.stream.IntStream.range(0, plans.size())
                                .boxed().toList());
                assertThat(plans.getLast().mergeEnd()).isTrue();
                assertThat(plans.subList(0, plans.size() - 1))
                        .allMatch(plan -> !plan.mergeEnd());
            }
        } finally {
            for (PageRunSegmentIo channel : channels) {
                channel.close();
            }
            try (var paths = Files.walk(root)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                        // Best-effort cleanup must not mask a property failure.
                    }
                });
            }
        }
    }
}
