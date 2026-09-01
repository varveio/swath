/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.finalize;

import static org.assertj.core.api.Assertions.assertThat;

import io.varve.swath.model.ListEntry;
import io.varve.swath.output.sorted.StagingNames;
import io.varve.swath.sort.ListEntryComparator;
import io.varve.swath.sort.SortMetrics;
import io.varve.swath.sort.spill.PageRef;
import io.varve.swath.sort.spill.PageRunReader;
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
        List<PageRunReader> channels = new ArrayList<>();
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
                Path path = SortTestSupport.writePages(
                        root.resolve("seg-" + segment + StagingNames.PAGE_RUN_SUFFIX), contents);
                channels.add(PageRunReader.open(path, SortMetrics.NO_OP));
                expectedRefs += pages;
            }
            FinalizationFailure failure = new FinalizationFailure();
            List<PartPlan> plans = new ArrayList<>();
            PartSizer sizer = new PartSizer(
                    PartSizer.Target.fixedRows(1 + random.nextInt(20)), Long.MAX_VALUE);
            try (PageRunHeaderStreams cursors = new PageRunHeaderStreams(channels,
                    PageRunHeaderStreams.planned(channels.size()), SortMetrics.NO_OP, failure)) {
                MergeRouter.Result result = new MergeRouter(
                        cursors, plans::add, sizer, SortMetrics.NO_OP, failure, () -> { },
                        FinalizationPlanner.MAX_PIPELINE_PLAN_REFS)
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
            for (PageRunReader channel : channels) {
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
