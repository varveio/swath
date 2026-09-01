/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort.stage;

import io.varve.swath.model.CommonPrefixEntry;
import io.varve.swath.model.DeleteMarkerEntry;
import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import io.varve.swath.model.PackedPage;
import io.varve.swath.model.PagePacker;
import io.varve.swath.sort.SortConfig;
import io.varve.swath.sort.spill.PageBlock;
import io.varve.swath.sort.spill.PageCompression;
import java.util.Comparator;
import java.util.List;

/**
 * The {@code --sort} {@link PagePacker}: runs on the fetch worker that built a page, compressing it
 * into a {@link PageBlock} with the configured payload codec — the SAME {@link PageBlock#pack} call
 * {@link SortBuffer#admit} otherwise makes on the drain thread, just moved upstream so it
 * parallelizes across fetch workers and the channel/downstream hold the compact packed page rather
 * than the parsed {@link ListEntry} objects.
 *
 * <p>Thread-safe: holds only the immutable {@code comparator} + {@code codec}; each {@link #pack} builds a
 * fresh {@link PageBlock} from local state — the codec is a per-call instance. The single
 * extra pass over {@code entries} here tallies the per-row-type counts + object-size sum that
 * {@code SortOutputStage} needs, so it never has to decode the packed payload on the drain thread.
 */
public final class SortPagePacker implements PagePacker {

    private final Comparator<ListEntry> comparator;
    private final PageCompression codec;

    /** Built from public config — {@code comparator} is the §0.3 preorder, the codec comes from {@code config}. */
    public SortPagePacker(Comparator<ListEntry> comparator, SortConfig config) {
        this.comparator = comparator;
        this.codec = config.segmentCodec();
    }

    @Override
    public PackedPage pack(List<ListEntry> entries) {
        long objects = 0;
        long commonPrefixes = 0;
        long deleteMarkers = 0;
        long totalObjectSize = 0;
        for (ListEntry e : entries) {
            switch (e) {
                case ObjectEntry o -> {
                    objects++;
                    totalObjectSize += o.size();
                }
                case CommonPrefixEntry ignored -> commonPrefixes++;
                case DeleteMarkerEntry ignored -> deleteMarkers++;
            }
        }
        PageBlock block = PageBlock.pack(entries, comparator, codec);
        return new PackedListingPage(block, objects, commonPrefixes, deleteMarkers, totalObjectSize);
    }
}
