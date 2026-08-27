/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.sort;

import io.varve.swath.model.ListEntry;
import java.util.Iterator;

/**
 * A forward-only stream of {@link ListEntry} in {@link ListEntryComparator} order — returned by a
 * fixture-staging chunk or the {@link SortTransform} merge phase. Backed either by an in-memory
 * sorted list or by a streaming k-way merge over open {@link EntryStream}s,
 * so it is {@link AutoCloseable}: use it in a try-with-resources to release any open readers.
 *
 * <p>Underlying read failures surface as {@link java.io.UncheckedIOException} from {@link #next()}
 * (the {@link Iterator} contract forbids checked exceptions).
 */
public interface SortedCursor extends Iterator<ListEntry>, AutoCloseable {

    /** Release any open readers. Never throws a checked exception. */
    @Override
    void close();
}
