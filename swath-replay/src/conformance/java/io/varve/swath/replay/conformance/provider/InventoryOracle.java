/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance.provider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** A small, provider-neutral token walker independent of all replay pagers. */
public final class InventoryOracle {
    private InventoryOracle() {
    }

    public static <T> Walk<T> walk(Function<String, Page<T>> fetch, int maxPages, int maxEntries) {
        if (maxPages < 1 || maxEntries < 0) {
            throw new IllegalArgumentException("invalid walk limits");
        }
        List<T> inventory = new ArrayList<>();
        List<Page<T>> pages = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        String token = null;
        do {
            if (pages.size() == maxPages) {
                throw new IllegalArgumentException("provider exceeded page limit");
            }
            Page<T> page = fetch.apply(token);
            pages.add(page);
            if (page.entries().size() > maxEntries - inventory.size()) {
                throw new IllegalArgumentException("provider exceeded entry limit");
            }
            inventory.addAll(page.entries());
            String next = page.nextToken();
            if (next == null || next.isEmpty()) {
                return new Walk<>(List.copyOf(inventory), List.copyOf(pages));
            }
            if (!seen.add(next)) {
                throw new IllegalArgumentException("provider repeated a continuation token");
            }
            token = next;
        } while (true);
    }

    public record Page<T>(List<T> entries, String nextToken) {
        public Page {
            entries = List.copyOf(entries);
        }
    }

    public record Walk<T>(List<T> inventory, List<Page<T>> pages) {
    }
}
