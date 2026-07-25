/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.model.ListEntry;
import io.varve.swath.model.ObjectEntry;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Keeps objects whose storage class is in the allow-set (case-insensitive,
 * e.g. {@code STANDARD,GLACIER}). Non-object rows always pass.
 */
public record StorageClassFilter(Set<String> allowed) implements Filter {

    public StorageClassFilter {
        allowed = allowed.stream().map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean matches(ListEntry e) {
        if (e instanceof ObjectEntry o) {
            String sc = o.storageClass();
            return sc != null && allowed.contains(sc.toUpperCase(Locale.ROOT));
        }
        return true;
    }

    @Override
    public int cost() {
        return 0;
    }
}
