/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.model.ListEntry;
import java.util.regex.Pattern;

/**
 * Keeps rows whose key matches the regex (substring {@code find}, so anchor with
 * {@code ^}/{@code $} as needed). The key is matched on its UTF-8 string view.
 */
public record IncludeRegexFilter(Pattern pattern) implements Filter {

    public static IncludeRegexFilter of(String regex) {
        return new IncludeRegexFilter(Pattern.compile(regex));
    }

    @Override
    public boolean matches(ListEntry e) {
        return pattern.matcher(e.key().asString()).find();
    }

    @Override
    public int cost() {
        return 10;
    }
}
