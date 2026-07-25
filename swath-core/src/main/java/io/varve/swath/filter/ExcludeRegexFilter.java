/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.model.ListEntry;
import java.util.regex.Pattern;

/** Drops rows whose key matches the regex; keeps everything else. */
public record ExcludeRegexFilter(Pattern pattern) implements Filter {

    public static ExcludeRegexFilter of(String regex) {
        return new ExcludeRegexFilter(Pattern.compile(regex));
    }

    @Override
    public boolean matches(ListEntry e) {
        return !pattern.matcher(e.key().asString()).find();
    }

    @Override
    public int cost() {
        return 10;
    }
}
