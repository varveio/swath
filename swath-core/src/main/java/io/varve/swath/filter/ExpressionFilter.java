/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.model.ListEntry;

/**
 * JEXL {@code --expr} filter — <b>dormant seam, deferred to v1.1</b>. It
 * stays in the sealed {@link Filter} permits so the shape exists, but is
 * never constructed or wired in v1.0.
 */
public final class ExpressionFilter implements Filter {

    private static final String NOT_YET = "--expr (JEXL ExpressionFilter) is deferred to v1.1";

    public ExpressionFilter() {
        throw new UnsupportedOperationException(NOT_YET);
    }

    @Override
    public boolean matches(ListEntry e) {
        throw new UnsupportedOperationException(NOT_YET);
    }

    @Override
    public int cost() {
        return 100;
    }
}
