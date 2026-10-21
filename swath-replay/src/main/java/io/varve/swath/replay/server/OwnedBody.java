/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.server;

import java.nio.ByteBuffer;
import java.util.List;

/** Ordered byte views retained by one response until its internal write callback finishes. */
public final class OwnedBody {
    private final BudgetedOutput owner;
    private final int length;
    private final List<ByteBuffer> views;

    private OwnedBody(BudgetedOutput owner, int length, List<ByteBuffer> views) {
        if (length < 0) throw new IllegalArgumentException("negative response length");
        this.owner = owner;
        this.length = length;
        this.views = List.copyOf(views);
        long counted = 0;
        for (ByteBuffer view : this.views) counted += view.remaining();
        if (counted != length) throw new IllegalArgumentException("response views have a length gap");
    }

    static OwnedBody budgeted(BudgetedOutput owner, int length, List<ByteBuffer> views) {
        return new OwnedBody(owner, length, views);
    }

    static OwnedBody unbudgeted(ByteBuffer body) {
        ByteBuffer view = body.slice().asReadOnlyBuffer();
        return new OwnedBody(null, view.remaining(), List.of(view));
    }

    BudgetedOutput owner() { return owner; }
    public int length() { return length; }
    public List<ByteBuffer> views() { return views; }
}
