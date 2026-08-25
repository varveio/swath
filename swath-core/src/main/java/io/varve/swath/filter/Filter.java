/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.filter;

import io.varve.swath.error.ListingException;
import io.varve.swath.model.ListEntry;

/**
 * A predicate over emitted rows. Sealed; {@link ExpressionFilter} is a
 * dormant permits entry — see its own javadoc.
 *
 * <p>{@code matches(e) == true} means the entry is <b>kept</b>.
 *
 * <p>Each filter declares an evaluation {@link #cost()} so the chain runs cheap
 * predicates (size/mtime/storage-class) before regex.
 */
public sealed interface Filter
        permits IncludeRegexFilter, ExcludeRegexFilter, SizeFilter, MtimeFilter,
                StorageClassFilter, ExpressionFilter {

    boolean matches(ListEntry e) throws ListingException;

    /** Lower = cheaper = evaluated first. */
    int cost();
}
