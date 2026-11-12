// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"math"

	seekparquet "github.com/varveio/seekparquet/go"
)

// PageRequest is one page of a delimited listing (ListRequest) with the
// recursive totals of its common prefixes (AggregateOptions, as
// Aggregates takes them). Stats, when set, receives the statistics of the
// listing and of the aggregation (see Page for a failure).
type PageRequest struct {
	ListRequest
	Aggregates AggregateOptions
}

// PageEntry is one entry of a Page: a listing entry and, for a common
// prefix, its totals.
type PageEntry struct {
	Entry
	// Aggregate is the prefix's totals, nil for an ordinary object. Its
	// fields are nil as Aggregates leaves them: not requested, or not
	// known exactly.
	Aggregate *PrefixAggregate
}

// Page is a page of entries, and the cursor of the next page, nil when
// none follows.
type Page struct {
	Entries []PageEntry
	Next    *Cursor
}

// Page lists at most req.Limit entries as List does, and the totals of
// the common prefixes among them as Aggregates computes them, in one call
// for those prefixes alone (a prefix carrying the object at its exact key
// included). Counts use the listing's row positions and add no storage
// reads. An unknown boundary or a cursor made up inside a prefix leaves
// that prefix's totals nil; it is never completed by seeking its keys.
// It lists one entry beyond the limit to learn whether more
// follow: Next is the last returned entry's Cursor when one does, nil
// otherwise, so the last page is never followed by an empty one.
//
// A Limit that is not positive, or leaves no room for the entry beyond
// it, and an invalid AggregateOptions are errors wrapping
// ErrInvalidRequest, before any read. Otherwise the errors are List's and
// Aggregates': a context that ends between the listing and the
// aggregation fails the page with its error. No entries are returned with
// an error. req.Stats, when set, is reset when the request starts and
// receives what the listing and the aggregation read, whether it
// succeeds or fails, a plan's that failed included.
func (d *Dataset) Page(ctx context.Context, req PageRequest) (Page, error) {
	st := req.Stats
	if st != nil {
		*st = ReadStats{}
	}
	if req.Limit <= 0 || req.Limit == math.MaxInt {
		return Page{}, fmt.Errorf("%w: a page's limit must be positive and below %d", ErrInvalidRequest, math.MaxInt)
	}
	if err := req.Aggregates.validate(); err != nil {
		return Page{}, err
	}
	list := req.ListRequest
	list.Limit++
	var listed ReadStats
	list.Stats = &listed
	var rows []seekparquet.RowSpan
	entries, err := d.list(ctx, list, &rows)
	addStats(st, listed)
	if err != nil {
		return Page{}, err
	}
	var page Page
	if len(entries) > req.Limit {
		entries = entries[:req.Limit]
		page.Next = entries[len(entries)-1].Cursor()
	}
	var targets []target
	for i, e := range entries {
		if e.CommonPrefix == nil {
			continue
		}
		_, start := rows[i].Start.Ordinal()
		_, end := rows[i].End.Ordinal()
		known := start && end
		if c := req.Cursor; c != nil {
			// Made-up cursors inside this prefix clip its first rows.
			// Returned page cursors resume beyond a complete entry.
			known = known && !bytes.HasPrefix(c.Key, e.CommonPrefix) &&
				!(len(c.Prefix) > len(e.CommonPrefix) && bytes.HasPrefix(c.Prefix, e.CommonPrefix))
		}
		targets = append(targets, target{span: prefixSpan(e.CommonPrefix), rows: rows[i], known: known})
	}
	aggs, err := d.aggregate(ctx, targets, req.Aggregates, st)
	if err != nil {
		return Page{}, err
	}
	page.Entries = make([]PageEntry, len(entries))
	for i, e := range entries {
		page.Entries[i].Entry = e
		if e.CommonPrefix != nil {
			page.Entries[i].Aggregate, aggs = &aggs[0], aggs[1:]
		}
	}
	return page, nil
}
