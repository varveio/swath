// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"bytes"
	"context"
	"strconv"

	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
)

// Entry is one listing entry: a Blob (Object set) or a BlobPrefix.
type Entry struct {
	Object *store.Object
	Prefix []byte
}

// Result is one page.
type Result struct {
	Request *Request
	Entries []Entry
	// NextMarker is "" on the last page.
	NextMarker string
	// Prefixes counts the page's BlobPrefix entries.
	Prefixes int
	// onPath is the pager's OnPath, for the paths rendering engages.
	onPath func(path, reason string)
}

// Pager lists one container in provider order over a range-only store.
// The store stays byte-sorted; cursor reads it in provider order.
type Pager struct {
	Store           store.Store
	FixtureIdentity string
	// OnPath, when set, is told each paging path a request engaged, with
	// the classification that selected it (the provider.path meter).
	OnPath func(path, reason string)
}

func (p *Pager) record(path, reason string) {
	if p.OnPath != nil {
		p.OnPath(path, reason)
	}
}

// ValidateMarker refuses a marker that is malformed or bound to another
// scope, before the request is admitted.
func (p *Pager) ValidateMarker(r *Request) error {
	if r.Marker == "" {
		return nil
	}
	if _, err := decodeMarker(r.Marker, binding(r, p.FixtureIdentity)); err != nil {
		p.record("marker_refused", "invalid_or_mismatched")
		return fail(Malformed, "invalid_marker")
	}
	return nil
}

// boundary is a lower bound compared in provider order.
type boundary struct {
	key       []byte
	inclusive bool
}

// later is the later of two lower bounds in provider order; on a tie the
// exclusive one wins.
func later(a, b boundary) boundary {
	if b.key == nil {
		return a
	}
	switch c := Compare(a.key, b.key); {
	case c > 0:
		return a
	case c < 0:
		return b
	}
	return boundary{a.key, a.inclusive && b.inclusive}
}

func nonNil(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

// List serves one page.
func (p *Pager) List(ctx context.Context, r *Request) (*Result, error) {
	prefix, delimiter := []byte(r.Prefix), []byte(r.Delimiter)
	ms := markerState{binding: binding(r, p.FixtureIdentity)}
	if r.MaxResultsSet {
		if n, err := strconv.ParseInt(r.MaxResults, 10, 64); err == nil && n > MaxPageSize {
			p.record("page_limit_clamped", "requested_above_5000")
		}
	}
	if r.Marker != "" && r.StartFrom != "" {
		p.record("marker_with_start_from", "matching_scope")
	}
	lower := later(boundary{prefix, true}, boundary{nonNil(r.StartFrom), true})
	if r.Marker != "" {
		m, err := decodeMarker(r.Marker, ms.binding)
		if err != nil {
			return nil, fail(Malformed, "invalid_marker")
		}
		lower = later(lower, boundary{m.boundary, m.inclusive})
		ms.progress = m.progress
	}
	if !bytes.HasPrefix(lower.key, prefix) {
		// At or after the prefix in provider order but outside it: past
		// the whole window.
		p.record("empty_window", "lower_bound_past_prefix")
		return &Result{Request: r, onPath: p.OnPath}, nil
	}
	if len(delimiter) == 0 {
		reason := "prefix_window"
		if r.StartFrom != "" {
			reason = "start_from"
		}
		p.record("flat", reason)
		c := newCursor(p.rowItems, p.firstKey, rangesFrom(lower.key, lower.inclusive, prefix))
		return p.page(ctx, r, nil, c, ms)
	}
	return p.delimited(ctx, r, prefix, delimiter, lower, ms)
}

// delimited serves a delimiter page. A lower bound inside a rolled prefix
// (a startFrom below a subtree) lists that prefix only when one of its
// names is at or after the bound, then continues after the subtree, so
// every byte range read later holds whole rolled subtrees and no prefix is
// listed twice.
func (p *Pager) delimited(ctx context.Context, r *Request, prefix, delimiter []byte, lower boundary, ms markerState) (*Result, error) {
	var leading []item
	from, inclusive := lower.key, lower.inclusive
	for from != nil {
		holder := rollup(from, prefix, delimiter)
		if holder == nil {
			break
		}
		names := newCursor(p.rowItems, p.firstKey, rangesFrom(from, inclusive, holder))
		_, listed, err := names.next(ctx, 1)
		if err != nil {
			return nil, err
		}
		if listed {
			p.record("delimiter_lower_in_prefix", "prefix_listed")
			leading = append(leading, item{name: holder})
		} else {
			p.record("delimiter_lower_in_prefix", "prefix_skipped")
		}
		from, inclusive = upperOf(holder), true
		if from != nil && !bytes.HasPrefix(from, prefix) {
			from = nil
		}
	}
	var c *cursor
	if from != nil {
		c = newCursor(p.rollups(prefix, delimiter), p.firstKey, rangesFrom(from, inclusive, prefix))
	}
	return p.page(ctx, r, leading, c, ms)
}

// page takes one page: the leading items first, then the cursor's. A page
// that ends on a Blob resumes after its name; one that ends on a
// BlobPrefix resumes at the prefix's provider-order upper bound, so its
// subtree is skipped. The marker keeps the progress byte the request
// resumed with.
func (p *Pager) page(ctx context.Context, r *Request, leading []item, c *cursor, ms markerState) (*Result, error) {
	res := &Result{Request: r, Entries: make([]Entry, 0, min(r.PageSize, 1024)), onPath: p.OnPath}
	var last item
	more := false
	for i := 0; ; {
		var it item
		var ok bool
		if i < len(leading) {
			it, ok = leading[i], true
			i++
		} else if c != nil {
			var err error
			if it, ok, err = c.next(ctx, r.PageSize+1-len(res.Entries)); err != nil {
				return nil, err
			}
		}
		if !ok {
			break
		}
		if len(res.Entries) == r.PageSize {
			more = true
			break
		}
		if it.isPrefix() {
			res.Entries = append(res.Entries, Entry{Prefix: it.name})
			res.Prefixes++
		} else {
			res.Entries = append(res.Entries, Entry{Object: it.object})
		}
		last = it
	}
	if c != nil && c.splits > 0 {
		p.record("utf16_order", "branch_swapped")
	}
	if more {
		if last.isPrefix() {
			res.NextMarker = ms.encode(true, upperOf(last.name))
		} else {
			res.NextMarker = ms.encode(false, last.name)
		}
	}
	return res, nil
}

// rollups is the byte-ordered Blob and BlobPrefix source of one range: the
// store's native rollup when it has one, else a walk of its rows that
// seeks past each rolled prefix. A rolled prefix is returned when any name
// of the range rolls up to it, so the range bounds decide membership, not
// the prefix spelling.
func (p *Pager) rollups(prefix, delimiter []byte) source {
	const (
		unasked = iota
		native
		walking
	)
	path := unasked
	return func(ctx context.Context, from []byte, inclusive bool, upper []byte, limit int) ([]item, error) {
		if path != walking {
			entries, ok, err := p.Store.DelimitedRollup(ctx, storeKey(from, inclusive), inclusive, upper, prefix, delimiter,
				limit-1, store.Projection{}, false)
			if err != nil {
				return nil, err
			}
			if ok {
				if path == unasked {
					p.record("delimiter_indexed", "native_skip_scan")
				}
				path = native
				items := make([]item, len(entries))
				for i, e := range entries {
					if e.Object == nil {
						items[i] = item{name: e.CommonPrefix}
					} else {
						items[i] = item{name: e.Object.Key, object: e.Object}
					}
				}
				return items, nil
			}
			path = walking
			p.record("delimiter_walk", "store_declined")
		}
		return p.walk(ctx, from, inclusive, upper, limit, prefix, delimiter)
	}
}

// walkBatch is how many rows one read of the delimiter walk asks for.
const walkBatch = 4096

func (p *Pager) walk(ctx context.Context, from []byte, inclusive bool, upper []byte, limit int, prefix, delimiter []byte) ([]item, error) {
	var items []item
	cur, curInclusive := from, inclusive
	for len(items) < limit {
		wanted := min(walkBatch, limit-len(items)+1)
		batch, err := p.rows(ctx, cur, curInclusive, upper, wanted)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(batch) && len(items) < limit; {
			o := &batch[i]
			rolled := rollup(o.Key, prefix, delimiter)
			if rolled == nil {
				items = append(items, item{name: o.Key, object: o})
				cur, curInclusive = o.Key, false
				i++
				continue
			}
			items = append(items, item{name: rolled})
			next, end := s3.Successor(rolled)
			if end {
				return items, nil
			}
			cur, curInclusive = next, true
			// Stay within this batch for a short subtree; a longer one
			// seeks to its successor.
			for i++; i < len(batch) && bytes.HasPrefix(batch[i].Key, rolled); i++ {
			}
		}
		if len(batch) < wanted {
			return items, nil
		}
	}
	return items, nil
}

func (p *Pager) rowItems(ctx context.Context, from []byte, inclusive bool, upper []byte, limit int) ([]item, error) {
	rows, err := p.rows(ctx, from, inclusive, upper, limit)
	if err != nil {
		return nil, err
	}
	items := make([]item, len(rows))
	for i := range rows {
		items[i] = item{name: rows[i].Key, object: &rows[i]}
	}
	return items, nil
}

func (p *Pager) firstKey(ctx context.Context, from []byte, inclusive bool) ([]byte, error) {
	rows, err := p.rows(ctx, from, inclusive, nil, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0].Key, nil
}

func (p *Pager) rows(ctx context.Context, from []byte, inclusive bool, upper []byte, limit int) ([]store.Object, error) {
	return p.Store.Rows(ctx, storeKey(from, inclusive), inclusive, upper, limit, store.Projection{})
}

// storeKey is a store lower bound: an inclusive empty bound is the open
// start of the keyspace.
func storeKey(from []byte, inclusive bool) []byte {
	if len(from) == 0 && inclusive {
		return nil
	}
	if from == nil {
		return []byte{}
	}
	return from
}

// rollup returns key up to and including the first delimiter after
// prefix, or nil when the key has none.
func rollup(key, prefix, delimiter []byte) []byte {
	if len(key) < len(prefix) {
		return nil
	}
	i := bytes.Index(key[len(prefix):], delimiter)
	if i < 0 {
		return nil
	}
	end := len(prefix) + i + len(delimiter)
	return key[:end:end]
}
