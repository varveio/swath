// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"

	"github.com/varveio/swath/internal/replay/s3"
	"github.com/varveio/swath/internal/replay/store"
)

// walkBatch is the most rows one range read of the delimiter walk asks
// for.
const walkBatch = 4096

// Page is one objects.list answer. Items and Prefixes keep their own
// order (the response renders them as two arrays); Token is "" on the
// last page.
type Page struct {
	Items    []Item
	Prefixes [][]byte
	Token    string
}

// Pager pages objects.list requests over a fixture.
type Pager struct {
	src             source
	fixtureIdentity string
	// OnPath, when set, is told which path served each page
	// ("flat", "delimiter_indexed", ...) and why, for the provider.path
	// meter.
	OnPath func(path, reason string)
}

// NewPager returns a pager over st. When st is a sorted fixture of the
// GCS objects schema (store.Sorted with schema id NativeSchemaID), items
// carry the fixture's own GCS metadata columns; otherwise they carry the
// synthetic profile.
func NewPager(st store.Store, fixtureIdentity string) *Pager {
	p := &Pager{fixtureIdentity: fixtureIdentity}
	if sorted, ok := st.(*store.Sorted); ok && sorted.SchemaID() == NativeSchemaID {
		p.src = nativeSource{sorted}
	} else {
		p.src = objectSource{st}
	}
	return p
}

// Native reports whether the pager renders the fixture's GCS metadata.
func (p *Pager) Native() bool {
	_, ok := p.src.(nativeSource)
	return ok
}

func (p *Pager) record(path, reason string) {
	if p.OnPath != nil {
		p.OnPath(path, reason)
	}
}

// ValidateToken checks a request's page token (its shape and its binding
// to the request's scope and this fixture) before the request takes any
// serving resource.
func (p *Pager) ValidateToken(r *Request) error {
	if r.PageToken == "" {
		return nil
	}
	_, err := decodeToken(r.PageToken, tokenBinding(r, p.fixtureIdentity))
	return err
}

// boundary is a lower bound; a nil key is open.
type boundary struct {
	key       []byte
	inclusive bool
}

// later is the tighter of two lower bounds; at the same key it is
// inclusive only when both are.
func later(a, b boundary) boundary {
	switch {
	case b.key == nil:
		return a
	case a.key == nil:
		return b
	}
	switch c := bytes.Compare(a.key, b.key); {
	case c > 0:
		return a
	case c < 0:
		return b
	}
	return boundary{a.key, a.inclusive && b.inclusive}
}

// List answers r.
func (p *Pager) List(ctx context.Context, r *Request) (*Page, error) {
	if r.Clamped {
		p.record("page_limit_clamped", "requested_above_1000")
	}
	ts := tokenState{binding: tokenBinding(r, p.fixtureIdentity)}
	lower := boundary{inclusive: true}
	if len(r.Prefix) > 0 {
		lower.key = r.Prefix
	}
	lower = later(lower, boundary{r.StartOffset, true})
	if r.PageToken != "" {
		c, err := decodeToken(r.PageToken, ts.binding)
		if err != nil {
			return nil, &Failure{Reason: "invalid_page_token"}
		}
		lower = later(lower, boundary{c.boundary, c.inclusive})
		ts.progress = c.progress
	}
	upper := minUpper(r.Prefix, r.EndOffset)
	if lower.key != nil && upper != nil && bytes.Compare(lower.key, upper) >= 0 {
		p.record("empty_intersection", "lower_at_or_after_upper")
		return &Page{}, nil
	}
	if len(r.Delimiter) == 0 {
		return p.flat(ctx, lower, upper, ts, r.PageSize)
	}
	return p.delimited(ctx, r, lower, upper, ts)
}

// minUpper is the tighter of the prefix's upper bound and endOffset; nil
// is open.
func minUpper(prefix, endOffset []byte) []byte {
	prefixEnd, open := s3.PrefixUpper(prefix)
	switch {
	case open:
		return endOffset
	case endOffset == nil:
		return prefixEnd
	case bytes.Compare(prefixEnd, endOffset) < 0:
		return prefixEnd
	}
	return endOffset
}

// flat reads one row past the page: its presence is the truncation
// signal, and the token resumes exclusive at the last item returned.
func (p *Pager) flat(ctx context.Context, lower boundary, upper []byte, ts tokenState, pageSize int) (*Page, error) {
	if upper == nil {
		p.record("flat", "open_upper")
	} else {
		p.record("flat", "bounded_upper")
	}
	rows, err := p.src.rows(ctx, lower, upper, pageSize+1)
	if err != nil {
		return nil, err
	}
	count := min(pageSize, len(rows))
	page := &Page{Items: rows[:count]}
	if len(rows) > count {
		page.Token = ts.encode(false, rows[count-1].Key)
	}
	return page, nil
}

// delimited answers a delimiter listing from the store's rollup when it
// accepts, otherwise by walking ranges. A rolled-up prefix is emitted even
// when the lower bound falls inside it (startOffset in the middle of a
// subtree), and a page that ends on a prefix resumes inclusive at its
// successor, past the whole subtree.
func (p *Pager) delimited(ctx context.Context, r *Request, lower boundary, upper []byte, ts tokenState) (*Page, error) {
	reason := func(plain, offset string) string {
		if r.StartOffset != nil {
			return offset
		}
		return plain
	}
	entries, ok, err := p.src.rollup(ctx, lower, upper, prefixBytes(r.Prefix), r.Delimiter, r.PageSize)
	if err != nil {
		return nil, err
	}
	if ok {
		p.record("delimiter_indexed", reason("native_skip_scan", "start_offset_intersection"))
		return indexedPage(r.PageSize, entries, ts), nil
	}
	p.record("delimiter_walk", reason("store_declined", "start_offset_store_declined"))
	page := &Page{}
	cursor := lower
	var last boundary
	emitted := 0
	for {
		rows, err := p.src.rows(ctx, cursor, upper, min(walkBatch, r.PageSize-emitted+1))
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return page, nil
		}
		for i := 0; i < len(rows); {
			if emitted == r.PageSize {
				page.Token = ts.encode(last.inclusive, last.key)
				return page, nil
			}
			rolled := rollup(rows[i].Key, r.Prefix, r.Delimiter)
			end := false
			if rolled == nil {
				page.Items = append(page.Items, rows[i])
				cursor = boundary{rows[i].Key, false}
				i++
			} else {
				page.Prefixes = append(page.Prefixes, rolled)
				next, none := s3.Successor(rolled)
				cursor, end = boundary{next, true}, none
				// Skip the subtree's rows already in this batch; when it
				// runs past the batch, the next read starts at the
				// successor.
				for i++; i < len(rows) && bytes.HasPrefix(rows[i].Key, rolled); i++ {
				}
			}
			last = cursor
			emitted++
			if end {
				return page, nil
			}
		}
	}
}

// prefixBytes is the rollup's prefix argument: empty for a root rollup.
func prefixBytes(prefix []byte) []byte {
	if prefix == nil {
		return []byte{}
	}
	return prefix
}

// indexedPage cuts a rollup answer of up to pageSize+1 entries into a
// page; the token resumes after the last entry kept, and a prefix with no
// successor (all 0xFF) ends the listing.
func indexedPage(pageSize int, entries []Entry, ts tokenState) *Page {
	page := &Page{}
	count := min(pageSize, len(entries))
	var last boundary
	end := false
	for _, e := range entries[:count] {
		if e.Item == nil {
			page.Prefixes = append(page.Prefixes, e.Prefix)
			next, none := s3.Successor(e.Prefix)
			last, end = boundary{next, true}, none
		} else {
			page.Items = append(page.Items, *e.Item)
			last, end = boundary{e.Item.Key, false}, false
		}
	}
	if len(entries) > count && !end {
		page.Token = ts.encode(last.inclusive, last.key)
	}
	return page
}

// rollup returns key up to and including the first delimiter after the
// prefix, or nil when the key has none there.
func rollup(key, prefix, delimiter []byte) []byte {
	if len(key) < len(prefix) {
		return nil
	}
	if i := bytes.Index(key[len(prefix):], delimiter); i >= 0 {
		return bytes.Clone(key[:len(prefix)+i+len(delimiter)])
	}
	return nil
}
