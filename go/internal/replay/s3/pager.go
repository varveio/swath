// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"

	"github.com/varveio/swath/go/internal/replay/store"
)

// Entry is one listing entry: an object, or a rolled-up common prefix
// (Object nil).
type Entry struct {
	Object       *store.Object
	CommonPrefix []byte
}

// Key is the entry's key bytes.
func (e Entry) Key() []byte {
	if e.Object != nil {
		return e.Object.Key
	}
	return e.CommonPrefix
}

// Result is one page of a listing.
type Result struct {
	Request   *Request
	Entries   []Entry
	Truncated bool
	// NextToken is the continuation token of the next page, "" when the
	// listing is complete.
	NextToken string
}

// CommonPrefixes counts the page's rolled-up prefixes.
func (r *Result) CommonPrefixes() int {
	n := 0
	for _, e := range r.Entries {
		if e.Object == nil {
			n++
		}
	}
	return n
}

// Delimiter paths, for the delimiter.path meter.
const (
	DelimiterPathRollup = "rollup"
	DelimiterPathWalk   = "walk"
)

const (
	// delimiterBatch is how many rows one range read of the delimiter walk
	// asks for.
	delimiterBatch = 4096
	// defaultSeekScanThreshold is how many keys of a common prefix's run
	// the walk scans in an already-read batch before it reads again from
	// the prefix's successor. It changes how many store reads a deep tree
	// costs, never the output.
	defaultSeekScanThreshold = 32
)

// Pager owns every ListObjectsV2 rule over a range-only store: max-keys
// accounting (objects and common prefixes count together), truncation,
// delimiter rollup, common-prefix resume, prefix and start-after interplay,
// token precedence over start-after, and the tokens themselves. The store
// answers byte ranges only, so two stores behind the same pager can only
// disagree by returning different rows.
type Pager struct {
	Store store.Store
	// SeekScanThreshold overrides defaultSeekScanThreshold when positive.
	SeekScanThreshold int
	// OnDelimiterPath, when set, is told which path served a delimiter
	// page.
	OnDelimiterPath func(path string)
}

// bound is a resolved lower bound; a nil key is open.
type bound struct {
	key       []byte
	inclusive bool
	// token says the bound came from a continuation token, not a
	// start-after. A delimiter listing suppresses a common prefix at or
	// before a start-after, which the client has passed; after a token it
	// rolls up whatever key comes next, so a flat walk's token that ends
	// inside a prefix lists that prefix again, as live S3 does
	// (TestLiveFlatTokenWithDelimiterRelistsItsPrefix). A delimiter walk's
	// own token resumes past the prefix's subtree, so nothing repeats.
	token bool
}

// List serves one page.
func (p *Pager) List(ctx context.Context, req *Request) (*Result, error) {
	boundary, err := resolveBoundary(req)
	if err != nil {
		return nil, err
	}
	pageSize := req.PageSize()
	if pageSize == 0 {
		return &Result{Request: req}, nil
	}
	if len(req.Delimiter) == 0 {
		return p.listObjects(ctx, req, boundary, pageSize)
	}
	return p.listDelimited(ctx, req, boundary, pageSize)
}

// resolveBoundary picks the resume boundary: a continuation token wins over
// start-after (a client may keep its opening start-after while it pages),
// but only a token that is present and not blank; a malformed token is an
// error even with a start-after that could have served.
func resolveBoundary(req *Request) (*bound, error) {
	if req.HasToken() {
		t, _, err := DecodeToken(req.Token)
		if err != nil {
			return nil, err
		}
		return &bound{key: t.Boundary, inclusive: t.Inclusive, token: true}, nil
	}
	if req.StartAfter != nil {
		return &bound{key: req.StartAfter}, nil
	}
	return nil, nil
}

// lowerBound combines the prefix window's inclusive floor with the resume
// boundary: whichever is later.
func lowerBound(prefix []byte, b *bound) bound {
	if b != nil {
		if len(prefix) > 0 && bytes.Compare(prefix, b.key) > 0 {
			return bound{key: prefix, inclusive: true, token: b.token}
		}
		return *b
	}
	if len(prefix) > 0 {
		return bound{key: prefix, inclusive: true}
	}
	return bound{inclusive: true}
}

func (p *Pager) listObjects(ctx context.Context, req *Request, b *bound, pageSize int) (*Result, error) {
	upper, _ := PrefixUpper(req.Prefix)
	rows, err := p.fetch(ctx, lowerBound(req.Prefix, b), upper, pageSize+1, req)
	if err != nil {
		return nil, err
	}
	res := &Result{Request: req, Truncated: len(rows) > pageSize}
	n := min(len(rows), pageSize)
	res.Entries = make([]Entry, n)
	for i := range n {
		res.Entries[i] = Entry{Object: &rows[i]}
	}
	if res.Truncated {
		res.NextToken = EncodeToken(rows[n-1].Key, false)
	}
	return res, nil
}

func (p *Pager) listDelimited(ctx context.Context, req *Request, b *bound, pageSize int) (*Result, error) {
	prefix, delimiter := req.Prefix, req.Delimiter
	upper, _ := PrefixUpper(prefix)
	cur := lowerBound(prefix, b)

	// The store may answer the whole rollup in one pass; it declines shapes
	// it has no fast path for, and the range walk below serves those.
	rollup, ok, err := p.Store.DelimitedRollup(ctx, cur.key, cur.inclusive, upper, prefix, delimiter, pageSize,
		store.Projection{Owner: req.FetchOwner}, !cur.token)
	if err != nil {
		return nil, err
	}
	if ok {
		p.delimiterPath(DelimiterPathRollup)
		return delimitedPage(req, rollup, pageSize), nil
	}
	p.delimiterPath(DelimiterPathWalk)

	threshold := p.SeekScanThreshold
	if threshold <= 0 {
		threshold = defaultSeekScanThreshold
	}
	res := &Result{Request: req}
	// cur is the boundary of everything not yet emitted or skipped: after
	// an object O it is (O, exclusive), after a rolled-up prefix P it is
	// (Successor(P), inclusive), so the next read starts past P's subtree.
	for len(res.Entries) < pageSize {
		rows, err := p.fetch(ctx, cur, upper, delimiterBatch, req)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return res, nil
		}
		i := 0
		for i < len(rows) && len(res.Entries) < pageSize {
			obj := &rows[i]
			cp := commonPrefix(obj.Key, prefix, delimiter)
			if cp == nil {
				res.Entries = append(res.Entries, Entry{Object: obj})
				cur = bound{key: obj.Key}
				i++
				continue
			}
			// A prefix at or before the request's start-after was on an
			// earlier page.
			if b == nil || b.token || bytes.Compare(cp, b.key) > 0 {
				res.Entries = append(res.Entries, Entry{CommonPrefix: cp})
			}
			next, end := Successor(cp)
			if end {
				// Nothing sorts after an all-0xFF prefix: the listing is
				// over, never reopened.
				return res, nil
			}
			cur = bound{key: next, inclusive: true}
			// Scan a short run of P's keys in this batch; a long run, or a
			// batch that ends inside P, reads again from Successor(P).
			j, scanned := i+1, 0
			for j < len(rows) && scanned < threshold && bytes.HasPrefix(rows[j].Key, cp) {
				j++
				scanned++
			}
			if j < len(rows) && !bytes.HasPrefix(rows[j].Key, cp) {
				i = j
				continue
			}
			break
		}
	}
	more, err := p.fetch(ctx, cur, upper, 1, req)
	if err != nil {
		return nil, err
	}
	if cur.key != nil && len(more) > 0 {
		res.Truncated = true
		res.NextToken = EncodeToken(cur.key, cur.inclusive)
	}
	return res, nil
}

// delimitedPage builds a page from a store rollup: up to pageSize+1
// entries, already filtered against the boundary and in key order; the
// extra one only signals truncation. The resume boundary follows the walk's
// rule.
func delimitedPage(req *Request, rollup []store.DelimitedEntry, pageSize int) *Result {
	res := &Result{Request: req, Entries: make([]Entry, 0, min(pageSize, len(rollup)))}
	var cur *bound
	for i := range rollup {
		if len(res.Entries) == pageSize {
			break
		}
		e := &rollup[i]
		if e.CommonPrefix != nil {
			res.Entries = append(res.Entries, Entry{CommonPrefix: e.CommonPrefix})
			next, end := Successor(e.CommonPrefix)
			if end {
				return res
			}
			cur = &bound{key: next, inclusive: true}
			continue
		}
		res.Entries = append(res.Entries, Entry{Object: e.Object})
		cur = &bound{key: e.Object.Key}
	}
	if len(rollup) > pageSize && cur != nil {
		res.Truncated = true
		res.NextToken = EncodeToken(cur.key, cur.inclusive)
	}
	return res
}

// fetch reads a range and keeps the rows under the request's prefix. With
// the bounds PrefixUpper gives, the filter removes nothing; it guards a
// store that returns rows outside the range it was asked for.
func (p *Pager) fetch(ctx context.Context, lower bound, upper []byte, limit int, req *Request) ([]store.Object, error) {
	rows, err := p.Store.Rows(ctx, lower.key, lower.inclusive, upper, limit, store.Projection{Owner: req.FetchOwner})
	if err != nil || len(req.Prefix) == 0 {
		return rows, err
	}
	kept := rows[:0:0]
	for _, r := range rows {
		if bytes.HasPrefix(r.Key, req.Prefix) {
			kept = append(kept, r)
		}
	}
	return kept, nil
}

func (p *Pager) delimiterPath(path string) {
	if p.OnDelimiterPath != nil {
		p.OnDelimiterPath(path)
	}
}

// commonPrefix returns key up to and including the first delimiter after
// prefix, or nil when the key has none (an object directly under prefix).
func commonPrefix(key, prefix, delimiter []byte) []byte {
	i := bytes.Index(key[len(prefix):], delimiter)
	if i < 0 {
		return nil
	}
	end := len(prefix) + i + len(delimiter)
	return key[:end:end]
}
