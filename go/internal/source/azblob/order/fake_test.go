// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
)

var (
	// errMarker: a marker the fake did not issue, or resent with another
	// prefix, delimiter or startFrom.
	errMarker = errors.New("fake: marker refused")
	// errInjected is the failure failAt injects.
	errInjected = errors.New("fake: injected failure")
)

// fakeRecord is one request: what was asked and what was answered.
type fakeRecord struct {
	In     client.ListInput
	Names  []string // the items' names, roll-ups with a trailing " (prefix)"
	Marker string
	Err    error
}

// fakeStore is List Blobs over a key list in Azure's order (sorted with
// azblobtest.CompareUTF16, written from the definition, independently of
// the adapter's byte ranks) with Azure's inclusive startFrom, markers bound
// to the query that issued them, maxresults, roll-ups by Azure's rule (a
// seek inside a rolled subtree returns that roll-up first), and injected
// short pages, empty continuing pages and failures; it records every
// request. Its hooks take the request's number, from 0 over every
// request (attempts, failed ones included); set them before the first
// request.
type fakeStore struct {
	// Short, when it returns n > 0, caps request n's page at n items.
	Short func(req int) int
	// Empty, when true, answers request n with an empty continuing page
	// (no items, a marker at the same position) when items would follow.
	Empty func(req int) bool
	// Fail, when it returns an error, fails request n with it.
	Fail func(req int) error

	keys []string // Azure's order

	mu      sync.Mutex
	markers map[string]fakeMarker
	log     []fakeRecord
}

type fakeMarker struct {
	prefix, delimiter, startFrom string
	pos                          int // the index in keys the listing continues at
}

// newFake is a fake holding keys (duplicates dropped).
func newFake(keys []string) *fakeStore {
	k := slices.Clone(keys)
	slices.SortFunc(k, azblobtest.CompareUTF16)
	return &fakeStore{keys: slices.Compact(k), markers: map[string]fakeMarker{}}
}

// failAt fails the requests numbered in reqs with errInjected.
func failAt(reqs ...int) func(int) error {
	return func(n int) error {
		if slices.Contains(reqs, n) {
			return errInjected
		}
		return nil
	}
}

// Keys is the store's keys in Azure's order.
func (s *fakeStore) Keys() []string { return slices.Clone(s.keys) }

// Log is every request so far.
func (s *fakeStore) Log() []fakeRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

// Attempts is the number of requests, failed ones included.
func (s *fakeStore) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.log)
}

// Responses is the number of successful listing responses.
func (s *fakeStore) Responses() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.log {
		if r.Err == nil {
			n++
		}
	}
	return n
}

// String is the request log, one request a line, for a failing test.
func (s *fakeStore) String() string {
	var b strings.Builder
	for i, r := range s.Log() {
		fmt.Fprintf(&b, "%3d prefix=%q delimiter=%q startFrom=%q marker=%q max=%d", i, r.In.Prefix, r.In.Delimiter, r.In.StartFrom,
			r.In.Marker, r.In.MaxResults)
		if r.Err != nil {
			fmt.Fprintf(&b, " -> error %v\n", r.Err)
			continue
		}
		fmt.Fprintf(&b, " -> %q next=%q\n", r.Names, r.Marker)
	}
	return b.String()
}

// List serves one page (client.Client.List's contract).
func (s *fakeStore) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.log)
	out, err := s.list(ctx, n, in)
	rec := fakeRecord{In: *in, Err: err}
	if out != nil {
		rec.Marker = out.NextMarker
		for _, it := range out.Items {
			name := it.Name
			if it.Prefix {
				name += " (prefix)"
			}
			rec.Names = append(rec.Names, name)
		}
	}
	s.log = append(s.log, rec)
	return out, err
}

func (s *fakeStore) list(ctx context.Context, n int, in *client.ListInput) (*client.ListOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Fail != nil {
		if err := s.Fail(n); err != nil {
			return nil, err
		}
	}
	if in.MaxResults < 0 || in.MaxResults > client.MaxPageSize {
		return nil, fmt.Errorf("fake: maxresults %d", in.MaxResults)
	}
	size := in.MaxResults
	if size == 0 {
		size = client.MaxPageSize
	}
	if s.Short != nil {
		if k := s.Short(n); k > 0 && k < size {
			size = k
		}
	}
	var pos int
	if in.Marker != "" {
		m, ok := s.markers[in.Marker]
		if !ok || m.prefix != in.Prefix || m.delimiter != in.Delimiter || m.startFrom != in.StartFrom {
			return nil, errMarker
		}
		pos = m.pos
	} else {
		lower := in.Prefix
		if azblobtest.CompareUTF16(in.StartFrom, lower) > 0 {
			lower = in.StartFrom
		}
		pos = sort.Search(len(s.keys), func(i int) bool { return azblobtest.CompareUTF16(s.keys[i], lower) >= 0 })
	}
	out := &client.ListOutput{}
	next := pos
	if s.Empty == nil || !s.Empty(n) || !s.within(pos, in.Prefix) {
		out.Items, next = s.page(in.Prefix, in.Delimiter, pos, size)
	}
	if s.within(next, in.Prefix) {
		out.NextMarker = fmt.Sprintf("m%d", len(s.markers)+1)
		s.markers[out.NextMarker] = fakeMarker{prefix: in.Prefix, delimiter: in.Delimiter, startFrom: in.StartFrom, pos: next}
	}
	return out, nil
}

// within reports whether keys[i] exists and is under prefix: the prefix
// window is contiguous in Azure's order and a listing starts inside it or
// past it.
func (s *fakeStore) within(i int, prefix string) bool {
	return i < len(s.keys) && strings.HasPrefix(s.keys[i], prefix)
}

// page lists up to size items from keys[pos]: a key with the delimiter
// after the prefix is listed as its roll-up, once for its whole subtree,
// which a listing starting inside it reaches first.
func (s *fakeStore) page(prefix, delimiter string, pos, size int) ([]client.Item, int) {
	var items []client.Item
	i := pos
	for len(items) < size && s.within(i, prefix) {
		k := s.keys[i]
		if delimiter != "" {
			if at := strings.Index(k[len(prefix):], delimiter); at >= 0 {
				r := k[:len(prefix)+at+len(delimiter)]
				items = append(items, client.Item{Name: r, Prefix: true})
				for i++; i < len(s.keys) && strings.HasPrefix(s.keys[i], r); i++ {
				}
				continue
			}
		}
		items = append(items, client.Item{Name: k, Size: int64(len(k)), BlobType: "BlockBlob"})
		i++
	}
	return items, i
}
