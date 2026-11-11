// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/varveio/swath/go/internal/source/azblob/client"
)

var (
	// ErrMarker: a marker the store did not issue, or resent with another
	// prefix, delimiter or startFrom.
	ErrMarker = errors.New("azblobtest: marker refused")
	// ErrInjected is the failure FailAt injects.
	ErrInjected = errors.New("azblobtest: injected failure")
)

// Record is one request: what was asked and what was answered.
type Record struct {
	In     client.ListInput
	Names  []string // the items' names, roll-ups with a trailing " (prefix)"
	Marker string
	Err    error
}

// Store is List Blobs over a key list in Azure's order (sorted with
// CompareUTF16, independently of the adapter's byte ranks), for the
// provider as a supplied client of a flat account and for its byte-order
// adapter as a Lister: Azure's inclusive startFrom, markers bound to the
// query that issued them, maxresults, roll-ups by Azure's rule (a seek
// inside a rolled subtree returns that roll-up first), and injected short
// pages, empty continuing pages and failures; it records every request.
// Its hooks take the request's number, from 0 over every request
// (attempts, failed ones included); set them before the first request.
type Store struct {
	// Short, when it returns n > 0, caps request n's page at n items.
	Short func(req int) int
	// Empty, when true, answers request n with an empty continuing page
	// (no items, a marker at the same position) when items would follow.
	Empty func(req int) bool
	// Fail, when it returns an error, fails request n with it.
	Fail func(req int) error

	keys    []string      // the names, Azure's order
	entries []client.Item // every entry, Azure's order (a name's entries in the order given)

	mu      sync.Mutex
	views   map[client.Include][]client.Item // view's, by include
	markers map[string]storeMarker
	log     []Record
}

type storeMarker struct {
	prefix, delimiter, startFrom string
	include                      client.Include
	pos                          int // the index in the include's view the listing continues at
}

// NewStore is a store holding keys (duplicates dropped), one current blob
// each.
func NewStore(keys []string) *Store {
	k := slices.Clone(keys)
	slices.SortFunc(k, CompareUTF16)
	k = slices.Compact(k)
	entries := make([]client.Item, len(k))
	for i, name := range k {
		entries[i] = client.Item{Name: name, Size: int64(len(name)), BlobType: "BlockBlob"}
	}
	return &Store{keys: k, entries: entries, markers: map[string]storeMarker{}}
}

// NewEntryStore is a store of a versioned or soft-deleting container:
// entries are blobs whose names may repeat, a name's entries listed in the
// order given (Azure's within a name: snapshots and versions oldest first,
// the base blob or current version last). An entry is listed only with the
// include it needs (Listed); startFrom lists from a name's first entry, and
// a marker continues inside a name.
func NewEntryStore(entries []client.Item) *Store {
	e := slices.Clone(entries)
	slices.SortStableFunc(e, func(a, b client.Item) int { return CompareUTF16(a.Name, b.Name) })
	var keys []string
	for _, it := range e {
		if len(keys) == 0 || keys[len(keys)-1] != it.Name {
			keys = append(keys, it.Name)
		}
	}
	return &Store{keys: keys, entries: e, markers: map[string]storeMarker{}}
}

// Listed reports whether a listing with inc lists entry it: a snapshot
// needs include=snapshots, a version other than the current one
// include=versions, a soft-deleted entry include=deleted.
func Listed(it *client.Item, inc client.Include) bool {
	return (it.Snapshot == "" || inc.Snapshots) && (!it.Deleted || inc.Deleted) &&
		(it.VersionID == "" || it.IsCurrentVersion || inc.Versions)
}

// view is the entries a listing with inc lists, in Azure's order, built
// once per include (s.mu is held).
func (s *Store) view(inc client.Include) []client.Item {
	if v, ok := s.views[inc]; ok {
		return v
	}
	v := slices.DeleteFunc(slices.Clone(s.entries), func(it client.Item) bool { return !Listed(&it, inc) })
	if s.views == nil {
		s.views = map[client.Include][]client.Item{}
	}
	s.views[inc] = v
	return v
}

// FailAt fails the requests numbered in reqs with ErrInjected.
func FailAt(reqs ...int) func(int) error {
	return func(n int) error {
		if slices.Contains(reqs, n) {
			return ErrInjected
		}
		return nil
	}
}

// Keys is the store's keys in Azure's order.
func (s *Store) Keys() []string { return slices.Clone(s.keys) }

// Log is every request so far.
func (s *Store) Log() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.log)
}

// Attempts is the number of requests, failed ones included.
func (s *Store) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.log)
}

// Responses is the number of successful listing responses.
func (s *Store) Responses() int {
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
func (s *Store) String() string {
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
func (s *Store) List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.log)
	out, err := s.list(ctx, n, in)
	rec := Record{In: *in, Err: err}
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

func (s *Store) list(ctx context.Context, n int, in *client.ListInput) (*client.ListOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Fail != nil {
		if err := s.Fail(n); err != nil {
			return nil, err
		}
	}
	if in.MaxResults < 0 || in.MaxResults > client.MaxPageSize {
		return nil, fmt.Errorf("azblobtest: maxresults %d", in.MaxResults)
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
	view := s.view(in.Include)
	var pos int
	if in.Marker != "" {
		m, ok := s.markers[in.Marker]
		if !ok || m.prefix != in.Prefix || m.delimiter != in.Delimiter || m.startFrom != in.StartFrom || m.include != in.Include {
			return nil, ErrMarker
		}
		pos = m.pos
	} else {
		lower := in.Prefix
		if CompareUTF16(in.StartFrom, lower) > 0 {
			lower = in.StartFrom
		}
		pos = sort.Search(len(view), func(i int) bool { return CompareUTF16(view[i].Name, lower) >= 0 })
	}
	out := &client.ListOutput{}
	next := pos
	if s.Empty == nil || !s.Empty(n) || !within(view, pos, in.Prefix) {
		out.Items, next = page(view, in.Prefix, in.Delimiter, pos, size)
	}
	if within(view, next, in.Prefix) {
		out.NextMarker = fmt.Sprintf("m%d", len(s.markers)+1)
		s.markers[out.NextMarker] = storeMarker{prefix: in.Prefix, delimiter: in.Delimiter, startFrom: in.StartFrom, include: in.Include, pos: next}
	}
	return out, nil
}

// within reports whether view[i] exists and is under prefix: the prefix
// window is contiguous in Azure's order and a listing starts inside it or
// past it.
func within(view []client.Item, i int, prefix string) bool {
	return i < len(view) && strings.HasPrefix(view[i].Name, prefix)
}

// page lists up to size items from view[pos]: a name with the delimiter
// after the prefix is listed as its roll-up, once for its whole subtree,
// which a listing starting inside it reaches first.
func page(view []client.Item, prefix, delimiter string, pos, size int) ([]client.Item, int) {
	var items []client.Item
	i := pos
	for len(items) < size && within(view, i, prefix) {
		k := view[i].Name
		if delimiter != "" {
			if at := strings.Index(k[len(prefix):], delimiter); at >= 0 {
				r := k[:len(prefix)+at+len(delimiter)]
				items = append(items, client.Item{Name: r, Prefix: true})
				for i++; i < len(view) && strings.HasPrefix(view[i].Name, r); i++ {
				}
				continue
			}
		}
		items = append(items, view[i])
		i++
	}
	return items, i
}

// AccountInfo answers that the account's namespace is flat.
func (s *Store) AccountInfo(context.Context, string) (bool, error) { return false, nil }
