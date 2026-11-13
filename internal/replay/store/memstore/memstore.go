// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package memstore is a store.Store over objects held in memory: the
// replay routes' test fixture, and a fixture format (a TSV file) for
// serving a key list without writing Parquet.
package memstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/varveio/swath/go/internal/replay/store"
)

// Options configures a Store.
type Options struct {
	// Rollup answers DelimitedRollup for the "/" delimiter from a walk of
	// the sorted keys; without it the store declines every rollup and the
	// pager walks ranges.
	Rollup bool
}

// Store is an immutable, sorted, in-memory store.Store.
type Store struct {
	objs   []store.Object
	opts   Options
	calls  atomic.Int64
	closed atomic.Bool
}

// New returns a store over objs, sorted by key. Duplicate keys are an
// error, as in a sorted fixture.
func New(objs []store.Object, opts Options) (*Store, error) {
	objs = slices.Clone(objs)
	slices.SortFunc(objs, func(a, b store.Object) int { return bytes.Compare(a.Key, b.Key) })
	for i := 1; i < len(objs); i++ {
		if bytes.Equal(objs[i-1].Key, objs[i].Key) {
			return nil, fmt.Errorf("memstore: duplicate key %q", objs[i].Key)
		}
	}
	return &Store{objs: objs, opts: opts}, nil
}

// Keys is a convenience for tests: a store of objects with these keys and
// no other fields.
func Keys(opts Options, keys ...string) *Store {
	objs := make([]store.Object, len(keys))
	for i, k := range keys {
		objs[i].Key = []byte(k)
	}
	s, err := New(objs, opts)
	if err != nil {
		panic(err)
	}
	return s
}

// Calls is the number of Rows and DelimitedRollup calls answered so far.
func (s *Store) Calls() int64 { return s.calls.Load() }

// Len is the number of objects.
func (s *Store) Len() int { return len(s.objs) }

var errClosed = errors.New("memstore: closed")

// Rows implements store.Store.
func (s *Store) Rows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, proj store.Projection) ([]store.Object, error) {
	if s.closed.Load() {
		return nil, errClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.calls.Add(1)
	var out []store.Object
	for i := s.start(from, fromInclusive); i < len(s.objs) && len(out) < limit; i++ {
		if toExclusive != nil && bytes.Compare(s.objs[i].Key, toExclusive) >= 0 {
			break
		}
		out = append(out, project(s.objs[i], proj))
	}
	return out, nil
}

// DelimitedRollup implements store.Store for the "/" delimiter when
// Options.Rollup is set, and declines otherwise.
func (s *Store) DelimitedRollup(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, prefix, delimiter []byte, limit int, proj store.Projection, suppressPrefixAtOrBeforeFloor bool) ([]store.DelimitedEntry, bool, error) {
	if s.closed.Load() {
		return nil, false, errClosed
	}
	if !s.opts.Rollup || !bytes.Equal(delimiter, []byte("/")) {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.calls.Add(1)
	var out []store.DelimitedEntry
	i := s.start(from, fromInclusive)
	for i < len(s.objs) && len(out) < limit+1 {
		key := s.objs[i].Key
		if toExclusive != nil && bytes.Compare(key, toExclusive) >= 0 {
			break
		}
		if !bytes.HasPrefix(key, prefix) {
			i++
			continue
		}
		j := bytes.Index(key[len(prefix):], delimiter)
		if j < 0 {
			out = append(out, store.DelimitedEntry{Object: ptr(project(s.objs[i], proj))})
			i++
			continue
		}
		cp := bytes.Clone(key[:len(prefix)+j+len(delimiter)])
		if !suppressPrefixAtOrBeforeFloor || from == nil || bytes.Compare(cp, from) > 0 {
			out = append(out, store.DelimitedEntry{CommonPrefix: cp})
		}
		// Skip the rest of cp's subtree.
		for i < len(s.objs) && bytes.HasPrefix(s.objs[i].Key, cp) {
			i++
		}
	}
	return out, true, nil
}

// Close implements store.Store.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return errClosed
	}
	return nil
}

func (s *Store) start(from []byte, inclusive bool) int {
	if from == nil {
		return 0
	}
	i, found := slices.BinarySearchFunc(s.objs, from, func(o store.Object, k []byte) int { return bytes.Compare(o.Key, k) })
	if found && !inclusive {
		i++
	}
	return i
}

func project(o store.Object, proj store.Projection) store.Object {
	if !proj.Owner {
		o.OwnerID, o.OwnerDisplayName = nil, nil
	}
	if !proj.Content {
		o.ContentType, o.MD5 = nil, nil
	}
	return o
}

func ptr[T any](v T) *T { return &v }
