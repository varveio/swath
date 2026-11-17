// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package bind_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// version is a payload of an object version: its size, ID and whether it
// is the key's latest.
type version struct {
	size, id int64
	latest   bool
}

var versions = schema.Schema[version]{
	Info: schema.Info{
		Name:     "versions",
		ID:       "test/versions",
		Key:      schema.String,
		Groups:   true,
		Versions: reader.VersionSpec{ID: "version_id", Latest: "is_latest"},
	},
	Columns: []schema.Column[version]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(p *version) (int64, bool) { return p.size, true }},
		{Spec: schema.Spec{Name: "version_id", Type: schema.Int64}, Int: func(p *version) (int64, bool) { return p.id, true }},
		{Spec: schema.Spec{Name: "is_latest", Type: schema.Bool, Required: true}, Bool: func(p *version) (bool, bool) { return p.latest, true }},
	},
	Bytes: func(*version) int64 { return 24 },
	Heap:  func(*version) int64 { return 0 },
}

// subtrees is a provider of n ranges below its root, each a directory
// d<i>/ that declares its interval and the prefix it owns, of ten
// versions of ten keys, the first of each the latest.
type subtrees struct{ n int }

func (t subtrees) Root() int { return -1 }

func (t subtrees) Expand(_ context.Context, r int, e source.Emitter[int, version]) error {
	if r < 0 {
		for i := range t.n {
			if err := e.Child(i); err != nil {
				return err
			}
		}
		return nil
	}
	dir := fmt.Sprintf("d%d/", r)
	if f, ok := e.(source.Fencer); ok {
		f.Span(dir, dir+"~", false)
	}
	if s, ok := e.(source.Subtreer); ok {
		s.Subtree(dir)
	}
	rows := make([]schema.Row[version], 10)
	for i := range rows {
		rows[i] = schema.Row[version]{Key: fmt.Sprintf("%sk%d", dir, i), Data: version{size: 1, id: 1, latest: true}}
	}
	return e.Rows(rows)
}

// treeSink is a caller's sink that takes runs, told their intervals and
// the subtrees that end; it counts each callback.
type treeSink struct {
	mu                                      sync.Mutex
	written, ran, runs, ends, from, through int
	subtrees                                []string
}

func (s *treeSink) Write(_ context.Context, segs [][]schema.Row[version]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range segs {
		s.written += len(seg)
	}
	return nil
}

func (s *treeSink) Run() order.RangeRun[version] {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs++
	return treeRun{s}
}

func (s *treeSink) SubtreeEnd(prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subtrees = append(s.subtrees, prefix)
}

type treeRun struct{ s *treeSink }

func (r treeRun) Write(_ context.Context, rows []schema.Row[version]) error {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.ran += len(rows)
	return nil
}

func (r treeRun) End() {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.ends++
}

func (r treeRun) From(string) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.from++
}

func (r treeRun) Through(string, bool) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	r.s.through++
}

// TestCountKeepsCallersSinkPlace: with the version counter and the
// per-prefix totals (with and without the view) between the count and a
// caller's sink, the sink still takes every row through its runs, each
// told its interval, and is told of each subtree that ends, as it is with
// nothing between.
func TestCountKeepsCallersSinkPlace(t *testing.T) {
	const n = 6
	b, err := bind.Provider[int, version](subtrees{n}, &versions)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		versions bool
		totals   *totals.Options
	}{
		{name: "none"},
		{name: "versions", versions: true},
		{name: "totals", totals: &totals.Options{Depth: 1}},
		{name: "view", totals: &totals.Options{Depth: 1, View: true}},
		{name: "versions and view", versions: true, totals: &totals.Options{Depth: 1, View: true}},
		{name: "versions and totals", versions: true, totals: &totals.Options{Depth: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := bind.Env{Order: order.Options{Workers: 3}}
			if tc.versions {
				env.Versions = &bind.VersionCounts{}
			}
			if tc.totals != nil {
				env.Totals = totals.New(*tc.totals)
			}
			l, err := b.Prepare(env)
			if err != nil {
				t.Fatal(err)
			}

			var s treeSink
			if _, err := l.Count(context.Background(), &s); err != nil {
				t.Fatal(err)
			}
			if s.written != 0 || s.ran != n*10 {
				t.Errorf("%d rows through Write and %d through runs, want 0 and %d", s.written, s.ran, n*10)
			}
			if s.runs != n || s.ends != n || s.from != n || s.through != n {
				t.Errorf("%d runs, %d ended, %d told From, %d told Through; want %d each", s.runs, s.ends, s.from, s.through, n)
			}
			if len(s.subtrees) != n {
				t.Errorf("subtrees ended %q, want %d", s.subtrees, n)
			}
			if tc.versions {
				if got := env.Versions.Versions.Load(); got != n*10 {
					t.Errorf("%d versions counted, want %d", got, n*10)
				}
			}
			if tc.totals != nil {
				if r := env.Totals.Result(); len(r) == 0 || r[0].Objects != n*10 {
					t.Errorf("totals %+v, want %d objects in all", r, n*10)
				}
			}
		})
	}
}
