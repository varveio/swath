// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type descriptorHandoff struct {
	t        *testing.T
	provider *Provider
	parent   *dirFD
	trace    []string
	failAt   string
	failure  error
}

func (e *descriptorHandoff) Rows(rows []Row) error {
	if got := e.parent.refs.Load(); got != 1 {
		e.t.Errorf("references at Rows = %d, want 1 for the reader", got)
	}
	for _, row := range rows {
		e.trace = append(e.trace, "row "+row.Key)
	}
	clear(rows[:cap(rows)])
	if len(rows) > 0 && e.trace[len(e.trace)-1] == e.failAt {
		return e.failure
	}
	return nil
}

func (e *descriptorHandoff) Child(child Range) error {
	e.trace = append(e.trace, "child "+child.prefix)
	if child.at != e.parent || child.below != 1 ||
		child.path != filepath.Join(e.provider.root, filepath.FromSlash(child.prefix)) {
		e.t.Errorf("child %q has path %q, parent %p, depth %d", child.prefix, child.path, child.at, child.below)
	}
	if got := e.parent.refs.Load(); got != 2 {
		e.t.Errorf("references at Child = %d, want 2 for the reader and child", got)
	}
	// Dispose synchronously to exercise the earliest possible release
	// of emitter-owned work, including when the handoff stops the walk.
	e.provider.Dispose(child)
	if got := e.parent.refs.Load(); got != 1 {
		e.t.Errorf("references after disposing child = %d, want 1 for the reader", got)
	}
	if e.trace[len(e.trace)-1] == e.failAt {
		return e.failure
	}
	return nil
}

func TestEmitDescriptorOwnership(t *testing.T) {
	for _, noDirs := range []bool{false, true} {
		first := []string{"row a"}
		if !noDirs {
			first = append(first, "row b/")
		}
		throughFirstChild := append(slices.Clone(first), "child b/")
		fullTrace := append(slices.Clone(throughFirstChild), "row c")
		for _, tc := range []struct {
			name   string
			failAt string
			want   []string
		}{
			{"success", "", fullTrace},
			{"rows-error", first[len(first)-1], first},
			{"child-error", "child b/", throughFirstChild},
			{"tail-error", "row c", fullTrace},
		} {
			t.Run(fmt.Sprintf("noDirs=%t/%s", noDirs, tc.name), func(t *testing.T) {
				p, err := New(t.TempDir(), Options{NoDirs: noDirs})
				if err != nil {
					t.Fatal(err)
				}
				var st Stats
				r := p.Root()
				d, ok, err := p.openPortable(context.Background(), r, &st)
				if !ok {
					t.Fatalf("opening root: %v, %+v", err, st)
				}
				t.Cleanup(func() { _ = d.f.Close() })
				e := &descriptorHandoff{t: t, provider: p, parent: d, failAt: tc.failAt, failure: errors.New("handoff failed")}
				rows := []Row{{Key: "a"}, {Key: "b/", Data: Entry{Kind: KindDir}}, {Key: "c"}}
				err = p.emit(r, d, rows, e, &st)
				if !slices.Equal(e.trace, tc.want) {
					t.Errorf("handoffs = %q, want %q", e.trace, tc.want)
				}
				var wantErr error
				if tc.failAt != "" {
					wantErr = e.failure
				}
				if !errors.Is(err, wantErr) {
					t.Errorf("emit returned %v, want %v", err, wantErr)
				}
				if got := d.refs.Load(); got != 1 {
					t.Errorf("references after emit = %d, want 1 for the reader", got)
				}
				if _, err := d.f.Stat(); err != nil {
					t.Errorf("reader descriptor closed during emit: %v", err)
				}
				d.release()
				if got := d.refs.Load(); got != 0 || p.keep.open.Load() != 0 {
					t.Errorf("after reader release: references = %d, kept descriptors = %d, want 0 for both", got, p.keep.open.Load())
				}
				if _, err := d.f.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Errorf("reader descriptor after final release: stat returned %v, want %v", err, os.ErrClosed)
				}
			})
		}
	}
}
