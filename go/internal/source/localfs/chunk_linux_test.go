// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
)

// kids collects one Expand's child ranges.
type kids struct{ r []Range }

func (k *kids) Rows([]Row) error    { return nil }
func (k *kids) Child(r Range) error { k.r = append(k.r, r); return nil }

// TestCancelledChunkReleasesItsFD: a chunk a worker claimed just before the
// run was cancelled is not queued, so the order stage does not dispose it;
// its Expand releases its share of the directory descriptor even when it
// returns at once on the cancelled context.
func TestCancelledChunkReleasesItsFD(t *testing.T) {
	if !nativeEngine {
		t.Skip("chunks are a getdents-engine feature")
	}
	root := t.TempDir()
	for i := range chunkThreshold + 1 {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("f%05d", i)), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	p, err := New(root, Options{Fields: Stat})
	if err != nil {
		t.Fatal(err)
	}
	k := &kids{}
	if err := p.Expand(context.Background(), p.Root(), k); err != nil {
		t.Fatal(err)
	}
	if len(k.r) < 2 || k.r[0].chunk == nil {
		t.Fatalf("%d children: want the directory's chunks", len(k.r))
	}
	d := k.r[0].chunk.dir
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, r := range k.r {
		if err := p.Expand(ctx, r, &kids{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled chunk: %v", err)
		}
	}
	if n := d.refs.Load(); n != 0 {
		t.Fatalf("%d references left on the directory descriptor after every chunk's cancelled Expand", n)
	}
}

// TestStatCountRaisesProcsAndRestores: a stat-tier count runs with
// countProcsFactor processors per CPU (at most the workers), records it,
// and restores GOMAXPROCS; a names-tier count leaves it alone.
func TestStatCountRaisesProcsAndRestores(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	for _, fields := range []Fields{Stat, Names} {
		src, err := open(context.Background(), registry.Request{Root: root, Options: &Options{Fields: fields}, Workers: 64})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 64}}); err != nil {
			t.Fatal(err)
		}
		if n := runtime.GOMAXPROCS(0); n != 2 {
			t.Fatalf("%s: GOMAXPROCS %d after the run, want 2 restored", fields, n)
		}
		want := int64(2 * countProcsFactor)
		if fields == Names {
			want = 0
		}
		if got := src.(*opened).p.Stats().CountProcs; got != want {
			t.Fatalf("%s: count_procs %d, want %d", fields, got, want)
		}
	}
}
