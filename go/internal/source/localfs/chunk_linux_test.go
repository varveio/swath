// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
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
