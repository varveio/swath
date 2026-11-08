// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
)

// TestCaptureNeedsSorting pins what the container smoke's fallback relies
// on: the server's store refuses the capture as not servable sorted, and
// sort-fixture turns it into a fixture holding every object.
func TestCaptureNeedsSorting(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	if err := write(dir, 500); err != nil {
		t.Fatal(err)
	}
	if err := write(dir, 500); err == nil {
		t.Fatal("a second write over the part succeeded")
	}
	if s, _, err := store.Open(context.Background(), dir, store.Options{}); err == nil {
		s.Close()
		t.Fatal("the store opened the unsorted capture")
	} else if !strings.Contains(err.Error(), "not servable sorted") {
		t.Fatalf("refusal %q does not say not servable sorted", err)
	}
	out := filepath.Join(t.TempDir(), "sorted")
	stats, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: dir, Output: out}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s, meta, err := store.Open(context.Background(), out, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if stats.Objects != 500 || meta.Rows != 500 {
		t.Fatalf("sorted %d objects, fixture %d rows; want 500", stats.Objects, meta.Rows)
	}
}
