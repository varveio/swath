// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// openTwoParts writes two one-group parts and opens them through a
// one-descriptor cache, returning the reader and the cache.
func openTwoParts(t *testing.T) (*Reader, *FileCache, []*CachedFile) {
	t.Helper()
	dir := t.TempDir()
	cache := NewFileCache(1)
	var sources []PartSource
	var files []*CachedFile
	for i, keys := range [][]string{{"a", "b"}, {"m", "n"}} {
		var rows []sortedparquettest.Row
		for j, k := range keys {
			rows = append(rows, sortedparquettest.Object(k, j))
		}
		path := filepath.Join(dir, fmt.Sprintf("part-%d.parquet", i))
		if err := sortedparquettest.Write(path, sortedparquettest.Part{Rows: rows}); err != nil {
			t.Fatal(err)
		}
		f, err := cache.Open(context.Background(), path, nil)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
		sources = append(sources, PartSource{Name: path, File: f, Size: f.Size(), BoundBytes: 64})
	}
	r, err := Open(context.Background(), sources, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return r, cache, files
}

// TestCacheWaitEndsWithContextAndClose occupies the one descriptor slot
// with a lease on part A, then reads part B: the read waits for the slot
// and returns its context's error as soon as it is cancelled, with no
// lease released in between. A second waiting read fails with ErrClosed
// as soon as the reader is closed, and Close returns while A's lease is
// still held; releasing it closes the last descriptor.
func TestCacheWaitEndsWithContextAndClose(t *testing.T) {
	r, cache, files := openTwoParts(t)
	if _, err := files[0].lease(context.Background()); err != nil {
		t.Fatal(err)
	}
	read := func(ctx context.Context) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := r.Read(ctx, Range{From: []byte("m"), FromInclusive: true, Limit: 1}, nil)
			done <- err
		}()
		return done
	}
	waitForWaiter(t, cache)
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := read(ctx)
	waitForWaiter(t, cache)
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled read still waits for a descriptor")
	}

	queued := read(context.Background())
	waitForWaiter(t, cache)
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-queued:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("read queued across Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a read waiting for a descriptor held Close")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if open := cache.Stats().Open; open != 1 {
		t.Fatalf("%d descriptors open while A is leased, want 1", open)
	}
	files[0].release()
	if st := cache.Stats(); st.Open != 0 || st.Peak > 1 {
		t.Fatalf("stats %+v after the last lease ended", st)
	}
}

// waitForWaiter waits until the cache's change channel has been taken by
// a waiter: the slot is full and a read is blocked on it. It polls the
// one state a waiter leaves, a full cache with nothing idle.
func waitForWaiter(t *testing.T, cache *FileCache) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cache.mu.Lock()
		full := cache.open >= cache.max && cache.idle.Len() == 0
		cache.mu.Unlock()
		if full {
			time.Sleep(10 * time.Millisecond) // let a starting read reach its wait
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the cache never filled")
}
