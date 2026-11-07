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
	waited := waiters(cache)
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
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := read(ctx)
	awaitWaiter(t, waited)
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
	awaitWaiter(t, waited)
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

// waiters returns a channel that receives each time a read of cache is
// about to wait for a descriptor: the explicit barrier the test
// synchronizes on (the read registers its wait before it parks, so a
// change after the barrier still wakes it).
func waiters(cache *FileCache) <-chan struct{} {
	ch := make(chan struct{}, 16)
	cache.mu.Lock()
	cache.waiting = func() {
		select {
		case ch <- struct{}{}:
		default: // never block under the cache's lock
		}
	}
	cache.mu.Unlock()
	return ch
}

// awaitWaiter receives from a waiters channel or fails after 5 s.
func awaitWaiter(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("no read waited for a descriptor")
	}
}
