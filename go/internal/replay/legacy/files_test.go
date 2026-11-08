// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package legacy_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

// TestFileCacheBoundsHandles reads ten files concurrently through a cache
// of three handles: every read sees its own file's bytes, no more than
// three handles are ever open, a file replaced after its handle was
// evicted is refused rather than read, and a closed file reads no more.
func TestFileCacheBoundsHandles(t *testing.T) {
	dir := t.TempDir()
	cache := legacy.NewFileCache(3)
	var files []*legacy.CachedFile
	for i := range 10 {
		path := filepath.Join(dir, fmt.Sprintf("f%d", i))
		if err := os.WriteFile(path, bytes.Repeat([]byte{byte('a' + i)}, 64), 0o644); err != nil {
			t.Fatal(err)
		}
		f, err := cache.Open(context.Background(), path, nil)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			buf := make([]byte, 8)
			for n := range 200 {
				i := (g*7 + n) % len(files)
				if _, err := files[i].ReadAt(buf, int64(n%56)); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(buf, bytes.Repeat([]byte{byte('a' + i)}, 8)) {
					t.Errorf("file %d read %q", i, buf)
					return
				}
			}
		})
	}
	wg.Wait()
	if st := cache.Stats(); st.Peak > 3 || st.Open > 3 || st.Opens <= 10 || st.Evictions != st.Opens-st.Open {
		t.Fatalf("stats %+v: at most 3 open, reopens after eviction, an eviction for every descriptor closed", st)
	}
	// Evict f0 by reading three others, then replace it.
	for _, i := range []int{1, 2, 3} {
		if _, err := files[i].ReadAt(make([]byte, 1), 0); err != nil {
			t.Fatal(err)
		}
	}
	path := files[0].Name()
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte{'z'}, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := files[0].ReadAt(make([]byte, 1), 0); err == nil {
		t.Fatal("a replaced file was read")
	}
	_ = files[1].Close()
	if _, err := files[1].ReadAt(make([]byte, 1), 0); !errors.Is(err, os.ErrClosed) || !errors.Is(err, legacy.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
	for _, f := range files {
		_ = f.Close()
	}
	if st := cache.Stats(); st.Open != 0 {
		t.Fatalf("%d handles open after every file closed", st.Open)
	}
}
