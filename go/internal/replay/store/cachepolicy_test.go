// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/replay/legacy"
	"github.com/varveio/swath/go/objfile"
)

// policySource checks the contexts that reach storage, including first
// part opens and context-bound scalar reads used without a RangeReader.
type policySource struct {
	*faultSource
	reads, cacheable atomic.Int64
}

func (s *policySource) check(ctx context.Context, path string) {
	if path != seekparquet.ManifestName {
		s.reads.Add(1)
		if objfile.CacheModeFromContext(ctx) != objfile.CacheBypass {
			s.cacheable.Add(1)
		}
	}
}

func (s *policySource) Open(ctx context.Context, path string) (seekparquet.SourceFile, error) {
	s.check(ctx, path)
	f, err := s.faultSource.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return policyFile{SourceFile: f, src: s, path: path}, nil
}

type policyFile struct {
	seekparquet.SourceFile
	src  *policySource
	path string
}

func (f policyFile) WithContext(ctx context.Context) io.ReaderAt {
	return policyRead(func(p []byte, off int64) (int, error) {
		f.src.check(ctx, f.path)
		return f.SourceFile.ReadAt(p, off)
	})
}

func (f policyFile) ReadAt(p []byte, off int64) (int, error) {
	return f.WithContext(context.Background()).ReadAt(p, off)
}

type policyRead func([]byte, int64) (int, error)

func (f policyRead) ReadAt(p []byte, off int64) (int, error) { return f(p, off) }

func TestBulkCachePolicy(t *testing.T) {
	dir := writeWide(t, 4, 120)
	for _, name := range []string{"Scan", "BuildListIndex"} {
		t.Run(name, func(t *testing.T) {
			src := &policySource{faultSource: &faultSource{dir: dir}}
			d, err := seekparquet.OpenDataset(t.Context(), src, seekparquet.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close() })
			r := &seekReader{d: d}
			if name == "BuildListIndex" {
				var b ListIndexBuild
				b, err = r.buildListIndex(t.Context(), ListIndex{})
				if b.Err != nil {
					t.Fatal(b.Err)
				}
			} else {
				err = r.Scan(t.Context(), legacy.Range{Limit: 1000, Columns: []string{"size"}}, nil, &slotSink{slots: make([][]string, 1)})
			}
			if err != nil {
				t.Fatal(err)
			}
			if src.reads.Load() == 0 || src.cacheable.Load() != 0 {
				t.Fatalf("bulk operation: %d storage calls, %d cacheable; want calls and none cacheable", src.reads.Load(), src.cacheable.Load())
			}
		})
	}
}
