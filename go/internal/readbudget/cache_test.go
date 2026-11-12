// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package readbudget_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/spsource"
	"github.com/varveio/swath/go/reader"
)

// cacheOpener opens immutable fixture files with a known identity, so every
// reopen can reuse cached bytes without a remote sized open's identity probe.
type cacheOpener struct {
	dir   string
	reads atomic.Int64
}

func (o *cacheOpener) Open(ctx context.Context, name string) (objfile.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(o.dir, name))
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return cacheFile{File: f, size: st.Size(), reads: &o.reads}, nil
}

func (o *cacheOpener) OpenSized(ctx context.Context, name string, _ int64) (objfile.Object, error) {
	return o.Open(ctx, name)
}

type cacheFile struct {
	*os.File
	size  int64
	reads *atomic.Int64
}

func (f cacheFile) Size() int64                             { return f.size }
func (cacheFile) Identity() string                          { return "fixture-v1" }
func (f cacheFile) WithContext(context.Context) io.ReaderAt { return f }
func (f cacheFile) ReadAt(p []byte, off int64) (int, error) {
	f.reads.Add(1)
	return f.File.ReadAt(p, off)
}

type cacheObserver struct{ hits atomic.Int64 }

func (*cacheObserver) Begin(objfile.Request) func(objfile.RequestResult) { return nil }
func (*cacheObserver) BatchPlanned(int64, int64, int64)                  {}
func (o *cacheObserver) CacheHit(n int64)                                { o.hits.Add(n) }

func cacheFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	mustDo(t, write(dir, &geometry{tenants: 1, browse: -1, partRows: 250, tenantFormat: "t-%03d/"}))
	return dir
}

func localCacheFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	mustDo(t, os.MkdirAll(filepath.Join(dir, dataset.DataDir), 0o755))
	router, err := parquetout.NewRouter(parquetout.Options{Out: dir, PartSize: 1 << 20, Encoders: 1, MaxRows: 2}, &localfs.Schema)
	mustDo(t, err)
	var rows []localfs.Row
	for i := range 4 {
		rows = append(rows, localfs.Row{Key: fmt.Sprintf("f-%04d", i), Data: localfs.Entry{
			Kind: localfs.KindFile, Size: int64(i), Mtime: 1_704_067_200_000_000,
			Present: localfs.HasSize | localfs.HasMtime}})
	}
	mustDo(t, router.Write(t.Context(), [][]localfs.Row{rows}))
	parts, err := router.Close()
	mustDo(t, err)
	mustDo(t, dataset.Publish(dir, dataset.Meta{SchemaID: localfs.Schema.ID, Groups: localfs.Schema.Groups,
		Source: dataset.Source{Kind: "local-fs", URI: "/fixture"}, Scope: dataset.Scope{Kind: "whole"},
		Coverage: dataset.Coverage{Complete: true, StopReason: "completed", Exclusions: []string{}, Filters: []string{}}}, parts, nil))
	return dir
}

// TestCallerBulkCacheBypass runs through a real Pin/Cache/spsource stack.
// The caller marks bulk reads as bypass. Reopening discards SeekParquet's
// metadata and memo: a point read after a bulk scan must still be cold,
// then a second point read must reuse bytes. A marked bulk scan over that
// warm cache must read storage without cache hits.
func TestCallerBulkCacheBypass(t *testing.T) {
	dir := cacheFixture(t)
	type scanCase struct {
		name, dir string
		bulk      func(context.Context, *reader.Dataset) error
	}
	scan := func(ctx context.Context, d *reader.Dataset) error {
		spec, _ := reader.Schema(d.Metadata().SchemaID)
		return d.SeekParquet().Scan(ctx, seekparquet.Query{Columns: spec.ColumnNames()}, nil, func(*seekparquet.Batch) error { return nil })
	}
	reductions := []seekparquet.Reduction{{Op: seekparquet.CountRows}, {Op: seekparquet.Sum, Column: "size"}, {Op: seekparquet.Max, Column: "last_modified"}}
	cases := []scanCase{
		{"ScanLocal", localCacheFixture(t), scan},
		{"Scan", dir, scan},
		{"Aggregate", dir, func(ctx context.Context, d *reader.Dataset) error {
			_, err := d.SeekParquet().Aggregate(ctx, []seekparquet.Selection{{}}, reductions,
				seekparquet.AggregateOptions{ValueBudgetBytes: -1, CheckKeys: true}, nil)
			return err
		}},
		{"AggregateExact", dir, func(ctx context.Context, d *reader.Dataset) error {
			_, err := d.SeekParquet().Aggregate(ctx, []seekparquet.Selection{{}, {Span: seekparquet.KeySpan([]byte("t-000/d-00/f-0000"))}}, reductions,
				seekparquet.AggregateOptions{ValueBudgetBytes: -1, CheckKeys: true}, nil)
			if err != nil {
				return err
			}
			_, err = d.SeekParquet().Get(ctx, []byte("t-000/d-00/f-0000"), nil, "size", "last_modified")
			return err
		}},
	}
	for _, workers := range []int{1, 4} {
		cases = append(cases, scanCase{fmt.Sprintf("ScanParallel/%d", workers), dir, func(ctx context.Context, d *reader.Dataset) error {
			plan, err := d.SeekParquet().Plan(ctx, []seekparquet.Selection{{}}, []string{"size", "last_modified", "etag", "storage_class"}, nil)
			if err != nil {
				return err
			}
			return plan.ScanParallel(ctx, workers, nil, func(*seekparquet.Batch) error { return nil })
		}})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &cacheOpener{dir: tc.dir}
			newOpener := func() objfile.Opener {
				cache, err := objfile.NewByteCache(8 << 20)
				mustDo(t, err)
				return objfile.Pin(objfile.Cache(src, "fixture", cache), "fixture")
			}
			o := newOpener()
			obs := &cacheObserver{}
			ctx := objfile.WithObserver(t.Context(), obs)
			open := func() *reader.Dataset {
				t.Helper()
				d, err := reader.Open(ctx, spsource.New(o), seekparquet.OpenOptions{})
				mustDo(t, err)
				t.Cleanup(func() { _ = d.Close() })
				src.reads.Store(0) // exclude the manifest read
				obs.hits.Store(0)
				return d
			}
			point := func() (reads int, hits int64) {
				t.Helper()
				d := open()
				key := d.Metadata().Manifest.Parts[0].MinKey
				rows, err := d.SeekParquet().Get(ctx, key, nil, "size", "last_modified")
				mustDo(t, err)
				if len(rows) != 1 {
					t.Fatalf("point read: %d rows, want 1", len(rows))
				}
				reads, hits = int(src.reads.Swap(0)), obs.hits.Load()
				mustDo(t, d.Close())
				return reads, hits
			}
			// A cold point read can hit bytes its own tail read admitted.
			// Measure it independently before testing bulk cache admission.
			coldReads, coldHits := point()
			if coldReads == 0 {
				t.Fatal("cold point read did not reach storage")
			}
			o = newOpener()

			bulk := func() {
				t.Helper()
				d := open()
				mustDo(t, tc.bulk(objfile.WithCacheMode(ctx, objfile.CacheBypass), d))
				reads := src.reads.Swap(0)
				if reads == 0 || obs.hits.Load() != 0 {
					t.Fatalf("bulk scan: %d storage reads, %d cache-hit bytes; want reads and no hits", reads, obs.hits.Load())
				}
				mustDo(t, d.Close())
			}
			bulk()
			if reads, hits := point(); reads != coldReads || hits != coldHits {
				t.Fatalf("bulk scan filled cache: point read made %d storage reads with %d cache-hit bytes, want cold %d/%d", reads, hits, coldReads, coldHits)
			}
			if reads, hits := point(); reads != 0 || hits == 0 {
				t.Fatalf("warm point read: %d storage reads, %d cache-hit bytes", reads, hits)
			}
			bulk()
		})
	}
}

// TestBrowseCacheRevisit keeps browse counts and sums on the cacheable
// path. A new Dataset must reuse the first page's bytes without relying
// on SeekParquet metadata retained by the previous Dataset.
func TestBrowseCacheRevisit(t *testing.T) {
	src := &cacheOpener{dir: cacheFixture(t)}
	cache, err := objfile.NewByteCache(1 << 20)
	mustDo(t, err)
	o := objfile.Pin(objfile.Cache(src, "fixture", cache), "fixture")
	obs := &cacheObserver{}
	ctx := objfile.WithObserver(t.Context(), obs)
	for _, warm := range []bool{false, true} {
		d, err := reader.Open(ctx, spsource.New(o), seekparquet.OpenOptions{})
		mustDo(t, err)
		t.Cleanup(func() { _ = d.Close() })
		src.reads.Store(0)
		obs.hits.Store(0)
		page, err := d.SeekParquet().Page(ctx, seekparquet.PageQuery{ListQuery: seekparquet.ListQuery{Prefix: []byte("t-000/"), Delimiter: '/', Limit: 30,
			Columns: []string{"size", "last_modified", "etag", "storage_class"}},
			Reductions: []seekparquet.Reduction{{Op: seekparquet.CountRows}, {Op: seekparquet.Sum, Column: "size"}},
			Aggregates: seekparquet.AggregateOptions{ValueBudgetBytes: -1}}, nil)
		mustDo(t, err)
		if len(page.Entries) != 20 {
			t.Fatalf("browse returned %d prefixes, want 20", len(page.Entries))
		}
		for _, e := range page.Entries {
			if e.CommonPrefix == nil || len(e.Aggregates) != 2 {
				t.Fatalf("expected a common prefix with count and sum reductions, got %+v", e)
			}
			for i, result := range e.Aggregates {
				if result.Status != seekparquet.AggregateExact {
					t.Fatalf("prefix %q reduction %d: status %v, want AggregateExact", e.CommonPrefix, i, result.Status)
				}
			}
		}
		reads := src.reads.Swap(0)
		if warm && (reads != 0 || obs.hits.Load() == 0) {
			t.Fatalf("warm browse: %d storage reads, %d cache-hit bytes", reads, obs.hits.Load())
		}
		if !warm && reads == 0 {
			t.Fatal("cold browse did not reach storage")
		}
		mustDo(t, d.Close())
	}
}
