// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

type wideRow struct {
	Key  string `parquet:"key"`
	Size int64  `parquet:"size"`
}

// writeWide writes a SeekParquet dataset of dirs directories of perDir keys
// each under p####/, some with a nested directory, and a few root keys,
// in pages of 50 rows: every directory spans several key pages.
func writeWide(t *testing.T, dirs, perDir int) string {
	t.Helper()
	var rows []wideRow
	for d := range dirs {
		for i := range perDir {
			key := fmt.Sprintf("p%04d/obj%05d", d, i)
			if d%3 == 0 && i%4 == 0 {
				key = fmt.Sprintf("p%04d/sub/obj%05d", d, i)
			}
			rows = append(rows, wideRow{Key: key, Size: int64(i)})
		}
	}
	for i := range 20 {
		rows = append(rows, wideRow{Key: fmt.Sprintf("root%03d", i), Size: int64(i)})
	}
	slices.SortFunc(rows, func(a, b wideRow) int { return strings.Compare(a.Key, b.Key) })
	half := len(rows) / 2
	return writeSeek(t, "swath-replay/objects/v1", 50, partOf(rows[:half]), partOf(rows[half:]))
}

// rollupWalk pages a "/" rollup scoped to prefix to its end, limit entries
// a page, resuming after each page's last entry as the S3 route does, and
// returns every page's entries and stats.
func rollupWalk(t *testing.T, s *Sorted, prefix string, limit int) ([][]legacy.Entry, []RollupStats) {
	t.Helper()
	var stats []RollupStats
	s.SetMeters(Meters{Rollup: func(st RollupStats) { stats = append(stats, st) }})
	defer s.SetMeters(Meters{})
	var pages [][]legacy.Entry
	var from []byte
	for {
		entries, ok, err := s.RollupRows(context.Background(), from, false, nil, []byte(prefix), []byte("/"), limit, nil, true)
		if err != nil || !ok {
			t.Fatalf("rollup %q from %q: ok %v, %v", prefix, from, ok, err)
		}
		if len(entries) > limit {
			entries = entries[:limit]
		}
		pages = append(pages, entries)
		if len(entries) < limit {
			return pages, stats
		}
		last := entries[len(entries)-1]
		if last.Row != nil {
			from = last.Row.Key
		} else {
			from = last.CommonPrefix
		}
	}
}

// TestListIndexRollupsMatchAndSkipKeyPages: over a SeekParquet dataset
// whose directories each span several key pages, the list index built at
// open answers every rollup page with the same entries as the store
// without it, and decodes O(1) key pages a page rather than one or more
// per common prefix emitted.
func TestListIndexRollupsMatchAndSkipKeyPages(t *testing.T) {
	dir := writeWide(t, 300, 400)
	open := func(li ListIndex) *Sorted {
		st, meta, err := Open(context.Background(), dir, Options{Readers: 1, ListIndex: li})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		if want := ListIndexOff; li.Off && meta.ListIndex.State != want {
			t.Fatalf("list index %+v, want %s", meta.ListIndex, want)
		}
		return st.(*Sorted)
	}
	on, off := open(ListIndex{}), open(ListIndex{Off: true})
	b := on.ListIndex()
	if b.State != ListIndexBuilt || b.Err != nil || b.Stats.GroupsIndexed == 0 || b.Stats.Entries == 0 || b.Stats.Pages == 0 ||
		!b.Stats.HasIndex || b.Stats.OptionsMismatch || b.MaxBytes != DefaultListIndexMaxBytes || b.Workers != DefaultListIndexWorkers() {
		t.Fatalf("list index build %+v", b)
	}
	for _, tc := range []struct {
		prefix string
		limit  int
	}{{"", 100}, {"", 1000}, {"p0003/", 30}, {"p01", 7}} {
		gotOn, statsOn := rollupWalk(t, on, tc.prefix, tc.limit)
		gotOff, statsOff := rollupWalk(t, off, tc.prefix, tc.limit)
		if !reflect.DeepEqual(gotOn, gotOff) {
			t.Fatalf("prefix %q limit %d: entries differ with the index on and off", tc.prefix, tc.limit)
		}
		var skips, declines int64
		for _, st := range statsOn {
			skips += st.ListIndex.Skips
			declines += st.ListIndex.MissingGroup + st.ListIndex.DensePage + st.ListIndex.UnsafeRun
		}
		for _, st := range statsOff {
			if st.ListIndex != (ListIndexReads{}) {
				t.Fatalf("index counts without an index: %+v", st.ListIndex)
			}
		}
		if tc.prefix == "" {
			// A root page of limit common prefixes, each over several
			// key pages: off decodes at least one page a prefix, on a
			// bounded handful.
			if skips == 0 {
				t.Fatalf("limit %d: no skips through the index (declines %d)", tc.limit, declines)
			}
			for i := range statsOn {
				if n := statsOn[i].PagesDecoded; n > 6 {
					t.Errorf("limit %d page %d: %d key pages decoded with the index", tc.limit, i, n)
				}
				if n := statsOff[i].PagesDecoded; i < len(statsOff)-1 && n < int64(tc.limit) {
					t.Errorf("limit %d page %d: %d key pages decoded without the index, want at least %d", tc.limit, i, n, tc.limit)
				}
			}
			t.Logf("limit %d: key pages decoded on %v, off %v", tc.limit, pagesOf(statsOn), pagesOf(statsOff))
		}
	}
}

func pagesOf(stats []RollupStats) []int64 {
	out := make([]int64, len(stats))
	for i := range stats {
		out[i] = stats[i].PagesDecoded
	}
	return out
}

// TestListIndexBuildFailureServes: a build that fails (here a negative
// budget the module refuses) is recorded, and the store serves without the
// index; a Java-era fixture has none.
func TestListIndexBuildFailureServes(t *testing.T) {
	dir := writeWide(t, 20, 120)
	st, meta, err := Open(context.Background(), dir, Options{Readers: 1, ListIndex: ListIndex{MaxBytes: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if meta.ListIndex.State != ListIndexFailed || meta.ListIndex.Err == nil {
		t.Fatalf("list index %+v", meta.ListIndex)
	}
	pages, stats := rollupWalk(t, st.(*Sorted), "", 10)
	if n := len(slices.Concat(pages...)); n != 40 || stats[0].ListIndex != (ListIndexReads{}) {
		t.Fatalf("%d entries, stats %+v", n, stats[0])
	}

	legacyDir := writeJavaEra(t, t.TempDir(), fixtureKeys(500), 1, 100)
	for _, li := range []ListIndex{{}, {Off: true}} {
		st, meta, err := Open(context.Background(), legacyDir, Options{Readers: 1, ListIndex: li})
		if err != nil {
			t.Fatal(err)
		}
		want := ListIndexUnsupported
		if li.Off {
			want = ListIndexOff
		}
		if meta.ListIndex.State != want || st.(*Sorted).ListIndex().State != want {
			t.Fatalf("java-era list index %+v, want %s", meta.ListIndex, want)
		}
		_ = st.Close()
	}
}

// faultSource opens a dataset's files from dir. Once armed, its next read
// fails with errInjected (fail) or cancels the build's context and then
// reads (cancel).
type faultSource struct {
	dir          string
	fail, cancel atomic.Bool
	stop         context.CancelFunc
}

var errInjected = errors.New("injected storage failure")

type faultFile struct {
	*os.File
	size int64
	src  *faultSource
}

func (s *faultSource) Open(_ context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := os.Open(filepath.Join(s.dir, filepath.FromSlash(path)))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &faultFile{File: f, size: info.Size(), src: s}, nil
}

func (f *faultFile) Size() int64 { return f.size }

func (f *faultFile) ReadAt(p []byte, off int64) (int, error) {
	if f.src.fail.Swap(false) {
		return 0, errInjected
	}
	if f.src.cancel.Swap(false) {
		f.src.stop()
	}
	return f.File.ReadAt(p, off)
}

// TestListIndexBuildStorageFailureAndCancellation: a storage failure during
// the build, after Verify, is recorded as a failed build with no retained
// index, and the dataset's reads still succeed without it; a cancellation
// during a build read is returned, so open fails rather than serving.
func TestListIndexBuildStorageFailureAndCancellation(t *testing.T) {
	dir := writeWide(t, 20, 120)
	for _, cancelled := range []bool{false, true} {
		ctx, stop := context.WithCancel(context.Background())
		src := &faultSource{dir: dir, stop: stop}
		d, err := seekparquet.OpenDataset(ctx, src, seekparquet.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Verify(ctx); err != nil {
			t.Fatal(err)
		}
		if cancelled {
			src.cancel.Store(true)
		} else {
			src.fail.Store(true)
		}
		b, err := (&seekReader{d: d}).buildListIndex(ctx, ListIndex{Workers: 1})
		switch {
		case cancelled:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("cancelled build: %+v, %v", b, err)
			}
		case err != nil || b.State != ListIndexFailed || !errors.Is(b.Err, errInjected) || b.Stats.HasIndex || b.Stats.Entries != 0 || b.Stats.Bytes != 0:
			t.Errorf("failed build: %+v, %v", b, err)
		default:
			var st seekparquet.ReadStats
			got, err := d.List(context.Background(), seekparquet.ListQuery{Delimiter: '/', Limit: 1000}, &st)
			if err != nil || len(got) != 40 || st.ListIndexSkips != 0 {
				t.Errorf("read after a failed build: %d entries, %+v, %v", len(got), st, err)
			}
		}
		stop()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
