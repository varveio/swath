// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
)

// The tests here and in aggregates_test.go port varve's internal/swath
// totals and aggregates tests; each names the one it ports.

// writeParts writes each of parts, in key order, as one part of a new
// dataset, in pages of pageRows rows and row groups of groupRows.
func writeParts(t *testing.T, s spec, pageRows int, groupRows int64, parts ...[]s3Row) string {
	t.Helper()
	dir := writeGeometry(t, s, parts[0], pageRows, groupRows)
	for _, p := range parts[1:] {
		appendPart(t, dir, writeGeometry(t, s, p, pageRows, groupRows))
	}
	return dir
}

// cut splits rows at the given row indexes.
func cut(rows []s3Row, at ...int) [][]s3Row {
	var parts [][]s3Row
	for i := 1; i < len(at); i++ {
		parts = append(parts, rows[at[i-1]:at[i]])
	}
	return parts
}

// keyRows returns one object per key, of size its index and a
// modification time, none for each key in undated.
func keyRows(keys []string, undated ...string) []s3Row {
	rows := make([]s3Row, len(keys))
	for i, k := range keys {
		rows[i] = s3Row{Key: k, Size: ptr(int64(i)), LastModified: ptr(1_700_000_000_000_000 + int64(i)*1_000_000), ETag: ptr(fmt.Sprintf("e%d", i)), StorageClass: ptr("STANDARD")}
		if slices.Contains(undated, k) {
			rows[i].LastModified = nil
		}
	}
	return rows
}

// randomKeys returns sorted distinct path-like keys, nested up to three
// levels under a few names, with a, a/, a/b and ab/x among them.
func randomKeys(r *rand.Rand, n int) []string {
	names := []string{"a", "ab", "b", "c", "d", "x"}
	keys := []string{"a", "a/", "a/b", "ab/x"}
	for range n {
		var k string
		for depth := range 1 + r.IntN(3) {
			if depth > 0 {
				k += "/"
			}
			k += names[r.IntN(len(names))]
		}
		if r.IntN(5) == 0 {
			k += "/"
		} else {
			k += fmt.Sprintf("/%03d", r.IntN(1000))
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// streamTotals computes Totals of path from a full Stream of the dataset,
// selecting objects as Totals documents it.
func streamTotals(objects []Object, path string) PathTotals {
	var out PathTotals
	undated := false
	for _, o := range objects {
		key := string(o.Key)
		if path != "" && key != path && !(strings.HasSuffix(path, "/") && strings.HasPrefix(key, path)) {
			continue
		}
		out.Count++
		out.Bytes += *o.Size
		if path != "" && key == path {
			exact := o
			out.Exact = &exact
		}
		switch {
		case o.LastModified == nil:
			undated = true
		case out.NewestModified == nil || o.LastModified.After(*out.NewestModified):
			out.NewestModified = ptr(*o.LastModified)
		}
	}
	if undated {
		out.NewestModified = nil
	}
	return out
}

// checkTotals requires one Totals call over paths to equal the Stream
// oracle.
func checkTotals(t *testing.T, d *Dataset, objects []Object, paths []string) {
	t.Helper()
	got, err := d.Totals(context.Background(), paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(paths) {
		t.Fatalf("%d totals for %d paths", len(got), len(paths))
	}
	for i, path := range paths {
		if want := streamTotals(objects, path); !reflect.DeepEqual(got[i], want) {
			t.Fatalf("path %d %q: %s, want %s", i, path, describeTotals(got[i]), describeTotals(want))
		}
	}
}

func describeTotals(p PathTotals) string {
	newest, exact := "nil", "nil"
	if p.NewestModified != nil {
		newest = p.NewestModified.Format(time.RFC3339Nano)
	}
	if p.Exact != nil {
		exact = fmt.Sprintf("%q size %d", p.Exact.Key, *p.Exact.Size)
	}
	return fmt.Sprintf("{count %d, bytes %d, newest %s, exact %s}", p.Count, p.Bytes, newest, exact)
}

// pathsOf returns root, every key, every "/"-ending prefix of a key, and
// absent prefixes and keys, with repeats in both orders.
func pathsOf(keys []string) []string {
	paths := []string{"", "a", "a/", "a/b", "ab/", "ab/x", "zzz/", "a/b/zzz", "a/", ""}
	seen := make(map[string]bool)
	for _, k := range keys {
		paths = append(paths, k)
		for i := range len(k) {
			if p := k[:i+1]; k[i] == '/' && !seen[p] {
				seen[p] = true
				paths = append(paths, p)
			}
		}
	}
	repeats := slices.Clone(paths[:10])
	slices.Reverse(repeats)
	return append(paths, repeats...)
}

// TestTotalsMatchStream (varve TestTotalsMatchStream) is the oracle test:
// Totals for root, nested, overlapping, exact and absent paths over parts
// and row groups cut inside prefixes, with small pages and two objects
// without a modification time, equals totals computed from a full Stream.
func TestTotalsMatchStream(t *testing.T) {
	keys := randomKeys(rand.New(rand.NewPCG(1, 2)), 420)
	rows := keyRows(keys, keys[7], keys[200])
	r := rand.New(rand.NewPCG(3, 4))
	for i := range rows {
		if rows[i].LastModified != nil {
			rows[i].LastModified = ptr(1_700_000_000_000_000 + r.Int64N(1_000_000))
		}
	}
	d := openDir(t, writeParts(t, s3Spec(), 7, 40, cut(rows, 0, 97, 210, 211, 330, len(rows))...))
	objects, err := stream(t, d)
	if err != nil {
		t.Fatal(err)
	}
	paths := pathsOf(keys)
	checkTotals(t, d, objects, paths)
	reversed := slices.Clone(paths)
	slices.Reverse(reversed)
	checkTotals(t, d, objects, slices.Concat(paths, reversed))
	checkTotals(t, d, objects, []string{"a/b/", "a/", "a"})
}

// TestTotalsSelection (varve TestTotalsSelection) pins the selection of a,
// a/, a/b and ab/x, and missing modification times inside and outside a
// path's rows.
func TestTotalsSelection(t *testing.T) {
	rows := keyRows([]string{"a", "a/", "a/b", "ab/x", "b/1", "b/2"}, "a/b", "b/1", "b/2")
	d := openDir(t, writeParts(t, s3Spec(), 1, 2, rows[:3], rows[3:]))
	got, err := d.Totals(context.Background(), []string{"a", "a/", "a/b", "ab/", "ab", "b/", "a/b/", "", "c/"})
	if err != nil {
		t.Fatal(err)
	}
	counts := []int64{1, 2, 1, 1, 0, 2, 0, 6, 0}
	exact := []string{"a", "a/", "a/b", "", "", "", "", "", ""}
	newest := []bool{true, false, false, true, false, false, false, false, false}
	for i, p := range got {
		key := ""
		if p.Exact != nil {
			key = string(p.Exact.Key)
		}
		if p.Count != counts[i] || key != exact[i] || (p.NewestModified != nil) != newest[i] {
			t.Fatalf("path %d: %s, want count %d, exact %q, newest %t", i, describeTotals(p), counts[i], exact[i], newest[i])
		}
	}
	want := Object{Position: Position{Key: []byte("a/")}, Size: ptr(int64(1)), LastModified: ptr(time.UnixMicro(1_700_000_001_000_000).UTC()),
		ETag: ptr("e1"), StorageClass: ptr("STANDARD")}
	if got[1].Bytes != 1+2 || !reflect.DeepEqual(*got[1].Exact, want) {
		t.Fatalf("a/: %s, exact %+v; want bytes 3 and %+v", describeTotals(got[1]), *got[1].Exact, want)
	}
}

// TestTotalsOfEmptyBucket (varve TestTotalsOfEmptyBucket).
func TestTotalsOfEmptyBucket(t *testing.T) {
	d := openDir(t, writeDataset[s3Row](t, s3Spec(), nil))
	got, err := d.Totals(context.Background(), []string{"", "a/", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, make([]PathTotals, 3)) {
		t.Fatalf("empty bucket totals %+v, want zeros", got)
	}
}

// TestTotalsSizeOverflowFails (varve TestTotalsSizeOverflowFails): one
// object of the largest size totals; two overflow, and Totals fails.
func TestTotalsSizeOverflowFails(t *testing.T) {
	rows := keyRows([]string{"a/0000", "a/0001"})
	rows[0].Size = ptr(int64(math.MaxInt64))
	d := openDir(t, writeDataset(t, s3Spec(), rows))
	if got, err := d.Totals(context.Background(), []string{"a/0000"}); err != nil || got[0].Bytes != math.MaxInt64 {
		t.Fatalf("one huge object: %+v, %v", got, err)
	}
	got, err := d.Totals(context.Background(), []string{"a/"})
	if se := semanticAt(t, err, KindRow, "size"); se.Row != nil || got != nil {
		t.Fatalf("overflowing totals %+v, %v", got, err)
	}
}

// TestTotalsIgnoreManifestBoundExcludingKey (varve
// TestTotalsIgnoreManifestBoundExcludingKey) lowers part 0's manifest
// max_key below its last key. varve found the totals anyway; the module
// trusts and checks the manifest's bounds, so Totals returns its refusal
// unchanged, never a total missing the excluded keys.
func TestTotalsIgnoreManifestBoundExcludingKey(t *testing.T) {
	var keys []string
	for _, p := range []string{"a/", "b/"} {
		for i := range 10 {
			keys = append(keys, fmt.Sprintf("%s%04d", p, i))
		}
	}
	rows := keyRows(keys)
	dir := writeParts(t, s3Spec(), 100, 0, rows[:10], rows[10:])
	editManifest(t, dir, func(m *seekparquet.Manifest) { m.Parts[0].MaxKey = []byte("a/0005") })
	got, err := openDir(t, dir).Totals(context.Background(), []string{"a/0009", "a/", "a/0008"})
	var refusal *seekparquet.RefusalError
	if !errors.As(err, &refusal) || refusal.Code != seekparquet.CodeBoundsMismatch || got != nil {
		t.Fatalf("totals %+v, %v; want the module's bounds refusal", got, err)
	}
}

// editManifest rewrites dir's manifest after edit.
func editManifest(t *testing.T, dir string, edit func(*seekparquet.Manifest)) {
	t.Helper()
	name := filepath.Join(dir, seekparquet.ManifestName)
	raw, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	m, err := seekparquet.ParseManifest(raw, seekparquet.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	edit(&m)
	if raw, err = seekparquet.MarshalManifest(m); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// countingSource serves a directory and records the parts it opens and
// every read; after failAt reads it fails each with failErr, and at read
// cancelAt it calls cancel.
type countingSource struct {
	root *os.Root

	mu       sync.Mutex
	opens    []string
	reads    []sourceRead
	failAt   int
	failErr  error
	cancelAt int
	cancel   context.CancelFunc
}

type sourceRead struct {
	path     string
	off, len int64
}

type countingFile struct {
	*os.File
	size int64
	path string
	src  *countingSource
}

func newCountingSource(t *testing.T, dir string) *countingSource {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return &countingSource{root: root}
}

func (s *countingSource) Open(_ context.Context, path string) (seekparquet.SourceFile, error) {
	f, err := s.root.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	s.mu.Lock()
	s.opens = append(s.opens, path)
	s.mu.Unlock()
	return countingFile{File: f, size: st.Size(), path: path, src: s}, nil
}

func (f countingFile) Size() int64 { return f.size }

func (f countingFile) ReadAt(p []byte, off int64) (int, error) {
	s := f.src
	s.mu.Lock()
	s.reads = append(s.reads, sourceRead{f.path, off, int64(len(p))})
	n := len(s.reads)
	fail := s.failAt > 0 && n >= s.failAt
	if n == s.cancelAt && s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	if fail {
		return 0, s.failErr
	}
	return f.File.ReadAt(p, off)
}

// reset forgets the opens and reads so far.
func (s *countingSource) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opens, s.reads = nil, nil
}

func (s *countingSource) taken() ([]string, []sourceRead) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.opens), slices.Clone(s.reads)
}

// openSource opens the dataset in dir through a countingSource.
func openSource(t *testing.T, dir string) (*Dataset, *countingSource) {
	t.Helper()
	src := newCountingSource(t, dir)
	d, err := Open(context.Background(), src, seekparquet.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, src
}

// columnBytes returns how many reads, and of how many bytes, start in a
// column chunk of the named columns in the dataset at dir.
func columnBytes(t *testing.T, dir string, reads []sourceRead, columns ...string) (n int, total int64) {
	t.Helper()
	extents := make(map[string][][2]int64)
	paths, err := filepath.Glob(filepath.Join(dir, "data", "*.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		st, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		pf, err := parquet.OpenFile(f, st.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
		if err != nil {
			t.Fatal(err)
		}
		name := "data/" + filepath.Base(path)
		for _, rg := range pf.Metadata().RowGroups {
			for _, c := range rg.Columns {
				if !slices.Contains(columns, c.MetaData.PathInSchema[0]) {
					continue
				}
				start := c.MetaData.DataPageOffset
				if dict := c.MetaData.DictionaryPageOffset; dict > 0 && dict < start {
					start = dict
				}
				extents[name] = append(extents[name], [2]int64{start, start + c.MetaData.TotalCompressedSize})
			}
		}
		_ = f.Close()
	}
	for _, r := range reads {
		for _, e := range extents[r.path] {
			if r.off >= e[0] && r.off < e[1] {
				n++
				total += r.len
			}
		}
	}
	return n, total
}

// TestTotalsReturnSourceFailures (varve TestTotalsReturnSourceFailures)
// fails the reads after Open: the failure is returned as the Source
// reported it, not as a refusal, even when it is an end-of-input error.
func TestTotalsReturnSourceFailures(t *testing.T) {
	rows := keyRows(randomKeys(rand.New(rand.NewPCG(1, 2)), 100))
	dir := writeParts(t, s3Spec(), 7, 40, cut(rows, 0, len(rows)/2, len(rows))...)
	interrupted := fmt.Errorf("remote body: %w", io.ErrUnexpectedEOF)
	for _, failure := range []error{errors.New("failing read"), io.ErrUnexpectedEOF, interrupted} {
		t.Run(failure.Error(), func(t *testing.T) {
			d, src := openSource(t, dir)
			src.mu.Lock()
			src.failAt, src.failErr = len(src.reads)+1, failure
			src.mu.Unlock()
			got, err := d.Totals(context.Background(), []string{"a/"})
			var refusal *seekparquet.RefusalError
			if !errors.Is(err, failure) || errors.As(err, &refusal) || got != nil {
				t.Fatalf("totals %+v, %v; want only the source's failure", got, err)
			}
		})
	}
}

// TestTotalsHonorCancellationOnWarmDirectory (varve
// TestTotalsHonorCancellationOnWarmDirectory) cancels Totals after an
// earlier call checked every part.
func TestTotalsHonorCancellationOnWarmDirectory(t *testing.T) {
	d := openDir(t, writeDataset(t, s3Spec(), s3Rows(50)))
	if _, err := d.Totals(context.Background(), []string{""}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, paths := range [][]string{nil, {"zzzz"}, slices.Repeat([]string{"zzzz"}, 1000)} {
		if got, err := d.Totals(ctx, paths); !errors.Is(err, context.Canceled) || got != nil {
			t.Fatalf("cancelled totals of %d paths: %+v, %v", len(paths), got, err)
		}
	}
}

// TestTotalsAfterCloseFails (varve TestTotalsAfterCloseFails).
func TestTotalsAfterCloseFails(t *testing.T) {
	d, err := OpenDir(context.Background(), writeDataset(t, s3Spec(), s3Rows(5)), seekparquet.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Totals(context.Background(), []string{""}); !errors.Is(err, seekparquet.ErrClosed) || got != nil {
		t.Fatalf("totals after close: %+v, %v", got, err)
	}
}

// TestTotalsReadNestedRowsOnce (varve TestTotalsReadNestedRowsOnce) totals
// root, every child and grandchild of r/, and repeats of each in one call:
// each matches the Stream oracle, and the call reads exactly the size and
// last_modified bytes that root alone reads.
func TestTotalsReadNestedRowsOnce(t *testing.T) {
	var keys []string
	for c := range 4 {
		for g := range 4 {
			for o := range 300 {
				keys = append(keys, fmt.Sprintf("r/c%d/g%d/%04d", c, g, o))
			}
		}
	}
	paths := []string{"", "r/"}
	for c := range 4 {
		paths = append(paths, fmt.Sprintf("r/c%d/", c))
		for g := range 4 {
			paths = append(paths, fmt.Sprintf("r/c%d/g%d/", c, g))
		}
	}
	paths = append(paths, "r/c1/g2/", "r/", "", "r/c3/", "r/c1/g2/")
	dir := writeParts(t, s3Spec(), 64, 1000, cut(keyRows(keys), 0, 1300, 3100, len(keys))...)
	d, src := openSource(t, dir)
	objects, err := stream(t, d)
	if err != nil {
		t.Fatal(err)
	}
	checkTotals(t, d, objects, paths)
	read := func(paths []string) [2]int64 {
		src.reset()
		if _, err := d.Totals(context.Background(), paths); err != nil {
			t.Fatal(err)
		}
		_, reads := src.taken()
		n, total := columnBytes(t, dir, reads, "size", "last_modified")
		return [2]int64{int64(n), total}
	}
	root, nested := read([]string{""}), read(paths)
	if root[1] == 0 || nested != root {
		t.Fatalf("nested paths read %v (reads, bytes) of size and last_modified pages, root alone %v", nested, root)
	}
}

// TestTotalsAndAggregatesRefuseVersions: neither reads a versions schema,
// whose rows are not current objects.
func TestTotalsAndAggregatesRefuseVersions(t *testing.T) {
	s := s3Spec()
	s.id, s.key.Unique = S3VersionsV1, false
	d, src := openSource(t, writeDataset(t, s, s3Rows(3)))
	src.reset()
	if got, err := d.Totals(context.Background(), []string{""}); !errors.Is(err, ErrSchema) || got != nil {
		t.Fatalf("totals of versions: %+v, %v", got, err)
	}
	if got, err := d.Aggregates(context.Background(), [][]byte{nil}, AggregateOptions{Counts: true}); !errors.Is(err, ErrSchema) || got != nil {
		t.Fatalf("aggregates of versions: %+v, %v", got, err)
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("%d reads before refusing", len(reads))
	}
}

// TestPrefixSpan: a prefix's keys end before its successor, the prefix
// without its trailing 0xff bytes with its last byte raised; one of 0xff
// bytes alone, or none, is unbounded.
func TestPrefixSpan(t *testing.T) {
	for _, tc := range []struct{ prefix, hi string }{
		{"a/", "a0"},
		{"a\xff", "b"},
		{"a\xff\xff", "b"},
		{"�", "\xef\xbf\xbe"},
	} {
		if s := prefixSpan([]byte(tc.prefix)); string(s.lo) != tc.prefix || string(s.hi) != tc.hi || s.hi == nil {
			t.Errorf("prefix %q: [%q, %q), want end %q", tc.prefix, s.lo, s.hi, tc.hi)
		}
	}
	for _, prefix := range []string{"", "\xff", "\xff\xff"} {
		if s := prefixSpan([]byte(prefix)); s.hi != nil {
			t.Errorf("prefix %q: end %q, want none", prefix, s.hi)
		}
	}
}
