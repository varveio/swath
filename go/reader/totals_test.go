// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
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
func writeParts(t testing.TB, s spec, pageRows int, groupRows int64, parts ...[]s3Row) string {
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
	out := PathTotals{Bytes: ptr(int64(0))}
	undated, unsized := false, false
	for _, o := range objects {
		key := string(o.Key)
		if path != "" && key != path && !(strings.HasSuffix(path, "/") && strings.HasPrefix(key, path)) {
			continue
		}
		out.Count++
		if o.Size == nil {
			unsized = true
		} else {
			*out.Bytes += *o.Size
		}
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
	if unsized {
		out.Bytes = nil
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
		exact = fmt.Sprintf("%q", p.Exact.Key)
		if p.Exact.Size != nil {
			exact += fmt.Sprintf(" size %d", *p.Exact.Size)
		}
	}
	bytes := "nil"
	if p.Bytes != nil {
		bytes = fmt.Sprint(*p.Bytes)
	}
	return fmt.Sprintf("{count %d, bytes %s, newest %s, exact %s}", p.Count, bytes, newest, exact)
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
	if *got[1].Bytes != 1+2 || !reflect.DeepEqual(*got[1].Exact, want) {
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
	zero := PathTotals{Bytes: ptr(int64(0))}
	if !reflect.DeepEqual(got, []PathTotals{zero, zero, zero}) {
		t.Fatalf("empty bucket totals %+v, want zeros", got)
	}
}

// TestTotalsSizeOverflowFails (varve TestTotalsSizeOverflowFails): one
// object of the largest size totals; two overflow, and Totals fails.
func TestTotalsSizeOverflowFails(t *testing.T) {
	rows := keyRows([]string{"a/0000", "a/0001"})
	rows[0].Size = ptr(int64(math.MaxInt64))
	d := openDir(t, writeDataset(t, s3Spec(), rows))
	if got, err := d.Totals(context.Background(), []string{"a/0000"}); err != nil || *got[0].Bytes != math.MaxInt64 {
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

// countingSource serves a directory and records the files it opens, how
// many are open, and every read; after failAt reads it fails each with failErr, and at read
// cancelAt it calls cancel.
type countingSource struct {
	root *os.Root

	mu       sync.Mutex
	live     int // files open
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
	s.live++
	s.mu.Unlock()
	return countingFile{File: f, size: st.Size(), path: path, src: s}, nil
}

func (f countingFile) Size() int64 { return f.size }

func (f countingFile) Close() error {
	f.src.mu.Lock()
	f.src.live--
	f.src.mu.Unlock()
	return f.File.Close()
}

// open returns the number of files opened and not closed.
func (s *countingSource) open() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live
}

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
	d, err := Open(context.Background(), src, seekparquet.OpenOptions{})
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
	d, err := OpenDir(context.Background(), writeDataset(t, s3Spec(), s3Rows(5)), seekparquet.OpenOptions{})
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

// azblobRow is a swath/azblob-objects/v1 row as swath writes it.
type azblobRow struct {
	Key          string  `parquet:"key"`
	Size         *int64  `parquet:"size,optional"`
	LastModified *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	ETag         *string `parquet:"etag,optional"`
	ContentMD5   *string `parquet:"content_md5,optional"`
	ContentType  *string `parquet:"content_type,optional"`
	BlobType     *string `parquet:"blob_type,optional"`
	AccessTier   *string `parquet:"access_tier,optional"`
	CreationTime *int64  `parquet:"creation_time,optional,timestamp(microsecond:utc)"`
}

// TestAzblobTotals: an Azure blob's access tier is its storage class, in
// an exact object's totals as in Stream, and prefix totals and aggregates
// read the blobs' sizes; a blob without a tier keeps it nil.
func TestAzblobTotals(t *testing.T) {
	var rows []azblobRow
	for i, k := range listKeys {
		r := azblobRow{Key: k, Size: ptr(int64(10 * i)), LastModified: ptr(int64(1_700_000_000_000_000 + i)), ETag: ptr(fmt.Sprintf(`"0x%d"`, i)),
			BlobType: ptr("BlockBlob"), AccessTier: ptr([]string{"Hot", "Cool", "Archive"}[i%3])}
		if k == "c/3/" {
			r.AccessTier = nil
		}
		rows = append(rows, r)
	}
	ext := swathExtension()
	ext["source"] = map[string]any{"kind": "azblob", "uri": "az://acct/b", "container": "b"}
	s := spec{id: AzblobObjectsV1, key: s3Spec().key, ext: ext}
	dir := writeGeometry(t, s, rows[:5], 2, 3)
	appendPart(t, dir, writeGeometry(t, s, rows[5:], 2, 3))
	objects, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	checkTotals(t, d, objects, append(pathsOf(listKeys), "", "c/3/"))
	got, err := d.Totals(context.Background(), []string{"a/", "c/3/"})
	if err != nil {
		t.Fatal(err)
	}
	if e := got[0].Exact; *e.StorageClass != "Cool" || *e.ETag != `"0x1"` || got[1].Exact.StorageClass != nil {
		t.Fatalf("exact blobs %+v, %+v", *got[0].Exact, *got[1].Exact)
	}
	aggs, err := d.Aggregates(context.Background(), [][]byte{nil, []byte("a/"), []byte("c/")}, AggregateOptions{Counts: true, Sums: SumsAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range []string{"", "a/", "c/"} {
		if count, size := prefixTotals(objects, []byte(p)); *aggs[i].Count != count || *aggs[i].Bytes != size {
			t.Fatalf("%q: %s, want %d and %d", p, describeAggregate(aggs[i]), count, size)
		}
	}
}

// serialTotals is Totals as it was at 6c507e01, before it read in shards,
// verbatim but for its name and its serial sum (serialSumSpans): the
// oracle of totals, its errors included, and the benchmarks' baseline.
func serialTotals(ctx context.Context, d *Dataset, paths []string) ([]PathTotals, error) {
	if err := d.usable(ctx); err != nil {
		return nil, err
	}
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return nil, notCurrent(d.spec.ID)
	}
	unique, of := distinct(paths)
	spans := make([]keySpan, len(unique))
	exact := make(map[string]int)
	for i, path := range unique {
		switch {
		case path == "":
			spans[i] = prefixSpan(nil)
		case strings.HasSuffix(path, "/"):
			spans[i] = prefixSpan([]byte(path))
			exact[path] = i
		default:
			spans[i] = keySpanOf([]byte(path))
			exact[path] = i
		}
	}
	sums, err := serialSumSpans(ctx, d, spans, []string{"size", "last_modified"}, nil)
	if err != nil {
		return nil, err
	}
	totals := make([]PathTotals, len(unique))
	for i, s := range sums.sums {
		n, err := d.count(ctx, spans[i], nil)
		if err != nil {
			return nil, err
		}
		if n == nil {
			return nil, fmt.Errorf("swath reader: the count of %q relies on a part no read has checked", unique[i])
		}
		if s.overflow {
			return nil, semantic(KindRow, "size", nil, "the sizes %q selects overflow an int64 total", unique[i])
		}
		totals[i] = PathTotals{Count: *n, Bytes: s.total(), NewestModified: s.newestTime()}
	}
	for path, o := range sums.exact {
		if i, ok := exact[path]; ok {
			totals[i].Exact = &o
		}
	}
	if err := d.fillExact(ctx, totals, columns[2:]); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]PathTotals, len(paths))
	for i, u := range of {
		out[i] = totals[u]
		if e := out[i].Exact; e != nil {
			c := *e
			c.Key = bytes.Clone(e.Key)
			out[i].Exact = &c
		}
	}
	return out, nil
}

// serialSumSpans and serialObjectSum are sumSpans and objectSum as they
// were at 6c507e01, verbatim but for their names.

// serialSpanSums is what serialSumSpans found: each span's sum, and the objects whose
// key equals a span's lower bound, by key.
type serialSpanSums struct {
	sums  []serialObjectSum
	exact map[string]Object
}

// eachRow is a batch callback handing fn an owning copy of every row of
// each batch (Batch.Row), as the module's row-callback Plan.Scan did.
func eachRow(fn func(seekparquet.Row) error) func(*seekparquet.Batch) error {
	return func(b *seekparquet.Batch) error {
		for i := range b.Len() {
			if err := fn(b.Row(i)); err != nil {
				return err
			}
		}
		return nil
	}
}

// serialSumSpans plans the rows of spans projecting columns ("size", and
// "last_modified" when it is to be summed too) and scans them once, adding
// each object to every span holding its key. It records each object whose
// key is a span's lower bound, with what columns gives of it.
func serialSumSpans(ctx context.Context, d *Dataset, spans []keySpan, columns []string, st *ReadStats) (serialSpanSums, error) {
	out := serialSpanSums{sums: make([]serialObjectSum, len(spans)), exact: make(map[string]Object)}
	if len(spans) == 0 {
		return out, nil
	}
	var planned ReadStats
	plan, err := d.sp.Plan(ctx, selections(spans), columns, &planned)
	addStats(st, planned)
	if err != nil {
		return serialSpanSums{}, err
	}
	order := make([]int, len(spans))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return bytes.Compare(spans[a].lo, spans[b].lo) })
	lower := make(map[string]bool, len(spans))
	for _, s := range spans {
		lower[string(s.lo)] = true
	}
	timed := slices.Contains(columns, "last_modified")
	var active []int
	next := 0
	var scanned ReadStats
	defer func() { addStats(st, scanned) }()
	err = plan.Scan(ctx, &scanned, eachRow(func(r seekparquet.Row) error {
		dec := decoder{spec: d.spec, row: &r}
		o := Object{Position: dec.position(), Size: dec.int64("size")}
		if timed {
			o.LastModified = dec.time("last_modified")
		}
		if dec.err != nil {
			return dec.err
		}
		if err := checkObject(&o, d.spec.ID == LocalV1); err != nil {
			return err
		}
		for ; next < len(order) && bytes.Compare(spans[order[next]].lo, r.Key) <= 0; next++ {
			active = append(active, order[next])
		}
		active = slices.DeleteFunc(active, func(i int) bool {
			return spans[i].hi != nil && bytes.Compare(r.Key, spans[i].hi) >= 0
		})
		for _, i := range active {
			out.sums[i].add(&o)
		}
		if lower[string(r.Key)] {
			out.exact[string(r.Key)] = o
		}
		return nil
	}))
	if err != nil {
		return serialSpanSums{}, err
	}
	return out, nil
}

// serialObjectSum is the sizes and newest modification time of some objects.
type serialObjectSum struct {
	bytes    int64
	overflow bool // the sizes overflow an int64
	unsized  bool // a filesystem entry has no size
	newest   time.Time
	dated    bool // newest is set
	undated  bool // an object has no modification time
}

func (s *serialObjectSum) add(o *Object) {
	if o.Size == nil {
		s.unsized = true
	} else {
		s.addSize(*o.Size)
	}
	switch {
	case o.LastModified == nil:
		s.undated = true
	case !s.dated || o.LastModified.After(s.newest):
		s.newest, s.dated = *o.LastModified, true
	}
}

func (s *serialObjectSum) addSize(size int64) {
	if s.overflow || size > math.MaxInt64-s.bytes {
		s.overflow = true
		return
	}
	s.bytes += size
}

// total is the sum of the sizes, nil when an object had none or they
// overflow an int64.
func (s *serialObjectSum) total() *int64 {
	if s.unsized || s.overflow {
		return nil
	}
	return ptrTo(s.bytes)
}

// newestTime is the newest modification time, nil when there was no
// object or one had no time.
func (s *serialObjectSum) newestTime() *time.Time {
	if !s.dated || s.undated {
		return nil
	}
	t := s.newest
	return &t
}

// totalsWorkers are the worker counts the sharded totals are checked at.
var totalsWorkers = []int{0, 1, 2, 3, 4, 7, 16}

// checkTotalsSerial requires totals over paths at every worker count to
// equal serialTotals: the same totals, or the same error.
func checkTotalsSerial(t *testing.T, d *Dataset, paths []string) {
	t.Helper()
	want, wantErr := serialTotals(context.Background(), d, paths)
	for _, workers := range totalsWorkers {
		got, err := d.totals(context.Background(), paths, workers)
		if !reflect.DeepEqual(err, wantErr) {
			t.Fatalf("workers=%d: error %v, want %v", workers, err, wantErr)
		}
		if len(got) != len(want) {
			t.Fatalf("workers=%d: %d totals, want %d", workers, len(got), len(want))
		}
		for i := range want {
			if !reflect.DeepEqual(got[i], want[i]) {
				t.Fatalf("workers=%d: path %d %q: %s, want %s", workers, i, paths[i], describeTotals(got[i]), describeTotals(want[i]))
			}
		}
	}
}

// deepKeys returns sorted distinct keys nested up to seven levels, with
// folder keys ("x/") among them.
func deepKeys(r *rand.Rand, n int) []string {
	names := []string{"p", "q", "q.", "r", "r0"}
	var keys []string
	for range n {
		var k string
		for range 1 + r.IntN(7) {
			k += names[r.IntN(len(names))] + "/"
		}
		if r.IntN(6) != 0 {
			k += fmt.Sprintf("%02d", r.IntN(50))
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// TestTotalsMatchSerial: sharded totals at every worker count equal the
// serial oracle over S3, Cloud Storage and Azure datasets, filesystem
// entries with and without a stat, an empty dataset and nested keys cut
// into many row groups and parts, for root, nested, overlapping, exact,
// repeated and absent paths and no path at all.
func TestTotalsMatchSerial(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 6))
	nested := randomKeys(r, 420)
	nestedRows := keyRows(nested, nested[7], nested[200])
	deep := deepKeys(r, 2500)
	deepRows := keyRows(deep, deep[3], deep[1500])
	var deepPaths []string
	for i, p := range pathsOf(deep) {
		if i < 20 || i%7 == 0 {
			deepPaths = append(deepPaths, p)
		}
	}
	batchPaths := []string{"", "k00000", "k00599", "k00600", "k01199", "k00042", "k00042/", "k/", "zzz/", "k00042", ""}
	cases := []struct {
		name  string
		dir   func(*testing.T) string
		paths []string
	}{
		{"nested", func(t *testing.T) string {
			return writeParts(t, s3Spec(), 7, 40, cut(nestedRows, 0, 97, 210, 211, 330, len(nestedRows))...)
		}, pathsOf(nested)},
		{"deep", func(t *testing.T) string {
			return writeParts(t, s3Spec(), 16, 64, cut(deepRows, 0, 900, 901, 1700, len(deepRows))...)
		}, deepPaths},
		{"s3", func(t *testing.T) string { return batchFixture(t, S3ObjectsV1) }, batchPaths},
		{"gcs", func(t *testing.T) string { return batchFixture(t, GCSObjectsV1) }, batchPaths},
		{"azblob", func(t *testing.T) string { return batchFixture(t, AzblobObjectsV1) }, batchPaths},
		{"local", func(t *testing.T) string { return localDir(t, false) }, localPaths},
		{"local-names", func(t *testing.T) string { return localDir(t, true) }, localPaths},
		{"empty", func(t *testing.T) string { return writeDataset[s3Row](t, s3Spec(), nil) }, []string{"", "a/", "a"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := openDir(t, c.dir(t))
			checkTotalsSerial(t, d, c.paths)
			checkTotalsSerial(t, d, slices.Concat(c.paths[len(c.paths)/2:], c.paths))
			checkTotalsSerial(t, d, nil)
			checkTotalsSerial(t, d, []string{"zzzz/", "zzzz", "\xff\xff/"})
		})
	}
}

// localPaths are paths of localTree: root, directories, files, a missing
// path and a directory named without its slash.
var localPaths = []string{"", "d/", "d/e/", "z/", "z/y/f", "d/link", "a.txt", "missing/", "d", "d/\xff", "z/y/"}

// TestTotalsShardErrors: a row Totals refuses fails every worker count.
// One bad row gives the serial oracle's error at every count; with several
// in different shards, one worker gives the oracle's (the first in key
// order) and more give one of theirs. A part lacking last_modified fails
// with KindSchema as the oracle does.
func TestTotalsShardErrors(t *testing.T) {
	keys := make([]string, 2000)
	for i := range keys {
		keys[i] = fmt.Sprintf("a/%02d/%04d", i/100, i)
	}
	bad := map[int]func(*s3Row){
		150:  func(r *s3Row) { r.Size = ptr(int64(-1)) },
		900:  func(r *s3Row) { r.LastModified = ptr(endObjectMicros) },
		1700: func(r *s3Row) { r.Size = nil },
	}
	columns := map[string]string{keys[150]: "size", keys[900]: "last_modified", keys[1700]: "size"}
	paths := []string{"", "a/01/", "a/09/", "a/17/0001", "a/19/"}
	for at, breaks := range bad {
		rows := keyRows(keys)
		breaks(&rows[at])
		d := openDir(t, writeParts(t, s3Spec(), 10, 50, cut(rows, 0, 1000, len(rows))...))
		checkTotalsSerial(t, d, paths)
		if _, err := d.Totals(context.Background(), paths); err == nil {
			t.Fatalf("row %d: no error", at)
		}
	}
	rows := keyRows(keys)
	for at, breaks := range bad {
		breaks(&rows[at])
	}
	d := openDir(t, writeParts(t, s3Spec(), 10, 50, cut(rows, 0, 1000, len(rows))...))
	_, want := serialTotals(context.Background(), d, paths)
	for _, workers := range totalsWorkers {
		got, err := d.totals(context.Background(), paths, workers)
		var se *SemanticError
		if !errors.As(err, &se) || se.Kind != KindRow || se.Row == nil || columns[string(se.Row.Key)] != se.Column || got != nil {
			t.Fatalf("workers=%d: %+v, %v; want the error of a bad row", workers, got, err)
		}
		if workers <= 1 && !reflect.DeepEqual(err, want) {
			t.Fatalf("workers=%d: %v, want the first bad row's %v", workers, err, want)
		}
	}

	type untimed struct {
		Key          string  `parquet:"key"`
		Size         *int64  `parquet:"size,optional"`
		ETag         *string `parquet:"etag,optional"`
		StorageClass *string `parquet:"storage_class,optional"`
	}
	dir := writeParts(t, s3Spec(), 10, 50, keyRows(keys)[:1000])
	lacking := make([]untimed, 1000)
	for i := range lacking {
		lacking[i] = untimed{Key: keys[1000+i], Size: ptr(int64(i))}
	}
	appendPart(t, dir, writeGeometry(t, s3Spec(), lacking, 10, 50))
	d = openDir(t, dir)
	checkTotalsSerial(t, d, []string{"a/01/", "a/0"})
	_, want = serialTotals(context.Background(), d, paths)
	if se := semanticAt(t, want, KindSchema, "last_modified"); string(se.Row.Key) != keys[1000] {
		t.Fatalf("oracle: %v", se)
	}
	for _, workers := range totalsWorkers {
		got, err := d.totals(context.Background(), paths, workers)
		if se := semanticAt(t, err, KindSchema, "last_modified"); se.Row == nil || got != nil {
			t.Fatalf("workers=%d: %+v, %v", workers, got, err)
		}
	}
}

// TestTotalsCancelledDuringScan cancels the context at a read inside the
// scan, at every worker count: Totals fails with the context's error and
// returns nothing.
func TestTotalsCancelledDuringScan(t *testing.T) {
	keys := make([]string, 3000)
	for i := range keys {
		keys[i] = fmt.Sprintf("a/%02d/%04d", i/100, i)
	}
	dir := writeParts(t, s3Spec(), 10, 50, cut(keyRows(keys), 0, 1000, 2000, len(keys))...)
	for _, workers := range totalsWorkers {
		d, src := openSource(t, dir)
		if err := d.CheckSchema(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		src.mu.Lock()
		src.cancelAt, src.cancel = len(src.reads)+5, cancel
		src.mu.Unlock()
		got, err := d.totals(ctx, []string{"", "a/07/", "a/20/0001"}, workers)
		cancel()
		if !errors.Is(err, context.Canceled) || got != nil {
			t.Fatalf("workers=%d: %+v, %v; want the context's error", workers, got, err)
		}
	}
}
