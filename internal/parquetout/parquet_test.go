// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	pq "github.com/parquet-go/parquet-go"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/buildinfo"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/schema"
)

func testRows(n int, seed int64) []row {
	rng := rand.New(rand.NewSource(seed))
	alpha := []string{"a", "b", "z", "~", "/", ".", "-", "é", "ÿ", "日", "A", "0"}
	seen := map[string]bool{}
	rows := make([]row, 0, n)
	for len(rows) < n {
		k := ""
		for range 1 + rng.Intn(14) {
			k += alpha[rng.Intn(len(alpha))]
		}
		if seen[k] {
			continue
		}
		seen[k] = true
		r := row{Key: k, Data: tr{Kind: "FILE", Size: int64(len(rows)), Mtime: 1_700_000_000_000_000 + int64(len(rows)), ETag: strconv.Itoa(len(rows) * 7919)}}
		if len(rows)%7 == 0 {
			r.Data.Kind, r.Data.Link, r.Data.Has = "SYMLINK", "t-"+k, 2
		}
		if len(rows)%11 == 0 {
			r.Data.Has |= 1
			r.Data.Mode, r.Data.Inode, r.Data.Small, r.Data.Flag = 1<<31+5, -1, -7, true
		}
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b row) int { return strings.Compare(a.Key, b.Key) })
	return rows
}

func writeFile(t *testing.T, path string, rows []row, columns int) dataset.Part {
	t.Helper()
	c := newCodec(columns)
	defer c.Close()
	f, err := openLocal(nil, path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := create(f, &testSchema, c, func() int { return columns }, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.write(rows); err != nil {
		t.Fatal(err)
	}
	p, err := w.close()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestColumnParallelEncodingIsByteIdentical(t *testing.T) {
	rows := testRows(150_000, 1)
	dir := t.TempDir()
	serial := writeFile(t, filepath.Join(dir, "1"), rows, 1)
	for _, n := range []int{2, 4, 15} {
		if p := writeFile(t, filepath.Join(dir, strconv.Itoa(n)), rows, n); p.SHA256 != serial.SHA256 {
			t.Fatalf("columns=%d: %s != %s", n, p.SHA256, serial.SHA256)
		}
	}
}

func route(t *testing.T, rows []row, encoders int, partSize int64) (string, []dataset.Part) {
	t.Helper()
	out, parts, _, _ := routeAdmit(t, rows, encoders, 0, -1, partSize)
	return out, parts
}

// routeAdmit is route with MaxEncoders max, Admit called after the
// admitAt-th Write (never when negative), with the zstd states the shared
// codec made before Admit and by Close.
func routeAdmit(t *testing.T, rows []row, encoders, maxEnc, admitAt int, partSize int64) (out string, parts []dataset.Part, before, after int) {
	t.Helper()
	out = t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, PartSize: partSize, Encoders: encoders, MaxEncoders: maxEnc}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += 1500 {
		if i/1500 == admitAt {
			before = r.codec.states()
			if n := r.Admit(); n != max(encoders, maxEnc) {
				t.Fatalf("admitted %d, want %d", n, max(encoders, maxEnc))
			}
		}
		segs := [][]row{rows[i:min(i+700, len(rows))], rows[min(i+700, len(rows)):min(i+1500, len(rows))]}
		if err := r.Write(context.Background(), segs); err != nil {
			t.Fatal(err)
		}
	}
	parts, err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	return out, parts, before, r.codec.states()
}

func TestRouterIsDeterministicAcrossEncoders(t *testing.T) {
	rows := testRows(60_000, 2)
	_, base := route(t, rows, 1, 16<<10)
	if len(base) < 4 {
		t.Fatalf("want >= 4 parts, got %d", len(base))
	}
	for _, enc := range []int{2, 4} {
		_, parts := route(t, rows, enc, 16<<10)
		if len(parts) != len(base) {
			t.Fatalf("enc=%d: %d parts vs %d", enc, len(parts), len(base))
		}
		for i := range parts {
			if parts[i].SHA256 != base[i].SHA256 || parts[i].Rows != base[i].Rows {
				t.Fatalf("enc=%d part %d differs", enc, i)
			}
			// The footer carries the SeekParquet stamps and the value
			// index bound; nothing about the part's place in the dataset.
			kv := footer(t, parts[i].Path)
			want := map[string]string{seekparquet.StampFormatVersion: "0", seekparquet.StampDraftRevision: "1",
				seekparquet.StampSchemaID: testSchema.ID, seekparquet.StampKeyColumn: "key", seekparquet.StampKeyUnique: "true"}
			if !maps.Equal(kv, want) {
				t.Errorf("part %d: footer %q, want %q", i, kv, want)
			}
		}
		if got := readKeys(t, parts); !slices.Equal(got, keysOf(rows)) {
			t.Fatalf("enc=%d: keys differ after round trip", enc)
		}
	}
}

// TestRouterAdmitIsByteIdentical: encoders admitted mid-run (as once
// listing has ended) change neither the parts' boundaries nor their bytes,
// and the router honours its raised count. Its shared codec makes no more
// zstd states than encoders admitted: before Admit, with MaxEncoders above
// Encoders, at most Encoders; after, at most MaxEncoders. Whether a run
// needs more than one state depends on how many compressions overlap (none
// at GOMAXPROCS 1): TestCodecLeasesFollowLimit forces the overlap. A pool
// of MaxEncoders handed round in turn warmed every state during listing,
// past what admission had charged (sol review 3, P2: 148.5 MiB live
// against 122.2 MiB priced at MaxEncoders 6).
func TestRouterAdmitIsByteIdentical(t *testing.T) {
	rows := testRows(60_000, 2)
	_, base := route(t, rows, 1, 16<<10)
	_, parts, before, after := routeAdmit(t, rows, 2, 6, 20, 16<<10)
	if before > 2 || after > 6 {
		t.Fatalf("%d states before admission (2 admitted), %d after (6)", before, after)
	}
	if len(parts) != len(base) {
		t.Fatalf("%d parts vs %d", len(parts), len(base))
	}
	for i := range parts {
		if parts[i].SHA256 != base[i].SHA256 || parts[i].Rows != base[i].Rows {
			t.Fatalf("part %d differs", i)
		}
	}
	r, err := NewRouter(Options{Out: t.TempDir(), PartSize: 1 << 20, Encoders: 2, MaxEncoders: 5}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	if r.Encoders() != 2 || r.Admit() != 5 || r.Admit() != 5 || r.Encoders() != 5 {
		t.Fatalf("encoders %d after admission", r.Encoders())
	}
	if _, err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRouterRejectsDescendingKeys(t *testing.T) {
	out := t.TempDir()
	os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
	r, _ := NewRouter(Options{Out: out, PartSize: 1 << 20, Encoders: 2}, &testSchema)
	_ = r.Write(context.Background(), [][]row{{{Key: "b"}, {Key: "a"}}})
	if _, err := r.Close(); err == nil {
		t.Fatal("expected an ordering error")
	}
}

// TestValuesAreCopied pins the parquet-go contract unsafeBytes relies on:
// byte-array values are copied before WriteRowValues returns.
func TestValuesAreCopied(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.parquet")
	f, _ := os.Create(path)
	w := pq.NewGenericWriter[any](f, pq.NewSchema("m", pq.Group{"k": pq.String()}))
	buf := []byte("original")
	if _, err := w.ColumnWriters()[0].WriteRowValues([]pq.Value{pq.ByteArrayValue(buf).Level(0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	copy(buf, "MUTATED!")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	rf, _ := os.Open(path)
	defer rf.Close()
	fi, _ := rf.Stat()
	pf, err := pq.OpenFile(rf, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]pq.Row, 1)
	n, _ := pq.NewReader(pf).ReadRows(rows)
	if n != 1 || string(rows[0][0].ByteArray()) != "original" {
		t.Fatalf("parquet-go no longer copies values: got %q", rows[0][0].ByteArray())
	}
}

// TestFooterMatchesSchema: the Parquet footer holds the schema's columns,
// in order, with standard logical types (STRING, INT(n, signed), TIMESTAMP
// micros UTC) and the declared repetition.
func TestFooterMatchesSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x")
	writeFile(t, path, testRows(100, 3), 1)
	pf, closeFile := openParquet(t, path)
	defer closeFile()
	elems := pf.Metadata().Schema[1:]
	specs := testSchema.Specs()
	if len(elems) != len(specs) {
		t.Fatalf("%d footer columns, schema has %d", len(elems), len(specs))
	}
	for i, e := range elems {
		c := specs[i]
		typ, _ := e.Type.Get()
		rep, _ := e.RepetitionType.Get()
		want := map[schema.Type]string{schema.String: "BYTE_ARRAY STRING", schema.Int32: "INT32 INT(32,true)",
			schema.Int64: "INT64 INT(64,true)", schema.TimestampMicros: "INT64 TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", schema.Bool: "BOOLEAN <nil>",
			schema.Uint32: "INT32 INT(32,false)", schema.Uint64: "INT64 INT(64,false)", schema.Binary: "BYTE_ARRAY <nil>"}[c.Type]
		logical := "<nil>"
		if e.LogicalType.Value != nil {
			logical = e.LogicalType.String()
		}
		if got := typ.String() + " " + logical; e.Name != c.Name || got != want || (rep.String() == "REQUIRED") != c.Required {
			t.Errorf("column %d: %s %s %s, want %s %s required=%v", i, e.Name, got, rep, c.Name, want, c.Required)
		}
	}
}

// openParquet opens path's Parquet footer; close closes the file (callers
// close each file when done with it: a test may read hundreds of parts).
func openParquet(t *testing.T, path string) (pf *pq.File, close func()) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err = pq.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	ok = true
	return pf, func() { f.Close() }
}

func footer(t *testing.T, path string) map[string]string {
	t.Helper()
	pf, closeFile := openParquet(t, path)
	defer closeFile()
	kv := map[string]string{}
	for _, e := range pf.Metadata().KeyValueMetadata {
		kv[e.Key] = e.Value
	}
	return kv
}

func readKeys(t *testing.T, parts []dataset.Part) []string {
	t.Helper()
	var keys []string
	for _, p := range parts {
		func() {
			pf, closeFile := openParquet(t, p.Path)
			defer closeFile() // also on a Fatal; the reader does not close the file
			r := pq.NewReader(pf)
			buf := make([]pq.Row, 512)
			for {
				n, err := r.ReadRows(buf)
				for _, row := range buf[:n] {
					keys = append(keys, string(row[0].ByteArray()))
				}
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("%s: %v", p.Path, err) // a decoding or I/O failure is not the end of the part
				}
				if n == 0 {
					t.Fatalf("%s: ReadRows returned no rows and no error", p.Path)
				}
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
		}()
	}
	return keys
}

func keysOf(rows []row) []string {
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].Key
	}
	return out
}

// TestKeyLimits: a schema with a 1,024-byte key cap (S3) refuses a key over 1,024 bytes with a typed
// error; an uncapped schema (filesystem) writes longer keys, and the footer records
// sorting_columns and exact bounds: the keys' and a 1,025-byte 0xFF link target's.
func TestKeyLimits(t *testing.T) {
	out := t.TempDir()
	os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
	long := strings.Repeat("k", 1500)
	r, _ := NewRouter(Options{Out: out, PartSize: 1 << 20, Encoders: 1}, &cappedSchema)
	_ = r.Write(context.Background(), [][]row{{{Key: long}}})
	if _, err := r.Close(); !errors.Is(err, schema.ErrKeyTooLong) {
		t.Fatalf("S3 schema: %v", err)
	}
	out = t.TempDir()
	os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
	r, _ = NewRouter(Options{Out: out, PartSize: 1 << 20, Encoders: 1}, &testSchema)
	// A link target of 1,025 0xFF bytes: parquet-go's 1,024-byte
	// shortening would leave a maximum of 1,024 0xFF bytes, below it.
	high := strings.Repeat("\xff", 1025)
	rows := []row{{Key: "a" + long, Data: tr{Link: high, Has: 2}}, {Key: "b" + long, Data: tr{Link: "t", Has: 2}}}
	if err := r.Write(context.Background(), [][]row{rows}); err != nil {
		t.Fatal(err)
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(parts[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	pf, err := pq.OpenFile(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	kv := map[string]string{}
	for _, m := range pf.Metadata().KeyValueMetadata {
		kv[m.Key] = m.Value
	}
	if _, ok := kv["swath.index_bound_bytes"]; ok {
		t.Fatalf("footer: %v (no column index bound is shortened)", kv)
	}
	link, _ := pf.Schema().Lookup("link_target")
	lci, err := pf.RowGroups()[0].ColumnChunks()[link.ColumnIndex].ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	if lo, hi := string(lci.MinValue(0).ByteArray()), string(lci.MaxValue(0).ByteArray()); lo != "t" || hi != high {
		t.Fatalf("link_target bounds %d/%d bytes, want the exact minimum and the 1,025-byte maximum", len(lo), len(hi))
	}
	rg := pf.Metadata().RowGroups[0]
	if len(rg.SortingColumns) != 1 || rg.SortingColumns[0].ColumnIdx != 0 || rg.SortingColumns[0].Descending {
		t.Fatalf("sorting_columns: %+v", rg.SortingColumns)
	}
	ci, err := pf.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	// The key's bounds are exact, never truncated (SeekParquet RG-5).
	lo, hi := ci.MinValue(0).ByteArray(), ci.MaxValue(0).ByteArray()
	if string(lo) != rows[0].Key || string(hi) != rows[1].Key {
		t.Fatalf("bounds %d/%d bytes, not the page's first and last keys", len(lo), len(hi))
	}
}

// TestLowerEncoderMemIsByteIdentical: the codec's states run with zstd's
// lower encoder memory (a history of one window plus a block instead of
// two windows: about 8 MiB less per state once a page passes one block),
// which changes allocation only: every page, of one block or many, random
// or repetitive, compresses to the same bytes as with the default.
func TestLowerEncoderMemIsByteIdentical(t *testing.T) {
	low, err := newState()
	if err != nil {
		t.Fatal(err)
	}
	def, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(pageLevel), zstd.WithEncoderCRC(false),
		zstd.WithZeroFrames(true), zstd.WithAllLitEntropyCompression(true))
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(3))
	for _, n := range []int{0, 1, 1000, 100 << 10, 128 << 10, 129 << 10, 1 << 20, 3<<20 + 7, 4 << 20, 8 << 20, 9 << 20} {
		for _, kind := range []string{"random", "letters", "repeats"} {
			src := make([]byte, n)
			for i := range src {
				switch kind {
				case "random":
					src[i] = byte(r.Uint32())
				case "letters":
					src[i] = byte('a' + r.Intn(26))
				default:
					src[i] = byte(i % 251 / 7)
				}
			}
			a, b := low.EncodeAll(src, nil), def.EncodeAll(src, nil)
			if !bytes.Equal(a, b) {
				t.Fatalf("%d bytes %s: %d compressed with lower memory, %d without, or they differ", n, kind, len(a), len(b))
			}
		}
	}
}

// TestCodecLeasesFollowLimit: leases held at once (the overlap forced,
// not left to scheduling) make states up to the limit and no further: a
// lease past it waits (observed for 50 ms) until one is returned, or until
// setLimit raises the limit, which then makes the states asked for. Before (sol review 4, P3), the admission test
// asserted growth from a run whose compressions need not overlap, and
// failed at GOMAXPROCS 1.
func TestCodecLeasesFollowLimit(t *testing.T) {
	c := newCodec(2)
	defer c.Close()
	var held []*zstd.Encoder
	lease := func() {
		e, err := c.take()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, e)
	}
	lease()
	lease()
	if c.states() != 2 {
		t.Fatalf("%d states for 2 leases", c.states())
	}
	got := make(chan *zstd.Encoder, 4)
	go func() {
		e, _ := c.take()
		got <- e
	}()
	select {
	case <-got:
		t.Fatal("a third lease past the limit of 2 did not wait")
	case <-time.After(50 * time.Millisecond):
	}
	c.setLimit(6) // admission: the waiting lease makes a state
	held = append(held, <-got)
	lease()
	lease()
	lease()
	if c.states() != 6 {
		t.Fatalf("%d states for 6 leases at a limit of 6", c.states())
	}
	go func() {
		e, _ := c.take()
		got <- e
	}()
	select {
	case <-got:
		t.Fatal("a seventh lease past the limit of 6 did not wait")
	case <-time.After(50 * time.Millisecond):
	}
	c.give(held[0]) // a returned state is reused, none made
	if e := <-got; e != held[0] || c.states() != 6 {
		t.Fatalf("the waiting lease got another state (%d made)", c.states())
	}
	for _, e := range held[1:] {
		c.give(e)
	}
	c.give(held[0])
}

// TestValueLimits: a column with a Spec.MaxBytes (a local path's link
// target, PATH_MAX) is held to it as the key is to its cap: a longer
// value stops the part with the typed schema.ErrValueTooLong, a value at
// the limit (any bytes) is taken. Before (sol review 4, P2) the limit
// only priced admission, and a longer value was written past the price.
func TestValueLimits(t *testing.T) {
	s := testSchema
	s.Columns = slices.Clone(testSchema.Columns)
	for i := range s.Columns {
		if s.Columns[i].Name == "link_target" {
			s.Columns[i].MaxBytes = 16
		}
	}
	write := func(link string) error {
		c := newCodec(1)
		defer c.Close()
		f, err := openLocal(nil, filepath.Join(t.TempDir(), "x.parquet"))
		if err != nil {
			t.Fatal(err)
		}
		w, err := create(f, &s, c, func() int { return 1 }, "")
		if err != nil {
			t.Fatal(err)
		}
		defer w.abort()
		return w.write([]row{{Key: "k", Data: tr{Kind: "LINK", Link: link, Has: 3}}})
	}
	if err := write(strings.Repeat("\xff", 16)); err != nil {
		t.Fatalf("a value at the limit: %v", err)
	}
	if err := write(strings.Repeat("a", 17)); !errors.Is(err, schema.ErrValueTooLong) {
		t.Fatalf("a value over the limit: %v", err)
	}
}

// TestFooterBuild: created_by carries the build's revision, a linker one
// included, shortened to 12 hex digits with its -dirty mark kept.
func TestFooterBuild(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"0123456789abcdef0123456789abcdef01234567", "0123456789ab"},
		{"0123456789abcdef0123456789abcdef01234567-dirty", "0123456789ab-dirty"},
		{"abc", "abc"},
		{"", ""},
	} {
		if app, v, rev := footerBuild(buildinfo.Build{Tool: buildinfo.Tool, Version: "v1", Revision: c.in}); app != buildinfo.Tool || v != "v1" || rev != c.want {
			t.Errorf("%q: %q %q %q, want revision %q", c.in, app, v, rev, c.want)
		}
	}
}
