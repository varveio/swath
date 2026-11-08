// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
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
	w, err := create(nil, path, &testSchema, 0, nil, columns, func() int { return columns })
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := w.write(&rows[i]); err != nil {
			t.Fatal(err)
		}
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
		if p := writeFile(t, filepath.Join(dir, strconv.Itoa(n)), rows, n); p.MD5 != serial.MD5 {
			t.Fatalf("columns=%d: %s != %s", n, p.MD5, serial.MD5)
		}
	}
}

func route(t *testing.T, rows []row, encoders int, partSize int64) (string, []dataset.Part) {
	t.Helper()
	return routeAdmit(t, rows, encoders, 0, -1, partSize)
}

// routeAdmit is route with MaxEncoders max, Admit called after the
// admitAt-th Write (never when negative).
func routeAdmit(t *testing.T, rows []row, encoders, maxEnc, admitAt int, partSize int64) (string, []dataset.Part) {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, PartSize: partSize, Encoders: encoders, MaxEncoders: maxEnc}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += 1500 {
		if i/1500 == admitAt {
			if n := r.Admit(); n != max(encoders, maxEnc) {
				t.Fatalf("admitted %d, want %d", n, max(encoders, maxEnc))
			}
		}
		segs := [][]row{rows[i:min(i+700, len(rows))], rows[min(i+700, len(rows)):min(i+1500, len(rows))]}
		if err := r.Write(context.Background(), segs); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	return out, parts
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
			if parts[i].MD5 != base[i].MD5 || parts[i].Rows != base[i].Rows {
				t.Fatalf("enc=%d part %d differs", enc, i)
			}
			// The footer carries the profile, the schema and the index
			// bound; nothing about the part's place in the dataset.
			kv := footer(t, parts[i].Path)
			want := map[string]string{dataset.StampProfile: dataset.Format, dataset.StampProfileVersion: strconv.Itoa(dataset.FormatVersion),
				dataset.StampSchema: testSchema.ID, dataset.StampIndexBound: "1024"}
			for k, v := range want {
				if kv[k] != v {
					t.Errorf("part %d: %s = %q, want %q", i, k, kv[k], v)
				}
			}
			for k := range kv {
				if _, ok := want[k]; !ok && strings.HasPrefix(k, "swath.") {
					t.Errorf("part %d: unexpected stamp %s", i, k)
				}
			}
		}
		if got := readKeys(t, parts); !slices.Equal(got, keysOf(rows)) {
			t.Fatalf("enc=%d: keys differ after round trip", enc)
		}
	}
}

// TestRouterAdmitIsByteIdentical: encoders admitted mid-run (as once
// listing has ended) change neither the parts' boundaries nor their bytes,
// and the router honours its raised count.
func TestRouterAdmitIsByteIdentical(t *testing.T) {
	rows := testRows(60_000, 2)
	_, base := route(t, rows, 1, 16<<10)
	_, parts := routeAdmit(t, rows, 1, 4, 10, 16<<10)
	if len(parts) != len(base) {
		t.Fatalf("%d parts vs %d", len(parts), len(base))
	}
	for i := range parts {
		if parts[i].MD5 != base[i].MD5 || parts[i].Rows != base[i].Rows {
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
	f, _ := os.Open(path)
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := pq.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
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

func footer(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := pq.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
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
		f, err := os.Open(p.Path)
		if err != nil {
			t.Fatal(err)
		}
		fi, _ := f.Stat()
		pf, err := pq.OpenFile(f, fi.Size())
		if err != nil {
			t.Fatal(err)
		}
		r := pq.NewReader(pf)
		buf := make([]pq.Row, 512)
		for {
			n, err := r.ReadRows(buf)
			for _, row := range buf[:n] {
				keys = append(keys, string(row[0].ByteArray()))
			}
			if err != nil || n == 0 {
				break
			}
		}
		f.Close()
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
// the bound truncation, sorting_columns and readable truncated bounds.
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
	rows := []row{{Key: "a" + long}, {Key: "b" + long}}
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
	if kv[dataset.StampIndexBound] != "1024" {
		t.Fatalf("footer: %v", kv)
	}
	rg := pf.Metadata().RowGroups[0]
	if len(rg.SortingColumns) != 1 || rg.SortingColumns[0].ColumnIdx != 0 || rg.SortingColumns[0].Descending {
		t.Fatalf("sorting_columns: %+v", rg.SortingColumns)
	}
	ci, err := pf.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := ci.MinValue(0).ByteArray(), ci.MaxValue(0).ByteArray()
	if len(lo) > 1024 || len(hi) > 1024 || string(lo) > rows[0].Key || string(hi) < rows[1].Key[:len(hi)] {
		t.Fatalf("bounds %d/%d bytes not conservative", len(lo), len(hi))
	}
}

// TestCodecStatesFollowAdmission: the router's shared codec makes no more
// zstd states than encoders admitted. Before Admit, with MaxEncoders above
// Encoders, its writers use at most Encoders states; after, up to
// MaxEncoders. A pool of MaxEncoders handed round in turn warmed every
// state during listing, past what admission had charged (sol review 3,
// P2: 148.5 MiB live against 122.2 MiB priced at MaxEncoders 6).
func TestCodecStatesFollowAdmission(t *testing.T) {
	rows := testRows(60_000, 2)
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, PartSize: 16 << 10, Encoders: 2, MaxEncoders: 6}, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	half := len(rows) / 2
	for i := 0; i < half; i += 1500 {
		if err := r.Write(context.Background(), [][]row{rows[i:min(i+1500, half)]}); err != nil {
			t.Fatal(err)
		}
	}
	before := r.codec.states()
	r.Admit()
	for i := half; i < len(rows); i += 1500 {
		if err := r.Write(context.Background(), [][]row{rows[i:min(i+1500, len(rows))]}); err != nil {
			t.Fatal(err)
		}
	}
	parts, err := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	after := r.codec.states()
	if before > 2 || after > 6 || after <= before {
		t.Fatalf("%d states before admission (2 admitted), %d after (6)", before, after)
	}
	if _, base := route(t, rows, 1, 16<<10); !slices.EqualFunc(parts, base, func(a, b dataset.Part) bool { return a.MD5 == b.MD5 }) {
		t.Fatal("parts differ from one encoder's")
	}
}
