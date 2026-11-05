// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import (
	"context"
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

func testRows(n int, seed int64) []schema.Row {
	rng := rand.New(rand.NewSource(seed))
	alpha := []string{"a", "b", "z", "~", "/", ".", "-", "é", "ÿ", "日", "A", "0"}
	seen := map[string]bool{}
	rows := make([]schema.Row, 0, n)
	for len(rows) < n {
		k := ""
		for range 1 + rng.Intn(14) {
			k += alpha[rng.Intn(len(alpha))]
		}
		if seen[k] {
			continue
		}
		seen[k] = true
		r := schema.Row{Key: k, Kind: schema.KindFile, Size: int64(len(rows)), Mtime: 1_700_000_000_000_000 + int64(len(rows)), Present: schema.HasSize | schema.HasMtime}
		if len(rows)%7 == 0 {
			r.Kind, r.LinkTarget, r.Present = schema.KindSymlink, "t-"+k, r.Present|schema.HasLink
		}
		if len(rows)%11 == 0 {
			r.Present |= schema.HasStat | schema.HasBtime
			r.UID, r.GID, r.Mode, r.Nlink, r.Inode, r.Dev, r.Blocks = 1<<31+5, 7, 0o4755, 3, -1, 42, 4096
			r.Ctime, r.Atime, r.Btime = -5, 6, 7
		}
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b schema.Row) int { return strings.Compare(a.Key, b.Key) })
	return rows
}

func writeFile(t *testing.T, path string, rows []schema.Row, columns int) dataset.Part {
	t.Helper()
	w, err := create(path, &schema.Local, 0, columns, func() int { return columns })
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := w.write(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	p, err := w.close(true)
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

func route(t *testing.T, rows []schema.Row, encoders int, partSize int64) (string, []dataset.Part) {
	t.Helper()
	out := t.TempDir()
	if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	r, err := NewRouter(Options{Out: out, Schema: &schema.Local, PartSize: partSize, Encoders: encoders})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(rows); i += 1500 {
		segs := [][]schema.Row{rows[i:min(i+700, len(rows))], rows[min(i+700, len(rows)):min(i+1500, len(rows))]}
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
			kv := footer(t, parts[i].Path)
			if kv[dataset.StampFileIndex] != strconv.Itoa(i+1) {
				t.Errorf("part %d file_index %q", i, kv[dataset.StampFileIndex])
			}
			if _, final := kv[dataset.StampFileFinal]; final != (i == len(parts)-1) {
				t.Errorf("part %d file_final=%v", i, final)
			}
		}
		if got := readKeys(t, parts); !slices.Equal(got, keysOf(rows)) {
			t.Fatalf("enc=%d: keys differ after round trip", enc)
		}
	}
}

func TestRouterRejectsDescendingKeys(t *testing.T) {
	out := t.TempDir()
	os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
	r, _ := NewRouter(Options{Out: out, Schema: &schema.Local, PartSize: 1 << 20, Encoders: 2})
	_ = r.Write(context.Background(), [][]schema.Row{{{Key: "b"}, {Key: "a"}}})
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

// TestFooterMatchesMessage ties the three views of the schema together: the
// column list, the Parquet footer and the manifest message string.
func TestFooterMatchesMessage(t *testing.T) {
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
	var specs []schema.Spec
	for _, e := range elems {
		typ, _ := e.Type.Get()
		rep, _ := e.RepetitionType.Get()
		spec := schema.Spec{Name: e.Name, Required: rep.String() == "REQUIRED"}
		switch {
		case typ.String() == "BYTE_ARRAY" && e.LogicalType.Value != nil && e.LogicalType.String() == "STRING":
			spec.Type = schema.String
		case typ.String() == "INT32" && e.LogicalType.Value == nil:
			spec.Type = schema.Int32
		case typ.String() == "INT64" && e.LogicalType.Value == nil:
			spec.Type = schema.Int64
		case typ.String() == "INT64" && e.LogicalType.String() == "TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)":
			spec.Type = schema.TimestampMicros
		default:
			t.Fatalf("%s: unexpected footer type %s %v", e.Name, typ, e.LogicalType.Value)
		}
		specs = append(specs, spec)
	}
	if got, want := schema.Message(pf.Schema().Name(), specs), schema.Local.Message(); got != want {
		t.Fatalf("footer schema renders\n%s\nwant\n%s", got, want)
	}
	if !strings.Contains(schema.Local.Message(), "required binary key (STRING);\n  optional int64 size;\n  optional int64 last_modified (TIMESTAMP(MICROS,true));") {
		t.Fatal("key/size/last_modified no longer match Swath's canonical columns")
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

func keysOf(rows []schema.Row) []string {
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].Key
	}
	return out
}
