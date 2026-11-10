// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	seekparquet "github.com/varveio/seekparquet/go"

	swathreader "github.com/varveio/swath/go/reader"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/store"
)

// keys returns n sorted keys spread over a few prefixes and depths, some
// non-ASCII, so rollups have something to roll up.
func keys(n int) []string {
	out := make([]string, 0, n)
	for i := range n {
		switch i % 4 {
		case 0:
			out = append(out, fmt.Sprintf("a/%03d/obj-%05d", i%13, i))
		case 1:
			out = append(out, fmt.Sprintf("b/deep/x/y/%05d.json", i))
		case 2:
			out = append(out, fmt.Sprintf("c/é/%05d", i))
		default:
			out = append(out, fmt.Sprintf("top-%05d", i))
		}
	}
	slices.Sort(out)
	return out
}

// javaLike rewrites a legacytest part's footer to the Java writer's form:
// the key column unannotated (parquet-go annotates a Go string STRING).
func javaLike(t *testing.T, path string) {
	t.Helper()
	if err := legacytest.RewriteFooter(path, func(m *format.FileMetaData) error {
		m.Schema[1].LogicalType = format.LogicalType{}
		m.Schema[1].ConvertedType = thrift.Null[deprecated.ConvertedType]{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// writeFixture writes rows as parts of the given sizes, stamped 1..n with
// the last final, each with several row groups and small pages.
func writeFixture(t *testing.T, dir string, rows []legacytest.Row, sizes ...int) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	next := 0
	for p, n := range sizes {
		path := filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", p))
		part := legacytest.Part{Rows: rows[next : next+n], Stamps: legacytest.Stamps(p+1, p == len(sizes)-1), GroupRows: 700, PageBytes: 2048}
		if err := legacytest.Write(path, part); err != nil {
			t.Fatal(err)
		}
		javaLike(t, path)
		next += n
	}
}

func objects(ks []string) []legacytest.Row {
	rows := make([]legacytest.Row, len(ks))
	for i, k := range ks {
		rows[i] = legacytest.Object(k, i)
	}
	return rows
}

func convert(t *testing.T, in string) (string, []Part, error) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	parts, err := Run(context.Background(), Options{Input: in, Output: out, Bucket: "bkt", Region: "us-east-2", Fixture: "test"}, nil)
	return out, parts, err
}

func footerOf(t *testing.T, path string) (raw []byte, start int64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	n := len(b)
	l := int(binary.LittleEndian.Uint32(b[n-8:]))
	return b[n-8-l : n-8], int64(n - 8 - l)
}

func TestConvertChangesOnlyTheFooter(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	ks := keys(5000)
	writeFixture(t, in, objects(ks), 2000, 1700, 1300)
	out, parts, err := convert(t, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("parts = %d", len(parts))
	}
	for i, p := range parts {
		src, err := os.ReadFile(filepath.Join(in, fmt.Sprintf("part-%05d.parquet", i)))
		if err != nil {
			t.Fatal(err)
		}
		dst, err := os.ReadFile(filepath.Join(out, p.Path))
		if err != nil {
			t.Fatal(err)
		}
		oldFooter, start := footerOf(t, filepath.Join(in, fmt.Sprintf("part-%05d.parquet", i)))
		newFooter, newStart := footerOf(t, filepath.Join(out, p.Path))
		if start != newStart || start != p.DataBytes || !bytes.Equal(src[:start], dst[:start]) {
			t.Fatalf("part %d: the bytes before the footer differ (%d, %d)", i, start, newStart)
		}
		// Old values all survive; what is new is only the additions.
		changed, added, err := footerDiff(oldFooter, newFooter)
		if err != nil {
			t.Fatal(err)
		}
		// parquet-go leaves an empty sorting_columns list, which is filled.
		for _, c := range changed {
			if c != ".5.len" && !(strings.HasPrefix(c, ".4[") && strings.HasSuffix(c, "].4.len")) {
				t.Fatalf("part %d: changed %v", i, changed)
			}
		}
		if len(added) == 0 {
			t.Fatalf("part %d: nothing added", i)
		}
		var meta format.FileMetaData
		if err := thrift.Unmarshal(new(thrift.CompactProtocol), newFooter, &meta); err != nil {
			t.Fatal(err)
		}
		if _, ok := meta.Schema[1].LogicalType.Value.(*format.StringType); !ok {
			t.Fatalf("part %d: key not annotated STRING", i)
		}
		for g, rg := range meta.RowGroups {
			if sc := rg.SortingColumns; len(sc) != 1 || sc[0] != (format.SortingColumn{ColumnIdx: 0}) {
				t.Fatalf("part %d row group %d: sorting columns %+v", i, g, rg.SortingColumns)
			}
		}
	}
	// The manifest's bounds are the exact first and last keys.
	if string(parts[0].MinKey) != ks[0] || string(parts[2].MaxKey) != ks[len(ks)-1] || string(parts[0].MaxKey) != ks[1999] || string(parts[1].MinKey) != ks[2000] {
		t.Fatalf("bounds %q %q %q %q", parts[0].MinKey, parts[0].MaxKey, parts[1].MinKey, parts[2].MaxKey)
	}
	for _, level := range []seekparquet.Level{seekparquet.LevelFull, seekparquet.LevelDigest} {
		report, err := seekparquet.ValidateDir(context.Background(), out, seekparquet.ValidateOptions{Level: level})
		if err != nil {
			t.Fatal(err)
		}
		if report.Result() != seekparquet.Complete {
			t.Fatalf("validate %s: %+v", level, report.Levels)
		}
	}
	// The swath reader identifies it and its columns match the schema id.
	d, err := swathreader.OpenDir(context.Background(), out, swathreader.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	sameStore(t, in, out, ks)
}

// sameStore opens both fixtures with the replay store and compares every
// row and the rollups of several prefixes.
func sameStore(t *testing.T, in, out string, ks []string) {
	t.Helper()
	ctx := context.Background()
	a, am, err := store.Open(ctx, in, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, bm, err := store.Open(ctx, out, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()
	if am.Kind != store.KindJavaEra || bm.Kind != store.KindSeekParquet || bm.SchemaID != SchemaID || am.Rows != bm.Rows {
		t.Fatalf("metadata %+v / %+v", am, bm)
	}
	proj := store.Projection{Owner: true}
	ra, err := a.Rows(ctx, nil, false, nil, len(ks)+1, proj)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := b.Rows(ctx, nil, false, nil, len(ks)+1, proj)
	if err != nil {
		t.Fatal(err)
	}
	if len(ra) != len(ks) || !reflect.DeepEqual(ra, rb) {
		t.Fatalf("rows differ: %d / %d", len(ra), len(rb))
	}
	for _, prefix := range []string{"", "a/", "a/007/", "b/deep/", "c/é/", "top-"} {
		for _, limit := range []int{1, 7, 1000} {
			end := successor(prefix)
			ea, oka, err := a.DelimitedRollup(ctx, []byte(prefix), true, end, []byte(prefix), []byte("/"), limit, proj, true)
			if err != nil {
				t.Fatal(err)
			}
			eb, okb, err := b.DelimitedRollup(ctx, []byte(prefix), true, end, []byte(prefix), []byte("/"), limit, proj, true)
			if err != nil {
				t.Fatal(err)
			}
			if oka != okb || !reflect.DeepEqual(ea, eb) {
				t.Fatalf("rollup %q limit %d differs: ok %v/%v, %d/%d entries%s", prefix, limit, oka, okb, len(ea), len(eb), firstDiff(ea, eb))
			}
		}
	}
}

func TestConvertRefuses(t *testing.T) {
	ks := keys(300)
	cases := []struct {
		name string
		rows func() []legacytest.Row
		want error
	}{
		{"common prefix row", func() []legacytest.Row {
			r := objects(ks)
			r[10].RowType = "COMMON_PREFIX"
			return r
		}, ErrNotObjects},
		{"delete marker", func() []legacytest.Row {
			r := objects(ks)
			r[20].IsDeleteMarker = true
			return r
		}, ErrNotObjects},
		{"version id", func() []legacytest.Row {
			r := objects(ks)
			v := "v1"
			r[30].VersionID = &v
			return r
		}, ErrNotObjects},
		{"invalid UTF-8 key", func() []legacytest.Row {
			k := append([]string(nil), ks...)
			k[40] = k[40][:len(k[40])-1] + "\xff"
			return objects(k)
		}, ErrKey},
		{"key over 1,024 bytes", func() []legacytest.Row {
			k := append([]string(nil), ks...)
			k[299] = "z" + strings.Repeat("x", MaxKeyBytes)
			return objects(k)
		}, ErrKey},
		{"unsorted", func() []legacytest.Row {
			k := append([]string(nil), ks...)
			k[50], k[51] = k[51], k[50]
			return objects(k)
		}, ErrOrder},
		{"duplicate key", func() []legacytest.Row {
			k := append([]string(nil), ks...)
			k[61] = k[60]
			return objects(k)
		}, ErrOrder},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := filepath.Join(t.TempDir(), "in")
			writeFixture(t, in, c.rows(), 150, 150)
			out, _, err := convert(t, in)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			if _, err := os.Stat(filepath.Join(out, seekparquet.ManifestName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a manifest was published: %v", err)
			}
		})
	}
}

func TestConvertRefusesUnsortedAcrossParts(t *testing.T) {
	ks := keys(300)
	rows := objects(ks)
	in := filepath.Join(t.TempDir(), "in")
	// Part 1 (file_index 1) holds the later keys.
	writeFixture(t, in, append(append([]legacytest.Row(nil), rows[150:]...), rows[:150]...), 150, 150)
	if _, _, err := convert(t, in); !errors.Is(err, ErrOrder) {
		t.Fatalf("err = %v", err)
	}
}

func TestConvertRefusesIncompleteOrUnstamped(t *testing.T) {
	ks := keys(300)
	in := filepath.Join(t.TempDir(), "in")
	writeFixture(t, in, objects(ks), 150, 150)
	if err := os.Remove(filepath.Join(in, "part-00001.parquet")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := convert(t, in); !errors.Is(err, ErrNotJavaEra) {
		t.Fatalf("incomplete: err = %v", err)
	}
	in = filepath.Join(t.TempDir(), "unstamped")
	if err := os.MkdirAll(in, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := legacytest.Write(filepath.Join(in, "p.parquet"), legacytest.Part{Rows: objects(ks)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := convert(t, in); !errors.Is(err, ErrNotJavaEra) {
		t.Fatalf("unstamped: err = %v", err)
	}
}

func TestConvertRefusesNonEmptyOutput(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	writeFixture(t, in, objects(keys(100)), 100)
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "x"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Options{Input: in, Output: out, Bucket: "bkt"}, nil)
	if !errors.Is(err, ErrOutputNotNew) {
		t.Fatalf("err = %v", err)
	}
}

// TestSpliceKeepsZeroFields: a field parquet-go's structs cannot carry (an
// optional zero) survives the rewrite, where a decode and re-encode drops it.
func TestSpliceKeepsZeroFields(t *testing.T) {
	in := filepath.Join(t.TempDir(), "in")
	writeFixture(t, in, objects(keys(100)), 100)
	raw, _ := footerOf(t, filepath.Join(in, "part-00000.parquet"))
	// Append an unknown field (id 99, i64 zero) to the footer struct.
	top, _, err := splitStruct(raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	top = append(top, field{id: 99, typ: tI64, val: []byte{0}})
	raw = joinStruct(top)
	var meta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), raw, &meta); err != nil {
		t.Fatal(err)
	}
	reencoded, err := thrift.Marshal(new(thrift.CompactProtocol), &meta)
	if err != nil {
		t.Fatal(err)
	}
	if f, _ := flatten(reencoded); f[".99"] != "" {
		t.Fatal("parquet-go kept the unknown field; this test no longer shows the difference")
	}
	spliced, err := rewriteFooter(raw, len(meta.RowGroups), len(javaColumns), true, len(meta.ColumnOrders) == 0)
	if err != nil {
		t.Fatal(err)
	}
	if f, _ := flatten(spliced); f[".99"] != "6:00" {
		t.Fatalf("the splice dropped field 99: %q", f[".99"])
	}
	if err := checkFooterChange(raw, spliced, len(meta.KeyValueMetadata), len(meta.ColumnOrders) == 0); err != nil {
		t.Fatal(err)
	}
}

func firstDiff(a, b []store.DelimitedEntry) string {
	for i := range min(len(a), len(b)) {
		if !reflect.DeepEqual(a[i], b[i]) {
			da, db := describe(a[i]), describe(b[i])
			return fmt.Sprintf(", entry %d: %s / %s", i, da, db)
		}
	}
	return ""
}

func describe(e store.DelimitedEntry) string {
	if e.Object != nil {
		return fmt.Sprintf("object %q", e.Object.Key)
	}
	return fmt.Sprintf("prefix %q", e.CommonPrefix)
}

// successor is the least key after every key beginning with p, nil for
// none: the bound the S3 route scopes a prefix listing with.
func successor(p string) []byte {
	b := []byte(p)
	for len(b) > 0 && b[len(b)-1] == 0xff {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return nil
	}
	b[len(b)-1]++
	return b
}
