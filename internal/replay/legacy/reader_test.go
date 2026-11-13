// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package legacy_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/replay/legacy"
	"github.com/varveio/swath/go/internal/replay/legacy/javaera"
	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
)

func TestSuccessorAndCommonPrefix(t *testing.T) {
	for in, want := range map[string]string{"a": "b", "a/": "a0", "a\xff": "b", "a\xfe\xff\xff": "a\xff"} {
		got, ok := legacy.Successor([]byte(in))
		if !ok || string(got) != want {
			t.Fatalf("Successor(%q) = %q, %v", in, got, ok)
		}
	}
	for _, in := range []string{"", "\xff\xff"} {
		if _, ok := legacy.Successor([]byte(in)); ok {
			t.Fatalf("Successor(%q) must have none", in)
		}
	}
	if cp := legacy.CommonPrefix([]byte("a/b/c"), []byte("a/"), '/'); string(cp) != "a/b/" {
		t.Fatalf("CommonPrefix = %q", cp)
	}
	if cp := legacy.CommonPrefix([]byte("a/b"), []byte("a/"), '/'); cp != nil {
		t.Fatalf("CommonPrefix = %q, want none", cp)
	}
}

func writeParts(t *testing.T, parts ...legacytest.Part) string {
	t.Helper()
	dir := t.TempDir()
	for i, part := range parts {
		part.Stamps = legacytest.Stamps(i+1, i == len(parts)-1)
		if err := legacytest.Write(filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", i+1)), part); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func rowsOf(keys []string) []legacytest.Row {
	rows := make([]legacytest.Row, len(keys))
	for i, k := range keys {
		rows[i] = legacytest.Object(k, i)
	}
	return rows
}

// TestTruncatedBoundsAreConservative reads, from every key, a part whose
// keys exceed the 64-byte index bound. When the 64 bytes kept are all 0xFF
// the writer cannot raise the maximum and keeps a prefix, which sorts
// below the page's keys; a reader trusting it as a maximum skips the page.
func TestTruncatedBoundsAreConservative(t *testing.T) {
	for _, stem := range []string{strings.Repeat("x", 100), strings.Repeat("\xff", 70)} {
		var keys []string
		for i := range 400 {
			keys = append(keys, fmt.Sprintf("%s%04d", stem, i))
		}
		dir := writeParts(t, legacytest.Part{Rows: rowsOf(keys), PageBytes: 4096, GroupRows: 150})
		fixture, err := javaera.Open(context.Background(), dir, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			rows, err := fixture.Reader.Read(context.Background(), legacy.Range{From: []byte(key), FromInclusive: true, Limit: 1}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || string(rows[0].Key) != key {
				t.Fatalf("stem %q…: from key %d got %d rows", stem[:4], i, len(rows))
			}
		}
		_ = fixture.Reader.Close()
	}
}

// TestDisorderIsCorrupt opens a part whose row-group first keys ascend but
// whose keys inside a page do not, and requires the read to fail.
func TestDisorderIsCorrupt(t *testing.T) {
	dir := writeParts(t, legacytest.Part{Rows: rowsOf([]string{"a", "c", "b", "d"})})
	fixture, err := javaera.Open(context.Background(), dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Reader.Close()
	_, err = fixture.Reader.Read(context.Background(), legacy.Range{Limit: 10}, nil)
	if !errors.Is(err, legacy.ErrCorrupt) {
		t.Fatalf("read of unordered keys: %v", err)
	}
}

// TestReadDecodesOnlyNamedColumns counts the pages a read decodes with no
// value column, one, and two.
func TestReadDecodesOnlyNamedColumns(t *testing.T) {
	var keys []string
	for i := range 50 {
		keys = append(keys, fmt.Sprintf("k%03d", i))
	}
	dir := writeParts(t, legacytest.Part{Rows: rowsOf(keys), PageBytes: 1 << 20})
	fixture, err := javaera.Open(context.Background(), dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Reader.Close()
	pages := func(columns ...string) int64 {
		var stats legacy.Stats
		rows, err := fixture.Reader.Read(context.Background(), legacy.Range{Limit: 5, Columns: columns}, &stats)
		if err != nil || len(rows) != 5 {
			t.Fatalf("read: %d rows, %v", len(rows), err)
		}
		return stats.PagesDecoded
	}
	if a, b, c := pages(), pages("size"), pages("size", "owner_id"); a != 1 || b != 2 || c != 3 {
		t.Fatalf("pages decoded %d, %d, %d; want 1, 2, 3", a, b, c)
	}
}

// TestKeyColumnAndPageIndexRefusals refuses Java-era parts whose key
// column the reader cannot route by, each with its reason: a key column
// without a page index, and a key that is optional, repeated, not a byte
// array or missing.
func TestKeyColumnAndPageIndexRefusals(t *testing.T) {
	cases := []struct {
		name, reason string
		write        func(t *testing.T, path string)
	}{
		{"no key page index", "no_page_index", func(t *testing.T, path string) {
			if err := legacytest.Write(path, legacytest.Part{Rows: rowsOf([]string{"a", "b"}), Stamps: legacytest.Stamps(1, true), NoKeyPageIndex: true}); err != nil {
				t.Fatal(err)
			}
		}},
		{"optional key", "unsupported_schema", func(t *testing.T, path string) {
			writeStamped(t, path, []struct {
				Key *string `parquet:"key,optional"`
			}{{Key: ptr("a")}, {Key: ptr("b")}})
		}},
		{"repeated key", "unsupported_schema", func(t *testing.T, path string) {
			writeStamped(t, path, []struct {
				Key []string `parquet:"key"`
			}{{Key: []string{"a"}}, {Key: []string{"b"}}})
		}},
		{"integer key", "unsupported_schema", func(t *testing.T, path string) {
			writeStamped(t, path, []struct {
				Key int64 `parquet:"key"`
			}{{Key: 1}, {Key: 2}})
		}},
		{"no key", "unsupported_schema", func(t *testing.T, path string) {
			writeStamped(t, path, []struct {
				Name string `parquet:"name"`
			}{{Name: "a"}, {Name: "b"}})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			c.write(t, filepath.Join(dir, "part-00001.parquet"))
			fixture, err := javaera.Open(context.Background(), dir, nil, nil)
			if got := legacy.ReasonOf(err); got != c.reason {
				if err == nil {
					_ = fixture.Reader.Close()
				}
				t.Fatalf("refusal %q (%v), want %q", got, err, c.reason)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// writeStamped writes rows as the one part of a Java-era fixture, with
// its stamps.
func writeStamped[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	var options []parquet.WriterOption
	for k, v := range legacytest.Stamps(1, true) {
		options = append(options, parquet.KeyValueMetadata(k, v))
	}
	if err := parquet.WriteFile(path, rows, options...); err != nil {
		t.Fatal(err)
	}
}

// scanRows rebuilds rows from what Scan hands its sink, checking the sink
// contract on the way: Keys, then every column in order, each given in
// full by Values calls or by one Missing call.
type scanRows struct {
	t       *testing.T
	columns []string
	rows    []legacy.Row
	run     int // the current run's first row
	next    int // the column expected next
	filled  int // rows of that column given so far
}

func (s *scanRows) Keys(keys [][]byte) {
	if s.rows != nil && s.next != len(s.columns) {
		s.t.Fatalf("a run started with column %d of %d still due", s.next, len(s.columns))
	}
	s.run, s.next, s.filled = len(s.rows), 0, 0
	for _, k := range keys {
		s.rows = append(s.rows, legacy.Row{Key: k, Columns: s.columns, Values: make([]parquet.Value, len(s.columns))})
	}
}

func (s *scanRows) Values(c int, values []parquet.Value) {
	if c != s.next {
		s.t.Fatalf("values of column %d while %d is due", c, s.next)
	}
	for _, v := range values {
		s.rows[s.run+s.filled].Values[c] = v.Clone()
		s.filled++
	}
	if s.run+s.filled == len(s.rows) {
		s.next, s.filled = s.next+1, 0
	}
}

func (s *scanRows) Missing(c int) {
	if c != s.next || s.filled != 0 {
		s.t.Fatalf("column %d missing while %d is due", c, s.next)
	}
	for i := s.run; i < len(s.rows); i++ {
		s.rows[i].Values[c] = parquet.NullValue()
	}
	s.next++
}

// TestScanMatchesRead reads ranges across pages, row groups and parts with
// a column no part holds, through Read and through Scan, and requires the
// same rows and values.
func TestScanMatchesRead(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("k%04d", i))
	}
	dir := writeParts(t,
		legacytest.Part{Rows: rowsOf(keys[:120]), PageBytes: 256, GroupRows: 37},
		legacytest.Part{Rows: rowsOf(keys[120:]), PageBytes: 512, GroupRows: 50})
	fixture, err := javaera.Open(context.Background(), dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Reader.Close()
	columns := []string{"size", "etag", "no_such_column", "owner_id", "last_modified"}
	for _, from := range []string{"", "k0000", "k0036", "k0037", "k0119", "k0150x", "k0299"} {
		for _, limit := range []int{1, 13, 40, 120, 400} {
			for _, to := range []string{"", "k0200"} {
				rng := legacy.Range{Limit: limit, Columns: columns, FromInclusive: true}
				if from != "" {
					rng.From = []byte(from)
				}
				if to != "" {
					rng.To = []byte(to)
				}
				want, err := fixture.Reader.Read(context.Background(), rng, nil)
				if err != nil {
					t.Fatal(err)
				}
				sink := &scanRows{t: t, columns: columns}
				if err := fixture.Reader.Scan(context.Background(), rng, nil, sink); err != nil {
					t.Fatal(err)
				}
				if len(sink.rows) > 0 && sink.next != len(columns) {
					t.Fatalf("the last run ended with column %d of %d due", sink.next, len(columns))
				}
				if len(sink.rows) != len(want) {
					t.Fatalf("from %q limit %d to %q: %d rows, want %d", from, limit, to, len(sink.rows), len(want))
				}
				for i := range want {
					got := sink.rows[i]
					if string(got.Key) != string(want[i].Key) {
						t.Fatalf("row %d key %q, want %q", i, got.Key, want[i].Key)
					}
					for c := range columns {
						if !parquet.Equal(got.Values[c], want[i].Values[c]) || got.Values[c].IsNull() != want[i].Values[c].IsNull() {
							t.Fatalf("row %d column %s: %v, want %v", i, columns[c], got.Values[c], want[i].Values[c])
						}
					}
				}
			}
		}
	}
}
