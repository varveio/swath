// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/format2"
	"github.com/varveio/swath/go/sortedparquet/javaera"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

func TestSuccessorAndCommonPrefix(t *testing.T) {
	for in, want := range map[string]string{"a": "b", "a/": "a0", "a\xff": "b", "a\xfe\xff\xff": "a\xff"} {
		got, ok := sortedparquet.Successor([]byte(in))
		if !ok || string(got) != want {
			t.Fatalf("Successor(%q) = %q, %v", in, got, ok)
		}
	}
	for _, in := range []string{"", "\xff\xff"} {
		if _, ok := sortedparquet.Successor([]byte(in)); ok {
			t.Fatalf("Successor(%q) must have none", in)
		}
	}
	if cp := sortedparquet.CommonPrefix([]byte("a/b/c"), []byte("a/"), '/'); string(cp) != "a/b/" {
		t.Fatalf("CommonPrefix = %q", cp)
	}
	if cp := sortedparquet.CommonPrefix([]byte("a/b"), []byte("a/"), '/'); cp != nil {
		t.Fatalf("CommonPrefix = %q, want none", cp)
	}
}

func writeParts(t *testing.T, parts ...sortedparquettest.Part) string {
	t.Helper()
	dir := t.TempDir()
	for i, part := range parts {
		part.Stamps = sortedparquettest.Stamps(i+1, i == len(parts)-1)
		if err := sortedparquettest.Write(filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", i+1)), part); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func rowsOf(keys []string) []sortedparquettest.Row {
	rows := make([]sortedparquettest.Row, len(keys))
	for i, k := range keys {
		rows[i] = sortedparquettest.Object(k, i)
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
		dir := writeParts(t, sortedparquettest.Part{Rows: rowsOf(keys), PageBytes: 4096, GroupRows: 150})
		fixture, err := javaera.Open(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			rows, err := fixture.Reader.Read(context.Background(), sortedparquet.Range{From: []byte(key), FromInclusive: true, Limit: 1}, nil)
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
	dir := writeParts(t, sortedparquettest.Part{Rows: rowsOf([]string{"a", "c", "b", "d"})})
	fixture, err := javaera.Open(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Reader.Close()
	_, err = fixture.Reader.Read(context.Background(), sortedparquet.Range{Limit: 10}, nil)
	if !errors.Is(err, sortedparquet.ErrCorrupt) {
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
	dir := writeParts(t, sortedparquettest.Part{Rows: rowsOf(keys), PageBytes: 1 << 20})
	fixture, err := javaera.Open(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Reader.Close()
	pages := func(columns ...string) int64 {
		var stats sortedparquet.Stats
		rows, err := fixture.Reader.Read(context.Background(), sortedparquet.Range{Limit: 5, Columns: columns}, &stats)
		if err != nil || len(rows) != 5 {
			t.Fatalf("read: %d rows, %v", len(rows), err)
		}
		return stats.PagesDecoded
	}
	if a, b, c := pages(), pages("size"), pages("size", "owner_id"); a != 1 || b != 2 || c != 3 {
		t.Fatalf("pages decoded %d, %d, %d; want 1, 2, 3", a, b, c)
	}
}

// TestFormat2Refusals mutates a generated dataset once per refusal.
func TestFormat2Refusals(t *testing.T) {
	cases := []struct {
		reason string
		mutate func(dir string, m map[string]any)
	}{
		{"invalid_manifest", func(dir string, m map[string]any) { m["format"] = "other" }},
		{"unknown_format_version", func(dir string, m map[string]any) { m["format_version"] = 3 }},
		{"incomplete_multifile", func(dir string, m map[string]any) { _ = os.Remove(filepath.Join(dir, "data", "part-00001.parquet")) }},
		{"incomplete_multifile", func(dir string, m map[string]any) { part(m, 0)["bytes"] = 7 }},
		{"incomplete_multifile", func(dir string, m map[string]any) { part(m, 1)["rows"] = 3 }},
		{"invalid_manifest", func(dir string, m map[string]any) { part(m, 0)["path"] = "../escape.parquet" }},
		{"sanity_failed", func(dir string, m map[string]any) {
			part(m, 0)["min_key_base64"] = base64.StdEncoding.EncodeToString([]byte("\x00"))
		}},
		{"sanity_failed", func(dir string, m map[string]any) {
			part(m, 1)["max_key_base64"] = base64.StdEncoding.EncodeToString([]byte("\xf0\x90\x80\x81"))
		}},
		{"no_stamp", func(dir string, m map[string]any) { m["schema_id"] = "swath/gcs-objects/v1" }},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("%d-%s", i, c.reason), func(t *testing.T) {
			dir := writeDataset(t)
			raw, _ := os.ReadFile(filepath.Join(dir, format2.ManifestName))
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			c.mutate(dir, m)
			raw, _ = json.Marshal(m)
			if err := os.WriteFile(filepath.Join(dir, format2.ManifestName), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := format2.Open(context.Background(), dir, nil)
			if got := sortedparquet.ReasonOf(err); got != c.reason {
				t.Fatalf("refusal %q (%v), want %q", got, err, c.reason)
			}
		})
	}
	if _, err := format2.Open(context.Background(), writeDataset(t), nil); err != nil {
		t.Fatalf("unchanged copy: %v", err)
	}
}

func part(m map[string]any, i int) map[string]any {
	return m["parts"].([]any)[i].(map[string]any)
}

// datasetKeys are the keys of the generated format-2 dataset: edge cases
// of byte order (a key that is a prefix of the next, an empty segment,
// percent text, two spellings of é, a 4-byte UTF-8 rune) and a bulk run.
func datasetKeys() []string {
	keys := []string{"a", "a/", "a/b", "a/b/", "a/b/x", "a//c", "a/%2F", "e\u0301", "\u00e9", "\U00010000"}
	for i := range 1100 {
		keys = append(keys, fmt.Sprintf("bulk/k%04d", i))
	}
	slices.SortFunc(keys, strings.Compare)
	return keys
}

// writeDataset writes datasetKeys as a two-part format-2 dataset, split
// after the 1,061st key, and returns its directory.
func writeDataset(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	rows := rowsOf(datasetKeys())
	if _, err := sortedparquettest.WriteFormat2(dir, sortedparquettest.Dataset{Parts: []sortedparquettest.Part{
		{Rows: rows[:1061], PageBytes: 2048, GroupRows: 400},
		{Rows: rows[1061:]},
	}}); err != nil {
		t.Fatal(err)
	}
	return dir
}
