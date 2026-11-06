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

	"github.com/parquet-go/parquet-go"

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
		fixture, err := javaera.Open(context.Background(), dir, nil, nil)
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
	fixture, err := javaera.Open(context.Background(), dir, nil, nil)
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
	fixture, err := javaera.Open(context.Background(), dir, nil, nil)
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
			_, err := format2.Open(context.Background(), dir, nil, nil)
			if got := sortedparquet.ReasonOf(err); got != c.reason {
				t.Fatalf("refusal %q (%v), want %q", got, err, c.reason)
			}
		})
	}
	if _, err := format2.Open(context.Background(), writeDataset(t), nil, nil); err != nil {
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

// TestFormat2RequiresBounds refuses a manifest whose non-empty part lacks
// an exact key bound. Without the check, a first part holding a..z with no
// bounds and a second holding m opened, and a read from n routed to the
// second part and lost z.
func TestFormat2RequiresBounds(t *testing.T) {
	cases := map[string]func(m map[string]any){
		"no minimum": func(m map[string]any) { delete(part(m, 0), "min_key_base64") },
		"no maximum": func(m map[string]any) { delete(part(m, 0), "max_key_base64") },
		"neither":    func(m map[string]any) { delete(part(m, 0), "min_key_base64"); delete(part(m, 0), "max_key_base64") },
		"null":       func(m map[string]any) { part(m, 0)["max_key_base64"] = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if _, err := sortedparquettest.WriteFormat2(dir, sortedparquettest.Dataset{Parts: []sortedparquettest.Part{
				{Rows: rowsOf([]string{"a", "z"})}, {Rows: rowsOf([]string{"m"})},
			}}); err != nil {
				t.Fatal(err)
			}
			raw, _ := os.ReadFile(filepath.Join(dir, format2.ManifestName))
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			mutate(m)
			raw, _ = json.Marshal(m)
			if err := os.WriteFile(filepath.Join(dir, format2.ManifestName), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			dataset, err := format2.Open(context.Background(), dir, nil, nil)
			if got := sortedparquet.ReasonOf(err); got != sortedparquet.ReasonInvalidManifest {
				if err == nil {
					rows, _ := dataset.Reader.Read(context.Background(), sortedparquet.Range{From: []byte("n"), FromInclusive: true, Limit: 1}, nil)
					_ = dataset.Reader.Close()
					t.Fatalf("opened; a read from n returned %d rows", len(rows))
				}
				t.Fatalf("refusal %q (%v), want invalid_manifest", got, err)
			}
		})
	}
}

// TestFormat2TruncatedFFBounds reads a format-2 dataset whose keys exceed
// its 1,024-byte index bound with a run of 0xFF bytes longer than the
// bound, so the writer cannot raise a truncated maximum and keeps a prefix
// that sorts below the page's keys. Every key is read from itself and from
// its predecessor, inclusive and exclusive, and the whole dataset is paged
// at limit 3 across several pages and both parts.
func TestFormat2TruncatedFFBounds(t *testing.T) {
	stem := strings.Repeat("\xff", 1030)
	keys := []string{"\xfe", strings.Repeat("\xfe", 1100), stem[:1024]}
	for i := range 120 {
		keys = append(keys, fmt.Sprintf("%s%04d", stem, i))
	}
	keys = append(keys, stem+"\xff")
	slices.Sort(keys)
	rows := rowsOf(keys)
	dir := t.TempDir()
	if _, err := sortedparquettest.WriteFormat2(dir, sortedparquettest.Dataset{Parts: []sortedparquettest.Part{
		{Rows: rows[:70], PageBytes: 4096, GroupRows: 40},
		{Rows: rows[70:], PageBytes: 4096},
	}}); err != nil {
		t.Fatal(err)
	}
	dataset, err := format2.Open(context.Background(), dir, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dataset.Reader.Close()
	read := func(from []byte, inclusive bool, limit int) []string {
		t.Helper()
		got, err := dataset.Reader.Read(context.Background(), sortedparquet.Range{From: from, FromInclusive: inclusive, Limit: limit}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, row := range got {
			out = append(out, string(row.Key))
		}
		return out
	}
	for i, key := range keys {
		if got := read([]byte(key), true, 1); len(got) != 1 || got[0] != key {
			t.Fatalf("from key %d inclusive: %d rows", i, len(got))
		}
		got := read([]byte(key), false, 1)
		if i+1 < len(keys) && (len(got) != 1 || got[0] != keys[i+1]) || i+1 == len(keys) && len(got) != 0 {
			t.Fatalf("from key %d exclusive: %d rows", i, len(got))
		}
	}
	var paged []string
	var from []byte
	for {
		page := read(from, false, 3)
		if len(page) == 0 {
			break
		}
		paged = append(paged, page...)
		from = []byte(page[len(page)-1])
	}
	if !slices.Equal(paged, keys) {
		t.Fatalf("paged %d keys of %d", len(paged), len(keys))
	}
}

// TestFormat2FooterAndSchemaRefusals refuses parts whose footers or key
// column do not meet the profile, each with its reason.
func TestFormat2FooterAndSchemaRefusals(t *testing.T) {
	stamped := func(change map[string]string) sortedparquettest.Dataset {
		return sortedparquettest.Dataset{Parts: []sortedparquettest.Part{{Rows: rowsOf([]string{"a", "b"}), Stamps: change}}}
	}
	cases := []struct {
		name, reason string
		dataset      sortedparquettest.Dataset
		replace      func(t *testing.T, dir string) // writes a part of another schema over the part
	}{
		{"no profile stamp", "no_stamp", stamped(map[string]string{"swath.profile": ""}), nil},
		{"another profile", "no_stamp", stamped(map[string]string{"swath.profile": "other"}), nil},
		{"no bound stamp", "no_stamp", stamped(map[string]string{"swath.index_bound_bytes": ""}), nil},
		{"zero bound", "no_stamp", stamped(map[string]string{"swath.index_bound_bytes": "0"}), nil},
		{"footer version", "unknown_format_version", stamped(map[string]string{"swath.profile_version": "3"}), nil},
		{"no profile version", "unknown_format_version", stamped(map[string]string{"swath.profile_version": ""}), nil},
		{"unsorted groups", "no_stamp", sortedparquettest.Dataset{Unsorted: true, Parts: []sortedparquettest.Part{{Rows: rowsOf([]string{"a", "b"})}}}, nil},
		{"no key page index", "no_page_index", sortedparquettest.Dataset{Parts: []sortedparquettest.Part{{Rows: rowsOf([]string{"a", "b"}), NoKeyPageIndex: true}}}, nil},
		{"optional key", "unsupported_schema", stamped(nil), func(t *testing.T, dir string) {
			replacePart(t, dir, []struct {
				Key *string `parquet:"key,optional"`
			}{{Key: ptr("a")}, {Key: ptr("b")}})
		}},
		{"repeated key", "unsupported_schema", stamped(nil), func(t *testing.T, dir string) {
			replacePart(t, dir, []struct {
				Key []string `parquet:"key"`
			}{{Key: []string{"a"}}, {Key: []string{"b"}}})
		}},
		{"integer key", "unsupported_schema", stamped(nil), func(t *testing.T, dir string) {
			replacePart(t, dir, []struct {
				Key int64 `parquet:"key"`
			}{{Key: 1}, {Key: 2}})
		}},
		{"no key", "unsupported_schema", stamped(nil), func(t *testing.T, dir string) {
			replacePart(t, dir, []struct {
				Name string `parquet:"name"`
			}{{Name: "a"}, {Name: "b"}})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			if _, err := sortedparquettest.WriteFormat2(dir, c.dataset); err != nil {
				t.Fatal(err)
			}
			if c.replace != nil {
				c.replace(t, dir)
			}
			dataset, err := format2.Open(context.Background(), dir, nil, nil)
			if got := sortedparquet.ReasonOf(err); got != c.reason {
				if err == nil {
					_ = dataset.Reader.Close()
				}
				t.Fatalf("refusal %q (%v), want %q", got, err, c.reason)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// replacePart writes rows over the dataset's one
// part with the format-2 stamps, and updates its manifest size.
func replacePart[T any](t *testing.T, dir string, rows []T) {
	t.Helper()
	path := filepath.Join(dir, "data", "part-00000.parquet")
	var options []parquet.WriterOption
	for k, v := range sortedparquettest.Format2Stamps(sortedparquettest.Format2SchemaID, 1024) {
		options = append(options, parquet.KeyValueMetadata(k, v))
	}
	options = append(options, parquet.SortingWriterConfig(parquet.SortingColumns(parquet.Ascending("key"))))
	if err := parquet.WriteFile(path, rows, options...); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, format2.ManifestName))
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	part(m, 0)["bytes"] = info.Size()
	raw, _ = json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, format2.ManifestName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
