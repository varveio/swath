// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/replay/legacy/format2"
	"github.com/varveio/swath/go/internal/replay/legacy/javaera"
	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/sortfixture"
)

// fixtureKeys returns n distinct keys in a directory-shaped keyspace: a
// few bare root objects, nested directories, objects whose key equals a
// common prefix, non-ASCII names, and a run of keys longer than the Java
// writer's 64-byte index bound sharing a long prefix (so their page bounds
// are truncated).
func fixtureKeys(n int) []string {
	keys := []string{"a", "b/", "d01/", "zz", "é/x", "é/y"}
	long := "long/" + strings.Repeat("x", 90) + "/"
	for i := 0; len(keys) < n; i++ {
		switch i % 5 {
		case 0:
			keys = append(keys, fmt.Sprintf("%s%05d", long, i))
		case 1:
			keys = append(keys, fmt.Sprintf("root%05d", i))
		default:
			keys = append(keys, fmt.Sprintf("d%02d/s%02d/obj%05d", i%13, i%7, i))
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)[:n]
}

// writeJavaEra writes keys (sorted) as a Java-era fixture of parts parts.
func writeJavaEra(t testing.TB, dir string, keys []string, parts, groupRows int) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	per := (len(keys) + parts - 1) / parts
	for p := range parts {
		var rows []legacytest.Row
		for i := p * per; i < min((p+1)*per, len(keys)); i++ {
			rows = append(rows, legacytest.Object(keys[i], i))
		}
		part := legacytest.Part{Rows: rows, Stamps: legacytest.Stamps(p+1, p == parts-1), GroupRows: groupRows, PageBytes: 2048}
		if err := legacytest.Write(filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", p+1)), part); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// writeLegacyFormat2 writes keys into a format-2 dataset of parts parts
// (legacytest.WriteFormat2), the format earlier builds wrote.
func writeLegacyFormat2(t testing.TB, dir string, keys []string, parts int) string {
	t.Helper()
	d := legacytest.Dataset{}
	per := (len(keys) + parts - 1) / parts
	for p := 0; p*per < len(keys); p++ {
		var rows []legacytest.Row
		for i := p * per; i < min((p+1)*per, len(keys)); i++ {
			rows = append(rows, legacytest.Object(keys[i], i))
		}
		d.Parts = append(d.Parts, legacytest.Part{Rows: rows, GroupRows: 1500, PageBytes: 2048})
	}
	if _, err := legacytest.WriteFormat2(dir, d); err != nil {
		t.Fatal(err)
	}
	return dir
}

// writeSorted writes keys, shuffled into an unstamped capture, and sorts
// them into a SeekParquet dataset with sort-fixture.
func writeSorted(t testing.TB, dir string, keys []string, keyLimit int) string {
	t.Helper()
	capture := filepath.Join(dir, "capture")
	if err := os.MkdirAll(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	var rows []legacytest.Row
	for i, key := range keys {
		rows = append(rows, legacytest.Object(key, i))
	}
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	if err := legacytest.Write(filepath.Join(capture, "c.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "dataset")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: out, KeyLimit: keyLimit}, io.Discard); err != nil {
		t.Fatal(err)
	}
	return out
}

// refRow is a row as a plain sequential read of the parts sees it.
type refRow struct {
	key    string
	size   *int64
	etag   *string
	owner  *string
	sumAlg *string
}

// reference reads a fixture's parts in order with parquet-go's row reader,
// independently of the page index, and reports the first key of every row
// group and every key page. A local recording's rows have no etag column
// and answer the S3 placeholder ETag (TestLocalRecordingS3View).
func reference(t testing.TB, path string) (rows []refRow, boundaries []string) {
	t.Helper()
	var files []string
	var placeholderETag *string
	if format2.IsDataset(path) {
		raw, err := os.ReadFile(filepath.Join(path, "manifest.json"))
		var manifest struct {
			SchemaID string `json:"schema_id"`
		}
		if err == nil {
			err = json.Unmarshal(raw, &manifest)
		}
		if err != nil {
			t.Fatal(err)
		}
		if manifest.SchemaID == "swath-local/v1" {
			placeholder := "swath-s3-synthetic-v1"
			placeholderETag = &placeholder
		}
		// Part names are dense and zero-padded: name order is key order.
		files, _ = filepath.Glob(filepath.Join(path, "data", "*.parquet"))
	} else {
		var err error
		if files, err = javaera.Resolve(path); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range files {
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		info, _ := f.Stat()
		file, err := parquet.OpenFile(f, info.Size())
		if err != nil {
			t.Fatal(err)
		}
		column := func(name string) int {
			if leaf, ok := file.Schema().Lookup(name); ok {
				return leaf.ColumnIndex
			}
			return -1
		}
		key, size, etag, owner, sumAlg := column("key"), column("size"), column("etag"), column("owner_id"), column("checksum_algorithm")
		base := len(rows)
		reader := parquet.NewReader(file)
		buffer := make([]parquet.Row, 100)
		for {
			n, err := reader.ReadRows(buffer)
			for _, row := range buffer[:n] {
				r := refRow{etag: placeholderETag}
				for _, v := range row {
					switch c := v.Column(); {
					case c == key:
						r.key = string(v.ByteArray())
					case v.IsNull():
					case c == size:
						x := v.Int64()
						r.size = &x
					case c == etag:
						s := string(v.ByteArray())
						r.etag = &s
					case c == owner:
						s := string(v.ByteArray())
						r.owner = &s
					case c == sumAlg:
						s := string(v.ByteArray())
						r.sumAlg = &s
					}
				}
				rows = append(rows, r)
			}
			if errors.Is(err, io.EOF) || n == 0 {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		_ = reader.Close()
		start := int64(base)
		for _, group := range file.RowGroups() {
			if group.NumRows() == 0 {
				continue
			}
			offsets, err := group.ColumnChunks()[key].OffsetIndex()
			if err != nil {
				t.Fatal(err)
			}
			for page := range offsets.NumPages() {
				boundaries = append(boundaries, rows[start+offsets.FirstRowIndex(page)].key)
			}
			start += group.NumRows()
		}
		_ = f.Close()
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1].key >= rows[i].key {
			t.Fatalf("reference keys out of order at %d", i)
		}
	}
	return rows, boundaries
}

// probes returns lower and upper bounds at, just before and just after
// every boundary key: the key, the previous key, a proper prefix of the key
// (below it), and the key followed by a zero byte (the least key after it).
func probes(rows []refRow, boundaries []string) []string {
	index := map[string]int{}
	for i, r := range rows {
		index[r.key] = i
	}
	set := map[string]bool{"": true}
	for _, b := range boundaries {
		set[b] = true
		set[b+"\x00"] = true
		if len(b) > 1 {
			set[b[:len(b)-1]] = true
		}
		if i := index[b]; i > 0 {
			set[rows[i-1].key] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// expectKeys returns the reference keys in [from, to) after the inclusive rule,
// at most limit.
func expectKeys(rows []refRow, from *string, inclusive bool, to *string, limit int) []string {
	var out []string
	for _, r := range rows {
		if from != nil && (r.key < *from || r.key == *from && !inclusive) {
			continue
		}
		if to != nil && r.key >= *to {
			break
		}
		if len(out) == limit {
			break
		}
		out = append(out, r.key)
	}
	return out
}

func keysOf(objects []Object) []string {
	out := make([]string, len(objects))
	for i, o := range objects {
		out[i] = string(o.Key)
	}
	return out
}
