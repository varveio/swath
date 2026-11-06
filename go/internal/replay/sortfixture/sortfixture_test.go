// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortfixture

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/format2"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// capture writes each slice of rows as one unstamped capture part.
func capture(t *testing.T, parts ...[]sortedparquettest.Row) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "capture")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, rows := range parts {
		name := filepath.Join(dir, "part-"+string(rune('a'+i))+".parquet")
		if err := sortedparquettest.Write(name, sortedparquettest.Part{Rows: rows}); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func obj(key string, i int) sortedparquettest.Row { return sortedparquettest.Object(key, i) }

// TestSortsTwoPartsIntoAFormat2Dataset sorts two capture parts, keeps only
// OBJECT rows with their nulls, and writes the replay-owned schema.
func TestSortsTwoPartsIntoAFormat2Dataset(t *testing.T) {
	marker := obj("p/", 9)
	marker.RowType, marker.Size = "PREFIX", nil
	deleted := obj("x", 8)
	deleted.RowType, deleted.IsDeleteMarker = "DELETE_MARKER", true
	untimed := obj("m", 0)
	untimed.Size, untimed.LastModified = nil, nil
	in := capture(t, []sortedparquettest.Row{untimed, obj("b", 1), marker}, []sortedparquettest.Row{obj("a", 2), deleted, obj("p/q", 3)})
	out := filepath.Join(t.TempDir(), "out")
	var log bytes.Buffer
	stats, err := Run(context.Background(), Options{Capture: in, Output: out}, &log)
	if err != nil {
		t.Fatal(err)
	}
	if stats.CaptureRows != 6 || stats.Objects != 4 || stats.Skipped != 2 || stats.Parts != 1 || stats.SchemaID != SchemaID {
		t.Fatalf("stats %+v", stats)
	}
	if !strings.HasPrefix(log.String(), "sort_fixture rows=6 objects=4 skipped=2 parts=1 ") {
		t.Fatalf("summary %q", log.String())
	}
	dataset, err := format2.Open(context.Background(), out, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dataset.Reader.Close()
	rows, err := dataset.Reader.Read(context.Background(), sortedparquet.Range{Limit: 10, Columns: []string{"size", "etag"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, r := range rows {
		keys = append(keys, string(r.Key))
	}
	if strings.Join(keys, ",") != "a,b,m,p/q" {
		t.Fatalf("keys %v", keys)
	}
	if size, _ := rows[0].Value("size"); size.Int64() != 2 {
		t.Fatalf("size of a: %v", size)
	}
	if size, _ := rows[2].Value("size"); !size.IsNull() {
		t.Fatalf("a null size must stay null: %v", size)
	}
	if _, err := os.Stat(filepath.Join(out, ".swath")); !os.IsNotExist(err) {
		t.Fatal("the workspace must not remain")
	}
}

func TestRefusesDuplicateKeys(t *testing.T) {
	in := capture(t, []sortedparquettest.Row{obj("a", 0), obj("b", 1)}, []sortedparquettest.Row{obj("b", 2)})
	_, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil)
	if !errors.Is(err, ErrDuplicateKey) || !strings.Contains(err.Error(), "'b'") {
		t.Fatalf("duplicate: %v", err)
	}
}

func TestRefusesVersionedRows(t *testing.T) {
	versioned := obj("m", 1)
	v := "v1"
	versioned.VersionID = &v
	in := capture(t, []sortedparquettest.Row{obj("a", 0), versioned})
	_, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil)
	if !errors.Is(err, ErrVersioned) || !strings.Contains(err.Error(), "'m'") {
		t.Fatalf("versioned: %v", err)
	}
}

// TestKeyLimit writes keys over 1,024 bytes only with a key limit, under
// the replay-owned schema, and serves them back.
func TestKeyLimit(t *testing.T) {
	long := strings.Repeat("n", 2010)
	in := capture(t, []sortedparquettest.Row{obj(long+"b", 0), obj("a", 1), obj(long+"a", 2)})
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("without a key limit: %v", err)
	}
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o"), KeyLimit: 2000}, nil); !errors.Is(err, ErrKeyTooLong) {
		t.Fatalf("under a smaller key limit: %v", err)
	}
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o"), KeyLimit: MaxKeyLimit + 1}, nil); err == nil {
		t.Fatal("a key limit over the maximum must be refused")
	}
	out := filepath.Join(t.TempDir(), "o")
	stats, err := Run(context.Background(), Options{Capture: in, Output: out, KeyLimit: 4096}, nil)
	if err != nil || stats.SchemaID != SchemaID {
		t.Fatalf("with a key limit: %+v %v", stats, err)
	}
	dataset, err := format2.Open(context.Background(), out, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dataset.Reader.Close()
	rows, err := dataset.Reader.Read(context.Background(), sortedparquet.Range{From: []byte(long + "a"), FromInclusive: false, Limit: 5}, nil)
	if err != nil || len(rows) != 1 || string(rows[0].Key) != long+"b" {
		t.Fatalf("read after a long key: %d rows, %v", len(rows), err)
	}
}

func TestRefusesANonEmptyOutput(t *testing.T) {
	in := capture(t, []sortedparquettest.Row{obj("a", 0)})
	out := t.TempDir()
	if err := os.WriteFile(filepath.Join(out, "keep"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); err == nil {
		t.Fatal("a non-empty output must be refused")
	}
	if _, err := os.Stat(filepath.Join(out, "keep")); err != nil {
		t.Fatal("the output's contents must be left alone")
	}
}
