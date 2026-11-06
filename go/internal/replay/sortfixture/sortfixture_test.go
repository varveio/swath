// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortfixture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/journal"
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
	marker.RowType, marker.Size = "COMMON_PREFIX", nil
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
	sorted, err := format2.Open(context.Background(), out, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Reader.Close()
	rows, err := sorted.Reader.Read(context.Background(), sortedparquet.Range{Limit: 10, Columns: []string{"size", "etag"}}, nil)
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
	if _, err := os.Stat(filepath.Join(out, dataset.Workspace, dataset.PartsDir)); !os.IsNotExist(err) {
		t.Fatal("unpublished parts must not remain")
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
	sorted, err := format2.Open(context.Background(), out, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Reader.Close()
	rows, err := sorted.Reader.Read(context.Background(), sortedparquet.Range{From: []byte(long + "a"), FromInclusive: false, Limit: 5}, nil)
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

// TestRejectsUnknownRowType refuses a row_type outside the canonical three,
// as spelled, and a null one; it accepted them before and dropped the
// rows.
func TestRejectsUnknownRowType(t *testing.T) {
	for _, rowType := range []string{"PREFIX", "object", "", "OBJECT "} {
		bad := obj("p/", 1)
		bad.RowType = rowType
		in := capture(t, []sortedparquettest.Row{obj("a", 0), bad})
		out := filepath.Join(t.TempDir(), "o")
		if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); !errors.Is(err, ErrRowType) || !strings.Contains(err.Error(), "'p/'") {
			t.Fatalf("row_type %q: %v", rowType, err)
		}
		if _, err := os.Stat(out); !os.IsNotExist(err) {
			t.Fatalf("row_type %q: the output was created", rowType)
		}
	}
	type nullable struct {
		Key     string  `parquet:"key"`
		RowType *string `parquet:"row_type,optional"`
	}
	dir := filepath.Join(t.TempDir(), "capture")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(filepath.Join(dir, "c.parquet"), []nullable{{Key: "a"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Options{Capture: dir, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrRowType) || !strings.Contains(err.Error(), "null") {
		t.Fatalf("null row_type: %v", err)
	}
}

// TestRejectsCaptureWithoutParts refuses a capture directory with no
// *.parquet part (it wrote an empty dataset before), and still sorts a
// capture whose one part holds no rows.
func TestRejectsCaptureWithoutParts(t *testing.T) {
	empty := t.TempDir()
	if err := os.WriteFile(filepath.Join(empty, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Options{Capture: empty, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrNoParts) {
		t.Fatalf("no parts: %v", err)
	}
	stats, err := Run(context.Background(), Options{Capture: capture(t, nil), Output: filepath.Join(t.TempDir(), "o")}, nil)
	if err != nil || stats.CaptureRows != 0 || stats.Objects != 0 {
		t.Fatalf("an empty part: %+v %v", stats, err)
	}
}

// TestDuplicateAndVersionedNonObjectRows applies the duplicate and version
// rules to every row type: an OBJECT and a COMMON_PREFIX row with one key
// in different parts, and a versioned DELETE_MARKER.
func TestDuplicateAndVersionedNonObjectRows(t *testing.T) {
	prefix := obj("d/", 5)
	prefix.RowType, prefix.Size = "COMMON_PREFIX", nil
	in := capture(t, []sortedparquettest.Row{obj("a", 0), obj("d/", 1)}, []sortedparquettest.Row{prefix})
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrDuplicateKey) || !strings.Contains(err.Error(), "'d/'") {
		t.Fatalf("an object and a prefix with one key: %v", err)
	}
	marker := obj("x", 2)
	version := "v9"
	marker.RowType, marker.IsDeleteMarker, marker.VersionID = "DELETE_MARKER", true, &version
	in = capture(t, []sortedparquettest.Row{obj("a", 0), marker})
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrVersioned) || !strings.Contains(err.Error(), "'x'") {
		t.Fatalf("a versioned delete marker: %v", err)
	}
}

// TestSortFixtureRefusesOwnedOutput runs against an empty output whose lock
// another owner holds: the run is refused with journal.ErrLocked and the
// output is left exactly as it was. Before, the run wrote into the output
// and removed the workspace with the other owner's lock in it.
func TestSortFixtureRefusesOwnedOutput(t *testing.T) {
	if !journal.LockEnforced {
		t.Skip("the output lock is not enforced on this platform")
	}
	out := filepath.Join(t.TempDir(), "out")
	lock, err := journal.TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	before := tree(t, out)
	in := capture(t, []sortedparquettest.Row{obj("a", 0)})
	if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("an owned output: %v", err)
	}
	if after := tree(t, out); after != before {
		t.Fatalf("the output changed:\n%s\nto\n%s", before, after)
	}
	if err := lock.Verify(); err != nil {
		t.Fatalf("the owner's lock: %v", err)
	}
}

// tree lists every entry under dir with its size and modification time.
func tree(t *testing.T, dir string) string {
	t.Helper()
	var b strings.Builder
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "%s %v %d %d\n", path, info.Mode(), info.Size(), info.ModTime().UnixNano())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}
