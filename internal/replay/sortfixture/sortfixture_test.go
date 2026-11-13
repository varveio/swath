// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortfixture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/replay/legacy/legacytest"
)

// capture writes each slice of rows as one unstamped capture part.
func capture(t *testing.T, parts ...[]legacytest.Row) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "capture")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, rows := range parts {
		name := filepath.Join(dir, "part-"+string(rune('a'+i))+".parquet")
		if err := legacytest.Write(name, legacytest.Part{Rows: rows}); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func obj(key string, i int) legacytest.Row { return legacytest.Object(key, i) }

// TestSortsTwoPartsIntoASeekParquetDataset sorts two capture parts, keeps only
// OBJECT rows with their nulls, and writes the replay-owned schema.
func TestSortsTwoPartsIntoASeekParquetDataset(t *testing.T) {
	marker := obj("p/", 9)
	marker.RowType, marker.Size = "COMMON_PREFIX", nil
	deleted := obj("x", 8)
	deleted.RowType, deleted.IsDeleteMarker = "DELETE_MARKER", true
	untimed := obj("m", 0)
	untimed.Size, untimed.LastModified = nil, nil
	in := capture(t, []legacytest.Row{untimed, obj("b", 1), marker}, []legacytest.Row{obj("a", 2), deleted, obj("p/q", 3)})
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
	sorted, err := seekparquet.OpenDir(context.Background(), out, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	if err := sorted.Verify(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	rows, err := sorted.Rows(context.Background(), seekparquet.Query{Limit: 10, Columns: []string{"size", "etag"}}, nil)
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
	if stats, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "small"), PartSize: 1}, nil); err != nil || stats.Parts != 4 {
		t.Fatalf("a one-byte part size cuts a part per object: %+v %v", stats, err)
	}
}

func TestRefusesDuplicateKeys(t *testing.T) {
	in := capture(t, []legacytest.Row{obj("a", 0), obj("b", 1)}, []legacytest.Row{obj("b", 2)})
	_, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil)
	if !errors.Is(err, ErrDuplicateKey) || !strings.Contains(err.Error(), "'b'") {
		t.Fatalf("duplicate: %v", err)
	}
}

func TestRefusesVersionedRows(t *testing.T) {
	versioned := obj("m", 1)
	v := "v1"
	versioned.VersionID = &v
	in := capture(t, []legacytest.Row{obj("a", 0), versioned})
	_, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil)
	if !errors.Is(err, ErrVersioned) || !strings.Contains(err.Error(), "'m'") {
		t.Fatalf("versioned: %v", err)
	}
}

// TestKeyLimit writes keys over 1,024 bytes only with a key limit, under
// the replay-owned schema, and serves them back.
func TestKeyLimit(t *testing.T) {
	long := strings.Repeat("n", 2010)
	in := capture(t, []legacytest.Row{obj(long+"b", 0), obj("a", 1), obj(long+"a", 2)})
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
	sorted, err := seekparquet.OpenDir(context.Background(), out, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	if err := sorted.Verify(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	rows, err := sorted.Rows(context.Background(), seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{Start: []byte(long + "a"), StartExclusive: true}}, Limit: 5}, nil)
	if err != nil || len(rows) != 1 || string(rows[0].Key) != long+"b" {
		t.Fatalf("read after a long key: %d rows, %v", len(rows), err)
	}
}

func TestRefusesANonEmptyOutput(t *testing.T) {
	in := capture(t, []legacytest.Row{obj("a", 0)})
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
		in := capture(t, []legacytest.Row{obj("a", 0), bad})
		out := filepath.Join(t.TempDir(), "o")
		if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); !errors.Is(err, ErrRowType) || !strings.Contains(err.Error(), "'p/'") {
			t.Fatalf("row_type %q: %v", rowType, err)
		}
		// The run owned the output (its lock is there) but wrote nothing.
		if entries, _ := os.ReadDir(out); len(entries) != 1 || entries[0].Name() != dataset.Workspace {
			t.Fatalf("row_type %q: the output holds %v", rowType, entries)
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
	in := capture(t, []legacytest.Row{obj("a", 0), obj("d/", 1)}, []legacytest.Row{prefix})
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrDuplicateKey) || !strings.Contains(err.Error(), "'d/'") {
		t.Fatalf("an object and a prefix with one key: %v", err)
	}
	marker := obj("x", 2)
	version := "v9"
	marker.RowType, marker.IsDeleteMarker, marker.VersionID = "DELETE_MARKER", true, &version
	in = capture(t, []legacytest.Row{obj("a", 0), marker})
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
	in := capture(t, []legacytest.Row{obj("a", 0)})
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
		mtime := info.ModTime().UnixNano()
		if d.IsDir() && runtime.GOOS == "windows" {
			mtime = 0 // NTFS updates a directory's modification time lazily, after the entry it records
		}
		fmt.Fprintf(&b, "%s %v %d %d\n", path, info.Mode(), info.Size(), mtime)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// replaceOutput moves out aside and puts a directory owned by another
// process (its lock held, its parts directory present) in its place, as a
// fault hook at point. The other owner's lock is released at the test's
// end.
func replaceOutput(t *testing.T, out, point string) {
	t.Helper()
	replaceOutputWith(t, out, point, true)
}

// replaceOutputWith is replaceOutput; with parts false the replacement
// holds only its workspace and lock.
func replaceOutputWith(t *testing.T, out, point string, parts bool) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Windows refuses to rename a directory while a file in it is open (the run's lock), so a running output cannot be replaced")
	}
	var other *journal.Lock
	restore := faults.Set(func(p string) error {
		if p != point || other != nil {
			return nil
		}
		if err := os.Rename(out, out+".old"); err != nil {
			t.Error(err)
			return err
		}
		lock, err := journal.TakeLock(out)
		if err != nil {
			t.Error(err)
			return err
		}
		other = lock
		if !parts {
			return nil
		}
		if err := os.MkdirAll(filepath.Join(out, dataset.Workspace, dataset.PartsDir), 0o755); err != nil {
			t.Error(err)
		}
		return nil
	})
	t.Cleanup(func() {
		restore()
		other.Release()
	})
}

// TestReplacedOutputAtPartCreation replaces the output between the
// router's ownership check and the creation of the first part: the part
// is created in the directory the run locked, never in the replacement,
// and the run fails with journal.ErrOutputReplaced before publishing.
// Before, the part was created by pathname in the replacement.
func TestReplacedOutputAtPartCreation(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	replaceOutput(t, out, "part-create")
	in := capture(t, []legacytest.Row{obj("a", 0), obj("b", 1)})
	if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); !errors.Is(err, journal.ErrOutputReplaced) {
		t.Fatalf("run: %v", err)
	}
	assertUntouched(t, out)
}

// TestReplacedOutputAtPublication replaces the output after the parts are
// renamed into data/ and before the manifest is installed (the published
// hook): the manifest is not installed in the replacement and the run
// fails with journal.ErrOutputReplaced. Before, the replacement received
// the manifest and the run succeeded.
func TestReplacedOutputAtPublication(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	replaceOutput(t, out, "published")
	in := capture(t, []legacytest.Row{obj("a", 0)})
	if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); !errors.Is(err, journal.ErrOutputReplaced) {
		t.Fatalf("run: %v", err)
	}
	assertUntouched(t, out)
}

// assertUntouched requires the replacement output to hold only what its
// owner made: the workspace with its lock and an empty parts directory.
func assertUntouched(t *testing.T, out string) {
	t.Helper()
	if got := tree(t, out); strings.Count(got, "\n") != 4 || strings.Contains(got, ".parquet") || strings.Contains(got, dataset.ManifestName) {
		t.Fatalf("the replacement output changed:\n%s", got)
	}
}

// TestRefusesMalformedFooterCounts changes the footer row count of an
// otherwise valid one-row capture: a negative count, one that disagrees
// with the row groups and the largest int64 are refused with
// ErrMalformedCapture. Before, math.MaxInt64 reached make and panicked
// (parquet-go itself rejects a negative file count, now classified the
// same way).
func TestRefusesMalformedFooterCounts(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*format.FileMetaData)
	}{
		{"negative", func(m *format.FileMetaData) { m.NumRows = -1 }},
		{"more than the groups", func(m *format.FileMetaData) { m.NumRows = 2 }},
		{"largest", func(m *format.FileMetaData) { m.NumRows = math.MaxInt64 }},
		{"negative group", func(m *format.FileMetaData) { m.RowGroups[0].NumRows = -1; m.NumRows = -1 }},
		{"overflowing groups", func(m *format.FileMetaData) {
			m.RowGroups = append(m.RowGroups, m.RowGroups[0])
			m.RowGroups[0].NumRows, m.RowGroups[1].NumRows = math.MaxInt64, math.MaxInt64
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			in := capture(t, []legacytest.Row{obj("a", 0)})
			part := filepath.Join(in, "part-a.parquet")
			if err := legacytest.RewriteFooter(part, func(m *format.FileMetaData) error { change.edit(m); return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrMalformedCapture) {
				t.Fatalf("run: %v", err)
			}
		})
	}
}

// TestCountsRowTypesForCapacity counts the OBJECT and other rows the
// arrays are sized from: groups proven all OBJECT and all COMMON_PREFIX
// by their statistics, and a mixed group counted from its row_type
// column. The object array is then allocated for the objects alone.
func TestCountsRowTypesForCapacity(t *testing.T) {
	kind := func(row legacytest.Row, rowType string) legacytest.Row {
		row.RowType = rowType
		return row
	}
	rows := []legacytest.Row{
		obj("a", 0), obj("b", 1), obj("c", 2), // all OBJECT
		kind(obj("d/", 3), "COMMON_PREFIX"), kind(obj("e/", 4), "COMMON_PREFIX"), // none
		obj("f", 5), kind(obj("g", 6), "DELETE_MARKER"), obj("h", 7), // mixed
	}
	dir := filepath.Join(t.TempDir(), "capture")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := legacytest.Write(filepath.Join(dir, "c.parquet"), legacytest.Part{Rows: rows, Groups: []int{3, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	objects, others, err := countRows(context.Background(), []string{filepath.Join(dir, "c.parquet")})
	if err != nil || objects != 5 || others != 3 {
		t.Fatalf("counted %d objects, %d others: %v", objects, others, err)
	}
	stats, err := Run(context.Background(), Options{Capture: dir, Output: filepath.Join(t.TempDir(), "o")}, nil)
	if err != nil || stats.Objects != 5 || stats.Skipped != 3 {
		t.Fatalf("run: %+v %v", stats, err)
	}
}

// TestReplacedOutputDuringCaptureRead replaces the output, while the
// capture is read, with one whose owner holds only its lock: the run fails
// with journal.ErrOutputReplaced and the replacement keeps exactly its
// workspace and lock. Before, Prepare created data/ and the parts
// directory in it before the ownership check.
func TestReplacedOutputDuringCaptureRead(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out")
	replaceOutputWith(t, out, "captured", false)
	in := capture(t, []legacytest.Row{obj("a", 0)})
	if _, err := Run(context.Background(), Options{Capture: in, Output: out}, nil); !errors.Is(err, journal.ErrOutputReplaced) {
		t.Fatalf("run: %v", err)
	}
	got := tree(t, out)
	if strings.Count(got, "\n") != 3 || !strings.Contains(got, filepath.Join(dataset.Workspace, dataset.LockName)) {
		t.Fatalf("the replacement output changed:\n%s", got)
	}
}

// TestRefusesUndercountedRowGroup rewrites a capture of two OBJECT rows
// in one group so that the file and the group declare one row, and clears
// the group's row_type statistics: the counting pass finds two OBJECT
// values in a group of one row and refuses the part with
// ErrMalformedCapture. Before, the other-row count came out at -1 and the
// allocation from it panicked.
func TestRefusesUndercountedRowGroup(t *testing.T) {
	in := capture(t, []legacytest.Row{obj("a", 0), obj("b", 1)})
	part := filepath.Join(in, "part-a.parquet")
	if err := legacytest.RewriteFooter(part, func(m *format.FileMetaData) error {
		m.NumRows, m.RowGroups[0].NumRows = 1, 1
		for i := range m.RowGroups[0].Columns {
			if md := &m.RowGroups[0].Columns[i].MetaData; slices.Equal(md.PathInSchema, []string{"row_type"}) {
				md.Statistics = format.Statistics{}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Options{Capture: in, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrMalformedCapture) {
		t.Fatalf("run: %v", err)
	}
}

// TestRefusesRepeatedRowType refuses a capture whose row_type column is
// repeated: its values are not one per row.
func TestRefusesRepeatedRowType(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "capture")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	type repeated struct {
		Key     string   `parquet:"key"`
		RowType []string `parquet:"row_type"`
	}
	if err := parquet.WriteFile(filepath.Join(dir, "c.parquet"), []repeated{{Key: "a", RowType: []string{"OBJECT", "OBJECT"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), Options{Capture: dir, Output: filepath.Join(t.TempDir(), "o")}, nil); !errors.Is(err, ErrMalformedCapture) {
		t.Fatalf("run: %v", err)
	}
}
