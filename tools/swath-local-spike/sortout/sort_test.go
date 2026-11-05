// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortout

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// Keys mixing ASCII and multi-byte UTF-8 (lead bytes >= 0xC3); unsigned byte
// order puts every "é.."/"日.." key after "z..", which a signed-byte compare would not.
func randomRows(n int, seed int64) []record.Row {
	rng := rand.New(rand.NewSource(seed))
	alpha := []string{"a", "b", "z", "~", "/", ".", "-", "é", "ÿ", "日", "🙂", "A", "0"}
	seen := map[string]bool{}
	rows := make([]record.Row, 0, n)
	for len(rows) < n {
		k := ""
		for j := 0; j < 1+rng.Intn(12); j++ {
			k += alpha[rng.Intn(len(alpha))]
		}
		if seen[k] {
			continue
		}
		seen[k] = true
		r := record.Row{Key: k, Kind: record.KindFile, Size: int64(len(rows)), Mtime: 1_700_000_000_000_000, Present: record.HasSize | record.HasMtime}
		if len(rows)%7 == 0 {
			r.Kind, r.LinkTarget, r.Present = record.KindSymlink, "target-"+k, r.Present|record.HasLink
		}
		if len(rows)%11 == 0 {
			r.Present |= record.HasStat | record.HasBtime
			r.UID, r.GID, r.Mode, r.Nlink, r.Inode, r.Dev, r.Blocks = 1<<31+5, 7, 0o4755, 3, -1, 42, 4096
			r.Ctime, r.Atime, r.Btime = -5, 6, 7
		}
		rows = append(rows, r)
	}
	return rows
}

func TestSortOrderUnsigned(t *testing.T) {
	rows := randomRows(50_000, 1)
	for _, w := range []int{1, 3, 4, 7} {
		order := SortPtrs(pointers([][]record.Row{rows}), w)
		if len(order) != len(rows) {
			t.Fatal("length")
		}
		for i := 1; i < len(order); i++ {
			if order[i-1].Key >= order[i].Key {
				t.Fatalf("workers=%d: order broken at %d", w, i)
			}
		}
	}
	// Explicit unsigned check: 'z' (0x7a) < 'é' (0xc3 0xa9) < '日' (0xe6..).
	small := []record.Row{{Key: "日"}, {Key: "é"}, {Key: "z"}, {Key: "\u007f"}}
	o := SortPtrs(pointers([][]record.Row{small}), 1)
	var got []string
	for _, r := range o {
		got = append(got, r.Key)
	}
	if !slices.Equal(got, []string{"z", "\u007f", "é", "日"}) {
		t.Fatalf("unsigned order wrong: %q", got)
	}
}

func readDataset(t *testing.T, out string, parts []pqout.PartInfo) []record.Row {
	var all []record.Row
	for i := range parts {
		rd, err := pqout.Open(filepath.Join(out, "data", PartName(i)))
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]record.Row, 0, 500)
		for {
			buf, err = rd.Read(buf)
			all = append(all, buf...)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		rd.Close()
	}
	return all
}

func runSort(t *testing.T, rows []record.Row, bufRows int, partSize int64) (string, []pqout.PartInfo, Stats) {
	out := t.TempDir()
	if err := Prepare(out, t.TempDir(), true); err != nil {
		t.Fatal(err)
	}
	cfg := pqout.DefaultConfig()
	cfg.GroupBytes = 64 << 10
	s, err := NewSorter(Options{Out: out, SortBufferRows: bufRows, PartSize: partSize, Writer: cfg, SortWorkers: 3})
	if err != nil {
		t.Fatal(err)
	}
	// Emit in shuffled batches from several goroutines like walkers do.
	shuffled := slices.Clone(rows)
	rand.New(rand.NewSource(9)).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func(g int) {
			for i := g * 997; i < len(shuffled); i += 4 * 997 {
				s.Emit(slices.Clone(shuffled[i:min(i+997, len(shuffled))]))
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < 4; g++ {
		<-done
	}
	parts, st, err := s.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(out, "/root", []string{"test"}, parts, st); err != nil {
		t.Fatal(err)
	}
	return out, parts, st
}

func TestExternalSortMultiSpillMerge(t *testing.T) {
	rows := randomRows(60_000, 2)
	out, parts, st := runSort(t, rows, 7_000, 256<<10)
	if st.Runs < 5 || !st.Merged {
		t.Fatalf("expected several spill runs, got %+v", st)
	}
	if len(parts) < 2 {
		t.Fatalf("expected multiple parts, got %d", len(parts))
	}
	got := readDataset(t, out, parts)
	want := slices.Clone(rows)
	slices.SortFunc(want, func(a, b record.Row) int { return record.Compare(&a, &b) })
	if len(got) != len(want) {
		t.Fatalf("rows %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %+v want %+v", i, got[i], want[i])
		}
	}
	for i := 1; i < len(parts); i++ {
		if parts[i-1].MaxKey >= parts[i].MinKey {
			t.Fatalf("parts overlap at %d", i)
		}
	}
	if _, err := os.Stat(filepath.Join(out, "_staging")); !os.IsNotExist(err) {
		t.Fatal("_staging not removed")
	}
	for _, f := range []string{"manifest.json", "_SUCCESS", "symlink.txt", ".swath-state.json", SummaryName} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSingleRunNoMerge(t *testing.T) {
	rows := randomRows(5_000, 3)
	_, parts, st := runSort(t, rows, 1_000_000, 64<<20)
	if st.Merged || st.Runs != 0 || len(parts) != 1 || parts[0].Rows != 5_000 {
		t.Fatalf("unexpected %+v parts=%d", st, len(parts))
	}
}

// Round trip through parquet-go's own file API: order, count, schema names,
// physical/logical types, footer stamps.
func TestRoundTripSchemaAndStamps(t *testing.T) {
	rows := randomRows(20_000, 4)
	out, parts, _ := runSort(t, rows, 1_000_000, 128<<10)
	type col struct {
		name, phys string
		logical    string
		optional   bool
	}
	want := []col{
		{"key", "BYTE_ARRAY", "STRING", false}, {"size", "INT64", "", true},
		{"last_modified", "INT64", "TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", true}, {"kind", "BYTE_ARRAY", "STRING", false},
		{"mode", "INT32", "", true}, {"uid", "INT32", "", true}, {"gid", "INT32", "", true}, {"nlink", "INT32", "", true},
		{"inode", "INT64", "", true}, {"dev", "INT64", "", true}, {"blocks", "INT64", "", true},
		{"ctime", "INT64", "TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", true},
		{"atime", "INT64", "TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", true},
		{"birth_time", "INT64", "TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", true},
		{"link_target", "BYTE_ARRAY", "STRING", true},
	}
	var total int64
	prev := ""
	for i := range parts {
		f, err := os.Open(filepath.Join(out, "data", PartName(i)))
		if err != nil {
			t.Fatal(err)
		}
		fi, _ := f.Stat()
		pf, err := parquet.OpenFile(f, fi.Size())
		if err != nil {
			t.Fatal(err)
		}
		fields := pf.Schema().Fields()
		if len(fields) != len(want) {
			t.Fatalf("columns %d", len(fields))
		}
		elems := pf.Metadata().Schema[1:] // raw footer SchemaElements, not parquet-go's derived types
		for j, fld := range fields {
			lt := ""
			if elems[j].LogicalType.Value != nil {
				lt = elems[j].LogicalType.String()
			}
			if ct, ok := elems[j].ConvertedType.Get(); ok && lt == "" {
				lt = fmt.Sprint("converted:", ct)
			}
			typ, _ := elems[j].Type.Get()
			got := col{elems[j].Name, typ.String(), lt, fld.Optional()}
			if got != want[j] {
				t.Errorf("column %d: got %+v want %+v", j, got, want[j])
			}
		}
		meta := func(k string) string { v, _ := pf.Lookup(k); return v }
		if meta(pqout.SortFileIndexKey) != fmt.Sprint(i+1) {
			t.Errorf("file_index %q", meta(pqout.SortFileIndexKey))
		}
		_, final := pf.Lookup(pqout.SortFileFinalKey)
		if final != (i == len(parts)-1) {
			t.Errorf("file_final on part %d = %v", i, final)
		}
		if meta(pqout.SortOrderKey) != pqout.SortOrderValue || meta(pqout.SortModeKey) != "objects" || meta(pqout.SortFormatKey) != "1" {
			t.Error("sort stamps")
		}
		r := parquet.NewReader(pf)
		buf := make([]parquet.Row, 256)
		for {
			n, err := r.ReadRows(buf)
			for _, row := range buf[:n] {
				k := string(row[0].ByteArray())
				if total > 0 && k <= prev {
					t.Fatalf("not ascending: %q after %q", k, prev)
				}
				prev = k
				total++
			}
			if err != nil {
				break
			}
		}
		f.Close()
	}
	if total != int64(len(rows)) {
		t.Fatalf("total %d", total)
	}
}

// Review L2: a failed merge closes run readers and leaves no _staging/ or
// .tmp parts behind; nothing is published.
func TestFailedFinishCleansUp(t *testing.T) {
	out := t.TempDir()
	if err := Prepare(out, t.TempDir(), false); err != nil {
		t.Fatal(err)
	}
	cfg := pqout.DefaultConfig()
	cfg.GroupBytes = 32 << 10
	s, err := NewSorter(Options{Out: out, SortBufferRows: 500, PartSize: 16 << 10, Writer: cfg})
	if err != nil {
		t.Fatal(err)
	}
	rows := randomRows(3000, 7)
	rows = append(rows, rows[1500]) // duplicate key: the writer must reject it during the merge
	for i := 0; i < len(rows); i += 400 {
		s.Emit(slices.Clone(rows[i:min(i+400, len(rows))]))
	}
	if _, st, err := s.Finish(); err == nil {
		t.Fatal("expected duplicate-key failure")
	} else if st.Runs < 2 {
		t.Fatalf("test needs spill runs, got %d", st.Runs)
	}
	if _, err := os.Stat(filepath.Join(out, "_staging")); !os.IsNotExist(err) {
		t.Fatal("_staging left behind")
	}
	if tmps, _ := filepath.Glob(filepath.Join(out, "data", "*.tmp")); len(tmps) != 0 {
		t.Fatalf("tmp parts left: %v", tmps)
	}
	if _, err := os.Stat(filepath.Join(out, "_SUCCESS")); !os.IsNotExist(err) {
		t.Fatal("_SUCCESS must not exist")
	}
}
