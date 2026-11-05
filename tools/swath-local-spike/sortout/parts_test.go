// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortout

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
)

type partsRun struct {
	parts []pqout.PartInfo
	out   string
}

func runScheme(t *testing.T, rows []record.Row, mode string, encoders int, scheme string) partsRun {
	t.Helper()
	out := t.TempDir()
	if err := Prepare(out, t.TempDir(), false); err != nil {
		t.Fatal(err)
	}
	cfg := pqout.DefaultConfig()
	cfg.GroupBytes, cfg.Encoders = 64<<10, encoders
	opt := Options{Out: out, PartSize: 96 << 10, Writer: cfg, EncodeScheme: scheme, SortWorkers: 2}
	var parts []pqout.PartInfo
	var err error
	switch mode {
	case "memory", "merge":
		opt.SortBufferRows = 1_000_000
		if mode == "merge" {
			opt.SortBufferRows = 3_000
		}
		s, e := NewSorter(opt)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < len(rows); i += 997 {
			s.Emit(slices.Clone(rows[i:min(i+997, len(rows))]))
		}
		var st Stats
		parts, st, err = s.Finish()
		if mode == "merge" && st.Runs < 2 {
			t.Fatalf("merge mode needs spills, got %d", st.Runs)
		}
	case "stream":
		sorted := slices.Clone(rows)
		slices.SortFunc(sorted, func(a, b record.Row) int { return record.Compare(&a, &b) })
		s := NewStreamer(opt)
		for i := 0; i < len(sorted); i += 1500 {
			s.EmitSegments([][]record.Row{sorted[i:min(i+700, len(sorted), i+1500)], sorted[min(i+700, len(sorted)):min(i+1500, len(sorted))]})
		}
		parts, _, err = s.Finish()
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Publish(out, "/r", nil, parts, Stats{}); err != nil {
		t.Fatal(err)
	}
	return partsRun{parts: parts, out: out}
}

// The per-part scheme's output must not depend on the encoder count:
// identical boundaries and byte-identical files, in all three input paths.
func TestPartSchemeByteIdenticalAcrossEncoders(t *testing.T) {
	rows := randomRows(40_000, 11)
	for _, mode := range []string{"memory", "merge", "stream"} {
		base := runScheme(t, rows, mode, 1, SchemePart)
		if len(base.parts) < 4 {
			t.Fatalf("%s: want >= 4 parts, got %d", mode, len(base.parts))
		}
		for _, enc := range []int{2, 4} {
			got := runScheme(t, rows, mode, enc, SchemePart)
			if len(got.parts) != len(base.parts) {
				t.Fatalf("%s enc=%d: %d parts vs %d", mode, enc, len(got.parts), len(base.parts))
			}
			for i := range got.parts {
				a, b := got.parts[i], base.parts[i]
				if a.MD5 != b.MD5 || a.Rows != b.Rows || a.MinKey != b.MinKey || a.MaxKey != b.MaxKey {
					t.Fatalf("%s enc=%d part %d differs: %s/%d vs %s/%d", mode, enc, i, a.MD5, a.Rows, b.MD5, b.Rows)
				}
			}
		}
		// Footer stamps: dense 1-based file_index, file_final only on the last part.
		for i := range base.parts {
			f, err := os.Open(filepath.Join(base.out, "data", PartName(i)))
			if err != nil {
				t.Fatal(err)
			}
			fi, _ := f.Stat()
			pf, err := parquet.OpenFile(f, fi.Size())
			if err != nil {
				t.Fatal(err)
			}
			idx, _ := pf.Lookup(pqout.SortFileIndexKey)
			_, final := pf.Lookup(pqout.SortFileFinalKey)
			if idx != strconv.Itoa(i+1) {
				t.Errorf("%s: part %d file_index %q", mode, i, idx)
			}
			if final != (i == len(base.parts)-1) {
				t.Errorf("%s: part %d file_final=%v", mode, i, final)
			}
			f.Close()
		}
		// Same rows as the column scheme (boundaries differ by design).
		col := runScheme(t, rows, mode, 4, SchemeColumn)
		a, b := readDataset(t, base.out, base.parts), readDataset(t, col.out, col.parts)
		if len(a) != len(rows) || len(a) != len(b) {
			t.Fatalf("%s: rows %d / %d / %d", mode, len(a), len(b), len(rows))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("%s: row %d differs between schemes", mode, i)
			}
		}
	}
}

func TestCheckPartsGate(t *testing.T) {
	p := func(i int, lo, hi string, n int64) pqout.PartInfo {
		return pqout.PartInfo{Path: "/x/data/" + PartName(i) + ".tmp", MinKey: lo, MaxKey: hi, Rows: n}
	}
	if err := checkParts([]pqout.PartInfo{p(0, "a", "c", 2), p(1, "d", "f", 3)}, 5); err != nil {
		t.Fatal(err)
	}
	for name, parts := range map[string][]pqout.PartInfo{
		"overlap":  {p(0, "a", "d", 2), p(1, "d", "f", 3)},
		"gap":      {p(0, "a", "c", 2), p(2, "d", "f", 3)},
		"rowcount": {p(0, "a", "c", 2), p(1, "d", "f", 2)},
	} {
		if err := checkParts(parts, 5); err == nil {
			t.Errorf("%s: gate passed", name)
		}
	}
}
