// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
)

// TestCutReproducesClaimedFooter: a part written by another build, one
// without the schema's middle column and with its own created_by, is
// reproduced byte for byte by this build when the Cut carries that part's
// footer (its row count, columns and created_by), and the part after it
// takes this build's columns and created_by. A Cut without the columns or
// without created_by gives different bytes: each is needed. Columns this
// schema does not hold in its order leave the schema whole.
func TestCutReproducesClaimedFooter(t *testing.T) {
	saved := createdBuild
	t.Cleanup(func() { createdBuild = saved })
	rows := testRows(6000, 11)
	route := func(s *schema.Schema[tr], build string, cut *Cut) []dataset.Part {
		t.Helper()
		createdBuild = build
		out := t.TempDir()
		if err := os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755); err != nil {
			t.Fatal(err)
		}
		r, err := NewRouter(Options{Out: out, PartSize: 64 << 20, Encoders: 2, MaxRows: 4000, Cut: cut}, s)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Write(context.Background(), [][]row{rows}); err != nil {
			t.Fatal(err)
		}
		parts, err := r.Close()
		if err != nil {
			t.Fatal(err)
		}
		return parts
	}
	footer := func(p dataset.Part) seekparquet.Footer {
		t.Helper()
		f, err := os.Open(p.Path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		ft, err := seekparquet.InspectFooter(f, p.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return ft
	}
	older := testSchema
	older.Columns = slices.Delete(slices.Clone(testSchema.Columns), 1, 2)
	claimed := route(&older, "older", nil)[0]
	ft := footer(claimed)
	if ft.Rows != 4000 || len(ft.Columns) != len(testSchema.Columns) || ft.Columns[0] != "key" || ft.CreatedBy != "swath version "+createdVersion+"(build older)" {
		t.Fatalf("claimed footer %+v", ft)
	}
	parts := route(&testSchema, "newer", &Cut{Ordinal: 0, Rows: ft.Rows, Columns: ft.Columns, CreatedBy: ft.CreatedBy})
	if parts[0].SHA256 != claimed.SHA256 || parts[0].Bytes != claimed.Bytes {
		t.Fatalf("part 0 is %d bytes, sha256 %s; the claimed part %d bytes, sha256 %s", parts[0].Bytes, parts[0].SHA256, claimed.Bytes, claimed.SHA256)
	}
	if next := footer(parts[1]); len(next.Columns) != len(testSchema.Columns)+1 || next.CreatedBy != "swath version "+createdVersion+"(build newer)" {
		t.Fatalf("part 1 footer %+v", next)
	}
	reversed := slices.Clone(ft.Columns[1:])
	slices.Reverse(reversed)
	for _, c := range []struct {
		name string
		cut  Cut
	}{
		{"no columns", Cut{Rows: ft.Rows, CreatedBy: ft.CreatedBy}},
		{"no created_by", Cut{Rows: ft.Rows, Columns: ft.Columns}},
		{"columns out of order", Cut{Rows: ft.Rows, Columns: append([]string{"key"}, reversed...), CreatedBy: ft.CreatedBy}},
	} {
		if p := route(&testSchema, "newer", &c.cut)[0]; p.SHA256 == claimed.SHA256 {
			t.Fatalf("%s: the claimed part reproduced", c.name)
		}
	}
	whole := route(&testSchema, "newer", &Cut{Rows: ft.Rows, CreatedBy: ft.CreatedBy})[0]
	if f := footer(whole); len(f.Columns) != len(testSchema.Columns)+1 {
		t.Fatalf("a Cut without columns wrote %v", f.Columns)
	}
}
