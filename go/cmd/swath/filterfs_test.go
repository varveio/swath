// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestFilterDropsDirectoryRows: a filter that drops every DIR row of a
// filesystem listing (--exclude '/$') keeps the files below them, on the
// native (Linux getdents) and the portable engine. The traversal reads each
// subdirectory before handing its row to the filter, which clears the rows
// it drops: the same rows as --no-dirs, and listed = kept + dropped.
func TestFilterDropsDirectoryRows(t *testing.T) {
	captureStderr(t)
	root := t.TempDir()
	files := []string{"a", "d/f", "d/e/g", "d/e/h/i", "z"}
	for _, f := range files {
		p := filepath.Join(root, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const dirs = 3 // d/, d/e/, d/e/h/
	for _, engine := range []string{"native", "portable"} {
		t.Run(engine, func(t *testing.T) {
			if engine == "portable" {
				t.Setenv("SWATH_LOCALFS_ENGINE", "portable")
			}
			ref := filepath.Join(t.TempDir(), "ref")
			if err := run(context.Background(), []string{root, "-o", ref, "--no-dirs"}, io.Discard, false); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(t.TempDir(), "out")
			report := filepath.Join(t.TempDir(), "r.json")
			if err := run(context.Background(), []string{root, "-o", out, "--exclude", "/$", "--report", report}, io.Discard, false); err != nil {
				t.Fatal(err)
			}
			sameRows(t, engine, ref, out)
			var r struct {
				Output struct {
					RowsListed   int64 `json:"rows_listed"`
					RowsFiltered int64 `json:"rows_filtered"`
				}
			}
			readJSON(t, report, &r)
			if r.Output.RowsListed != int64(len(files)) || r.Output.RowsFiltered != dirs {
				t.Fatalf("listed %d, filtered %d; want %d files kept and %d directories dropped", r.Output.RowsListed, r.Output.RowsFiltered, len(files), dirs)
			}
			var m dataset.Manifest
			readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
			var rows int64
			for _, p := range m.Parts {
				rows += p.Rows
			}
			if rows != r.Output.RowsListed {
				t.Fatalf("manifest holds %d rows, the report %d", rows, r.Output.RowsListed)
			}
		})
	}
}
