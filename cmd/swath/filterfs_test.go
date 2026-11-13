// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/listing"
)

// TestExcludedUnstattableIsNotUnreadable: in a directory the walk can read
// but not stat in (mode 0400), an entry the filters leave out by its key
// alone is not unreadable: --exclude ignored lists the right empty selection
// and exits 0 (it exited 3), on the native and the portable engine, and the
// local counters say why (filtered_stat_errors). A directory whose row is
// excluded but whose descendants are not stays unreadable (its subtree is
// missing), unless they are pruned (sub/**); a size filter cannot decide
// alone, so the key rules still do.
func TestExcludedUnstattableIsNotUnreadable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root stats in a directory of mode 0400")
	}
	captureStderr(t)
	t.Cleanup(func() { logs.Store(nil) })
	root := t.TempDir()
	for _, p := range []string{"ignored", "sub/x"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, p)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, p), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(root, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })
	for _, engine := range []string{"", "portable"} {
		t.Setenv("SWATH_LOCALFS_ENGINE", engine)
		for _, c := range []struct {
			filters              []string
			unreadable, filtered int64
		}{
			{[]string{"--exclude", "ignored", "--exclude", "sub/", "--exclude", "sub/**"}, 0, 2},
			{[]string{"--exclude", "ignored", "--exclude", "sub/", "--exclude", "sub/**", "--min-size", "1"}, 0, 2},
			{[]string{"--exclude", "ignored", "--exclude", "sub/"}, 1, 1}, // sub's rows below it are selected, and missing
			{[]string{"--exclude", "ignored"}, 1, 1},
		} {
			report := filepath.Join(t.TempDir(), "report.json")
			var out bytes.Buffer
			err := run(context.Background(), append([]string{root, "--format=tsv", "--no-stats", "--report", report}, c.filters...), &out, false)
			var rep listing.Report
			readJSON(t, report, &rep)
			want := exitCompleted
			if c.unreadable > 0 {
				want = exitIncomplete
			}
			stats, _ := json.Marshal(rep.Source.Stats)
			if exitOf(err) != want || rep.Outcome.Unreadable != c.unreadable || strings.Count(out.String(), "\n") != 1 ||
				!strings.Contains(string(stats), fmt.Sprintf(`"filtered_stat_errors":%d`, c.filtered)) {
				t.Errorf("engine %q %v: exit %d (%v), unreadable %d, rows %q", engine, c.filters, exitOf(err), err, rep.Outcome.Unreadable, out.String())
			}
		}
	}
}

// TestFilterDropsDirectoryRows: a filter that drops every DIR row of a
// filesystem listing (--exclude-regex '/$') keeps the files below them, on the
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
			if err := run(context.Background(), []string{root, "-o", out, "--exclude-regex", "/$", "--report", report}, io.Discard, false); err != nil {
				t.Fatal(err)
			}
			sameRows(t, engine, ref, out)
			var r struct {
				Output struct {
					RowsEmitted  int64 `json:"rows_emitted"`
					RowsFiltered int64 `json:"rows_filtered"`
				}
			}
			readJSON(t, report, &r)
			if r.Output.RowsEmitted != int64(len(files)) || r.Output.RowsFiltered != dirs {
				t.Fatalf("listed %d, filtered %d; want %d files kept and %d directories dropped", r.Output.RowsEmitted, r.Output.RowsFiltered, len(files), dirs)
			}
			var m dataset.Manifest
			readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
			var rows int64
			for _, p := range m.Parts {
				rows += p.Rows
			}
			if rows != r.Output.RowsEmitted {
				t.Fatalf("manifest holds %d rows, the report %d", rows, r.Output.RowsEmitted)
			}
		})
	}
}
