// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
)

// meterValue is a meter's count, or a gauge's value, in a scrape; -1 when
// the scrape has no such meter.
func meterValue(p scrape, name string, tags map[string]string) float64 {
	for _, m := range p.Meters {
		if m["name"] != name {
			continue
		}
		got, _ := m["tags"].(map[string]any)
		match := len(got) == len(tags)
		for k, v := range tags {
			match = match && got[k] == v
		}
		if !match {
			continue
		}
		if v, ok := m["value"].(float64); ok {
			return v
		}
		v, _ := m["count"].(float64)
		return v
	}
	return -1
}

// TestListIndexMetersEndToEnd serves a SeekParquet dataset, converted by
// sort-fixture, whose 40 directories each span several key pages, with the
// list index on and off: the S3 delimiter page is byte-identical, and with
// the index the delimiter.index meters publish the build and count the
// skips the rollup took through it; without it they read zero.
func TestListIndexMetersEndToEnd(t *testing.T) {
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	if err := os.MkdirAll(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	var rows []legacytest.Row
	for d := range 40 {
		for i := range 3000 {
			rows = append(rows, legacytest.Object(fmt.Sprintf("logs/d%03d/obj%05d", d, i), len(rows)))
		}
	}
	if err := legacytest.Write(filepath.Join(capture, "part-00000.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	converted := filepath.Join(dir, "converted")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: converted}, io.Discard); err != nil {
		t.Fatal(err)
	}
	const target = "/bucket?list-type=2&delimiter=/&prefix=logs/"
	var bodies []string
	for _, off := range []bool{false, true} {
		st, _, err := store.Open(context.Background(), converted, store.Options{ListIndex: store.ListIndex{Off: off}})
		if err != nil {
			t.Fatal(err)
		}
		s := start(t, config(st))
		r := get(t, s, target)
		if r.status != 200 {
			t.Fatalf("off=%v: %d %s", off, r.status, r.body)
		}
		bodies = append(bodies, r.body)
		_, p := scrapeMetrics(t, s)
		skips := meterValue(p, metrics.DelimiterIndexSkips.Name, nil)
		indexed := meterValue(p, metrics.DelimiterIndexBuildGroups.Name, map[string]string{"outcome": "indexed"})
		entries := meterValue(p, metrics.DelimiterIndexEntries.Name, nil)
		budget := meterValue(p, metrics.DelimiterIndexMaxBytes.Name, nil)
		declines := meterValue(p, metrics.DelimiterIndexDeclines.Name, map[string]string{"reason": "missing_group"})
		if off {
			if skips != 0 || indexed != 0 || entries != 0 || budget != 0 {
				t.Errorf("index off: skips %v, groups indexed %v, entries %v, budget %v", skips, indexed, entries, budget)
			}
			continue
		}
		if skips < 39 || indexed < 1 || entries < 1 || budget != float64(store.DefaultListIndexMaxBytes) || declines != 0 {
			t.Errorf("index on: skips %v, groups indexed %v, entries %v, budget %v, missing_group declines %v",
				skips, indexed, entries, budget, declines)
		}
	}
	if bodies[0] != bodies[1] {
		t.Errorf("delimiter page differs with the index on and off:\n%s\n%s", bodies[0], bodies[1])
	}
}
