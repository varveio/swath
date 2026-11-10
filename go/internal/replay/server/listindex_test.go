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

// convert writes keys as a capture and converts it with sort-fixture into
// a SeekParquet dataset, whose directory it returns.
func convert(t *testing.T, keys []string) string {
	t.Helper()
	dir := t.TempDir()
	capture := filepath.Join(dir, "capture")
	if err := os.MkdirAll(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := make([]legacytest.Row, len(keys))
	for i, key := range keys {
		rows[i] = legacytest.Object(key, i)
	}
	if err := legacytest.Write(filepath.Join(capture, "part-00000.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	converted := filepath.Join(dir, "converted")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: converted}, io.Discard); err != nil {
		t.Fatal(err)
	}
	return converted
}

// TestListIndexMetersEndToEnd serves a SeekParquet dataset, converted by
// sort-fixture, whose 40 directories each span several key pages, with the
// list index on and off: the S3 delimiter page is byte-identical, and with
// the index the delimiter.index meters publish the build and count the
// skips the rollup took through it; without it they read zero.
func TestListIndexMetersEndToEnd(t *testing.T) {
	var keys []string
	for d := range 40 {
		for i := range 3000 {
			keys = append(keys, fmt.Sprintf("logs/d%03d/obj%05d", d, i))
		}
	}
	converted := convert(t, keys)
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

// TestListIndexMetersOfFailedAndEmptyBuilds: a build that retains nothing,
// over a dataset with no directories, and a build that fails (a negative
// budget the module refuses) still publish their attempt: the build's
// latency, and for the empty build its pages, row groups and budget, while
// retained entries and bytes read zero. Rollups over either take no skips.
func TestListIndexMetersOfFailedAndEmptyBuilds(t *testing.T) {
	var flat []string
	for i := range 5000 {
		flat = append(flat, fmt.Sprintf("obj%05d", i))
	}
	for _, tc := range []struct {
		name  string
		keys  []string
		index store.ListIndex
		state store.ListIndexState
	}{
		{"empty", flat, store.ListIndex{}, store.ListIndexBuilt},
		{"failed", []string{"a/1", "a/2", "b/1"}, store.ListIndex{MaxBytes: -1}, store.ListIndexFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, meta, err := store.Open(context.Background(), convert(t, tc.keys), store.Options{ListIndex: tc.index})
			if err != nil {
				t.Fatal(err)
			}
			if meta.ListIndex.State != tc.state || meta.ListIndex.Stats.HasIndex {
				t.Fatalf("list index %+v", meta.ListIndex)
			}
			s := start(t, config(st))
			if r := get(t, s, "/bucket?list-type=2&delimiter=/"); r.status != 200 {
				t.Fatalf("%d %s", r.status, r.body)
			}
			_, p := scrapeMetrics(t, s)
			builds := meterValue(p, metrics.DelimiterIndexBuildLatency.Name, nil)
			entries := meterValue(p, metrics.DelimiterIndexEntries.Name, nil)
			bytes := meterValue(p, metrics.DelimiterIndexBytes.Name, nil)
			skips := meterValue(p, metrics.DelimiterIndexSkips.Name, nil)
			if builds != 1 || entries != 0 || bytes != 0 || skips != 0 {
				t.Errorf("builds %v, entries %v, bytes %v, skips %v", builds, entries, bytes, skips)
			}
			if tc.state == store.ListIndexBuilt {
				pages := meterValue(p, metrics.DelimiterIndexBuildPages.Name, nil)
				skipped := meterValue(p, metrics.DelimiterIndexBuildGroups.Name, map[string]string{"outcome": "skipped"})
				budget := meterValue(p, metrics.DelimiterIndexMaxBytes.Name, nil)
				if pages < 1 || skipped < 1 || budget != float64(store.DefaultListIndexMaxBytes) {
					t.Errorf("empty build: pages %v, groups skipped %v, budget %v", pages, skipped, budget)
				}
			}
		})
	}
}
