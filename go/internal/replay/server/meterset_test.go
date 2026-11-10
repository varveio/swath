// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store"
)

// meterMix is the request mix the meter set is pinned on: on S3 a full
// page, a one-key pivot, a "/" rollup, a delimiter the store declines, a
// URL-encoded page and an unknown bucket; on GCS and Azure a page and a
// "/" rollup each.
var meterMix = []struct{ target, header string }{
	{"/bucket?list-type=2&max-keys=1000", ""},
	{"/bucket?list-type=2&max-keys=1&start-after=logs/b", ""},
	{"/bucket?list-type=2&delimiter=/&prefix=logs/", ""},
	{"/bucket?list-type=2&delimiter=x", ""},
	{"/bucket?list-type=2&encoding-type=url&max-keys=5", ""},
	{"/nosuch?list-type=2", ""},
	{"/storage/v1/b/bucket/o?maxResults=10", ""},
	{"/storage/v1/b/bucket/o?delimiter=/&prefix=logs/", ""},
	{"/replay/bucket?restype=container&comp=list&maxresults=10", "2026-06-06"},
	{"/replay/bucket?restype=container&comp=list&delimiter=/&prefix=logs/", "2026-06-06"},
}

// sortedFixture writes keys (sorted here) as a stamped sorted fixture of
// parts of perPart rows in row groups of groupRows, and opens it.
func sortedFixture(t *testing.T, keys []string, perPart, groupRows, readers int) (*store.Sorted, store.Metadata) {
	t.Helper()
	keys = slices.Clone(keys)
	slices.Sort(keys)
	dir := t.TempDir()
	parts := (len(keys) + perPart - 1) / perPart
	for p := range parts {
		var rows []legacytest.Row
		for i := p * perPart; i < min(len(keys), (p+1)*perPart); i++ {
			rows = append(rows, legacytest.Object(keys[i], i))
		}
		part := legacytest.Part{Rows: rows, GroupRows: groupRows, Stamps: legacytest.Stamps(p+1, p == parts-1)}
		if err := legacytest.Write(filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", p+1)), part); err != nil {
			t.Fatal(err)
		}
	}
	st, meta, err := store.Open(context.Background(), dir, store.Options{Readers: readers})
	if err != nil {
		t.Fatal(err)
	}
	return st.(*store.Sorted), meta
}

// logKeys are n keys shaped like the synthetic bench fixture's.
func logKeys(n int) []string {
	var keys []string
	for i := range n {
		keys = append(keys, fmt.Sprintf("logs/%s/dt=2026-%02d-%02d/part-%05d.json.gz",
			[]string{"alpha", "beta", "gamma", "delta"}[i%4], 1+(i/400)%12, 1+(i/13)%28, i))
	}
	return keys
}

type scrape struct {
	Meters []map[string]any `json:"meters"`
}

func scrapeMetrics(t *testing.T, s *Server) (string, scrape) {
	t.Helper()
	r := do(t, "GET", fmt.Sprintf("http://127.0.0.1:%d/metrics", s.MetricsPort()))
	var p scrape
	if err := json.Unmarshal([]byte(r.body), &p); err != nil {
		t.Fatalf("%v: %s", err, r.body)
	}
	return r.body, p
}

// meterLines spells each meter as "name type {tags}", in payload order.
func meterLines(p scrape) []string {
	var lines []string
	for _, m := range p.Meters {
		tags, _ := json.Marshal(m["tags"]) // map keys marshal sorted
		lines = append(lines, fmt.Sprintf("%s %s %s", m["name"], m["type"], tags))
	}
	return lines
}

// TestMeterSetMatchesTheContract serves the meter mix over a sorted
// fixture on all three routes and compares the payload's meters, names,
// types and tags in payload order, with testdata/meters.txt: the Java
// server's scrape after the same mix over the synthetic-10k fixture,
// without the meters the Go server has no counterpart for (the sort
// progress counter, the JVM and host meters) and
// with the handle_cache meters that stand for the Java delimiter reader
// pool and the delimiter.index meters of the SeekParquet list index. docs/internals/replay-java.md (the meters side by side) lists
// every difference.
// The test then sums the http.requests count as the bench scripts do.
func TestMeterSetMatchesTheContract(t *testing.T) {
	st, meta := sortedFixture(t, logKeys(10_000), 6_000, 2_000, 2)
	cfg := config(st)
	cfg.Protocols, cfg.AzureAccount = []string{ProtocolS3, ProtocolGCS, ProtocolAzure}, DefaultAzureAccount
	cfg.IndexLoad, cfg.IndexEntries = 1, meta.RowGroups
	s := start(t, cfg)
	for _, rq := range meterMix {
		var r response
		if rq.header != "" {
			r = azureDo(t, s, "GET", rq.target, "x-ms-version", rq.header)
		} else {
			r = get(t, s, rq.target)
		}
		if want := 200; strings.HasPrefix(rq.target, "/nosuch") {
			if r.status != 404 {
				t.Fatalf("%s: %d", rq.target, r.status)
			}
		} else if r.status != want {
			t.Fatalf("%s: %d %s", rq.target, r.status, r.body)
		}
	}
	body, p := scrapeMetrics(t, s)
	want, err := os.ReadFile("testdata/meters.txt")
	if err != nil {
		t.Fatal(err)
	}
	var wantLines []string
	for _, l := range strings.Split(string(want), "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			wantLines = append(wantLines, l)
		}
	}
	got := meterLines(p)
	if !slices.Equal(got, wantLines) {
		t.Errorf("meters:\n%s\nwant (testdata/meters.txt):\n%s", strings.Join(got, "\n"), strings.Join(wantLines, "\n"))
	}

	// The bench scripts' extraction: the sum of count over the meters named
	// swath.replay.http.requests is the requests answered.
	var requests float64
	for _, m := range p.Meters {
		if m["name"] == "swath.replay.http.requests" {
			c, _ := m["count"].(float64)
			requests += c
		}
	}
	if requests != float64(len(meterMix)) {
		t.Errorf("http.requests sums to %v, want %d", requests, len(meterMix))
	}
	benchScriptCount(t, body, len(meterMix))

	// Every timer carries the six fields; the startup-only ones publish no
	// percentiles.
	for _, m := range p.Meters {
		if m["type"] != "timer" {
			continue
		}
		for _, f := range []string{"count", "sum_ms", "mean_ms", "max_ms", "p50_ms", "p99_ms"} {
			if _, ok := m[f]; !ok {
				t.Errorf("%s has no %s", m["name"], f)
			}
		}
		noPercentiles := m["name"] == metrics.IndexLoadLatency.Name || m["name"] == metrics.SortFixtureBuildLatency.Name ||
			m["name"] == metrics.DelimiterIndexBuildLatency.Name
		if (m["p50_ms"] == nil) != noPercentiles {
			t.Errorf("%s p50_ms %v", m["name"], m["p50_ms"])
		}
	}
}

// benchScriptCount runs the extraction bench/defaults_replay.sh and
// bench/policy_replay.sh use, the Python one-liner taken from the script
// itself, over a scrape; skipped without python3.
func benchScriptCount(t *testing.T, scrape string, want int) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Log("python3 not found: the scripts' own extraction is not run")
		return
	}
	for _, script := range []string{"defaults_replay.sh", "policy_replay.sh"} {
		src, err := os.ReadFile(filepath.Join("..", "..", "..", "bench", script))
		if err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile(`(?s)python3 -c '(.*?)'`).FindSubmatch(src)
		if m == nil {
			t.Fatalf("%s: no python3 -c extraction found", script)
		}
		cmd := exec.Command(python, "-I", "-c", string(m[1]))
		cmd.Stdin = strings.NewReader(scrape)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%s extraction: %v", script, err)
		}
		if got := string(bytes.TrimSpace(out)); got != fmt.Sprint(want) {
			t.Errorf("%s extraction counts %s requests, want %d", script, got, want)
		}
	}
}

// TestStoreMetersFollowTheReads checks the store meters against the reads
// a known mix makes over a three-part fixture: one query and one slot
// lease per store call, the rollup's work per rollup, every part opened
// once through the descriptor cache, and the slot peak within the bound.
func TestStoreMetersFollowTheReads(t *testing.T) {
	st, meta := sortedFixture(t, logKeys(3_000), 1_000, 250, 2)
	cfg := config(st)
	cfg.IndexLoad, cfg.IndexEntries = 1, meta.RowGroups
	s := start(t, cfg)
	for _, target := range []string{
		"/bucket?list-type=2&max-keys=1000", "/bucket?list-type=2&max-keys=1&start-after=logs/b",
		"/bucket?list-type=2&delimiter=/&prefix=logs/", "/bucket?list-type=2&delimiter=/",
	} {
		if r := get(t, s, target); r.status != 200 {
			t.Fatalf("%s: %d", target, r.status)
		}
	}
	queries := count(s, metrics.ParquetQueryLatency)
	if queries < 4 || count(s, metrics.PageReadLatency) != queries || count(s, metrics.ParquetQueryRows) != queries {
		t.Errorf("queries %d, slot leases %d, row samples %d", queries, count(s, metrics.PageReadLatency), count(s, metrics.ParquetQueryRows))
	}
	if count(s, metrics.DelimiterSkipScanDecodedKeyRows) != 2 || count(s, metrics.DelimiterSkipScanPageReseeks) != 2 ||
		count(s, metrics.DelimiterSkipScanRowGroupOpens)+count(s, metrics.DelimiterSkipScanWholeGroupShortcuts) < 2 {
		t.Errorf("skip-scan meters: %s", s.reg.AppendMeters(nil))
	}
	if count(s, metrics.HandleCacheOpens) != 3 || count(s, metrics.HandleCacheEvictions) != 0 ||
		count(s, metrics.IndexLoadLatency, "derived") != 1 || count(s, metrics.ParquetQueryErrors) != 0 {
		t.Errorf("cache and index meters: %s", s.reg.AppendMeters(nil))
	}
	out := string(s.reg.AppendMeters(nil))
	for _, want := range []string{
		`"swath.replay.handle_cache.limit","type":"gauge","tags":{},"value":` + fmt.Sprint(st.Files().Max()),
		`"swath.replay.index.entries","type":"distribution","tags":{},"count":1,"total":` + fmt.Sprint(meta.RowGroups),
		`"swath.replay.parquet.queries.in_flight","type":"gauge","tags":{},"value":0.000000`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %s in %s", want, out)
		}
	}
	if peak := st.Counters().PeakReaders.Load(); peak < 1 || peak > 2 {
		t.Errorf("reader peak %d with 2 readers", peak)
	}
}
