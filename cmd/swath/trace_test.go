// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestTraceFlag: swath diag list --trace writes the run trace from
// run_start to run_end, which carries the run's status; the report's
// source counters account for its events; a source off the range engine
// and swath list refuse the flag, and a trace may not land on the output.
func TestTraceFlag(t *testing.T) {
	ctx := context.Background()
	url := s3test.Bucket(t, s3test.Keys(60, 3))
	dir := t.TempDir()
	trace, report, out := filepath.Join(dir, "run.trace.jsonl"), filepath.Join(dir, "r.json"), filepath.Join(dir, "ds")
	args := append([]string{"s3://bkt", "--endpoint-url", url, "--s3-path-style", "--anonymous", "--page-size", "10", "--workers", "4",
		"--report", report, "--trace", trace, "-o", out}, unjournaled...)
	if err := run(ctx, args, io.Discard, true); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(trace)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var evs []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e map[string]any
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %q: %v", sc.Text(), err)
		}
		evs = append(evs, e)
	}
	kinds := map[string]int{}
	for _, e := range evs {
		kinds[e["event"].(string)]++
	}
	if len(evs) < 3 || evs[0]["event"] != "run_start" || evs[len(evs)-1]["event"] != "run_end" || kinds["page"] == 0 || kinds["claimed"] == 0 {
		t.Fatalf("trace events %v", kinds)
	}
	end := evs[len(evs)-1]
	if end["status"] != listing.StatusCommitted || end["dropped"] != 0.0 || end["events"] != float64(len(evs)-1) {
		t.Fatalf("run_end %v", end)
	}
	var rep struct {
		Source struct {
			Stats struct {
				Counters struct {
					Features map[string]bool `json:"features"`
					Values   map[string]any  `json:"values"`
				} `json:"counters"`
			} `json:"stats"`
		} `json:"source"`
	}
	readJSON(t, report, &rep)
	c := rep.Source.Stats.Counters
	if !c.Features["trace"] || c.Values["trace.events"] != float64(len(evs)-1) || c.Values["trace.dropped"] != 0.0 {
		t.Fatalf("report trace counters: features %v, events %v dropped %v; %d events written", c.Features["trace"], c.Values["trace.events"], c.Values["trace.dropped"], len(evs)-1)
	}

	x := func() string { return filepath.Join(t.TempDir(), "x") }
	empty := t.TempDir()
	s3 := []string{"s3://bkt", "--endpoint-url", url, "--s3-path-style", "--anonymous"}
	for _, refused := range []struct {
		args []string
		diag bool
		want string
	}{
		{[]string{t.TempDir(), "--trace", filepath.Join(dir, "local.jsonl"), "-o", x()}, true, "--trace applies to"},
		{append(s3, "--trace", filepath.Join(dir, "list.jsonl"), "-o", x()), false, "swath diag"},
		{append(s3, "-o", out, "--overwrite", "--trace", filepath.Join(out, "manifest.json")), true, "--trace"},
		// Opened before the run, in an empty output directory the run would
		// then refuse as not empty: refused up front, as a profile is.
		{append(s3, "-o", empty, "--trace", filepath.Join(empty, "run.trace.jsonl")), true, "is in the output directory"},
	} {
		if err := run(ctx, refused.args, io.Discard, refused.diag); err == nil || !strings.Contains(err.Error(), refused.want) {
			t.Fatalf("%v: %v, want a refusal naming %q", refused.args, err, refused.want)
		}
	}
	if ents, _ := os.ReadDir(empty); len(ents) != 0 {
		t.Fatalf("the refused trace left %d entries in the output directory", len(ents))
	}
}
