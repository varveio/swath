// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// TestBothSchemesThroughTheCommand: one command line, with every
// provider's flags registered on one flag set, selects S3 or GCS by the
// source's scheme, and each provider gets its own defaults and the shared
// engine flags: by default no seed, splits on, the limiter on, no structure
// probes, floors or probe; -seed reaches the selected provider, with the default
// descent (seed.levels.*), and diag's -seed-descent=false turns the descent
// off for either (Astra, kernel-seam follow-up, item 7).
func TestBothSchemesThroughTheCommand(t *testing.T) {
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	keys := gcstest.Keys(3000)
	for _, k := range keys {
		b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil)
	}
	s3srv := httptest.NewServer(gofakes3.New(b).Server())
	t.Cleanup(s3srv.Close)
	gcs := gcstest.Bucket(t, keys, 0)
	schemes := map[string][]string{
		"s3":  {"s3://bkt", "--endpoint", s3srv.URL, "--path-style", "--anonymous"},
		"gcs": {"gs://bkt", "--gcs-endpoint", gcs, "--gcs-anonymous"},
	}
	type report struct {
		Outcome struct{ Status string }
		Output  struct {
			RowsListed int64 `json:"rows_listed"`
		}
		Source struct {
			Name  string
			Stats struct {
				Anonymous bool
				Counters  struct {
					Features map[string]bool
					Values   map[string]any
				}
			}
		}
	}
	runJSON := func(diag bool, args ...string) report {
		t.Helper()
		var out bytes.Buffer
		args = append(args, "--report-format", "json", "-o", filepath.Join(t.TempDir(), "ds"))
		if err := run(context.Background(), args, &out, diag); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		var r report
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	defaults := map[string]bool{"floors": false, "limiter": true, "max_keys": false, "probe": false, "request_rate": false, "resume": false,
		"seed": false, "split": true, "structure": false}
	for name, src := range schemes {
		r := runJSON(false, src...)
		if r.Outcome.Status != "committed" || r.Output.RowsListed != int64(len(keys)) || r.Source.Name != name || !r.Source.Stats.Anonymous {
			t.Fatalf("%s: %+v", name, r)
		}
		if f := r.Source.Stats.Counters.Features; !maps.Equal(f, defaults) {
			t.Fatalf("%s defaults: features %v, want %v", name, f, defaults)
		}
		descended := func(r report) bool {
			for _, k := range []string{"seed.levels.narrow", "seed.levels.flat_wide", "seed.levels.fanout_tiled"} {
				if v, _ := r.Source.Stats.Counters.Values[k].(float64); v > 0 {
					return true
				}
			}
			return false
		}
		r = runJSON(false, append([]string{"--seed"}, src...)...)
		if !r.Source.Stats.Counters.Features["seed"] || !descended(r) {
			t.Fatalf("%s --seed: features %v, the descent ran %v", name, r.Source.Stats.Counters.Features, descended(r))
		}
		r = runJSON(true, append([]string{"--seed", "--seed-descent=false"}, src...)...)
		if !r.Source.Stats.Counters.Features["seed"] || descended(r) {
			t.Fatalf("%s --seed --seed-descent=false: features %v, the descent ran %v", name, r.Source.Stats.Counters.Features, descended(r))
		}
		t.Logf("%s: %s", name, fmt.Sprint(r.Source.Stats.Counters.Features))
	}
}
