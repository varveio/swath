// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestBothSchemesThroughTheCommand: one command line, with every
// provider's flags registered on one flag set, selects S3 or GCS by the
// source's scheme, and each provider gets its own defaults and the shared
// engine flags: by default 64 workers whatever the CPU count, no seed, splits on, the limiter on, no structure
// probes or floors; -seed reaches the selected provider (seed.prefixes
// counted), on swath list and swath diag list alike (Astra, kernel-seam
// follow-up, item 7).
func TestBothSchemesThroughTheCommand(t *testing.T) {
	keys := gcstest.Keys(3000)
	s3url := s3test.Bucket(t, keys)
	gcs := gcstest.Bucket(t, keys, 0)
	schemes := map[string][]string{
		"s3":  {"s3://bkt", "--endpoint", s3url, "--path-style", "--anonymous"},
		"gcs": {"gs://bkt", "--gcs-endpoint", gcs, "--gcs-anonymous"},
	}
	type report struct {
		Invocation struct{ Workers int }
		Outcome    struct{ Status string }
		Output     struct {
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
	defaults := map[string]bool{"floors": false, "limiter": true, "max_keys": false, "request_rate": false, "resume": false, "slow_start": false,
		"seed": false, "split": true, "structure": false}
	for name, src := range schemes {
		r := runJSON(false, src...)
		if r.Outcome.Status != "committed" || r.Output.RowsListed != int64(len(keys)) || r.Source.Name != name || !r.Source.Stats.Anonymous {
			t.Fatalf("%s: %+v", name, r)
		}
		if r.Invocation.Workers != 64 {
			t.Fatalf("%s: default workers %d, want 64", name, r.Invocation.Workers)
		}
		if f := r.Source.Stats.Counters.Features; !maps.Equal(f, defaults) {
			t.Fatalf("%s defaults: features %v, want %v", name, f, defaults)
		}
		seeded := func(r report) bool {
			_, ok := r.Source.Stats.Counters.Values["seed.prefixes"]
			return r.Source.Stats.Counters.Features["seed"] && ok
		}
		r = runJSON(false, append([]string{"--seed"}, src...)...)
		if !seeded(r) || r.Output.RowsListed != int64(len(keys)) {
			t.Fatalf("%s --seed: features %v, rows %d", name, r.Source.Stats.Counters.Features, r.Output.RowsListed)
		}
		r = runJSON(true, append([]string{"--seed"}, src...)...)
		if r.Invocation.Workers != 64 {
			t.Fatalf("%s diag: default workers %d, want 64", name, r.Invocation.Workers)
		}
		if !seeded(r) || r.Output.RowsListed != int64(len(keys)) {
			t.Fatalf("%s diag --seed: features %v, rows %d", name, r.Source.Stats.Counters.Features, r.Output.RowsListed)
		}
		t.Logf("%s: %s", name, fmt.Sprint(r.Source.Stats.Counters.Features))
	}
}
