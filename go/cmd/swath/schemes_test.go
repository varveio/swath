// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// TestBothSchemesThroughTheCommand: one command line, with every
// provider's flags registered on one flag set, selects S3 or GCS by the
// source's scheme, and each provider gets its own defaults and the shared
// engine flags: by default 64 workers whatever the CPU count, the seed (seed.prefixes counted), splits on,
// the limiter on, no structure probes or floors; swath diag list -seed=false reaches the
// selected provider, and swath list refuses it (a diagnostic flag).
func TestBothSchemesThroughTheCommand(t *testing.T) {
	keys := gcstest.Keys(3000)
	s3url := s3test.Bucket(t, keys)
	gcs := gcstest.Bucket(t, keys, 0)
	schemes := map[string][]string{
		"s3":  {"s3://bkt", "--endpoint", s3url, "--path-style", "--anonymous"},
		"gcs": {"gs://bkt", "--endpoint", gcs, "--anonymous"},
	}
	type report struct {
		Invocation struct{ Workers int }
		Outcome    struct{ Status string }
		Output     struct {
			RowsEmitted int64 `json:"rows_emitted"`
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
		args = append(args, unjournaled...)
		if err := run(context.Background(), args, &out, diag || diagList); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		var r report
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	defaults := map[string]bool{"floors": false, "groups": false, "limiter": true, "max_keys": false, "request_rate": false, "resume": false, "slow_split": false, "slow_start": false,
		"seed": true, "seed_from": false, "split": true, "structure": true}
	for name, src := range schemes {
		r := runJSON(false, src...)
		if r.Outcome.Status != "committed" || r.Output.RowsEmitted != int64(len(keys)) || r.Source.Name != name || !r.Source.Stats.Anonymous {
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
		if !seeded(r) {
			t.Fatalf("%s: not seeded by default: %v", name, r.Source.Stats.Counters.Values)
		}
		// --seed is diagnostic: swath list refuses it, naming swath diag.
		if err := run(context.Background(), append([]string{"--seed=false"}, src...), io.Discard, false); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "swath diag list") {
			t.Fatalf("%s swath list --seed=false: %v", name, err)
		}
		r = runJSON(true, append([]string{"--seed=false"}, src...)...)
		if r.Invocation.Workers != 64 {
			t.Fatalf("%s diag: default workers %d, want 64", name, r.Invocation.Workers)
		}
		if seeded(r) || r.Output.RowsEmitted != int64(len(keys)) {
			t.Fatalf("%s diag --seed=false: features %v, rows %d", name, r.Source.Stats.Counters.Features, r.Output.RowsEmitted)
		}
		t.Logf("%s: %s", name, fmt.Sprint(r.Source.Stats.Counters.Features))
	}
}
