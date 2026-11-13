// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// fakeS3 serves n keys in 17 folders under logs/, without the host's
// AWS credentials.
func fakeS3(t *testing.T, n int) string {
	t.Helper()
	s3test.NoCredentials(t)
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("logs/%02d/obj-%05d", i%17, i)
	}
	return s3test.Bucket(t, keys)
}

func TestS3Dataset(t *testing.T) {
	url := fakeS3(t, 3000)
	for _, tc := range []struct {
		root, target string
		seed         bool
	}{{"s3://bkt", "s3://bkt", false}, {"s3://bkt", "s3://bkt", true}, {"s3://bkt/logs/03/", "s3://bkt/logs/03/", false}} {
		out := filepath.Join(t.TempDir(), "ds")
		sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: tc.root, Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Seed: tc.seed}}}, Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 16 << 10}})
		if err != nil {
			t.Fatal(err)
		}
		if s3Stats(sum) == nil || sum.Output.RowsEmitted != s3Stats(sum).Counters.Value("objects") || sum.Output.RowsEmitted == 0 {
			t.Fatalf("%+v", sum)
		}
		var m dataset.Manifest
		readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
		var rows int64
		for _, p := range m.Parts {
			rows += p.Rows
		}
		scope := dataset.Scope{Kind: "whole"}
		if tc.root != "s3://bkt" {
			scope = dataset.Scope{Kind: "prefix", Prefix: "logs/03/"}
		}
		if m.Source != (dataset.Source{Kind: "s3", URI: tc.root, Container: "bkt"}) || m.Scope != scope || m.SchemaID != s3.Schema.ID ||
			!m.Coverage.Complete || m.Coverage.StopReason != "completed" || m.Coverage.Snapshot || rows != sum.Output.RowsEmitted ||
			m.Coverage.ListingStartedAt.IsZero() || m.Coverage.ListingFinishedAt.Before(m.Coverage.ListingStartedAt) {
			t.Fatalf("manifest %+v", m)
		}
		var run Report
		readJSON(t, filepath.Join(out, dataset.ReportName), &run)
		if run.ReportVersion != ReportVersion || run.Outcome.Status != "committed" || !run.Outcome.Complete || run.Invocation.Source != tc.target ||
			run.Output.RowsEmitted != sum.Output.RowsEmitted || run.Output.RowsCommitted != rows || run.Output.PartsWritten != len(m.Parts) ||
			run.Source == nil || run.Source.Name != "s3" || run.Timings.WallMs < run.Timings.ListingMs {
			t.Fatalf("run report %+v", run)
		}
	}
	for _, c := range s3.Schema.Columns {
		if slices.Contains([]string{"version_id", "is_latest", "is_delete_marker", "row_type"}, c.Name) {
			t.Fatalf("the current-object schema has %s", c.Name)
		}
	}
}

func readJSON(t testing.TB, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

// TestSummaryDistinguishesOffFromZero: a policy turned off is listed under
// "disabled" and its counters are absent; a policy that was on reports its
// counters even when they are zero.
func TestSummaryDistinguishesOffFromZero(t *testing.T) {
	url := fakeS3(t, 300)
	run := func(cfg Config) map[string]any {
		t.Helper()
		cfg.Source.URI, cfg.Source.Options.(*S3Config).Endpoint, cfg.Source.Options.(*S3Config).PathStyle, cfg.Source.Options.(*S3Config).Anonymous = "s3://bkt", url, true, true
		sum, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(sum)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	has := func(m map[string]any, path ...string) bool {
		var v any = m
		for _, p := range path {
			mm, ok := v.(map[string]any)
			if !ok {
				return false
			}
			if v, ok = mm[p]; !ok {
				return false
			}
		}
		return true
	}
	feat := func(m map[string]any, path ...string) map[string]any {
		var v any = m
		for _, p := range path {
			v = v.(map[string]any)[p]
		}
		return v.(map[string]any)
	}
	vals := []string{"source", "stats", "counters", "values"}
	on := run(Config{Source: SourceConfig{Options: &S3Config{Policy: keyrange.Policy{Seed: true}}}, Output: OutputConfig{Dir: filepath.Join(t.TempDir(), "ds"), NoJournal: datasettest.NoJournal()}, Limits: Limits{Workers: 4}})
	if !has(on, append(vals, "seed.cuts")...) || !has(on, "resources", "spill", "rows") || !has(on, "resources", "encoder_wait_ns") {
		t.Fatalf("enabled policies must report their counters: %v", on)
	}
	if f := feat(on, "source", "stats", "counters", "features"); f["seed"] != true || f["split"] != true || f["resume"] != false || f["max_keys"] != false {
		t.Fatalf("features = %v", f)
	}
	if f := feat(on, "resources", "features"); f["spill"] != true {
		t.Fatalf("pipeline features = %v", f)
	}
	cfg := Config{Source: SourceConfig{Options: &S3Config{}}, Limits: Limits{Workers: 4, Spill: SpillLimits{Off: true}}}
	m := run(cfg)
	if has(m, append(vals, "seed.cuts")...) || has(m, "resources", "spill") || has(m, "resources", "encoder_wait_ns") {
		t.Fatalf("disabled policies must not report counters: %v", m)
	}
	if f := feat(m, "source", "stats", "counters", "features"); f["seed"] != false || f["split"] != true {
		t.Fatalf("features = %v", f)
	}
	if f := feat(m, "resources", "features"); f["spill"] != false {
		t.Fatalf("pipeline features = %v", f)
	}
}

// stripTimes removes from a manifest read as JSON the times that differ
// between runs: published_at and the coverage's observation times, under
// extensions.swath.
func stripTimes(m map[string]any) {
	ext, _ := m["extensions"].(map[string]any)
	sw, _ := ext["swath"].(map[string]any)
	delete(sw, "published_at")
	if c, ok := sw["coverage"].(map[string]any); ok {
		delete(c, "listing_started_at")
		delete(c, "listing_finished_at")
	}
}
