// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/schema"
)

func fakeS3(t *testing.T, n int) string {
	t.Helper()
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		k := fmt.Sprintf("logs/%02d/obj-%05d", i%17, i)
		if _, err := b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(gofakes3.New(b).Server())
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestS3Dataset(t *testing.T) {
	url := fakeS3(t, 3000)
	for _, tc := range []struct {
		root, target string
		seed         bool
	}{{"s3://bkt", "s3://bkt", false}, {"s3://bkt", "s3://bkt", true}, {"s3://bkt/logs/03/", "s3://bkt/logs/03/", false}} {
		out := filepath.Join(t.TempDir(), "ds")
		sum, err := Run(context.Background(), Config{Root: tc.root, Out: out, PartSize: 16 << 10,
			Source: &S3Config{Endpoint: url, PathStyle: true, Seed: tc.seed}})
		if err != nil {
			t.Fatal(err)
		}
		if s3Stats(sum) == nil || sum.Rows != s3Stats(sum).Objects || sum.Rows == 0 {
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
		if m.Source != (dataset.Source{Kind: "s3", URI: tc.root, Container: "bkt"}) || m.Scope != scope || m.SchemaID != schema.S3.ID ||
			!m.Coverage.Complete || m.Coverage.StopReason != "completed" || m.Coverage.Snapshot || rows != sum.Rows ||
			m.Coverage.ListingStartedAt.IsZero() || m.Coverage.ListingFinishedAt.Before(m.Coverage.ListingStartedAt) {
			t.Fatalf("manifest %+v", m)
		}
		var run RunSummary
		readJSON(t, filepath.Join(out, dataset.SwathSummaryName), &run)
		if run.SchemaVersion != 2 || !run.Completed || run.Config.Target != tc.target || run.Config.Filters == nil ||
			run.Objects != sum.Rows || run.Output.Files != sum.Parts || !run.Sort.Enabled || run.Cost.APICalls != s3Stats(sum).Requests ||
			run.Go == nil || !bytes.Contains(readFile(t, filepath.Join(out, dataset.SwathSummaryName)), []byte(`"s3":`)) {
			t.Fatalf("run summary %+v", run)
		}
		if _, err := os.Stat(filepath.Join(out, dataset.SummaryName)); !os.IsNotExist(err) {
			t.Fatalf("S3 dataset has %s: %v", dataset.SummaryName, err)
		}
	}
	for _, reserved := range []string{"version_id", "is_latest", "is_delete_marker", "row_type"} {
		if strings.Contains(schema.S3.Message(), " "+reserved+";") {
			t.Fatalf("the current-object schema has %s", reserved)
		}
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

// T5: the parallel provider's dataset is byte-identical to the sequential
// provider's (Parquet parts and manifest, apart from its creation time) on
// the same static bucket with request latency.
func TestS3ParallelEqualsSequential(t *testing.T) {
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	b := s3mem.New()
	if err := b.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for i := range 12000 {
		k := fmt.Sprintf("d%02d/sub%d/obj-%06d", i%7, i%3, i)
		if i%500 == 0 {
			k = fmt.Sprintf("flat-%05d", i)
		}
		if _, err := b.PutObject("bkt", k, nil, bytes.NewReader([]byte(k)), int64(len(k)), nil); err != nil {
			t.Fatal(err)
		}
	}
	h := gofakes3.New(b).Server()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	build := func(s3cfg S3Config, workers int) string {
		out := filepath.Join(t.TempDir(), "ds")
		s3cfg.Endpoint, s3cfg.PathStyle = srv.URL, true
		sum, err := Run(context.Background(), Config{Root: "s3://bkt", Out: out, PartSize: 64 << 10, Workers: workers, Source: &s3cfg})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v splits=%d child_empty=%+v max_active=%d", s3cfg, s3Stats(sum).Split.AcceptedBounded+s3Stats(sum).Split.AcceptedFrontier, s3Stats(sum).ChildEmpty, s3Stats(sum).MaxActive)
		return out
	}
	ref := build(S3Config{Sequential: true}, 1)
	for _, tc := range []S3Config{{}, {Seed: true}} {
		got := build(tc, 16)
		parts, _ := filepath.Glob(filepath.Join(ref, "data", "*.parquet"))
		if len(parts) < 2 {
			t.Fatalf("want several parts, got %d", len(parts))
		}
		for _, p := range parts {
			a, _ := os.ReadFile(p)
			b, err := os.ReadFile(filepath.Join(got, "data", filepath.Base(p)))
			if err != nil || !bytes.Equal(a, b) {
				t.Fatalf("%+v: %s differs (%v)", tc, filepath.Base(p), err)
			}
		}
		if more, _ := filepath.Glob(filepath.Join(got, "data", "*.parquet")); len(more) != len(parts) {
			t.Fatalf("%+v: %d parts, want %d", tc, len(more), len(parts))
		}
		strip := func(dir string) map[string]any {
			var m map[string]any
			readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
			delete(m, "published_at")
			delete(m["coverage"].(map[string]any), "listing_started_at")
			delete(m["coverage"].(map[string]any), "listing_finished_at")
			return m
		}
		if fmt.Sprint(strip(ref)) != fmt.Sprint(strip(got)) {
			t.Fatalf("%+v: manifests differ", tc)
		}
	}
}

// TestSummaryDistinguishesOffFromZero: a policy turned off is listed under
// "disabled" and its counters are absent; a policy that was on reports its
// counters even when they are zero.
func TestSummaryDistinguishesOffFromZero(t *testing.T) {
	url := fakeS3(t, 300)
	run := func(cfg Config) map[string]any {
		t.Helper()
		cfg.Root, cfg.Source.(*S3Config).Endpoint, cfg.Source.(*S3Config).PathStyle = "s3://bkt", url, true
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
	off := func(m map[string]any, path ...string) []string {
		var v any = m
		for _, p := range path {
			v = v.(map[string]any)[p]
		}
		var out []string
		for _, x := range v.([]any) {
			out = append(out, x.(string))
		}
		return out
	}
	on := run(Config{Out: filepath.Join(t.TempDir(), "ds"), Workers: 4, Source: &S3Config{Seed: true}})
	if !has(on, "s3", "probe", "requests") || !has(on, "s3", "seed", "cuts") || !has(on, "spill", "rows") || !has(on, "encoder_wait_ns") {
		t.Fatalf("enabled policies must report their counters: %v", on)
	}
	if got := off(on, "s3", "disabled"); strings.Join(got, ",") != "max_keys,resume" {
		t.Fatalf("disabled = %v", got)
	}
	if has(on, "disabled") {
		t.Fatalf("nothing in the run is off: %v", on["disabled"])
	}
	cfg := Config{Workers: 4, NoSpill: true, Source: &S3Config{NoProbe: true}}
	m := run(cfg)
	if has(m, "s3", "probe") || has(m, "s3", "seed") || has(m, "spill") || has(m, "encoder_wait_ns") {
		t.Fatalf("disabled policies must not report counters: %v", m)
	}
	if got := off(m, "s3", "disabled"); strings.Join(got, ",") != "max_keys,probe,resume,seed" {
		t.Fatalf("disabled = %v", got)
	}
	if got := off(m, "disabled"); strings.Join(got, ",") != "spill" {
		t.Fatalf("disabled = %v", got)
	}
}
