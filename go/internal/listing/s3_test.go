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
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
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
			S3: S3Config{Endpoint: url, PathStyle: true, Seed: tc.seed}})
		if err != nil {
			t.Fatal(err)
		}
		if sum.S3 == nil || sum.Rows != sum.S3.Objects || sum.Rows == 0 {
			t.Fatalf("%+v", sum)
		}
		var m struct {
			SourceBucket string `json:"sourceBucket"`
			FileSchema   string `json:"fileSchema"`
			SchemaID     string `json:"schemaId"`
			TotalRows    int64  `json:"totalRows"`
		}
		readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
		if m.SourceBucket != "bkt" || m.FileSchema != schema.S3.Message() || m.TotalRows != sum.Rows {
			t.Fatalf("manifest %+v", m)
		}
		var run dataset.RunSummary
		readJSON(t, filepath.Join(out, dataset.SwathSummaryName), &run)
		if run.SchemaVersion != 2 || !run.Completed || run.Config.Target != tc.target || run.Config.Filters == nil {
			t.Fatalf("run summary %+v", run)
		}
	}
	if !strings.Contains(schema.S3.Message(), "required boolean is_delete_marker;") {
		t.Fatal(schema.S3.Message())
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
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
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
		sum, err := Run(context.Background(), Config{Root: "s3://bkt", Out: out, PartSize: 64 << 10, Workers: workers, S3: s3cfg})
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%+v splits=%d child_empty=%+v max_active=%d", s3cfg, sum.S3.Split.AcceptedBounded+sum.S3.Split.AcceptedFrontier, sum.S3.ChildEmpty, sum.S3.MaxActive)
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
			delete(m, "creationTimestamp")
			return m
		}
		if fmt.Sprint(strip(ref)) != fmt.Sprint(strip(got)) {
			t.Fatalf("%+v: manifests differ", tc)
		}
	}
}
