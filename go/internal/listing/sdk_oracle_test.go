// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/dataset/datasettest"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/s3/sdkoracle"
)

// oracleKeys mixes the shapes that stress encoding: spaces, '+', '%',
// XML metacharacters, non-ASCII, long keys, dense numeric runs.
func oracleKeys() []string {
	keys := []string{"a b", "a+b", "a%2Fb", "a%", "x&y<z>\"'", "é/ü", "日本/語", "\x7f", "~", "long/" + string(bytes.Repeat([]byte("k"), 900))}
	for i := range 6000 {
		keys = append(keys, fmt.Sprintf("d%02d/sub %d/obj+%06d", i%7, i%3, i))
		if i%400 == 0 {
			keys = append(keys, fmt.Sprintf("flat-%05d%%", i))
		}
	}
	return keys
}

// TestDatasetsMatchSDK is the step-1 gate: datasets listed through the
// custom client are byte-identical to datasets listed through the AWS SDK,
// sequential and parallel, on the same fake bucket.
func TestDatasetsMatchSDK(t *testing.T) {
	s3test.NoCredentials(t)
	b := s3test.Backend(t, oracleKeys(), s3mem.WithTimeSource(&step{t: time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)}))
	srvURL := s3test.Serve(t, s3test.Delayed(s3test.Handler(b), 2*time.Millisecond))
	build := func(s3cfg S3Config, workers int) string {
		out := filepath.Join(t.TempDir(), "ds")
		s3cfg.Endpoint, s3cfg.PathStyle, s3cfg.FetchOwner = srvURL, true, true
		if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", Options: &s3cfg}, Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 8 << 10, Workers: workers}}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	sdk := sdkoracle.New(srvURL, "us-east-1", true)
	ref := build(S3Config{Client: sdk, Policy: keyrange.Policy{Sequential: true}}, 1)
	for name, tc := range map[string]struct {
		cfg     S3Config
		workers int
	}{
		"client sequential":    {S3Config{Policy: keyrange.Policy{Sequential: true}}, 1},
		"client parallel 4":    {S3Config{}, 4},
		"client parallel seed": {S3Config{Policy: keyrange.Policy{Seed: true}}, 8},
		"sdk parallel 4":       {S3Config{Client: sdk}, 4},
	} {
		sameDataset(t, name, ref, build(tc.cfg, tc.workers))
	}
}

type step struct{ t time.Time }

func (c *step) Now() time.Time                  { c.t = c.t.Add(1001 * time.Millisecond); return c.t }
func (c *step) Since(t time.Time) time.Duration { return c.t.Sub(t) }

// sameDataset compares parts byte for byte and manifests apart from their
// creation time.
func sameDataset(t *testing.T, name, ref, got string) {
	t.Helper()
	parts, _ := filepath.Glob(filepath.Join(ref, "data", "*.parquet"))
	if len(parts) < 2 {
		t.Fatalf("want several parts, got %d", len(parts))
	}
	for _, p := range parts {
		a, _ := os.ReadFile(p)
		b, err := os.ReadFile(filepath.Join(got, "data", filepath.Base(p)))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("%s: %s differs (%v)", name, filepath.Base(p), err)
		}
	}
	if more, _ := filepath.Glob(filepath.Join(got, "data", "*.parquet")); len(more) != len(parts) {
		t.Fatalf("%s: %d parts, want %d", name, len(more), len(parts))
	}
	strip := func(dir string) string {
		var m map[string]any
		readJSON(t, filepath.Join(dir, dataset.ManifestName), &m)
		stripTimes(m)
		return fmt.Sprint(m)
	}
	if strip(ref) != strip(got) {
		t.Fatalf("%s: manifests differ", name)
	}
}
